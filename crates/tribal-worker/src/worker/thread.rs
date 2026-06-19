//! Stage assembly for the thread runtime: bindings and thread setup.
//!
//! The worker derives each stage's agent definition from the boot-time
//! stage specs, the job's prompt versions, and the agentic
//! configuration, through the same derivation the ingest-time
//! fingerprint uses, so the recorded composite names exactly the binding
//! execution resolves. The default binding is one-shot with no tools and
//! no budget caps, reproducing launched behaviour exactly.

use std::collections::HashMap;

use tribal_agent_runtime::{
    AgentRuntimeError, StageThread, cancel_thread_in_txn, ensure_stage_thread, resolve_binding,
};
use tribal_config::ExecutorChoice;
use tribal_db::{
    PgPromptVersionRepository, PgTaskRepository, PromptVersionRepository, TaskRepository as _,
};
use tribal_domain::{
    AgentDefinition, AgentThread, AgentThreadStatus, Job, JobOutcome, JobState, ProjectScope,
    PromptClass, PromptRole, PromptVersionId, StageExecutorKind, Task, TaskErrorKind, TaskType,
    ToolBinding, normalise_endpoint_url,
};
use tribal_inference::{CompletionStageSpec, CompletionStageSpecs};

use crate::{
    definition::{
        AgenticPromptHashes, StagePromptHashes, derive_stage_definition, prompt_stage_of,
        resolve_agentic_prompt_hashes,
    },
    error::StageError,
    stages::prompt_version_ids_for_task,
    tools::stage_tool_bindings,
    worker::{Worker, coupling},
};

impl Worker {
    /// Establishes the thread a claimed stage task drives: derives the
    /// stage's definition, resolves its content-addressed binding, and
    /// finds or creates the thread.
    ///
    /// Runs on a fresh connection before the stage executes, on the claim
    /// side of the no-transaction-across-inference rule.
    pub(crate) async fn establish_stage_thread(
        &self,
        job: &Job,
        task: &Task,
    ) -> Result<StageThread, StageError> {
        let stage = task.task_type().as_str();
        let Some(claim_token) = task.claim_token() else {
            return Err(StageError::OwnershipLost);
        };
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for thread setup".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;

        let definition = self.stage_definition(&mut conn, job, task).await?;
        let binding = resolve_binding(&mut conn, &definition)
            .await
            .map_err(|source| map_runtime_error(stage, "resolving the binding version", source))?;

        let stage_thread = ensure_stage_thread(&mut conn, job, task, claim_token, &binding)
            .await
            .map_err(|source| map_runtime_error(stage, "establishing the thread", source))?;

        guard_binding(
            stage,
            stage_thread.binding.definition(),
            stage_spec(self.stage_specs(), task.task_type()),
        )?;
        Ok(stage_thread)
    }

    /// Applies the claim-time crash-window rules: a worker that claims a
    /// task whose thread is suspended, terminal, or carries a durable
    /// cancellation intent disposes of the task accordingly and never
    /// executes. A cancelled thread never starts a turn after the intent
    /// is visible at a claim. Returns `true` when the task was disposed
    /// (the stage must not run).
    pub(crate) async fn dispose_for_thread_state(
        &self,
        task: &Task,
        thread: &AgentThread,
    ) -> Result<bool, StageError> {
        let stage = task.task_type().as_str();
        let claim_token = task.claim_token().ok_or(StageError::OwnershipLost)?;

        let intent_pending = thread.cancel_requested_at().is_some();
        let needs_disposal = intent_pending
            || thread.status().is_terminal()
            || thread.status() == AgentThreadStatus::Suspended;
        if !needs_disposal {
            return Ok(false);
        }

        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for claim-time disposal".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let mut txn =
            sqlx::Connection::begin(&mut *conn)
                .await
                .map_err(|e| StageError::Database {
                    stage: stage.into(),
                    context: "beginning the disposal transaction".into(),
                    source: tribal_db::DbError::QueryFailed {
                        context: "begin".into(),
                        source: e,
                    },
                })?;

        let owed: Option<coupling::OwedNotification>;
        let dead_lettered: bool;
        if intent_pending && !thread.status().is_terminal() {
            owed = cancel_at_claim(&mut txn, stage, task, thread, claim_token).await?;
            dead_lettered = true;
        } else {
            match thread.status() {
                AgentThreadStatus::Suspended => {
                    // Unreachable through any committed history (a suspend
                    // clears the claim in its own commit); kept as defence
                    // for unmodelled states: re-block and walk away.
                    let rows = PgTaskRepository
                        .block(&mut txn, task.id(), claim_token)
                        .await
                        .map_err(|e| stage_db(stage, "re-blocking the suspended task", e))?;
                    if rows == 0 {
                        return Err(StageError::OwnershipLost);
                    }
                    owed = None;
                    dead_lettered = false;
                }
                AgentThreadStatus::Completed => {
                    // The thread's terminal commit landed but the task half
                    // was re-queued (mid-upgrade or partial history): finish
                    // the task and fire the idempotent coupling. No task
                    // histogram here. The run that did the work recorded
                    // its own metrics.
                    let rows = PgTaskRepository
                        .complete(&mut txn, task.id(), claim_token)
                        .await
                        .map_err(|e| stage_db(stage, "completing the disposed task", e))?;
                    if rows == 0 {
                        return Err(StageError::OwnershipLost);
                    }
                    let fired = if task.task_type() == TaskType::Triage {
                        coupling::triage_fan_in(&mut txn, task.job_id(), task.id())
                            .await
                            .map_err(|e| stage_db(stage, "fan-in for the disposed task", e))?
                    } else {
                        // Extraction's fan-out and relation's batch seal
                        // cannot be re-derived here, so this reconciliation
                        // closes the task and leaves the job where it
                        // stands, loudly, since an operator must finish
                        // the job's convergence by hand.
                        tracing::error!(
                            task_id = %task.id(),
                            job_id = %task.job_id(),
                            task_type = %task.task_type(),
                            "claim-time disposal completed a non-triage task without its \
                             stage coupling; the job may need manual convergence",
                        );
                        false
                    };
                    owed = fired.then_some(coupling::OwedNotification {
                        job_id: task.job_id(),
                        state: JobState::Relating,
                    });
                    dead_lettered = false;
                }
                AgentThreadStatus::Queued
                | AgentThreadStatus::Running
                | AgentThreadStatus::Failed
                | AgentThreadStatus::Cancelled
                | AgentThreadStatus::DeadLetter => {
                    let rows = PgTaskRepository
                        .dead_letter_claimed(
                            &mut txn,
                            task.id(),
                            claim_token,
                            TaskErrorKind::InternalError,
                            "thread already terminal",
                        )
                        .await
                        .map_err(|e| stage_db(stage, "dead-lettering the disposed task", e))?;
                    if rows == 0 {
                        return Err(StageError::OwnershipLost);
                    }
                    owed = coupling::couple_dead_lettered_task(
                        &mut txn,
                        task,
                        "thread already terminal",
                    )
                    .await
                    .map_err(|e| stage_db(stage, "coupling the disposed job", e))?;
                    dead_lettered = true;
                }
            }
        }

        txn.commit().await.map_err(|e| StageError::Database {
            stage: stage.into(),
            context: "committing the disposal transaction".into(),
            source: tribal_db::DbError::QueryFailed {
                context: "commit".into(),
                source: e,
            },
        })?;

        // Metrics and notifications mirror the launched dead-letter path,
        // fired only after the commit.
        if dead_lettered {
            self.metrics()
                .record_task_dead_lettered(task.task_type().as_str());
        }
        if let Some(notice) = owed {
            if notice.state == JobState::Failed {
                self.metrics()
                    .record_job_completed(JobOutcome::Failure.as_str(), None);
            }
            self.notify_job_state(notice.job_id, notice.state);
        }

        tracing::warn!(
            task_id = %task.id(),
            thread_id = %thread.id(),
            thread_status = thread.status().as_str(),
            intent_pending,
            "claim-time disposal: the task never executed",
        );
        Ok(true)
    }

    /// Builds the stage's definition from its boot-time endpoint spec,
    /// the job's prompt versions' content hashes, and the agentic
    /// configuration, so a prompt, model, endpoint, sampling-parameter,
    /// executor, budget, or tool-surface edit is a new binding version.
    async fn stage_definition(
        &self,
        conn: &mut sqlx::PgConnection,
        job: &Job,
        task: &Task,
    ) -> Result<AgentDefinition, StageError> {
        let stage = task.task_type().as_str();
        let spec = stage_spec(self.stage_specs(), task.task_type());
        let (system_pv_id, user_pv_id) = prompt_version_ids_for_task(job, task);

        let system_hash = prompt_hash(conn, system_pv_id)
            .await
            .map_err(|source| map_runtime_error(stage, "deriving the stage binding", source))?;
        let user_hash = prompt_hash(conn, user_pv_id)
            .await
            .map_err(|source| map_runtime_error(stage, "deriving the stage binding", source))?;

        let prompts = StagePromptHashes {
            system: system_hash,
            user: user_hash,
            agentic: self.active_agentic_hashes(conn, task).await?,
        };
        derive_stage_definition(task.task_type(), spec, &prompts, self.agents()).map_err(|source| {
            StageError::BindingDerivation {
                stage: stage.into(),
                context: source.to_string(),
            }
        })
    }

    /// Resolves the active loop and verifier prompt-hash pairs for a stage
    /// whose configuration selects the loop executor: the claim-time half of
    /// the agentic-slot resolution the ingest fingerprint also runs through
    /// [`resolve_agentic_prompt_hashes`], so the two cannot disagree on which
    /// slots a stage's binding needs. Empty under the one-shot executor, so
    /// the default path reads nothing.
    async fn active_agentic_hashes(
        &self,
        conn: &mut sqlx::PgConnection,
        task: &Task,
    ) -> Result<AgenticPromptHashes, StageError> {
        let stage = task.task_type();
        let executor = match stage {
            TaskType::Extraction => self.agents().extraction.executor,
            TaskType::Triage => self.agents().triage.executor,
            TaskType::Relation => self.agents().relation.executor,
        };
        if executor != ExecutorChoice::Loop {
            return Ok(AgenticPromptHashes::default());
        }

        // Make available the active loop and verifier slots this stage might
        // need; the shared resolver selects from them by the same decision
        // the fingerprint uses. A slot with no active prompt is omitted, and
        // the resolver errors only if its decision actually requires it.
        let prompt_stage = prompt_stage_of(stage);
        let mut resolved: HashMap<(PromptClass, PromptRole), String> = HashMap::new();
        for (class, role) in [
            (PromptClass::Loop, PromptRole::System),
            (PromptClass::Loop, PromptRole::User),
            (PromptClass::Verifier, PromptRole::System),
            (PromptClass::Verifier, PromptRole::User),
        ] {
            if let Some(id) = self
                .active_prompts()
                .version_id(prompt_stage, class, role)
                .await
            {
                let version = PgPromptVersionRepository
                    .find_by_id(conn, id)
                    .await
                    .map_err(|source| StageError::Database {
                        stage: stage.as_str().into(),
                        context: "loading an active agentic prompt".into(),
                        source,
                    })?;
                resolved.insert((class, role), version.content_hash().to_owned());
            }
        }

        resolve_agentic_prompt_hashes(stage, self.agents(), |_stage, class, role| {
            resolved
                .get(&(class, role))
                .cloned()
                .ok_or_else(|| StageError::BindingDerivation {
                    stage: stage.as_str().into(),
                    context: format!(
                        "the {} binding needs a {class:?} {role:?} prompt, but none is active",
                        stage.as_str(),
                    ),
                })
        })
    }
}

/// Fails a resumed thread or driver child whose recorded binding diverges
/// from the current configuration in an aspect execution takes live: the
/// gateway's route, or the tool surface the turn loop projects from the live
/// registry. Either would run the thread under a contract its recorded
/// binding does not name, so failing fast preserves the
/// recorded-binding-names-what-ran invariant. Aspects the runtime reads back
/// from the recorded binding (its sampling parameters) need no guard; a fresh
/// thread pairs the current configuration, so it matches by construction.
pub(super) fn guard_binding(
    stage: &str,
    recorded: &AgentDefinition,
    spec: &CompletionStageSpec,
) -> Result<(), StageError> {
    if !(recorded.provider == spec.provider
        && recorded.model == spec.model
        && base_urls_match(&recorded.base_url, &spec.base_url))
    {
        return Err(StageError::ResumeBindingDivergence {
            stage: stage.to_owned(),
            aspect: "route",
            recorded: format!(
                "{}/{}@{}",
                recorded.provider, recorded.model, recorded.base_url
            ),
            current: format!("{}/{}@{}", spec.provider, spec.model, spec.base_url),
        });
    }

    // The turn loop projects the tool surface from the live registry, not the
    // recorded binding, so a binary that reshaped a tool descriptor or its
    // reach must fail rather than resume under a surface the recorded binding
    // does not name. Only the loop executor projects tools; a one-shot (a
    // verifier child included) projects none, so it cannot diverge here.
    if recorded.executor == StageExecutorKind::BuiltInLoop {
        let current_tools = stage_tool_bindings(recorded.pipeline_stage);
        if recorded.tools != current_tools {
            return Err(StageError::ResumeBindingDivergence {
                stage: stage.to_owned(),
                aspect: "tool surface",
                recorded: render_tool_surface(&recorded.tools),
                current: render_tool_surface(&current_tools),
            });
        }
    }

    Ok(())
}

/// Renders a tool surface as a stable, readable summary for a divergence
/// diagnostic: each tool's name and reach, in wire order. The binding hash
/// covers more than this (descriptions and schemas too), so two surfaces can
/// differ while this summary reads alike; the terminal failure stands either
/// way.
fn render_tool_surface(tools: &[ToolBinding]) -> String {
    tools
        .iter()
        .map(|tool| {
            let scope = match tool.project_scope {
                ProjectScope::Fenced => "fenced",
                ProjectScope::CrossProject => "cross",
            };
            format!("{}[{scope}]", tool.descriptor.name)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Whether two base URLs name the same endpoint. Canonicalises both (the
/// normalisation the gateway routes by) so a benign re-spelling (trailing
/// slash, case, default port) is not read as a route change; an unparseable
/// URL, which never booted a gateway, falls back to an exact compare rather
/// than collapsing two unparseable spellings together.
fn base_urls_match(recorded: &str, current: &str) -> bool {
    match (
        normalise_endpoint_url(recorded),
        normalise_endpoint_url(current),
    ) {
        (Ok(recorded), Ok(current)) => recorded == current,
        _ => recorded == current,
    }
}

/// Selects the boot-time endpoint spec for a stage.
pub(super) fn stage_spec(specs: &CompletionStageSpecs, stage: TaskType) -> &CompletionStageSpec {
    match stage {
        TaskType::Extraction => &specs.extraction,
        TaskType::Triage => &specs.triage,
        TaskType::Relation => &specs.relation,
    }
}

/// Reads one prompt version's content hash.
async fn prompt_hash(
    conn: &mut sqlx::PgConnection,
    id: PromptVersionId,
) -> Result<String, AgentRuntimeError> {
    let version = PgPromptVersionRepository
        .find_by_id(conn, id)
        .await
        .map_err(|source| AgentRuntimeError::Database {
            context: format!("loading prompt version {id} for the binding"),
            source,
        })?;
    Ok(version.content_hash().to_owned())
}

/// Maps a runtime error onto the stage error taxonomy: lost ownership
/// stays lost ownership, database faults stay database faults, and
/// consistency faults surface as internal errors.
pub(crate) fn map_runtime_error(
    stage: &str,
    context: &str,
    source: AgentRuntimeError,
) -> StageError {
    match source {
        AgentRuntimeError::StatusCasMissed { .. }
        | AgentRuntimeError::LeaseLost { .. }
        | AgentRuntimeError::DrivingTaskNotBlocked { .. } => StageError::OwnershipLost,
        AgentRuntimeError::Database {
            context: inner,
            source,
        } => StageError::Database {
            stage: stage.into(),
            context: inner,
            source,
        },
        AgentRuntimeError::Inference {
            context: inner,
            source,
        } => StageError::Provider {
            context: inner,
            source: *source,
        },
        source @ (AgentRuntimeError::ThreadMissing { .. }
        | AgentRuntimeError::ContentSerialisation { .. }
        | AgentRuntimeError::ToolExecution { .. }
        | AgentRuntimeError::LogProjection { .. }) => StageError::Runtime {
            context: context.to_owned(),
            source,
        },
    }
}

/// The worker-held cancel at claim time: claim-guarded task
/// dead-letter, the cancellation record and status, and the job
/// coupling, all in the caller's disposal transaction. Returns the owed
/// notification.
async fn cancel_at_claim(
    txn: &mut sqlx::PgConnection,
    stage: &str,
    task: &Task,
    thread: &AgentThread,
    claim_token: uuid::Uuid,
) -> Result<Option<coupling::OwedNotification>, StageError> {
    let rows = PgTaskRepository
        .dead_letter_claimed(
            txn,
            task.id(),
            claim_token,
            TaskErrorKind::InternalError,
            "thread cancelled",
        )
        .await
        .map_err(|e| stage_db(stage, "dead-lettering the cancelled task", e))?;
    if rows == 0 {
        return Err(StageError::OwnershipLost);
    }
    cancel_thread_in_txn(txn, thread.id())
        .await
        .map_err(|source| map_runtime_error(stage, "cancelling the thread", source))?;
    coupling::couple_dead_lettered_task(txn, task, "thread cancelled")
        .await
        .map_err(|e| stage_db(stage, "coupling the cancelled job", e))
}

/// Shorthand for the disposal transaction's database-error mapping.
fn stage_db(stage: &str, context: &str, source: tribal_db::DbError) -> StageError {
    StageError::Database {
        stage: stage.into(),
        context: context.to_owned(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use tribal_test_utils::{a_completion_stage_spec, an_agent_definition};

    use super::*;

    /// A spec on the default route's provider and endpoint, with `model`
    /// the divergence axis.
    fn a_stage_spec(model: &str) -> CompletionStageSpec {
        CompletionStageSpec {
            model: model.to_owned(),
            ..a_completion_stage_spec()
        }
    }

    #[test]
    fn test_guard_passes_a_matching_route() {
        // A fresh thread pairs the current spec, so the recorded and
        // current routes are identical and the guard is a no-op.
        let recorded = an_agent_definition().build();
        assert!(guard_binding("relation", &recorded, &a_stage_spec("llama3")).is_ok());
    }

    #[test]
    fn test_guard_fails_a_diverged_route() {
        // A configuration edit moved the model after admission: the resume
        // fails fast rather than running under the new endpoint while the
        // recorded binding still names the old one.
        let recorded = an_agent_definition().build();
        assert!(matches!(
            guard_binding("relation", &recorded, &a_stage_spec("a-different-model")),
            Err(StageError::ResumeBindingDivergence { .. }),
        ));
    }

    #[test]
    fn test_guard_treats_a_re_spelled_base_url_as_the_same_route() {
        // A trailing-slash re-spelling of the same endpoint is not a route
        // change: the guard canonicalises both sides, so a benign config
        // re-write does not fail an otherwise-resumable thread.
        let recorded = an_agent_definition()
            .base_url("http://localhost:11434".to_owned())
            .build();
        let respelled = CompletionStageSpec {
            base_url: "http://localhost:11434/".to_owned(),
            ..a_completion_stage_spec()
        };
        assert!(guard_binding("relation", &recorded, &respelled).is_ok());

        // A genuinely different host is still a divergence.
        let elsewhere = CompletionStageSpec {
            base_url: "http://elsewhere:11434".to_owned(),
            ..a_completion_stage_spec()
        };
        assert!(matches!(
            guard_binding("relation", &recorded, &elsewhere),
            Err(StageError::ResumeBindingDivergence { .. }),
        ));
    }

    #[test]
    fn test_guard_fails_a_diverged_tool_surface() {
        // A loop thread resumes after a binary dropped a relation tool: the
        // recorded surface no longer matches what the live registry projects,
        // so the resume fails rather than running under the changed contract.
        let mut diverged = stage_tool_bindings(TaskType::Relation);
        diverged.pop();
        let recorded = an_agent_definition()
            .pipeline_stage(TaskType::Relation)
            .executor(StageExecutorKind::BuiltInLoop)
            .tools(diverged)
            .build();
        assert!(matches!(
            guard_binding("relation", &recorded, &a_stage_spec("llama3")),
            Err(StageError::ResumeBindingDivergence { aspect, .. }) if aspect == "tool surface",
        ));
    }

    #[test]
    fn test_guard_passes_a_matching_loop_tool_surface() {
        // A loop thread whose recorded surface matches the live registry
        // resumes: the tool guard fires only on an actual change.
        let recorded = an_agent_definition()
            .pipeline_stage(TaskType::Relation)
            .executor(StageExecutorKind::BuiltInLoop)
            .tools(stage_tool_bindings(TaskType::Relation))
            .build();
        assert!(guard_binding("relation", &recorded, &a_stage_spec("llama3")).is_ok());
    }

    #[test]
    fn test_guard_skips_the_tool_surface_for_a_one_shot() {
        // A one-shot (a verifier child included) projects no tools, so an
        // empty recorded surface must not read as diverging from the stage's
        // loop surface: the tool guard is a loop-only concern.
        let recorded = an_agent_definition()
            .pipeline_stage(TaskType::Relation)
            .executor(StageExecutorKind::OneShot)
            .build();
        assert!(guard_binding("relation", &recorded, &a_stage_spec("llama3")).is_ok());
    }
}
