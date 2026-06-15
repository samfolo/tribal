//! Stage assembly for the thread runtime: bindings and thread setup.
//!
//! The worker derives each stage's agent definition from the boot-time
//! stage specs, the job's prompt versions, and the agentic
//! configuration — through the same derivation the ingest-time
//! fingerprint uses, so the recorded composite names exactly the binding
//! execution resolves. The default binding is one-shot with no tools and
//! no budget caps, reproducing launched behaviour exactly.

use tribal_agent_runtime::{
    AgentRuntimeError, StageThread, cancel_thread_in_txn, ensure_stage_thread, resolve_binding,
};
use tribal_config::ExecutorChoice;
use tribal_db::{
    PgPromptVersionRepository, PgTaskRepository, PromptVersionRepository, TaskRepository as _,
};
use tribal_domain::{
    AgentDefinition, AgentThread, AgentThreadStatus, Job, JobOutcome, JobState, PromptClass,
    PromptRole, PromptStage, PromptVersionId, Task, TaskErrorKind, TaskType,
};
use tribal_inference::{CompletionStageSpec, CompletionStageSpecs};

use crate::{
    definition::{StagePromptHashes, derive_stage_definition},
    error::StageError,
    stages::prompt_version_ids_for_task,
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

        guard_resume_route(stage, &definition, &stage_thread)?;
        Ok(stage_thread)
    }

    /// Applies the claim-time crash-window rules: a worker that claims a
    /// task whose thread is suspended, terminal, or carries a durable
    /// cancellation intent disposes of the task accordingly and never
    /// executes — a cancelled thread never starts a turn after the intent
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
                    // histogram here — the run that did the work recorded
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
                        // stands — loudly, since an operator must finish
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
        let spec = stage_spec(self.stage_specs(), task);
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
            loop_pair: self.active_loop_hashes(conn, task).await?,
        };
        derive_stage_definition(task.task_type(), spec, &prompts, self.agents()).map_err(|source| {
            StageError::BindingDerivation {
                stage: stage.into(),
                context: source.to_string(),
            }
        })
    }

    /// Resolves the active loop templates' content hashes for a stage
    /// whose configuration selects the loop executor — the binding-hash
    /// half of claim-time prompt resolution. `None` everywhere else, so
    /// the default path reads nothing.
    async fn active_loop_hashes(
        &self,
        conn: &mut sqlx::PgConnection,
        task: &Task,
    ) -> Result<Option<(String, String)>, StageError> {
        let (prompt_stage, executor) = match task.task_type() {
            TaskType::Triage => (PromptStage::Triage, self.agents().triage.executor),
            TaskType::Relation => (PromptStage::Relation, self.agents().relation.executor),
            // Extraction has no loop executor, so it carries no loop pair.
            TaskType::Extraction => return Ok(None),
        };
        if executor != ExecutorChoice::Loop {
            return Ok(None);
        }
        let stage = task.task_type().as_str();
        let mut hashes = Vec::with_capacity(2);
        for role in [PromptRole::System, PromptRole::User] {
            let id = self
                .active_prompts()
                .version_id(prompt_stage, PromptClass::Loop, role)
                .await
                .ok_or_else(|| StageError::BindingDerivation {
                    stage: stage.into(),
                    context: format!("no active {stage} loop {} prompt", role.as_str()),
                })?;
            let version = PgPromptVersionRepository
                .find_by_id(conn, id)
                .await
                .map_err(|source| StageError::Database {
                    stage: stage.into(),
                    context: "loading an active loop prompt".into(),
                    source,
                })?;
            hashes.push(version.content_hash().to_owned());
        }
        let user = hashes.pop().expect("two roles were pushed");
        let system = hashes.pop().expect("two roles were pushed");
        Ok(Some((system, user)))
    }
}

/// Fails a resumed thread whose recorded binding names a different route
/// than the configuration now resolves.
///
/// The gateway routes by the current stage, so a divergent resume would
/// run under an endpoint its recorded binding, eval row, and attribution
/// do not name; failing fast preserves the recorded-binding-names-what-ran
/// invariant rather than silently re-routing. A fresh thread pairs the
/// current binding, so its route matches by construction and the guard is
/// a no-op.
fn guard_resume_route(
    stage: &str,
    current: &AgentDefinition,
    stage_thread: &StageThread,
) -> Result<(), StageError> {
    let recorded = stage_thread.binding.definition();
    if recorded.provider == current.provider
        && recorded.model == current.model
        && recorded.base_url == current.base_url
    {
        return Ok(());
    }
    Err(StageError::ResumeRouteDivergence {
        stage: stage.to_owned(),
        recorded: format!(
            "{}/{}@{}",
            recorded.provider, recorded.model, recorded.base_url
        ),
        current: format!(
            "{}/{}@{}",
            current.provider, current.model, current.base_url
        ),
    })
}

/// Selects the boot-time endpoint spec for a task's stage.
fn stage_spec<'a>(specs: &'a CompletionStageSpecs, task: &Task) -> &'a CompletionStageSpec {
    match task.task_type() {
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
    use tribal_test_utils::{an_agent_binding, an_agent_definition, an_agent_thread};

    use super::*;

    fn a_stage_thread() -> StageThread {
        StageThread {
            thread: an_agent_thread().build(),
            input: None,
            binding: an_agent_binding().build(),
        }
    }

    #[test]
    fn test_guard_passes_a_matching_route() {
        // A fresh thread pairs the current binding, so the recorded and
        // current routes are identical and the guard is a no-op.
        let current = an_agent_definition().build();
        assert!(guard_resume_route("relation", &current, &a_stage_thread()).is_ok());
    }

    #[test]
    fn test_guard_fails_a_diverged_route() {
        // A configuration edit moved the model after admission: the resume
        // fails fast rather than running under the new endpoint while the
        // recorded binding still names the old one.
        let current = an_agent_definition()
            .model("a-different-model".to_owned())
            .build();
        assert!(matches!(
            guard_resume_route("relation", &current, &a_stage_thread()),
            Err(StageError::ResumeRouteDivergence { .. }),
        ));
    }
}
