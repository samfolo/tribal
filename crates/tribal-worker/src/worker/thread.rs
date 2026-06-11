//! Stage assembly for the thread runtime: bindings and thread setup.
//!
//! The worker derives each stage's agent definition from the boot-time
//! stage specs and the job's prompt versions — bindings are populated
//! from the existing configuration, never replacing it — then hands
//! execution to the runtime. The default binding is one-shot with no
//! tools and no budget caps, reproducing launched behaviour exactly.

use tribal_agent_runtime::{
    AgentRuntimeError, StageThread, cancel_thread_in_txn, ensure_stage_thread, resolve_binding,
};
use tribal_db::{
    PgPromptVersionRepository, PgTaskRepository, PromptVersionRepository, TaskRepository as _,
};
use tribal_domain::{
    AgentDefinition, AgentThread, AgentThreadStatus, Job, JobOutcome, JobState, PromptVersionId,
    Task, TaskErrorKind, TaskType,
};
use tribal_inference::{CompletionStageSpec, CompletionStageSpecs};

use crate::{
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

        let definition = self
            .stage_definition(&mut conn, job, task)
            .await
            .map_err(|source| map_runtime_error(stage, "deriving the stage binding", source))?;
        let binding = resolve_binding(&mut conn, &definition)
            .await
            .map_err(|source| map_runtime_error(stage, "resolving the binding version", source))?;

        ensure_stage_thread(&mut conn, job, task, &binding)
            .await
            .map_err(|source| map_runtime_error(stage, "establishing the thread", source))
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
            // The worker-held cancel: claim-guarded task dead-letter, the
            // cancellation record and status, and the job coupling.
            let rows = PgTaskRepository
                .dead_letter_claimed(
                    &mut txn,
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
            cancel_thread_in_txn(&mut txn, thread.id())
                .await
                .map_err(|source| map_runtime_error(stage, "cancelling the thread", source))?;
            owed = coupling::couple_dead_lettered_task(&mut txn, task, "thread cancelled")
                .await
                .map_err(|e| stage_db(stage, "coupling the cancelled job", e))?;
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
                        false
                    };
                    owed = fired.then_some(coupling::OwedNotification {
                        job_id: task.job_id(),
                        state: JobState::Relating,
                    });
                    dead_lettered = false;
                }
                _ => {
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

    /// Builds the stage's definition from its boot-time endpoint spec and
    /// the job's prompt versions' content hashes, so a prompt, model,
    /// endpoint, or sampling-parameter edit is a new binding version.
    async fn stage_definition(
        &self,
        conn: &mut sqlx::PgConnection,
        job: &Job,
        task: &Task,
    ) -> Result<AgentDefinition, AgentRuntimeError> {
        let spec = stage_spec(self.stage_specs(), task);
        let (system_pv_id, user_pv_id) = prompt_version_ids_for_task(job, task);

        let system_hash = prompt_hash(conn, system_pv_id).await?;
        let user_hash = prompt_hash(conn, user_pv_id).await?;

        Ok(AgentDefinition::one_shot(
            task.task_type(),
            spec.provider,
            spec.model.clone(),
            spec.base_url.clone(),
            spec.parameters.clone(),
            system_hash,
            user_hash,
        ))
    }
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
        AgentRuntimeError::StatusCasMissed { .. } | AgentRuntimeError::LeaseLost { .. } => {
            StageError::OwnershipLost
        }
        AgentRuntimeError::Database {
            context: inner,
            source,
        } => StageError::Database {
            stage: stage.into(),
            context: inner,
            source,
        },
        source @ (AgentRuntimeError::ThreadMissing { .. }
        | AgentRuntimeError::ContentSerialisation { .. }) => StageError::Runtime {
            context: context.to_owned(),
            source,
        },
    }
}

/// Shorthand for the disposal transaction's database-error mapping.
fn stage_db(stage: &str, context: &str, source: tribal_db::DbError) -> StageError {
    StageError::Database {
        stage: stage.into(),
        context: context.to_owned(),
        source,
    }
}
