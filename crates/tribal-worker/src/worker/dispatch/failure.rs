//! Failure handling: backoff computation, task failure persistence, and
//! lifecycle event emission.

use chrono::Utc;
use tribal_db::{
    AgentThreadRepository, JobRepository, JobStatusTransition, PgAgentThreadRepository,
    PgJobRepository, PgTaskRepository, TaskRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadStatus, AgentThreadTerminal, Disposition, DispositionCounters,
    ErrorOutcome, Job, JobOutcome, JobState, JobStatus, Task, TaskErrorKind, TaskType, TurnOutcome,
    decide_disposition,
};

use super::Worker;
use crate::{
    error::StageError,
    worker::{
        backoff::backoff_duration,
        coupling,
        heartbeat::{THREAD_RECOVERY_CAP, recovery_backoff_seconds},
    },
};

// ---------------------------------------------------------------------------
// FailureOutcome
// ---------------------------------------------------------------------------

/// Data needed to emit lifecycle events after a failure transaction
/// commits.  Bundled into a struct to avoid passing many parameters.
/// `is_dead_lettered` is the legacy (no-thread) computation; for
/// thread-driving tasks the persistence's disposition is authoritative
/// and is what the post-commit events read.
pub(super) struct FailureOutcome<'a> {
    pub error: &'a StageError,
    pub error_kind: TaskErrorKind,
    pub retry_count: u32,
    pub available_at: chrono::DateTime<Utc>,
    pub is_dead_lettered: bool,
}

// ---------------------------------------------------------------------------
// Worker impl
// ---------------------------------------------------------------------------

impl Worker {
    /// Handles a stage failure: computes backoff, fails the task via
    /// [`TaskRepository::fail`], and optionally transitions the parent
    /// job to `Failed` when the task is dead-lettered.
    ///
    /// All mutations (task fail + optional job transition) are composed
    /// in a single transaction so they commit or roll back atomically.
    pub(crate) async fn handle_stage_failure(
        &self,
        task: &Task,
        job: Option<&Job>,
        error: &StageError,
    ) {
        tracing::error!(
            task_id = %task.id(),
            task_type = %task.task_type(),
            job_id = %task.job_id(),
            error_kind = %error.to_error_kind(),
            error_message = %error,
            "stage execution failed",
        );

        let error_kind = error.to_error_kind();
        let error_message = error.to_string();
        let post_increment_retry = task.retry_count() + 1;
        #[allow(clippy::cast_possible_truncation)]
        let task_seed = task.id().inner().as_u128() as u64;
        let available_at = Utc::now() + backoff_duration(post_increment_retry, task_seed);
        let is_dead_lettered = post_increment_retry > self.config().task_max_retries;

        let outcome = FailureOutcome {
            error,
            error_kind,
            retry_count: post_increment_retry,
            available_at,
            is_dead_lettered,
        };

        match self.persist_failure(task, &outcome, &error_message).await {
            Ok(FailurePersistence::Committed {
                task_dead_lettered,
                job_failed,
            }) => {
                self.record_failure_metrics(task, job, task_dead_lettered, job_failed);
                self.log_failure_outcome(task, &outcome, task_dead_lettered, job_failed);
            }
            Ok(FailurePersistence::OwnershipLost) => {} // nothing committed — no metrics
            Err(e) => {
                tracing::error!(
                    error = %e,
                    task_id = %task.id(),
                    task_type = %task.task_type(),
                    job_id = %task.job_id(),
                    "failed to persist failure",
                );
            }
        }
    }

    /// Persists the failure state for a task within a single
    /// transaction: fails the task, optionally transitions the parent
    /// job to `Failed`, commits, and emits lifecycle events.
    async fn persist_failure(
        &self,
        task: &Task,
        outcome: &FailureOutcome<'_>,
        error_message: &str,
    ) -> Result<FailurePersistence, tribal_db::DbError> {
        let Some(claim_token) = task.claim_token() else {
            tracing::error!(task_id = %task.id(), "task has no claim token");
            return Ok(FailurePersistence::OwnershipLost);
        };

        let mut conn =
            self.pool()
                .acquire()
                .await
                .map_err(|source| tribal_db::DbError::QueryFailed {
                    context: "acquiring connection for failure persistence".to_owned(),
                    source,
                })?;
        let mut txn = sqlx::Connection::begin(&mut *conn)
            .await
            .map_err(|source| tribal_db::DbError::QueryFailed {
                context: "beginning failure transaction".to_owned(),
                source,
            })?;

        // For a thread-driving task the disposition decision is the one
        // authority — the same function the reclaim sweep consults — so
        // raising the recovery cap changes one constant, not two code
        // paths. A task with no thread predates the runtime and keeps the
        // legacy SQL CASE.
        let thread = PgAgentThreadRepository
            .find_by_stage_task_id(&mut txn, task.id())
            .await?;

        let task_dead_lettered = if let Some(thread) = &thread {
            let turn_outcome = match outcome.error_kind.outcome() {
                ErrorOutcome::Terminal => TurnOutcome::ThreadTerminal {
                    terminal: AgentThreadTerminal::Failed,
                },
                ErrorOutcome::Retryable => TurnOutcome::RetryableFailure,
            };
            let counters = DispositionCounters {
                retry_count: task.retry_count(),
                max_retries: self.config().task_max_retries,
                recovery_attempts: thread.recovery_attempts(),
                max_recovery_attempts: THREAD_RECOVERY_CAP,
            };
            let disposition = decide_disposition(turn_outcome, counters);
            let applied = self
                .apply_inline_disposition(
                    &mut txn,
                    task,
                    thread,
                    claim_token,
                    disposition,
                    outcome,
                    error_message,
                )
                .await?;
            let Some(task_dead_lettered) = applied else {
                tracing::warn!(task_id = %task.id(), "ownership lost during failure handling");
                return Ok(FailurePersistence::OwnershipLost);
            };
            task_dead_lettered
        } else {
            let rows_affected = PgTaskRepository
                .fail(
                    &mut txn,
                    task.id(),
                    claim_token,
                    self.config().task_max_retries,
                    outcome.available_at,
                    outcome.error_kind,
                    error_message,
                )
                .await?;
            if rows_affected == 0 {
                tracing::warn!(task_id = %task.id(), "ownership lost during failure handling");
                return Ok(FailurePersistence::OwnershipLost);
            }
            outcome.is_dead_lettered
        };

        // When a task dead-letters, that is terminal for Extraction and
        // Relation tasks — both imply the job cannot progress, so the
        // job transitions to Failed. Triage failures are non-fatal:
        // remaining triage tasks can still succeed, and the relation
        // stage runs on whatever triage results are available.
        let job_failed = task_dead_lettered
            && matches!(task.task_type(), TaskType::Extraction | TaskType::Relation);
        let mut job_failed_committed = false;
        if job_failed {
            let transition = JobStatusTransition::builder()
                .status(JobStatus::Failed)
                .outcome(Some(JobOutcome::Failure))
                .error_message(Some(error_message.to_owned()))
                .completed_at(Some(Utc::now()))
                .build();
            let moved = PgJobRepository
                .update_status_if_live(&mut txn, task.job_id(), &transition)
                .await?;
            // A terminal job no-ops silently: the metric and the watcher
            // notification follow the transition, never the intent.
            job_failed_committed = moved.is_some();
        }

        // When a triage task is dead-lettered, check whether it is the
        // last non-terminal sibling.  If so, create the relation task
        // and advance the job to Relating.
        let fan_in_fired = task_dead_lettered
            && task.task_type() == TaskType::Triage
            && coupling::triage_fan_in(&mut txn, task.job_id(), task.id()).await?;

        txn.commit()
            .await
            .map_err(|source| tribal_db::DbError::QueryFailed {
                context: "committing failure transaction".to_owned(),
                source,
            })?;

        if fan_in_fired {
            self.notify_job_state(task.job_id(), JobState::Relating);
        }

        Ok(FailurePersistence::Committed {
            task_dead_lettered,
            job_failed: job_failed_committed,
        })
    }

    /// Applies one inline disposition under the claim guard: the task
    /// write, and for cycle and terminal arms the thread-side write, all
    /// in the caller's transaction. Returns `Some(task_dead_lettered)`,
    /// or `None` when the claim guard missed.
    #[allow(clippy::too_many_arguments)] // the transaction's full guard context
    async fn apply_inline_disposition(
        &self,
        txn: &mut sqlx::PgConnection,
        task: &Task,
        thread: &AgentThread,
        claim_token: uuid::Uuid,
        disposition: Disposition,
        outcome: &FailureOutcome<'_>,
        error_message: &str,
    ) -> Result<Option<bool>, tribal_db::DbError> {
        match disposition {
            Disposition::Requeue { retry_count } => {
                let rows = PgTaskRepository
                    .requeue_claimed(
                        txn,
                        task.id(),
                        claim_token,
                        retry_count,
                        outcome.available_at,
                        outcome.error_kind,
                        error_message,
                    )
                    .await?;
                if rows == 0 {
                    return Ok(None);
                }
                Ok(Some(false))
            }
            Disposition::RecoveryCycle {
                retry_count,
                recovery_attempts,
            } => {
                PgAgentThreadRepository
                    .increment_recovery_attempts(txn, thread.id())
                    .await?;
                let available_at = Utc::now()
                    + chrono::Duration::seconds(i64::from(recovery_backoff_seconds(
                        recovery_attempts,
                    )));
                let rows = PgTaskRepository
                    .requeue_claimed(
                        txn,
                        task.id(),
                        claim_token,
                        retry_count,
                        available_at,
                        outcome.error_kind,
                        error_message,
                    )
                    .await?;
                if rows == 0 {
                    return Ok(None);
                }
                Ok(Some(false))
            }
            Disposition::DeadLetterTask | Disposition::ExhaustThread => {
                let terminal = match outcome.error_kind.outcome() {
                    ErrorOutcome::Terminal => AgentThreadTerminal::Failed,
                    ErrorOutcome::Retryable => AgentThreadTerminal::DeadLetter,
                };
                let rows = PgTaskRepository
                    .dead_letter_claimed(
                        txn,
                        task.id(),
                        claim_token,
                        outcome.error_kind,
                        error_message,
                    )
                    .await?;
                if rows == 0 {
                    return Ok(None);
                }
                let moved = PgAgentThreadRepository
                    .complete(txn, thread.id(), terminal, AgentThreadStatus::Running)
                    .await?;
                // A thread that never reached running (its mark-running
                // CAS failed on every attempt) would otherwise strand
                // queued forever with a dead-lettered task no sweep
                // targets. The held claim keeps both writes race-free.
                let moved = if moved == 0 {
                    PgAgentThreadRepository
                        .complete(txn, thread.id(), terminal, AgentThreadStatus::Queued)
                        .await?
                } else {
                    moved
                };
                if moved == 0 {
                    tracing::warn!(
                        task_id = %task.id(),
                        thread_id = %thread.id(),
                        "thread was neither running nor queued at task dead-letter; leaving its status",
                    );
                }
                Ok(Some(true))
            }
            Disposition::CompleteTask => {
                // A failure never maps onto the completed-thread arm.
                tracing::error!(
                    task_id = %task.id(),
                    "failure disposition was CompleteTask; leaving the row",
                );
                Ok(None)
            }
        }
    }

    /// Records metric counters and histograms for a task failure. Both
    /// flags are committed outcomes, not intents: a late failure against
    /// a terminal job records no job metric.
    fn record_failure_metrics(
        &self,
        task: &Task,
        job: Option<&Job>,
        task_dead_lettered: bool,
        job_failed: bool,
    ) {
        if task_dead_lettered {
            self.metrics()
                .record_task_dead_lettered(task.task_type().as_str());
        } else {
            self.metrics()
                .record_task_retried(task.task_type().as_str());
        }

        if job_failed {
            // chrono i64 milliseconds to f64 — precision loss negligible at this scale
            #[allow(clippy::cast_precision_loss)]
            let duration_ms = job.map(|j| (Utc::now() - j.created_at()).num_milliseconds() as f64);
            self.metrics()
                .record_job_completed(JobOutcome::Failure.as_str(), duration_ms);
        }
    }

    /// Emits lifecycle events after a failure transaction commits and
    /// notifies job-state subscribers when the job's failed transition
    /// actually moved the row.
    fn log_failure_outcome(
        &self,
        task: &Task,
        outcome: &FailureOutcome<'_>,
        task_dead_lettered: bool,
        job_failed: bool,
    ) {
        if task_dead_lettered {
            tracing::error!(
                task_id = %task.id(),
                task_type = %task.task_type(),
                job_id = %task.job_id(),
                error_kind = %outcome.error_kind,
                error_message = %outcome.error,
                retry_count = outcome.retry_count,
                "task.dead_lettered",
            );
        } else {
            tracing::warn!(
                task_id = %task.id(),
                task_type = %task.task_type(),
                job_id = %task.job_id(),
                error_kind = %outcome.error_kind,
                error_message = %outcome.error,
                retry_count = outcome.retry_count,
                available_at = %outcome.available_at,
                "task.failed",
            );
        }

        if job_failed {
            self.notify_job_state(task.job_id(), JobState::Failed);
        }
    }
}

/// What persisting a failure concluded.
enum FailurePersistence {
    /// The failure committed. `task_dead_lettered` is the disposition's
    /// verdict; `job_failed` is whether the job's failed transition
    /// actually moved the row (a terminal job no-ops).
    Committed {
        task_dead_lettered: bool,
        job_failed: bool,
    },
    /// The lease was lost; nothing committed.
    OwnershipLost,
}
