//! Failure handling: backoff computation, task failure persistence, and
//! lifecycle event emission.

use chrono::Utc;
use tribal_db::{
    AgentThreadRepository, JobRepository, JobStatusTransition, PgAgentThreadRepository,
    PgJobRepository, PgTaskRepository, TaskRepository,
};
use tribal_domain::{
    AgentThreadStatus, AgentThreadTerminal, ErrorOutcome, Job, JobOutcome, JobState, JobStatus,
    Task, TaskErrorKind, TaskType,
};

use super::Worker;
use crate::{
    error::StageError,
    worker::{backoff::backoff_duration, coupling},
};

// ---------------------------------------------------------------------------
// FailureOutcome
// ---------------------------------------------------------------------------

/// Data needed to emit lifecycle events after a failure transaction
/// commits.  Bundled into a struct to avoid passing many parameters.
pub(super) struct FailureOutcome<'a> {
    pub error: &'a StageError,
    pub error_kind: TaskErrorKind,
    pub retry_count: u32,
    pub available_at: chrono::DateTime<Utc>,
    pub is_dead_lettered: bool,
    pub job_failed: bool,
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

        // Determine upfront whether the job should fail — needed after
        // commit to decide whether to notify and clean up the watch map.
        let job_failed = is_dead_lettered
            && matches!(task.task_type(), TaskType::Extraction | TaskType::Relation);

        let outcome = FailureOutcome {
            error,
            error_kind,
            retry_count: post_increment_retry,
            available_at,
            is_dead_lettered,
            job_failed,
        };

        match self.persist_failure(task, &outcome, &error_message).await {
            Ok(true) => self.record_failure_metrics(task, job, &outcome),
            Ok(false) => {} // ownership lost — no metrics
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
    ///
    /// Returns `true` if the failure was committed, `false` if
    /// ownership was lost (another worker claimed the task).
    async fn persist_failure(
        &self,
        task: &Task,
        outcome: &FailureOutcome<'_>,
        error_message: &str,
    ) -> Result<bool, tribal_db::DbError> {
        let Some(claim_token) = task.claim_token() else {
            tracing::error!(task_id = %task.id(), "task has no claim token");
            return Ok(false);
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
            return Ok(false);
        }

        // The dead-lettered task's thread reaches its terminal in the same
        // transaction — the disposition mapping's exhaust leg: a terminal
        // error class fails the thread, retry exhaustion dead-letters it.
        // A re-queued task's thread stays running between attempts. (A
        // task with no thread predates the runtime; legacy semantics.)
        if outcome.is_dead_lettered
            && let Some(thread) = PgAgentThreadRepository
                .find_by_stage_task(&mut txn, task.id())
                .await?
        {
            let terminal = match outcome.error_kind.outcome() {
                ErrorOutcome::Terminal => AgentThreadTerminal::Failed,
                ErrorOutcome::Retryable => AgentThreadTerminal::DeadLetter,
            };
            let moved = PgAgentThreadRepository
                .complete(&mut txn, thread.id(), terminal, AgentThreadStatus::Running)
                .await?;
            // A thread that never reached running (its mark-running CAS
            // failed on every attempt) would otherwise strand queued
            // forever with a dead-lettered task no sweep targets.
            let moved = if moved == 0 {
                PgAgentThreadRepository
                    .complete(&mut txn, thread.id(), terminal, AgentThreadStatus::Queued)
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
        }

        // When a task exhausts its retry budget, dead-lettering is
        // terminal for Extraction and Relation tasks — both imply the
        // job cannot progress, so the job transitions to Failed.
        // Triage failures are non-fatal: remaining triage tasks can
        // still succeed, and the relation stage runs on whatever
        // triage results are available.
        if outcome.job_failed {
            let transition = JobStatusTransition::builder()
                .status(JobStatus::Failed)
                .outcome(Some(JobOutcome::Failure))
                .error_message(Some(error_message.to_owned()))
                .completed_at(Some(Utc::now()))
                .build();
            PgJobRepository
                .update_status_if_live(&mut txn, task.job_id(), &transition)
                .await?;
        }

        // When a triage task is dead-lettered, check whether it is the
        // last non-terminal sibling.  If so, create the relation task
        // and advance the job to Relating.
        let fan_in_fired = outcome.is_dead_lettered
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

        self.log_failure_outcome(task, outcome);
        Ok(true)
    }

    /// Records metric counters and histograms for a task failure.
    fn record_failure_metrics(&self, task: &Task, job: Option<&Job>, outcome: &FailureOutcome<'_>) {
        if outcome.is_dead_lettered {
            self.metrics()
                .record_task_dead_lettered(task.task_type().as_str());
        } else {
            self.metrics()
                .record_task_retried(task.task_type().as_str());
        }

        if outcome.job_failed {
            // chrono i64 milliseconds to f64 — precision loss negligible at this scale
            #[allow(clippy::cast_precision_loss)]
            let duration_ms = job.map(|j| (Utc::now() - j.created_at()).num_milliseconds() as f64);
            self.metrics()
                .record_job_completed(JobOutcome::Failure.as_str(), duration_ms);
        }
    }

    /// Emits lifecycle events after a failure transaction commits and
    /// notifies job-state subscribers when the job is dead-lettered.
    fn log_failure_outcome(&self, task: &Task, outcome: &FailureOutcome<'_>) {
        if outcome.is_dead_lettered {
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

        if outcome.job_failed {
            self.notify_job_state(task.job_id(), JobState::Failed);
        }
    }
}
