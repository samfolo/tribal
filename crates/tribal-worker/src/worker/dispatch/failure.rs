//! Failure handling: backoff computation, task failure persistence, and
//! lifecycle event emission.

use chrono::Utc;
use tribal_db::{
    JobRepository, JobStatusTransition, PgJobRepository, PgTaskRepository, TaskRepository,
};
use tribal_domain::{JobOutcome, JobStatus, Task, TaskErrorKind, TaskType};

use super::Worker;
use crate::{error::StageError, worker::backoff::backoff_duration};

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
    pub(crate) async fn handle_stage_failure(&self, task: &Task, error: &StageError) {
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

        if let Err(e) = self.persist_failure(task, &outcome, &error_message).await {
            tracing::error!(
                error = %e,
                task_id = %task.id(),
                task_type = %task.task_type(),
                job_id = %task.job_id(),
                "failed to persist failure",
            );
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
    ) -> Result<(), tribal_db::DbError> {
        let Some(claim_token) = task.claim_token() else {
            tracing::error!(task_id = %task.id(), "task has no claim token");
            return Ok(());
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
            return Ok(());
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
                .update_status(&mut txn, task.job_id(), &transition)
                .await?;
        }

        txn.commit()
            .await
            .map_err(|source| tribal_db::DbError::QueryFailed {
                context: "committing failure transaction".to_owned(),
                source,
            })?;
        self.log_failure_outcome(task, outcome);
        Ok(())
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
            self.notify_job_state(task.job_id());
            self.remove_job_state(task.job_id());
        }
    }
}
