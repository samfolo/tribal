//! The job-coupling seam: how task-level outcomes touch job state.
//!
//! Transaction-composable by construction — every function takes the
//! caller's connection and commits nothing itself — so the worker's
//! commit and failure paths, the healing sweeps, and the runtime's
//! thread-terminal transactions all couple through the same code. This is
//! the only path by which any actor outside dispatch touches job state.

use chrono::Utc;
use tribal_agent_runtime::{AgentRuntimeError, CancelOutcome, cancel_thread_in_txn};
use tribal_db::{
    DbError, JobRepository, JobStatusTransition, NewTask, PgJobRepository, PgTaskRepository,
    TaskRepository,
};
use tribal_domain::{
    AgentThread, JobId, JobOutcome, JobStatus, Task, TaskErrorKind, TaskId, TaskType,
};

/// Fires the triage fan-in when the current task is the last live triage
/// sibling: upserts the relation task and advances the job to `Relating`.
/// Returns whether it fired.
///
/// A `blocked` sibling counts as live — the count is NOT-in-terminal by
/// construction — so relation never fires while a triage thread is
/// suspended. This in-commit count is a latency optimisation over the
/// authoritative convergence mechanism, the stuck-triaging healing sweep,
/// which must converge the job on its own.
///
/// # Errors
///
/// Returns [`DbError`] on database errors; the caller's transaction
/// decides what commits.
pub async fn triage_fan_in(
    conn: &mut sqlx::PgConnection,
    job_id: JobId,
    current_task_id: TaskId,
) -> Result<bool, DbError> {
    let remaining = PgTaskRepository
        .count_live_siblings(conn, job_id, TaskType::Triage, current_task_id)
        .await?;

    if remaining > 0 {
        return Ok(false);
    }

    let new_task = NewTask::builder()
        .job_id(job_id)
        .task_type(TaskType::Relation)
        .build();

    let rows_affected = PgTaskRepository.upsert(conn, &new_task).await?;

    if rows_affected > 0 {
        tracing::info!(job_id = %job_id, "relation task created (triage fan-in)");
    } else {
        tracing::debug!(job_id = %job_id, "relation task already exists for job");
    }

    let transition = JobStatusTransition::builder()
        .status(JobStatus::Relating)
        .build();

    PgJobRepository
        .update_status_if_live(conn, job_id, &transition)
        .await?;

    Ok(true)
}

/// Couples a task's terminal disposition to its job, exactly as a worker
/// dead-letter does: the triage fan-in for a triage task, the job-failed
/// transition for extraction and relation (whose dead-letter means the
/// job cannot progress).
///
/// # Errors
///
/// Returns [`DbError`] on database errors; the caller's transaction
/// decides what commits.
pub async fn couple_dead_lettered_task(
    conn: &mut sqlx::PgConnection,
    task: &Task,
    error_message: &str,
) -> Result<(), DbError> {
    match task.task_type() {
        TaskType::Triage => {
            triage_fan_in(conn, task.job_id(), task.id()).await?;
        }
        TaskType::Extraction | TaskType::Relation => {
            let transition = JobStatusTransition::builder()
                .status(JobStatus::Failed)
                .outcome(Some(JobOutcome::Failure))
                .error_message(Some(error_message.to_owned()))
                .completed_at(Some(Utc::now()))
                .build();
            PgJobRepository
                .update_status_if_live(conn, task.job_id(), &transition)
                .await?;
        }
    }
    Ok(())
}

/// Cancels an unclaimed thread and couples its job, in one transaction:
/// the locked-unclaimed task disposal, the cancellation record and
/// status, and the launched job coupling. The sweep's cancel fallback and
/// the control plane share this seam. Returns whether the cancellation
/// committed.
///
/// A claimed driving task means a live worker observes the intent at its
/// own boundary, so this rolls back untouched; a task already terminal
/// (a stranded pairing) still cancels the thread and couples the job.
///
/// # Errors
///
/// Returns [`AgentRuntimeError`] on database or transition errors;
/// nothing commits on error.
pub async fn cancel_thread(
    conn: &mut sqlx::PgConnection,
    thread: &AgentThread,
) -> Result<bool, AgentRuntimeError> {
    let mut txn = sqlx::Connection::begin(&mut *conn)
        .await
        .map_err(|source| {
            AgentRuntimeError::database(
                "beginning the cancel transaction",
                DbError::QueryFailed {
                    context: "begin".to_owned(),
                    source,
                },
            )
        })?;

    let task = match thread.stage_task_id() {
        Some(task_id) => {
            let task = PgTaskRepository
                .find_by_id(&mut txn, task_id)
                .await
                .map_err(|source| {
                    AgentRuntimeError::database("loading the driving task", source)
                })?;
            let disposed = PgTaskRepository
                .dead_letter_unclaimed(
                    &mut txn,
                    task_id,
                    TaskErrorKind::InternalError,
                    "thread cancelled",
                )
                .await
                .map_err(|source| {
                    AgentRuntimeError::database("disposing of the driving task", source)
                })?;
            if disposed == 0 && !task.status().is_terminal() {
                // Claimed: the live worker performs the cancel at its own
                // boundary. Nothing commits.
                return Ok(false);
            }
            Some(task)
        }
        None => None,
    };

    let outcome = cancel_thread_in_txn(&mut txn, thread.id()).await?;
    if !matches!(outcome, CancelOutcome::Cancelled) {
        return Ok(false);
    }

    if let Some(task) = &task {
        couple_dead_lettered_task(&mut txn, task, "thread cancelled")
            .await
            .map_err(|source| AgentRuntimeError::database("coupling the cancelled job", source))?;
    }

    txn.commit().await.map_err(|source| {
        AgentRuntimeError::database(
            "committing the cancel transaction",
            DbError::QueryFailed {
                context: "commit".to_owned(),
                source,
            },
        )
    })?;
    Ok(true)
}
