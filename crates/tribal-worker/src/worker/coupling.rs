//! The job-coupling seam: how task-level outcomes touch job state.
//!
//! Transaction-composable by construction — every function takes the
//! caller's connection and commits nothing itself — so the worker's
//! commit and failure paths, the healing sweeps, and the runtime's
//! thread-terminal transactions all couple through the same code. This is
//! the only path by which any actor outside dispatch touches job state.

use tribal_db::{
    DbError, JobRepository, JobStatusTransition, NewTask, PgJobRepository, PgTaskRepository,
    TaskRepository,
};
use tribal_domain::{JobId, JobStatus, TaskId, TaskType};

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
