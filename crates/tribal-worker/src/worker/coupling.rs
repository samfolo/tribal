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
    AgentThread, JobId, JobOutcome, JobState, JobStatus, Task, TaskErrorKind, TaskId, TaskType,
};

/// A notification a committed coupling owes its caller: sent to the
/// job-state watch hub only after the enclosing transaction commits,
/// never from inside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwedNotification {
    /// The job whose watchers are notified.
    pub job_id: JobId,
    /// The state the coupling moved the job towards.
    pub state: JobState,
}

/// Fires the triage fan-in when the current task is the last live triage
/// sibling: upserts the relation task and advances the job to `Relating`.
/// Returns whether the job actually moved — a terminal job makes this
/// `false` even when the relation task was upserted, so callers never
/// publish a transition that did not commit.
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

    let moved = PgJobRepository
        .update_status_if_live(conn, job_id, &transition)
        .await?;

    Ok(moved.is_some())
}

/// Couples a task's terminal disposition to its job, exactly as a worker
/// dead-letter does: the triage fan-in for a triage task, the job-failed
/// transition for extraction and relation (whose dead-letter means the
/// job cannot progress). Returns the notification the caller owes its
/// watchers once the transaction commits — none when the job was already
/// terminal, so a late coupling never publishes a state that did not
/// commit.
///
/// # Errors
///
/// Returns [`DbError`] on database errors; the caller's transaction
/// decides what commits.
pub async fn couple_dead_lettered_task(
    conn: &mut sqlx::PgConnection,
    task: &Task,
    error_message: &str,
) -> Result<Option<OwedNotification>, DbError> {
    match task.task_type() {
        TaskType::Triage => {
            let fired = triage_fan_in(conn, task.job_id(), task.id()).await?;
            Ok(fired.then_some(OwedNotification {
                job_id: task.job_id(),
                state: JobState::Relating,
            }))
        }
        TaskType::Extraction | TaskType::Relation => {
            let transition = JobStatusTransition::builder()
                .status(JobStatus::Failed)
                .outcome(Some(JobOutcome::Failure))
                .error_message(Some(error_message.to_owned()))
                .completed_at(Some(Utc::now()))
                .build();
            let moved = PgJobRepository
                .update_status_if_live(conn, task.job_id(), &transition)
                .await?;
            Ok(moved.map(|_| OwedNotification {
                job_id: task.job_id(),
                state: JobState::Failed,
            }))
        }
    }
}

/// What [`cancel_thread`] concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelThreadOutcome {
    /// Nothing committed: a live worker owns the boundary, or the thread
    /// was already terminal or gone.
    Skipped,
    /// The cancellation committed; the caller sends the owed
    /// notification now that the transaction has returned.
    Cancelled {
        /// The job coupling's notification, for a thread with a job.
        notification: Option<OwedNotification>,
    },
}

/// Cancels an unclaimed thread and couples its job, in one transaction:
/// the locked-unclaimed task disposal, the cancellation record and
/// status, and the launched job coupling. The sweep's cancel fallback and
/// the control plane share this seam.
///
/// A claimed driving task means a live worker observes the intent at its
/// own boundary, so this rolls back untouched. A task already terminal
/// still cancels the thread but never re-couples the job: whichever
/// transaction made the task terminal fired its coupling, and coupling a
/// completed task as if dead-lettered would fail a healthy job.
///
/// # Errors
///
/// Returns [`AgentRuntimeError`] on database or transition errors;
/// nothing commits on error.
pub async fn cancel_thread(
    conn: &mut sqlx::PgConnection,
    thread: &AgentThread,
) -> Result<CancelThreadOutcome, AgentRuntimeError> {
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
                return Ok(CancelThreadOutcome::Skipped);
            }
            // The coupling fires only for the task this transaction
            // disposed; a terminal task was coupled by whichever
            // transaction made it terminal.
            (disposed > 0).then_some(task)
        }
        None => None,
    };

    let outcome = cancel_thread_in_txn(&mut txn, thread.id()).await?;
    if !matches!(outcome, CancelOutcome::Cancelled) {
        return Ok(CancelThreadOutcome::Skipped);
    }

    let mut notification = None;
    if let Some(task) = &task {
        notification = couple_dead_lettered_task(&mut txn, task, "thread cancelled")
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
    Ok(CancelThreadOutcome::Cancelled { notification })
}
