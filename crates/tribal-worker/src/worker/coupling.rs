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
    AgentDriverTaskRepository, AgentThreadRepository, DbError, JobRepository, JobStatusTransition,
    NewTask, PgAgentDriverTaskRepository, PgAgentThreadRepository, PgJobRepository,
    PgTaskRepository, TaskRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadId, JobId, JobOutcome, JobState, JobStatus, Task, TaskErrorKind,
    TaskId, TaskType,
};

/// The requester recorded on a cancellation intent the §10.3 cascade
/// writes, distinguishing a cascaded intent from an operator's in the
/// durable record.
pub(crate) const CANCEL_CASCADE_REQUESTED_BY: &str = "parent-terminal cascade";

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

/// What disposing a cancelled thread's driving task implies for the rest
/// of the cancel transaction.
enum DrivingDisposition {
    /// A live worker holds the task and performs the cancel at its own
    /// boundary, so the cancel transaction rolls back untouched.
    Claimed,
    /// A stage task this transaction disposed; its job couples here.
    CoupleStage(Task),
    /// Nothing to couple: a stage task already terminal (coupled by
    /// whoever made it so), a child's driver task (a child has no job
    /// coupling of its own), or no driving task.
    NoCoupling,
}

/// Disposes a cancelled thread's driving task under the locked-unclaimed
/// guard, branching on whether it is a stage task or a child's driver
/// task.
async fn dispose_cancelled_driving_task(
    txn: &mut sqlx::PgConnection,
    thread: &AgentThread,
) -> Result<DrivingDisposition, AgentRuntimeError> {
    if let Some(task_id) = thread.stage_task_id() {
        let task = PgTaskRepository
            .find_by_id(txn, task_id)
            .await
            .map_err(|source| AgentRuntimeError::database("loading the driving task", source))?;
        let disposed = PgTaskRepository
            .dead_letter_unclaimed(
                txn,
                task_id,
                TaskErrorKind::InternalError,
                "thread cancelled",
            )
            .await
            .map_err(|source| {
                AgentRuntimeError::database("disposing of the driving task", source)
            })?;
        if disposed == 0 && !task.status().is_terminal() {
            return Ok(DrivingDisposition::Claimed);
        }
        return Ok(if disposed > 0 {
            DrivingDisposition::CoupleStage(task)
        } else {
            DrivingDisposition::NoCoupling
        });
    }

    if let Some(driver_task_id) = thread.driver_task_id() {
        let task = PgAgentDriverTaskRepository
            .find_by_id(txn, driver_task_id)
            .await
            .map_err(|source| {
                AgentRuntimeError::database("loading the child's driver task", source)
            })?;
        let disposed = PgAgentDriverTaskRepository
            .dispose_unclaimed(txn, driver_task_id, "thread cancelled")
            .await
            .map_err(|source| {
                AgentRuntimeError::database("disposing of the child's driver task", source)
            })?;
        if disposed == 0 && task.is_some_and(|task| !task.state().is_terminal()) {
            return Ok(DrivingDisposition::Claimed);
        }
        return Ok(DrivingDisposition::NoCoupling);
    }

    Ok(DrivingDisposition::NoCoupling)
}

/// Cancels an unclaimed thread and couples its job, in one transaction:
/// the locked-unclaimed driving-task disposal, the cancellation record and
/// status, and the launched job coupling. The sweep's cancel fallback and
/// the control plane share this seam.
///
/// A claimed driving task means a live worker observes the intent at its
/// own boundary, so this rolls back untouched. A stage task already
/// terminal still cancels the thread but never re-couples the job:
/// whichever transaction made it terminal fired its coupling, and coupling
/// a completed task as if dead-lettered would fail a healthy job. A
/// cancelled root thread cascades the intent to its live descendants once
/// the transaction commits (§10.3).
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

    let coupling_task = match dispose_cancelled_driving_task(&mut txn, thread).await? {
        DrivingDisposition::Claimed => return Ok(CancelThreadOutcome::Skipped),
        DrivingDisposition::CoupleStage(task) => Some(task),
        DrivingDisposition::NoCoupling => None,
    };

    let outcome = cancel_thread_in_txn(&mut txn, thread.id()).await?;
    if !matches!(outcome, CancelOutcome::Cancelled) {
        return Ok(CancelThreadOutcome::Skipped);
    }

    let mut notification = None;
    if let Some(task) = &coupling_task {
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

    // The fast path of the §10.3 cascade: a cancelled root writes intents
    // to its live descendants post-commit. Best-effort; the sweep's orphan
    // janitor heals a crash in this window. Only a root has descendants (a
    // child binding is flat by construction), so the gate skips children.
    if thread.parent_thread_id().is_none() {
        cascade_cancel_to_children(conn, thread.id()).await;
    }

    Ok(CancelThreadOutcome::Cancelled { notification })
}

/// Records cancellation intents on a cancelled root's live descendants,
/// post-commit and best-effort: a failure leaves the orphan janitor to
/// heal the descendants on a later sweep.
async fn cascade_cancel_to_children(conn: &mut sqlx::PgConnection, parent_id: AgentThreadId) {
    match PgAgentThreadRepository
        .cascade_cancel_to_children(conn, parent_id, CANCEL_CASCADE_REQUESTED_BY)
        .await
    {
        Ok(0) => {}
        Ok(count) => tracing::info!(
            parent_thread_id = %parent_id,
            count,
            "cancellation cascaded to live descendants",
        ),
        Err(e) => tracing::warn!(
            parent_thread_id = %parent_id,
            error = %e,
            "post-commit cancellation cascade failed; the orphan janitor will heal it",
        ),
    }
}
