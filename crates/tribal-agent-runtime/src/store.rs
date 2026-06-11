//! Thread-store orchestration: how a claimed stage task gets its thread.
//!
//! A stage-bound thread is created at first claim and survives the task's
//! whole lifetime — the same row blocks, re-queues, and recovers; nothing
//! completes a task and inserts a successor, and nothing creates a second
//! thread for one. Resume is a read: an interrupted thread's committed
//! records are the only state, so re-execution starts from whatever the
//! log already holds.

use sqlx::PgConnection;
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, DbError, DrivingTaskRef, NewAgentThread,
    PgAgentThreadRecordRepository, PgAgentThreadRepository,
};
use tribal_domain::{
    AGENT_THREAD_FORMAT_VERSION, AgentBinding, AgentThread, AgentThreadRecord,
    AgentThreadRecordKind, AgentThreadStatus, Job, Task,
};

use crate::AgentRuntimeError;

/// A claimed stage task's thread, with the log prefix resume needs.
pub struct StageThread {
    /// The thread, post any claim-time status move.
    pub thread: AgentThread,
    /// The thread's input record, when one has committed: the rendered
    /// conversation as sent, which re-execution re-sends verbatim rather
    /// than re-rendering.
    pub input: Option<AgentThreadRecord>,
    /// The binding the thread runs under: the content-addressed pin the
    /// stage reads its execution parameters from.
    pub binding: AgentBinding,
}

/// Finds or creates the thread a claimed stage task drives, and moves a
/// queued thread to running.
///
/// First claim creates the thread queued and immediately marks it
/// running. A reclaim after a crash finds it already running and
/// proceeds — inference is at-least-once. A suspended or terminal thread
/// is returned untouched: the claim-time crash-window rules decide what
/// the worker does with the task, never this function.
///
/// Call on a plain connection, never inside a caller's transaction: the
/// race-converge path re-reads after a unique violation, which would be
/// poisoned inside an aborted transaction. This is pre-call setup, on the
/// claim side of the no-transaction-across-inference rule.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::Database`] on database errors. A
/// concurrent creator losing the one-thread-per-task race converges on
/// the winner's row.
pub async fn ensure_stage_thread(
    conn: &mut PgConnection,
    job: &Job,
    task: &Task,
    binding: &AgentBinding,
) -> Result<StageThread, AgentRuntimeError> {
    let existing = PgAgentThreadRepository
        .find_by_stage_task(conn, task.id())
        .await
        .map_err(|source| AgentRuntimeError::database("finding the stage task's thread", source))?;

    let thread = match existing {
        Some(thread) => thread,
        None => create_stage_thread(conn, job, task, binding).await?,
    };

    let thread = if thread.status() == AgentThreadStatus::Queued {
        let moved = PgAgentThreadRepository
            .mark_running(conn, thread.id(), AgentThreadStatus::Queued)
            .await
            .map_err(|source| AgentRuntimeError::database("marking the thread running", source))?;
        if moved == 0 {
            // Another actor moved it between the read and the CAS; the
            // re-read is the converged truth.
            reload(conn, thread).await?
        } else {
            PgAgentThreadRepository
                .find_by_id(conn, thread.id())
                .await
                .map_err(|source| {
                    AgentRuntimeError::database("re-reading the running thread", source)
                })?
                .ok_or(AgentRuntimeError::ThreadMissing { task_id: task.id() })?
        }
    } else {
        thread
    };

    let input = PgAgentThreadRecordRepository
        .find_by_thread(conn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("reading the thread's log", source))?
        .into_iter()
        .find(|record| {
            record.kind() == AgentThreadRecordKind::Input
                && crate::turn::is_rendered_conversation(record.content())
        });

    Ok(StageThread {
        thread,
        input,
        binding: binding.clone(),
    })
}

/// Creates the thread for a first-claimed stage task, converging on the
/// winner's row when two claimers race the one-thread-per-task unique
/// constraint.
async fn create_stage_thread(
    conn: &mut PgConnection,
    job: &Job,
    task: &Task,
    binding: &AgentBinding,
) -> Result<AgentThread, AgentRuntimeError> {
    let new = NewAgentThread::builder()
        .pipeline_stage(task.task_type())
        .binding_version_id(binding.id())
        .driving_task(DrivingTaskRef::Stage(task.id()))
        .principal_id(job.principal_id())
        .format_version(AGENT_THREAD_FORMAT_VERSION)
        .build();

    match PgAgentThreadRepository.insert(conn, &new).await {
        Ok(thread) => Ok(thread),
        Err(DbError::UniqueViolation { .. }) => PgAgentThreadRepository
            .find_by_stage_task(conn, task.id())
            .await
            .map_err(|source| {
                AgentRuntimeError::database("re-reading the race winner's thread", source)
            })?
            .ok_or(AgentRuntimeError::ThreadMissing { task_id: task.id() }),
        Err(source) => Err(AgentRuntimeError::database("creating the thread", source)),
    }
}

/// Re-reads a thread after a CAS miss, falling back to the row already
/// in hand only if the re-read races a deletion (impossible while the
/// task is live, so the fallback is theoretical).
async fn reload(
    conn: &mut PgConnection,
    current: AgentThread,
) -> Result<AgentThread, AgentRuntimeError> {
    let reread = PgAgentThreadRepository
        .find_by_id(conn, current.id())
        .await
        .map_err(|source| AgentRuntimeError::database("re-reading the moved thread", source))?;
    Ok(reread.unwrap_or(current))
}
