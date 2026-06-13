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
    AgentBindingVersionRepository, AgentThreadRecordRepository, AgentThreadRepository, DbError,
    DrivingTaskRef, NewAgentThread, PgAgentBindingVersionRepository, PgAgentThreadRecordRepository,
    PgAgentThreadRepository, PgTaskRepository, TaskRepository,
};
use tribal_domain::{
    AGENT_THREAD_FORMAT_VERSION, AgentBinding, AgentThread, AgentThreadRecord,
    AgentThreadRecordKind, AgentThreadStatus, Job, Task,
};

use crate::{
    AgentRuntimeError,
    txn::{begin, commit},
};

/// A claimed stage task's thread, with the log prefix resume needs.
pub struct StageThread {
    /// The thread, post any claim-time status move.
    pub thread: AgentThread,
    /// The thread's input record, when one has committed: the rendered
    /// conversation as sent, which re-execution re-sends verbatim rather
    /// than re-rendering.
    pub input: Option<AgentThreadRecord>,
    /// The binding the thread's row records — the stored pin for an
    /// existing thread, the freshly resolved one at creation. The stage
    /// reads its sampling parameters from here, so the recorded and the
    /// sent parameters cannot drift apart across a configuration change;
    /// the endpoint itself routes through the boot-time stage specs
    /// until execution becomes binding-driven.
    pub binding: AgentBinding,
}

/// Finds or creates the thread a claimed stage task drives, and moves a
/// queued thread to running.
///
/// First claim creates the thread queued, pinned to the supplied
/// binding, and immediately marks it running. A reclaim after a crash
/// finds it already running and proceeds — inference is at-least-once —
/// carrying the binding the thread row records rather than the supplied
/// one, so a configuration change between attempts cannot change the
/// parameters a resumed thread sends. A suspended or terminal thread is
/// returned untouched: the claim-time crash-window rules decide what
/// the worker does with the task, never this function.
///
/// The thread insert and the queued-to-running move each commit with the
/// caller's claim verified inside their transaction, so a worker whose
/// lease was reclaimed cannot create a thread for (or set running under)
/// a task it no longer owns.
///
/// Call on a plain connection, never inside a caller's transaction: the
/// race-converge path re-reads after a unique violation, which would be
/// poisoned inside an aborted transaction. This is pre-call setup, on the
/// claim side of the no-transaction-across-inference rule.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::LeaseLost`] when the claim guard misses,
/// and [`AgentRuntimeError::Database`] on database errors. A concurrent
/// creator losing the one-thread-per-task race converges on the winner's
/// row.
pub async fn ensure_stage_thread(
    conn: &mut PgConnection,
    job: &Job,
    task: &Task,
    claim_token: uuid::Uuid,
    binding: &AgentBinding,
) -> Result<StageThread, AgentRuntimeError> {
    let existing = PgAgentThreadRepository
        .find_by_stage_task(conn, task.id())
        .await
        .map_err(|source| AgentRuntimeError::database("finding the stage task's thread", source))?;

    let thread = match existing {
        Some(thread) => thread,
        // A loser of the one-thread-per-task race converges on the
        // winner's row, whose recorded binding can differ from the
        // supplied one; the id comparison below pairs it correctly.
        None => create_stage_thread(conn, job, task, claim_token, binding).await?,
    };

    let binding = if thread.binding_version_id() == binding.id() {
        binding.clone()
    } else {
        load_recorded_binding(conn, &thread).await?
    };

    let thread = if thread.status() == AgentThreadStatus::Queued {
        let moved = mark_running_guarded(conn, &thread, task, claim_token).await?;
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
        binding,
    })
}

/// Creates the thread for a first-claimed stage task, converging on the
/// winner's row when two claimers race the one-thread-per-task unique
/// constraint.
async fn create_stage_thread(
    conn: &mut PgConnection,
    job: &Job,
    task: &Task,
    claim_token: uuid::Uuid,
    binding: &AgentBinding,
) -> Result<AgentThread, AgentRuntimeError> {
    let new = NewAgentThread::builder()
        .pipeline_stage(task.task_type())
        .binding_version_id(binding.id())
        .driving_task(DrivingTaskRef::Stage(task.id()))
        .principal_id(job.principal_id())
        .format_version(AGENT_THREAD_FORMAT_VERSION)
        .build();

    let mut txn = begin(conn, "beginning the thread-creation transaction").await?;
    if !holds_claim(&mut txn, task, claim_token).await? {
        return Err(AgentRuntimeError::LeaseLost {
            driving_task: DrivingTaskRef::Stage(task.id()),
        });
    }
    let inserted = PgAgentThreadRepository.insert(&mut txn, &new).await;
    match inserted {
        Ok(thread) => {
            commit(txn, "committing the thread creation").await?;
            Ok(thread)
        }
        Err(DbError::UniqueViolation { .. }) => {
            // The aborted transaction drops here; the converge re-read
            // runs on the clean connection.
            drop(txn);
            PgAgentThreadRepository
                .find_by_stage_task(conn, task.id())
                .await
                .map_err(|source| {
                    AgentRuntimeError::database("re-reading the race winner's thread", source)
                })?
                .ok_or(AgentRuntimeError::ThreadMissing { task_id: task.id() })
        }
        Err(source) => Err(AgentRuntimeError::database("creating the thread", source)),
    }
}

/// Moves a queued thread to running with the caller's claim verified in
/// the same transaction, so a reclaimed-away worker cannot flip a thread
/// whose task it no longer owns. Returns the CAS's affected-row count.
async fn mark_running_guarded(
    conn: &mut PgConnection,
    thread: &AgentThread,
    task: &Task,
    claim_token: uuid::Uuid,
) -> Result<u64, AgentRuntimeError> {
    let mut txn = begin(conn, "beginning the mark-running transaction").await?;
    if !holds_claim(&mut txn, task, claim_token).await? {
        return Err(AgentRuntimeError::LeaseLost {
            driving_task: DrivingTaskRef::Stage(task.id()),
        });
    }
    let moved = PgAgentThreadRepository
        .mark_running(&mut txn, thread.id(), AgentThreadStatus::Queued)
        .await
        .map_err(|source| AgentRuntimeError::database("marking the thread running", source))?;
    commit(txn, "committing the mark-running transaction").await?;
    Ok(moved)
}

/// Verifies the claim with a shared row lock, holding it for the rest of
/// the enclosing transaction.
async fn holds_claim(
    txn: &mut PgConnection,
    task: &Task,
    claim_token: uuid::Uuid,
) -> Result<bool, AgentRuntimeError> {
    PgTaskRepository
        .holds_claim(txn, task.id(), claim_token)
        .await
        .map_err(|source| AgentRuntimeError::database("verifying the claim", source))
}

/// Loads the binding a thread's row records — the pin a resumed thread's
/// parameters come from.
async fn load_recorded_binding(
    conn: &mut PgConnection,
    thread: &AgentThread,
) -> Result<AgentBinding, AgentRuntimeError> {
    PgAgentBindingVersionRepository
        .find_by_id(conn, thread.binding_version_id())
        .await
        .map_err(|source| {
            AgentRuntimeError::database("loading the thread's recorded binding", source)
        })?
        .ok_or_else(|| {
            // Binding rows are never deleted while a thread references
            // them; absence is a consistency fault.
            AgentRuntimeError::database(
                "loading the thread's recorded binding",
                DbError::NotFound {
                    entity: "agent_binding_version",
                    id: thread.binding_version_id().to_string(),
                },
            )
        })
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
