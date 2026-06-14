//! The guarded transitions: suspend, resolve, and cancel.
//!
//! Every transition names its transaction and its guard. Worker-held
//! writes commit with the driving task's claim-token compare-and-set in
//! the same transaction; non-worker writers guard with the thread-status
//! CAS plus a lock on the driving-task row wherever they dispose of it.
//! Zero-row CAS misses, deadlock aborts, and serialisation failures are
//! transient: a caller treats them as lost ownership or leaves
//! convergence to the next sweep cycle, and a terminal status ends every
//! path. No production caller suspends a thread yet; these transitions
//! are exercised by the runtime's own tests until one does.

use serde::Serialize;
use sqlx::PgConnection;
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, DrivingTaskRef, NewAgentThreadRecord,
    PgAgentThreadRecordRepository, PgAgentThreadRepository, PgTaskRepository, TaskRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadId, AgentThreadRecordKind, AgentThreadStatus, AgentThreadSuspension,
    AgentThreadTerminal, TaskId,
};
use tribal_telemetry::{current_span_id, current_trace_id};

use crate::{
    AgentRuntimeError,
    txn::{begin, commit},
};

// ---------------------------------------------------------------------------
// Suspend
// ---------------------------------------------------------------------------

/// Suspends a stage-driven thread: one transaction commits the suspension
/// record (the typed cause), the thread's suspended status, and the
/// driving task's move to `blocked` with its lease cleared.
///
/// The thread CAS additionally requires no durable cancellation intent;
/// a caller whose suspend returns [`SuspendOutcome::CancelIntervened`]
/// performs the cancel transaction at that boundary instead. The task
/// move is claim-token guarded: losing the lease rolls the whole commit
/// back.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::LeaseLost`] when the claim guard misses
/// (nothing committed), plus the serialisation and database errors of
/// the parts.
pub async fn suspend_stage_thread(
    conn: &mut PgConnection,
    thread: &AgentThread,
    task_id: TaskId,
    claim_token: uuid::Uuid,
    suspension: &AgentThreadSuspension,
    wake_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<SuspendOutcome, AgentRuntimeError> {
    let content = serialise_control(thread, suspension)?;

    let mut txn = begin(conn, "beginning the suspend transaction").await?;

    // Lock order: the driving-task row first (the claim guard's write),
    // then the thread row for the seq derivation and status CAS.
    let blocked = PgTaskRepository
        .block(&mut txn, task_id, claim_token)
        .await
        .map_err(|source| AgentRuntimeError::database("blocking the driving task", source))?;
    if blocked == 0 {
        return Err(AgentRuntimeError::LeaseLost {
            driving_task: DrivingTaskRef::Stage(task_id),
        });
    }

    PgAgentThreadRepository
        .lock(&mut txn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("locking the thread for suspend", source))?;
    let seq = next_seq(&mut txn, thread.id()).await?;
    PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
                .seq(seq)
                .kind(AgentThreadRecordKind::Suspension)
                .content(content)
                .trace_id(current_trace_id())
                .span_id(current_span_id())
                .build(),
        )
        .await
        .map_err(|source| {
            AgentRuntimeError::database("committing the suspension record", source)
        })?;

    let moved = PgAgentThreadRepository
        .suspend(&mut txn, thread.id(), suspension, wake_at)
        .await
        .map_err(|source| AgentRuntimeError::database("suspending the thread", source))?;
    if moved == 0 {
        // The CAS clause covers two cases the caller must distinguish at
        // this boundary: a durable cancel intent (perform the cancel
        // transaction instead) or a status move (roll back and re-read).
        return Ok(SuspendOutcome::CancelIntervened);
    }

    commit(txn, "committing the suspend transaction").await?;
    Ok(SuspendOutcome::Suspended)
}

/// What a suspend attempt concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuspendOutcome {
    /// The suspension committed.
    Suspended,
    /// The thread-status CAS refused: a durable cancellation intent (or a
    /// rival status move) intervened; nothing committed, and the caller
    /// performs the cancel transaction at this boundary.
    CancelIntervened,
}

// ---------------------------------------------------------------------------
// Resolve
// ---------------------------------------------------------------------------

/// Resolves a suspended stage-driven thread: one transaction appends the
/// resolving input record, marks the thread running, and re-queues its
/// blocked driving task, immediately available.
///
/// Every resolution locks the thread row before evaluating state, so two
/// concurrent resolutions cannot both judge themselves final without one
/// waking the thread. An arrival at a terminal thread is recorded and
/// discarded — it never re-enqueues work.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::Database`] and serialisation errors of
/// the parts.
pub async fn resolve_stage_thread(
    conn: &mut PgConnection,
    thread_id: AgentThreadId,
    resolution: &serde_json::Value,
) -> Result<ResolveOutcome, AgentRuntimeError> {
    let mut txn = begin(conn, "beginning the resolve transaction").await?;

    let Some(thread) = PgAgentThreadRepository
        .lock(&mut txn, thread_id)
        .await
        .map_err(|source| AgentRuntimeError::database("locking the thread for resolve", source))?
    else {
        return Ok(ResolveOutcome::Vanished);
    };

    let seq = next_seq(&mut txn, thread.id()).await?;
    PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
                .seq(seq)
                .kind(AgentThreadRecordKind::Input)
                .content(resolution.clone())
                .trace_id(current_trace_id())
                .span_id(current_span_id())
                .build(),
        )
        .await
        .map_err(|source| {
            AgentRuntimeError::database("committing the resolution record", source)
        })?;

    if thread.status().is_terminal() {
        // Recorded and discarded: the arrival is durable in the log, but
        // a terminal thread never re-enqueues work.
        commit(txn, "committing a discarded resolution").await?;
        return Ok(ResolveOutcome::RecordedAtTerminal);
    }
    if thread.status() != AgentThreadStatus::Suspended {
        // A running thread cannot be woken; the bounded-retry caller
        // re-reads. Nothing commits.
        return Ok(ResolveOutcome::NotSuspended);
    }

    let moved = PgAgentThreadRepository
        .mark_running(&mut txn, thread.id(), AgentThreadStatus::Suspended)
        .await
        .map_err(|source| AgentRuntimeError::database("waking the thread", source))?;
    if moved == 0 {
        return Err(AgentRuntimeError::StatusCasMissed {
            thread_id: thread.id(),
            expected: AgentThreadStatus::Suspended,
        });
    }

    if let Some(task_id) = thread.stage_task_id() {
        let rows = PgTaskRepository
            .requeue_from_blocked(&mut txn, task_id)
            .await
            .map_err(|source| {
                AgentRuntimeError::database("re-queueing the driving task", source)
            })?;
        if rows == 0 {
            // A suspended thread's task is blocked by construction, so a
            // zero-row requeue is an unmodelled pairing. Committing the
            // wake would leave the thread running with an unclaimable
            // task, so roll the whole wake back rather than strand it; a
            // sweep or retry re-attempts.
            return Err(AgentRuntimeError::DrivingTaskNotBlocked { task_id });
        }
    }

    commit(txn, "committing the resolve transaction").await?;
    Ok(ResolveOutcome::Woken)
}

/// What a resolution attempt concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveOutcome {
    /// The thread woke: record appended, status running, task re-queued.
    Woken,
    /// The thread was terminal; the arrival was recorded and discarded.
    RecordedAtTerminal,
    /// The thread was running (not suspended); nothing committed. The
    /// caller re-reads or leaves the arrival to a later cycle.
    NotSuspended,
    /// The thread row no longer exists (prune raced); nothing committed.
    Vanished,
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

/// Commits the cancellation record and the thread's cancelled status
/// inside the caller's transaction, composing with the caller's task
/// disposal (claim-guarded for a worker, locked-unclaimed for a sweep)
/// and job coupling so the whole cancellation is one atomic commit.
///
/// Locks the thread row before deriving the seq. The caller disposes of
/// the driving task before calling, honouring the lock order.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::Database`] and serialisation errors of
/// the parts.
pub async fn cancel_thread_in_txn(
    txn: &mut PgConnection,
    thread_id: AgentThreadId,
) -> Result<CancelOutcome, AgentRuntimeError> {
    let locked = PgAgentThreadRepository
        .lock(txn, thread_id)
        .await
        .map_err(|source| AgentRuntimeError::database("locking the thread for cancel", source))?;
    let Some(current) = locked else {
        return Ok(CancelOutcome::Vanished);
    };
    if current.status().is_terminal() {
        return Ok(CancelOutcome::AlreadyTerminal);
    }

    let seq = next_seq(txn, thread_id).await?;
    PgAgentThreadRecordRepository
        .append(
            txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread_id)
                .seq(seq)
                .kind(AgentThreadRecordKind::Cancellation)
                .content(serde_json::json!({
                    "requested_by": current.cancel_requested_by(),
                    "requested_at": current.cancel_requested_at(),
                }))
                .trace_id(current_trace_id())
                .span_id(current_span_id())
                .build(),
        )
        .await
        .map_err(|source| {
            AgentRuntimeError::database("committing the cancellation record", source)
        })?;

    let moved = PgAgentThreadRepository
        .complete(
            txn,
            thread_id,
            AgentThreadTerminal::Cancelled,
            current.status(),
        )
        .await
        .map_err(|source| AgentRuntimeError::database("cancelling the thread", source))?;
    if moved == 0 {
        return Err(AgentRuntimeError::StatusCasMissed {
            thread_id,
            expected: current.status(),
        });
    }

    Ok(CancelOutcome::Cancelled)
}

/// What a cancel attempt concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The cancellation committed; the caller couples job state in the
    /// same transaction.
    Cancelled,
    /// The thread was already terminal; the intent is harmless dead data.
    AlreadyTerminal,
    /// The thread row no longer exists; nothing committed.
    Vanished,
}

// ---------------------------------------------------------------------------
// Shared parts
// ---------------------------------------------------------------------------

async fn next_seq(
    txn: &mut PgConnection,
    thread_id: AgentThreadId,
) -> Result<tribal_domain::AgentThreadRecordSeq, AgentRuntimeError> {
    PgAgentThreadRecordRepository
        .next_seq(txn, thread_id)
        .await
        .map_err(|source| AgentRuntimeError::database("deriving the next seq", source))
}

fn serialise_control(
    thread: &AgentThread,
    suspension: &impl Serialize,
) -> Result<serde_json::Value, AgentRuntimeError> {
    serde_json::to_value(suspension).map_err(|source| AgentRuntimeError::ContentSerialisation {
        context: format!("serialising a control record of thread {}", thread.id()),
        source,
    })
}
