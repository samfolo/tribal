//! The guarded transitions: suspend, resolve, and cancel.
//!
//! Every transition names its transaction and its guard. Worker-held
//! writes commit with the driving task's claim-token compare-and-set in
//! the same transaction; non-worker writers guard with the thread-status
//! CAS plus a lock on the driving-task row wherever they dispose of it.
//! Zero-row CAS misses, deadlock aborts, and serialisation failures are
//! retryable; every retry loop is bounded and a terminal status ends
//! every loop. No production path suspends until the agentic loop ships;
//! these transitions are exercised by the runtime's own tests, which is
//! the ticket's stated expectation.

use serde::Serialize;
use sqlx::{Connection, PgConnection};
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, NewAgentThreadRecord,
    PgAgentThreadRecordRepository, PgAgentThreadRepository, PgTaskRepository, TaskRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadId, AgentThreadRecordKind, AgentThreadStatus, AgentThreadSuspension,
    AgentThreadTerminal, TaskErrorKind, TaskId,
};
use tribal_telemetry::{current_span_id, current_trace_id};

use crate::AgentRuntimeError;

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
        return Err(AgentRuntimeError::LeaseLost { task_id });
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
        PgTaskRepository
            .requeue_from_blocked(&mut txn, task_id)
            .await
            .map_err(|source| {
                AgentRuntimeError::database("re-queueing the driving task", source)
            })?;
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
    /// caller's bounded retry loop re-reads until suspended or terminal.
    NotSuspended,
    /// The thread row no longer exists (prune raced); nothing committed.
    Vanished,
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

/// Cancels an unclaimed thread under the locked-unclaimed guard: one
/// transaction commits the cancellation record, the thread's cancelled
/// status, and the unclaimed driving task's dead-letter disposition (a
/// cancelled stage deliberately reads as a failed task on the launched
/// surface).
///
/// This is the sweep-fallback and janitor path. A worker observing the
/// intent at a turn boundary or claim performs the same transaction with
/// its claim token as the guard instead. Job coupling is the caller's:
/// the worker composes it through the coupling seam after this commits
/// zero rows of job state.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::Database`] and serialisation errors of
/// the parts.
pub async fn cancel_unclaimed_thread(
    conn: &mut PgConnection,
    thread: &AgentThread,
) -> Result<CancelOutcome, AgentRuntimeError> {
    let mut txn = begin(conn, "beginning the cancel transaction").await?;

    // The locked-unclaimed guard on the driving task: a row a live worker
    // holds is never disposed of from outside.
    if let Some(task_id) = thread.stage_task_id() {
        let disposed = PgTaskRepository
            .dead_letter_unclaimed(
                &mut txn,
                task_id,
                TaskErrorKind::InternalError,
                "thread cancelled",
            )
            .await
            .map_err(|source| {
                AgentRuntimeError::database("disposing of the unclaimed driving task", source)
            })?;
        if disposed == 0 {
            // Claimed (a worker is live: it observes the intent at its
            // own boundary) or already terminal. Nothing commits here.
            return Ok(CancelOutcome::TaskHeld);
        }
    }

    let locked = PgAgentThreadRepository
        .lock(&mut txn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("locking the thread for cancel", source))?;
    let Some(current) = locked else {
        return Ok(CancelOutcome::Vanished);
    };
    if current.status().is_terminal() {
        return Ok(CancelOutcome::AlreadyTerminal);
    }

    let seq = next_seq(&mut txn, thread.id()).await?;
    PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
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
            &mut txn,
            thread.id(),
            AgentThreadTerminal::Cancelled,
            current.status(),
        )
        .await
        .map_err(|source| AgentRuntimeError::database("cancelling the thread", source))?;
    if moved == 0 {
        return Err(AgentRuntimeError::StatusCasMissed {
            thread_id: thread.id(),
            expected: current.status(),
        });
    }

    commit(txn, "committing the cancel transaction").await?;
    Ok(CancelOutcome::Cancelled)
}

/// What a cancel attempt concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The cancellation committed; the caller couples job state next.
    Cancelled,
    /// The driving task is claimed: the live worker observes the intent
    /// at its own boundary; nothing committed.
    TaskHeld,
    /// The thread was already terminal; the intent is harmless dead data.
    AlreadyTerminal,
    /// The thread row no longer exists; nothing committed.
    Vanished,
}

// ---------------------------------------------------------------------------
// Shared parts
// ---------------------------------------------------------------------------

async fn begin<'c>(
    conn: &'c mut PgConnection,
    context: &str,
) -> Result<sqlx::Transaction<'c, sqlx::Postgres>, AgentRuntimeError> {
    conn.begin().await.map_err(|source| {
        AgentRuntimeError::database(
            context,
            tribal_db::DbError::QueryFailed {
                context: context.to_owned(),
                source,
            },
        )
    })
}

async fn commit(
    txn: sqlx::Transaction<'_, sqlx::Postgres>,
    context: &str,
) -> Result<(), AgentRuntimeError> {
    txn.commit().await.map_err(|source| {
        AgentRuntimeError::database(
            context,
            tribal_db::DbError::QueryFailed {
                context: context.to_owned(),
                source,
            },
        )
    })
}

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
