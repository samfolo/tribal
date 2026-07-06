//! The managed-run thread primitives: the guarded operations a managed
//! worker drives a run through, each fenced on the run-claim token rather
//! than a product family's task row.
//!
//! A managed run is an agent thread anchored to a `run_job` by an opaque
//! run key, with no product stage, binding, or principal. The fence is the
//! thread's own `run_claim_token`: adoption writes the presenting worker's
//! token under the row's exclusive lock, and every subsequent guarded
//! write compare-and-sets against it, so a reclaimed worker's adoption
//! serialises ahead of a stale worker's commit. These primitives are the
//! thread-side half of the two-plane reconciliation; the job-plane settle
//! is the worker's.

use sqlx::PgConnection;
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, DbError, DrivingTaskRef, NewAgentThread,
    NewAgentThreadRecord, PgAgentThreadRecordRepository, PgAgentThreadRepository,
};
use tribal_domain::{
    AGENT_THREAD_FORMAT_VERSION, AgentThread, AgentThreadRecordKind, AgentThreadRecordSeq,
    AgentThreadStage, AgentThreadStatus, AgentThreadSuspension, AgentThreadTerminal, RunJobId,
};
use tribal_telemetry::{current_span_id, current_trace_id};

use crate::{
    AgentRuntimeError, DrivingClaim,
    transitions::next_seq,
    txn::{begin, commit},
};

// ---------------------------------------------------------------------------
// Ensure
// ---------------------------------------------------------------------------

/// Adopts or creates the managed thread for a run, returning it under the
/// presenting worker's claim token.
///
/// The row is the fence: whichever thread bears the run key is adopted by
/// writing the new token onto it, and only a fresh run key creates one. A
/// lost create race — two workers racing the run-key uniqueness — converges
/// by re-adopting the winner. The opening input is committed once, idempotent
/// on the empty log, so a crash between the row insert and the input heals on
/// the next adoption.
///
/// Call on a plain connection, never inside a caller's transaction: each
/// plane-step commits independently, and the race-converge path re-reads
/// after a unique violation.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::Database`] on database errors, and
/// [`AgentRuntimeError::LeaseLost`] if the run is adopted out from under this
/// worker before its opening input commits.
pub async fn ensure_managed_thread(
    conn: &mut PgConnection,
    run_key: RunJobId,
    claim_token: uuid::Uuid,
    opening_input: serde_json::Value,
) -> Result<AgentThread, AgentRuntimeError> {
    let thread = match PgAgentThreadRepository
        .adopt_managed(conn, run_key, claim_token)
        .await
        .map_err(|source| AgentRuntimeError::database("adopting the managed thread", source))?
    {
        // The reclaim-and-retry path: an existing run resumes under the
        // rotated token, never repeats.
        Some(adopted) => adopted,
        None => create_managed_thread(conn, run_key, claim_token).await?,
    };

    commit_opening_input(conn, &thread, run_key, claim_token, opening_input).await?;
    Ok(thread)
}

/// Inserts a fresh managed thread, converging a lost create race by
/// re-adopting the winner under this worker's token.
async fn create_managed_thread(
    conn: &mut PgConnection,
    run_key: RunJobId,
    claim_token: uuid::Uuid,
) -> Result<AgentThread, AgentRuntimeError> {
    let new = NewAgentThread::builder()
        .stage(AgentThreadStage::Managed)
        .driving_task(DrivingTaskRef::Managed(run_key))
        .run_claim_token(Some(claim_token))
        .format_version(AGENT_THREAD_FORMAT_VERSION)
        .build();

    match PgAgentThreadRepository.insert(conn, &new).await {
        Ok(created) => Ok(created),
        Err(DbError::UniqueViolation { .. }) => PgAgentThreadRepository
            .adopt_managed(conn, run_key, claim_token)
            .await
            .map_err(|source| {
                AgentRuntimeError::database("re-adopting after a create race", source)
            })?
            .ok_or_else(|| {
                AgentRuntimeError::database(
                    "the run-key uniqueness held but no thread bears the key",
                    DbError::NotFound {
                        entity: "managed thread",
                        id: run_key.to_string(),
                    },
                )
            }),
        Err(source) => Err(AgentRuntimeError::database(
            "creating the managed thread",
            source,
        )),
    }
}

/// Commits the run's opening input once, guarded by the claim and idempotent
/// on the empty log: a resumed or re-adopted run whose input is already
/// durable is left untouched.
async fn commit_opening_input(
    conn: &mut PgConnection,
    thread: &AgentThread,
    run_key: RunJobId,
    claim_token: uuid::Uuid,
    opening_input: serde_json::Value,
) -> Result<(), AgentRuntimeError> {
    let mut txn = begin(conn, "beginning the opening-input transaction").await?;
    DrivingClaim::managed(run_key, claim_token)
        .require(&mut txn)
        .await?;

    let seq = next_seq(&mut txn, thread.id()).await?;
    if seq != AgentThreadRecordSeq::FIRST {
        // The log already carries the opening input (a resume or a
        // re-adoption); nothing to commit.
        commit(txn, "committing a skipped opening input").await?;
        return Ok(());
    }

    PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
                .seq(seq)
                .kind(AgentThreadRecordKind::Input)
                .content(opening_input)
                .trace_id(current_trace_id())
                .span_id(current_span_id())
                .build(),
        )
        .await
        .map_err(|source| AgentRuntimeError::database("committing the opening input", source))?;

    commit(txn, "committing the opening-input transaction").await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Terminal
// ---------------------------------------------------------------------------

/// How a managed run reached its terminal: the money-driven disposition the
/// worker settles the job under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedRunDisposition {
    /// The run finished its work; the thread moves to its success terminal.
    Completed,
    /// The run failed — a refusal, an exhausted cap; the thread moves to its
    /// own failed terminal. Never the cancel path, whose cancellation record
    /// would claim an intent no managed run carries.
    Failed,
}

/// Drives a managed thread to its terminal under the claim guard: the money
/// disposition maps to the thread's success or failed terminal, and the
/// status CAS from `running` fences a rival that already settled the run.
///
/// A stale-terminal convergence surfaces here as [`AgentRuntimeError::LeaseLost`]
/// (the guard misses) or [`AgentRuntimeError::StatusCasMissed`] (a rival
/// already moved the thread terminal): the caller re-derives the settled run
/// under its own token rather than re-driving it.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::LeaseLost`] when the claim guard misses,
/// [`AgentRuntimeError::StatusCasMissed`] when the thread is no longer
/// `running`, and [`AgentRuntimeError::Database`] on database errors.
pub async fn commit_managed_terminal(
    conn: &mut PgConnection,
    thread: &AgentThread,
    claim: &DrivingClaim,
    disposition: ManagedRunDisposition,
) -> Result<(), AgentRuntimeError> {
    let terminal = match disposition {
        ManagedRunDisposition::Completed => AgentThreadTerminal::Completed,
        ManagedRunDisposition::Failed => AgentThreadTerminal::Failed,
    };

    let mut txn = begin(conn, "beginning the managed terminal transaction").await?;
    claim.require(&mut txn).await?;

    let moved = PgAgentThreadRepository
        .complete(&mut txn, thread.id(), terminal, AgentThreadStatus::Running)
        .await
        .map_err(|source| AgentRuntimeError::database("completing the managed thread", source))?;
    if moved == 0 {
        return Err(AgentRuntimeError::StatusCasMissed {
            thread_id: thread.id(),
            expected: AgentThreadStatus::Running,
        });
    }

    commit(txn, "committing the managed terminal transaction").await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Suspend
// ---------------------------------------------------------------------------

/// Parks a managed thread under the claim guard, committing the suspension
/// record (the typed cause) and the suspended status in one transaction.
///
/// This is the stage suspend without the task plane: a managed run has no
/// driving task to block, so the guard is the run-claim CAS alone. The
/// requeue on wake is the job plane's — the timer-wake finder re-enqueues the
/// run, and its worker re-adopts and drives on.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::LeaseLost`] when the claim guard misses
/// (nothing committed), [`AgentRuntimeError::StatusCasMissed`] when the thread
/// is no longer `running`, and [`AgentRuntimeError::Database`] on database
/// errors.
pub async fn suspend_managed_thread(
    conn: &mut PgConnection,
    thread: &AgentThread,
    claim: &DrivingClaim,
    suspension: &AgentThreadSuspension,
    wake_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(), AgentRuntimeError> {
    let content = serde_json::to_value(suspension).map_err(|source| {
        AgentRuntimeError::ContentSerialisation {
            context: format!("serialising the suspension of thread {}", thread.id()),
            source,
        }
    })?;

    let mut txn = begin(conn, "beginning the managed suspend transaction").await?;
    // The guard takes the thread row's exclusive lock, under which the seq
    // derivation and the status CAS run; there is no task row to block.
    claim.require(&mut txn).await?;

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
            AgentRuntimeError::database("committing the managed suspension record", source)
        })?;

    let moved = PgAgentThreadRepository
        .suspend(&mut txn, thread.id(), suspension, wake_at)
        .await
        .map_err(|source| AgentRuntimeError::database("suspending the managed thread", source))?;
    if moved == 0 {
        return Err(AgentRuntimeError::StatusCasMissed {
            thread_id: thread.id(),
            expected: AgentThreadStatus::Running,
        });
    }

    commit(txn, "committing the managed suspend transaction").await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

/// Disarms a cancelled managed run's wake deadline under the claim guard, so the
/// timer-wake sweep stops re-finding a thread whose job has settled off the
/// other plane. The thread stays suspended — a cancelled managed run is never
/// terminalized, carrying no cancellation intent — but no longer sweepable,
/// matching a durable wait's own null deadline.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::LeaseLost`] when the claim guard misses (nothing
/// committed), and [`AgentRuntimeError::Database`] on database errors.
pub async fn clear_managed_wake(
    conn: &mut PgConnection,
    thread: &AgentThread,
    claim: &DrivingClaim,
) -> Result<(), AgentRuntimeError> {
    let mut txn = begin(conn, "beginning the clear-managed-wake transaction").await?;
    claim.require(&mut txn).await?;

    PgAgentThreadRepository
        .disarm_wake(&mut txn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("disarming the managed wake", source))?;

    commit(txn, "committing the clear-managed-wake transaction").await?;
    Ok(())
}
