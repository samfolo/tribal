//! The availability sweep: the universal convergence actor.
//!
//! A composition of named, independently tested predicates over one loop,
//! never one growing query. Each predicate scans with `SKIP LOCKED`, so
//! concurrent serve processes never contend, and each acts through the
//! runtime's guarded transitions. The sweep is the structural half of the
//! no-strand guarantee: every suspended thread has a live resolver, a
//! wake-at deadline this sweep drives, or a terminal outcome whose
//! cancel-fallback this sweep performs.

use tribal_agent_runtime::{ResolveOutcome, resolve_stage_thread};
use tribal_db::{AgentThreadRepository, PgAgentThreadRepository};

use crate::worker::{Worker, coupling};

/// How many rows each predicate handles per sweep cycle.
const SWEEP_BATCH: u32 = 32;

/// Counts of what one sweep cycle converged.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ThreadSweepStats {
    /// Suspended threads woken by an elapsed timer.
    pub(crate) timer_wakes: u32,
    /// Threads cancelled through the fallback (unclaimed, intent pending).
    pub(crate) cancelled: u32,
}

impl Worker {
    /// Runs one availability-sweep cycle: the timer-wake predicate, then
    /// the cancel-fallback predicate. Best-effort like every sweep — a
    /// failing predicate warns and leaves convergence to the next cycle.
    pub(crate) async fn run_thread_sweep(&self) -> ThreadSweepStats {
        let mut stats = ThreadSweepStats::default();
        let Ok(mut conn) = self.pool().acquire().await else {
            tracing::warn!("pool acquire failed for the thread sweep");
            return stats;
        };

        stats.timer_wakes = sweep_timer_wakes(&mut conn).await;
        stats.cancelled = sweep_cancel_fallback(self, &mut conn).await;
        stats
    }
}

/// The timer-wake predicate: suspended threads whose `wake_at` elapsed
/// get the full resolve transaction — a timer-fired input record, the
/// running status, and the driving task re-queued.
async fn sweep_timer_wakes(conn: &mut sqlx::PgConnection) -> u32 {
    let due = match PgAgentThreadRepository
        .find_due_timer_wakes(conn, SWEEP_BATCH)
        .await
    {
        Ok(due) => due,
        Err(e) => {
            tracing::warn!(error = %e, "timer-wake scan failed");
            return 0;
        }
    };

    let mut woken = 0;
    for thread in due {
        let resolution = serde_json::json!({ "cause": "timer", "fired_at": chrono::Utc::now() });
        match resolve_stage_thread(conn, thread.id(), &resolution).await {
            Ok(ResolveOutcome::Woken) => woken += 1,
            Ok(outcome) => {
                tracing::debug!(thread_id = %thread.id(), ?outcome, "timer wake skipped");
            }
            Err(e) => {
                tracing::warn!(thread_id = %thread.id(), error = %e, "timer wake failed");
            }
        }
    }
    if woken > 0 {
        tracing::info!(woken, "timer wakes resolved");
    }
    woken
}

/// The cancel-fallback predicate: live threads carrying a durable intent
/// whose driving task is unclaimed (a suspended thread with no live
/// worker) get the cancel transaction. A claimed task means a live
/// worker will observe the intent at its own boundary, so the fallback
/// skips it. The orphan-spotting janitor that writes intents to
/// abandoned descendants arrives with the first parent-thread producer;
/// until then every intent is operator-written.
async fn sweep_cancel_fallback(worker: &Worker, conn: &mut sqlx::PgConnection) -> u32 {
    let intents = match PgAgentThreadRepository
        .find_cancel_intents(conn, SWEEP_BATCH)
        .await
    {
        Ok(intents) => intents,
        Err(e) => {
            tracing::warn!(error = %e, "cancel-intent scan failed");
            return 0;
        }
    };

    let mut cancelled = 0;
    for thread in intents {
        // A running thread's worker handles the intent itself unless the
        // worker died; the unclaimed guard inside the transaction is the
        // arbiter, so the sweep simply attempts every candidate. Job
        // coupling rides the same transaction through the seam; the owed
        // notification goes out after it commits.
        match coupling::cancel_thread(conn, &thread).await {
            Ok(coupling::CancelThreadOutcome::Cancelled { notification }) => {
                cancelled += 1;
                if let Some(notice) = notification {
                    worker.notify_job_state(notice.job_id, notice.state);
                }
                tracing::info!(
                    thread_id = %thread.id(),
                    status = thread.status().as_str(),
                    "cancel fallback terminated a thread",
                );
            }
            Ok(coupling::CancelThreadOutcome::Skipped) => {
                tracing::debug!(thread_id = %thread.id(), "cancel fallback skipped");
            }
            Err(e) => {
                tracing::warn!(thread_id = %thread.id(), error = %e, "cancel fallback failed");
            }
        }
    }
    cancelled
}
