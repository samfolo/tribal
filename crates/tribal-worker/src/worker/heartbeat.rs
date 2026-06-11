//! Heartbeat, reclaim sweep, and startup reclaim.
//!
//! Provides per-task background heartbeat with ownership-loss
//! signalling, a periodic reclaim sweep for abandoned tasks, and a
//! startup reclaim pass for crash recovery.

use std::time::Duration;

use sqlx::PgPool;
use tokio::{sync::oneshot, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tribal_db::{
    AgentThreadRepository, PgAgentThreadRepository, PgTaskRepository, ReclaimOutcome,
    TaskRepository,
};
use tribal_domain::{
    AgentThreadStatus, AgentThreadTerminal, Disposition, DispositionCounters, TaskErrorKind,
    TaskId, TurnOutcome, decide_disposition,
};

use crate::{
    error::WorkerError,
    worker::{Worker, coupling},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of tasks to reclaim in a single startup sweep.
pub(crate) const STARTUP_RECLAIM_LIMIT: u32 = 100;

/// Error message written to tasks reclaimed during startup.
pub(crate) const STARTUP_RECLAIM_MESSAGE: &str = "startup_reclaim";

/// Error message written to tasks reclaimed by the periodic sweep.
pub(crate) const HEARTBEAT_EXPIRED_MESSAGE: &str = "heartbeat_expired";

/// The thread recovery-cycle budget. Zero reproduces launched behaviour
/// exactly: a stage task whose retry budget exhausts under reclaim
/// dead-letters at the same moment it always did, with its thread and
/// job coupled in the same commit. Raising the cap opens fresh cycles
/// (reset retry budget, escalating per-cycle backoff) before the thread
/// fails.
pub(crate) const THREAD_RECOVERY_CAP: u32 = 0;

/// Ceiling on the per-cycle backoff ladder (`2^recovery_attempts`
/// seconds), so the never-resetting cycle counter cannot push a task's
/// availability out indefinitely.
const RECOVERY_BACKOFF_CAP_SECONDS: u32 = 3_600;

// ---------------------------------------------------------------------------
// ReclaimStats
// ---------------------------------------------------------------------------

/// Summarises the outcome of a reclaim operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReclaimStats {
    /// Tasks reset to `queued` for another attempt.
    pub requeued: u32,
    /// Tasks moved to `dead_letter` (retry budget exhausted).
    pub dead_lettered: u32,
}

impl ReclaimStats {
    /// Total number of tasks affected by the reclaim.
    pub fn total(self) -> u32 {
        self.requeued.saturating_add(self.dead_lettered)
    }
}

impl From<ReclaimOutcome> for ReclaimStats {
    fn from(outcome: ReclaimOutcome) -> Self {
        Self {
            requeued: u32::try_from(outcome.requeued).unwrap_or(u32::MAX),
            dead_lettered: u32::try_from(outcome.dead_lettered).unwrap_or(u32::MAX),
        }
    }
}

// ---------------------------------------------------------------------------
// Thread-aware reclaim
// ---------------------------------------------------------------------------

/// Counts of what one thread-aware reclaim pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ThreadReclaimStats {
    /// Stale tasks re-queued within their retry budget.
    pub requeued: u32,
    /// Fresh recovery cycles opened (retry budget reset).
    pub recovery_cycles: u32,
    /// Threads exhausted: thread and task dead-lettered, job coupled.
    pub exhausted: u32,
}

impl ThreadReclaimStats {
    /// Total number of tasks the pass acted on.
    #[must_use]
    pub fn total(self) -> u32 {
        self.requeued
            .saturating_add(self.recovery_cycles)
            .saturating_add(self.exhausted)
    }
}

/// The escalating per-cycle delay: `2^recovery_attempts` seconds, capped
/// at [`RECOVERY_BACKOFF_CAP_SECONDS`]. The cycle counter never resets,
/// so it carries the backoff ladder the per-cycle retry reset would
/// otherwise discard.
fn recovery_backoff_seconds(recovery_attempts: u32) -> u32 {
    2u32.saturating_pow(recovery_attempts)
        .min(RECOVERY_BACKOFF_CAP_SECONDS)
}

impl Worker {
    /// One thread-aware reclaim pass over stale claimed thread-driving
    /// tasks, each handled in its own transaction under the staleness
    /// predicate, the row lock, and the thread-status CAS. The
    /// disposition (re-queue, fresh recovery cycle, or thread
    /// exhaustion) is [`decide_disposition`]'s alone: the task never
    /// transits dead-letter on the way to a fresh cycle, and exhaustion
    /// dead-letters thread and task with the job coupled in the same
    /// commit, the owed notification sent after it.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::ReclaimFailed`] on database failures.
    pub async fn run_thread_aware_reclaim(
        &self,
        limit: u32,
        recovery_cap: u32,
        error_kind: TaskErrorKind,
        error_message: &str,
        flat_backoff_seconds: Option<u32>,
    ) -> Result<ThreadReclaimStats, WorkerError> {
        let timeout_seconds =
            u32::try_from(self.config().task_timeout().as_secs()).unwrap_or(u32::MAX);
        let mut stats = ThreadReclaimStats::default();

        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| WorkerError::ReclaimFailed {
                context: "thread-aware reclaim".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;

        for _ in 0..limit {
            let mut txn = sqlx::Connection::begin(&mut *conn).await.map_err(|e| {
                WorkerError::ReclaimFailed {
                    context: "thread-aware reclaim".into(),
                    source: tribal_db::DbError::QueryFailed {
                        context: "begin".into(),
                        source: e,
                    },
                }
            })?;

            let Some(task) = PgTaskRepository
                .lock_stale_thread_driving(&mut txn, timeout_seconds)
                .await
                .map_err(reclaim_db)?
            else {
                break;
            };
            let Some(thread) = PgAgentThreadRepository
                .find_by_stage_task(&mut txn, task.id())
                .await
                .map_err(reclaim_db)?
            else {
                // The scan's EXISTS clause makes this unreachable; the
                // rolled-back row falls to the legacy pass.
                break;
            };
            let Some(claim_token) = task.claim_token() else {
                tracing::error!(task_id = %task.id(), "stale claimed task carries no claim token");
                break;
            };

            let counters = DispositionCounters {
                retry_count: task.retry_count(),
                max_retries: self.config().task_max_retries,
                recovery_attempts: thread.recovery_attempts(),
                max_recovery_attempts: recovery_cap,
            };

            let mut owed = None;
            match decide_disposition(TurnOutcome::RetryableFailure, counters) {
                Disposition::Requeue { retry_count } => {
                    let backoff = flat_backoff_seconds
                        .unwrap_or_else(|| 2u32.saturating_pow(retry_count.saturating_sub(1)));
                    PgTaskRepository
                        .reclaim_requeue(
                            &mut txn,
                            task.id(),
                            retry_count,
                            backoff,
                            error_kind,
                            error_message,
                        )
                        .await
                        .map_err(reclaim_db)?;
                    stats.requeued += 1;
                }
                Disposition::RecoveryCycle {
                    retry_count,
                    recovery_attempts,
                } => {
                    PgAgentThreadRepository
                        .increment_recovery_attempts(&mut txn, thread.id())
                        .await
                        .map_err(reclaim_db)?;
                    PgTaskRepository
                        .reclaim_requeue(
                            &mut txn,
                            task.id(),
                            retry_count,
                            recovery_backoff_seconds(recovery_attempts),
                            error_kind,
                            error_message,
                        )
                        .await
                        .map_err(reclaim_db)?;
                    stats.recovery_cycles += 1;
                }
                Disposition::ExhaustThread => {
                    // The thread dead-letters from running, or from queued
                    // for a thread whose mark-running CAS never landed —
                    // the same fallback the inline failure path applies.
                    let moved = PgAgentThreadRepository
                        .complete(
                            &mut txn,
                            thread.id(),
                            AgentThreadTerminal::DeadLetter,
                            AgentThreadStatus::Running,
                        )
                        .await
                        .map_err(reclaim_db)?;
                    let moved = if moved == 0 {
                        PgAgentThreadRepository
                            .complete(
                                &mut txn,
                                thread.id(),
                                AgentThreadTerminal::DeadLetter,
                                AgentThreadStatus::Queued,
                            )
                            .await
                            .map_err(reclaim_db)?
                    } else {
                        moved
                    };
                    if moved == 0 {
                        tracing::warn!(
                            task_id = %task.id(),
                            thread_id = %thread.id(),
                            "thread was neither running nor queued at reclaim exhaustion; leaving its status",
                        );
                    }
                    PgTaskRepository
                        .dead_letter_claimed(
                            &mut txn,
                            task.id(),
                            claim_token,
                            error_kind,
                            error_message,
                        )
                        .await
                        .map_err(reclaim_db)?;
                    owed = coupling::couple_dead_lettered_task(&mut txn, &task, error_message)
                        .await
                        .map_err(reclaim_db)?;
                    stats.exhausted += 1;
                }
                Disposition::CompleteTask | Disposition::DeadLetterTask => {
                    // Terminal-outcome dispositions need a turn outcome; a
                    // retryable failure never maps onto them.
                    tracing::error!(
                        task_id = %task.id(),
                        "reclaim disposition was a terminal-outcome variant; leaving the row",
                    );
                    break;
                }
            }

            txn.commit()
                .await
                .map_err(|e| WorkerError::ReclaimFailed {
                    context: "thread-aware reclaim".into(),
                    source: tribal_db::DbError::QueryFailed {
                        context: "commit".into(),
                        source: e,
                    },
                })?;

            if let Some(notice) = owed {
                self.notify_job_state(notice.job_id, notice.state);
            }
        }

        Ok(stats)
    }
}

/// Shorthand for the reclaim pass's database-error mapping.
fn reclaim_db(source: tribal_db::DbError) -> WorkerError {
    WorkerError::ReclaimFailed {
        context: "thread-aware reclaim".into(),
        source,
    }
}

// ---------------------------------------------------------------------------
// HeartbeatHandle
// ---------------------------------------------------------------------------

/// Handle to a running heartbeat background task.
pub(crate) struct HeartbeatHandle {
    /// Fires when heartbeat detects ownership loss (0 rows affected).
    pub(crate) ownership_lost_rx: oneshot::Receiver<()>,
    abort_handle: tokio::task::AbortHandle,
}

impl HeartbeatHandle {
    /// Aborts the heartbeat background task.
    pub(crate) fn abort(&self) {
        self.abort_handle.abort();
    }
}

// ---------------------------------------------------------------------------
// spawn_heartbeat
// ---------------------------------------------------------------------------

/// Spawns a background heartbeat task for the given claimed task.
///
/// The task periodically updates `heartbeat_at` via
/// [`TaskRepository::heartbeat`].  When the update affects zero rows
/// (ownership lost), it fires the `ownership_lost` signal and exits.
/// On cancellation, it exits immediately without signalling ownership
/// loss.
pub(crate) fn spawn_heartbeat(
    pool: PgPool,
    task_id: TaskId,
    claim_token: uuid::Uuid,
    interval: Duration,
    cancellation_token: CancellationToken,
) -> HeartbeatHandle {
    let (ownership_lost_tx, ownership_lost_rx) = oneshot::channel();

    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await; // skip first immediate tick

        let mut ownership_lost_tx = Some(ownership_lost_tx);

        loop {
            tokio::select! {
                () = cancellation_token.cancelled() => {
                    return;
                }
                _ = ticker.tick() => {}
            }

            let mut conn = match pool.acquire().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(
                        task_id = %task_id,
                        error = %e,
                        "heartbeat connection acquisition failed",
                    );
                    continue;
                }
            };

            match PgTaskRepository
                .heartbeat(&mut conn, task_id, claim_token)
                .await
            {
                Ok(0) => {
                    tracing::warn!(task_id = %task_id, "heartbeat detected ownership loss");
                    if let Some(tx) = ownership_lost_tx.take() {
                        let _ = tx.send(());
                    }
                    return;
                }
                Ok(_) => {
                    tracing::trace!(task_id = %task_id, "heartbeat updated");
                }
                Err(e) => {
                    tracing::warn!(
                        task_id = %task_id,
                        error = %e,
                        "heartbeat update failed",
                    );
                }
            }
        }
    });

    HeartbeatHandle {
        ownership_lost_rx,
        abort_handle: handle.abort_handle(),
    }
}

// ---------------------------------------------------------------------------
// run_reclaim_sweep
// ---------------------------------------------------------------------------

/// Runs a single reclaim sweep: finds tasks with expired heartbeats
/// and resets them to queued (or dead-letters if retries exhausted).
pub(crate) async fn run_reclaim_sweep(
    pool: &PgPool,
    heartbeat_timeout: Duration,
    max_retries: u32,
    limit: u32,
) -> Result<ReclaimStats, WorkerError> {
    let timeout_seconds = u32::try_from(heartbeat_timeout.as_secs()).unwrap_or(u32::MAX);

    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| WorkerError::ReclaimFailed {
            context: "periodic reclaim sweep".into(),
            source: tribal_db::DbError::QueryFailed {
                context: "pool acquire".into(),
                source: e,
            },
        })?;

    let outcome = PgTaskRepository
        .reclaim_stale(
            &mut conn,
            timeout_seconds,
            max_retries,
            limit,
            TaskErrorKind::HeartbeatExpired,
            HEARTBEAT_EXPIRED_MESSAGE,
            None,
        )
        .await
        .map_err(|e| WorkerError::ReclaimFailed {
            context: "periodic reclaim sweep".into(),
            source: e,
        })?;

    Ok(ReclaimStats::from(outcome))
}

// ---------------------------------------------------------------------------
// run_startup_reclaim
// ---------------------------------------------------------------------------

/// Runs a single startup reclaim pass: finds tasks orphaned by a
/// previous crashed worker instance and resets them to queued (or
/// dead-letters if retries exhausted).
pub(crate) async fn run_startup_reclaim(
    pool: &PgPool,
    heartbeat_timeout: Duration,
    max_retries: u32,
) -> Result<ReclaimStats, WorkerError> {
    let timeout_seconds = u32::try_from(heartbeat_timeout.as_secs()).unwrap_or(u32::MAX);

    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| WorkerError::ReclaimFailed {
            context: "startup reclaim".into(),
            source: tribal_db::DbError::QueryFailed {
                context: "pool acquire".into(),
                source: e,
            },
        })?;

    let outcome = PgTaskRepository
        .reclaim_stale(
            &mut conn,
            timeout_seconds,
            max_retries,
            STARTUP_RECLAIM_LIMIT,
            TaskErrorKind::StartupReclaim,
            STARTUP_RECLAIM_MESSAGE,
            Some(1),
        )
        .await
        .map_err(|e| WorkerError::ReclaimFailed {
            context: "startup reclaim".into(),
            source: e,
        })?;

    Ok(ReclaimStats::from(outcome))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reclaim_stats_total() {
        let stats = ReclaimStats {
            requeued: 3,
            dead_lettered: 2,
        };
        assert_eq!(stats.total(), 5);
    }

    #[test]
    fn test_reclaim_stats_total_zero() {
        let stats = ReclaimStats {
            requeued: 0,
            dead_lettered: 0,
        };
        assert_eq!(stats.total(), 0);
    }

    #[test]
    fn test_reclaim_stats_from_reclaim_outcome() {
        let outcome = ReclaimOutcome {
            requeued: 7,
            dead_lettered: 3,
        };
        let stats = ReclaimStats::from(outcome);
        assert_eq!(stats.requeued, 7);
        assert_eq!(stats.dead_lettered, 3);
    }

    #[test]
    fn test_recovery_backoff_escalates_with_the_cycle_counter() {
        assert_eq!(recovery_backoff_seconds(1), 2);
        assert_eq!(recovery_backoff_seconds(2), 4);
        assert_eq!(recovery_backoff_seconds(5), 32);
    }

    #[test]
    fn test_recovery_backoff_is_capped() {
        assert_eq!(recovery_backoff_seconds(30), RECOVERY_BACKOFF_CAP_SECONDS);
        assert_eq!(
            recovery_backoff_seconds(u32::MAX),
            RECOVERY_BACKOFF_CAP_SECONDS
        );
    }
}
