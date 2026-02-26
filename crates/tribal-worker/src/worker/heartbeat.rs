//! Heartbeat, reclaim sweep, and startup reclaim.
//!
//! Provides per-task background heartbeat with ownership-loss
//! signalling, a periodic reclaim sweep for abandoned tasks, and a
//! startup reclaim pass for crash recovery.

use std::time::Duration;

use sqlx::PgPool;
use tokio::{sync::oneshot, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tribal_db::{PgTaskRepository, ReclaimOutcome, TaskRepository};
use tribal_domain::{TaskErrorKind, TaskId};

use crate::error::WorkerError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of tasks to reclaim in a single startup sweep.
pub(crate) const STARTUP_RECLAIM_LIMIT: u32 = 100;

/// Error message written to tasks reclaimed during startup.
pub(crate) const STARTUP_RECLAIM_MESSAGE: &str = "startup_reclaim";

/// Error message written to tasks reclaimed by the periodic sweep.
pub(crate) const HEARTBEAT_EXPIRED_MESSAGE: &str = "heartbeat_expired";

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
// HeartbeatHandle
// ---------------------------------------------------------------------------

/// Handle to a running heartbeat background task.
pub(crate) struct HeartbeatHandle {
    /// Fires when heartbeat detects ownership loss (0 rows affected).
    pub(crate) ownership_lost_rx: oneshot::Receiver<()>,
    /// Aborts the heartbeat background task.
    pub(crate) abort_handle: tokio::task::AbortHandle,
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
}
