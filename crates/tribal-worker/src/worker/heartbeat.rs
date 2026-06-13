//! Heartbeat, reclaim sweeps, and startup reclaim.
//!
//! Provides per-task background heartbeat with ownership-loss
//! signalling, the per-row thread-aware reclaim (recovery cycles and
//! thread exhaustion for stage tasks that drive a thread), the legacy
//! bulk reclaim for rows with no thread, and a startup reclaim pass for
//! crash recovery.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use sqlx::PgPool;
use tokio::{sync::oneshot, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tribal_db::{
    AgentThreadRepository, PgAgentThreadRepository, PgTaskRepository, ReclaimOutcome,
    TaskRepository,
};
use tribal_domain::{
    AgentThreadTerminal, Disposition, DispositionCounters, JobOutcome, JobState, TaskErrorKind,
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
pub(crate) fn recovery_backoff_seconds(recovery_attempts: u32) -> u32 {
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
            let mut task_dead_lettered = false;
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
                    owed = exhaust_thread(
                        &mut txn,
                        &task,
                        &thread,
                        claim_token,
                        error_kind,
                        error_message,
                    )
                    .await?;
                    task_dead_lettered = true;
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

            txn.commit().await.map_err(|e| WorkerError::ReclaimFailed {
                context: "thread-aware reclaim".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "commit".into(),
                    source: e,
                },
            })?;

            // Metrics and notifications mirror the launched dead-letter
            // path, fired only after the commit.
            if task_dead_lettered {
                self.metrics()
                    .record_task_dead_lettered(task.task_type().as_str());
            }
            if let Some(notice) = owed {
                if notice.state == JobState::Failed {
                    self.metrics()
                        .record_job_completed(JobOutcome::Failure.as_str(), None);
                }
                self.notify_job_state(notice.job_id, notice.state);
            }
        }

        Ok(stats)
    }
}

/// The exhaustion transaction's body: the thread row is locked before
/// its status is judged — a concurrent guarded queued-to-running move
/// serialises against this write rather than racing it — then the thread
/// dead-letters from whatever live status the locked row holds, the
/// driving task dead-letters under its claim guard, and the job couples,
/// returning the owed notification.
async fn exhaust_thread(
    txn: &mut sqlx::PgConnection,
    task: &tribal_domain::Task,
    thread: &tribal_domain::AgentThread,
    claim_token: uuid::Uuid,
    error_kind: TaskErrorKind,
    error_message: &str,
) -> Result<Option<coupling::OwedNotification>, WorkerError> {
    let locked = PgAgentThreadRepository
        .lock(txn, thread.id())
        .await
        .map_err(reclaim_db)?;
    match locked {
        Some(current) if !current.status().is_terminal() => {
            let moved = PgAgentThreadRepository
                .complete(
                    txn,
                    thread.id(),
                    AgentThreadTerminal::DeadLetter,
                    current.status(),
                )
                .await
                .map_err(reclaim_db)?;
            if moved == 0 {
                // Unreachable under the row lock; recorded if it ever fires.
                tracing::warn!(
                    task_id = %task.id(),
                    thread_id = %thread.id(),
                    "thread status moved under its row lock at reclaim exhaustion",
                );
            }
        }
        Some(_) => {
            // Already terminal: the task half still needs its disposition.
        }
        None => {
            tracing::warn!(
                task_id = %task.id(),
                thread_id = %thread.id(),
                "thread row vanished at reclaim exhaustion; dead-lettering the task alone",
            );
        }
    }

    PgTaskRepository
        .dead_letter_claimed(txn, task.id(), claim_token, error_kind, error_message)
        .await
        .map_err(reclaim_db)?;
    coupling::couple_dead_lettered_task(txn, task, error_message)
        .await
        .map_err(reclaim_db)
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
    /// While set, the timer skips its beats: a streamed inference call's
    /// deltas carry liveness instead.
    suppressed: Arc<AtomicBool>,
    pool: PgPool,
    task_id: TaskId,
    claim_token: uuid::Uuid,
    interval: Duration,
}

impl HeartbeatHandle {
    /// Aborts the heartbeat background task.
    pub(crate) fn abort(&self) {
        self.abort_handle.abort();
    }

    /// Builds the loop's phase-aware liveness seam over this heartbeat:
    /// streamed deltas beat (debounced) with the timer suppressed, and
    /// the pump's abort stops the timer before a suspension commits.
    pub(crate) fn pump(&self) -> WorkerHeartbeatPump {
        WorkerHeartbeatPump {
            suppressed: Arc::clone(&self.suppressed),
            pool: self.pool.clone(),
            task_id: self.task_id,
            claim_token: self.claim_token,
            interval: self.interval,
            last_delta_beat: Mutex::new(std::time::Instant::now()),
            abort_handle: self.abort_handle.clone(),
        }
    }
}

/// The worker's [`HeartbeatPump`] implementation: phase-aware liveness
/// over the per-task heartbeat row.
pub(crate) struct WorkerHeartbeatPump {
    suppressed: Arc<AtomicBool>,
    pool: PgPool,
    task_id: TaskId,
    claim_token: uuid::Uuid,
    interval: Duration,
    last_delta_beat: Mutex<std::time::Instant>,
    abort_handle: tokio::task::AbortHandle,
}

impl tribal_agent_runtime::HeartbeatPump for WorkerHeartbeatPump {
    fn inference_started(&self) {
        self.suppressed.store(true, Ordering::SeqCst);
    }

    fn stream_delta(&self) {
        {
            let mut last = self
                .last_delta_beat
                .lock()
                .expect("the delta-beat clock is never poisoned");
            if last.elapsed() < self.interval {
                return;
            }
            *last = std::time::Instant::now();
        }
        let pool = self.pool.clone();
        let task_id = self.task_id;
        let claim_token = self.claim_token;
        // Fire-and-forget: a delta beat is pure liveness. Ownership loss
        // is not signalled from here — the next claim-guarded commit
        // refuses deterministically, and the timer resumes after the
        // call.
        tokio::spawn(async move {
            if let Ok(mut conn) = pool.acquire().await {
                let _ = PgTaskRepository
                    .heartbeat(&mut conn, task_id, claim_token)
                    .await;
            }
        });
    }

    fn inference_finished(&self) {
        self.suppressed.store(false, Ordering::SeqCst);
    }

    fn abort(&self) {
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
    let suppressed = Arc::new(AtomicBool::new(false));

    let timer_suppressed = Arc::clone(&suppressed);
    let timer_pool = pool.clone();
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

            // A streamed call's deltas carry liveness; the timer stands
            // down rather than doubling the writes.
            if timer_suppressed.load(Ordering::SeqCst) {
                continue;
            }

            let mut conn = match timer_pool.acquire().await {
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
        suppressed,
        pool,
        task_id,
        claim_token,
        interval,
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
