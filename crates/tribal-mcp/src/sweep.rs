//! Background TTL sweep for per-job watch channel entries.
//!
//! Evicts entries from the shared `job_state_txs` map based on two
//! criteria: terminal TTL (entry reached a terminal state and the
//! configured retention period has elapsed) and hard TTL (entry
//! exceeded the maximum lifetime regardless of state).

use std::time::{Duration, Instant};

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tribal_common::JobStateTxs;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Interval between sweep passes, in seconds.
const SWEEP_INTERVAL_SECONDS: u64 = 60;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the periodic job-state sweep until the cancellation token fires.
///
/// Each pass evicts entries where:
/// - `terminal_at` is set and older than `terminal_ttl`, or
/// - `inserted_at` is older than `hard_ttl` (regardless of terminal state).
///
/// The sweep is time-based only — it never consults the database.
pub async fn run_job_state_sweep(
    job_state_txs: JobStateTxs,
    terminal_ttl: Duration,
    hard_ttl: Duration,
    cancellation_token: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(SWEEP_INTERVAL_SECONDS));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Skip the immediate first tick — no entries to sweep at startup.
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => sweep_once(&job_state_txs, terminal_ttl, hard_ttl, Instant::now()),
            () = cancellation_token.cancelled() => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Sweep logic
// ---------------------------------------------------------------------------

/// Executes a single sweep pass, evicting stale entries.
///
/// Accepts `now` as a parameter for deterministic testing.
fn sweep_once(txs: &JobStateTxs, terminal_ttl: Duration, hard_ttl: Duration, now: Instant) {
    txs.retain(|_id, entry| {
        if let Some(terminal_at) = entry.terminal_at
            && now.saturating_duration_since(terminal_at) >= terminal_ttl
        {
            return false;
        }
        if now.saturating_duration_since(entry.inserted_at) >= hard_ttl {
            return false;
        }
        true
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use dashmap::DashMap;
    use tokio::sync::watch;
    use tribal_common::JobWatchEntry;
    use tribal_domain::{JobId, JobState};

    use super::*;

    fn make_entry(inserted_ago: Duration, terminal_ago: Option<Duration>) -> JobWatchEntry {
        let now = Instant::now();
        let (tx, rx) = watch::channel(JobState::Queued);
        JobWatchEntry::with_timestamps_for_test(
            tx,
            rx,
            now.checked_sub(inserted_ago)
                .expect("test duration within range"),
            terminal_ago.map(|d| now.checked_sub(d).expect("test duration within range")),
        )
    }

    #[test]
    fn test_sweep_once_evicts_terminal_past_ttl() {
        let txs = Arc::new(DashMap::new());
        let job_id = JobId::new();
        txs.insert(
            job_id,
            make_entry(Duration::from_secs(60), Some(Duration::from_secs(400))),
        );

        let terminal_ttl = Duration::from_secs(300);
        let hard_ttl = Duration::from_secs(3600);
        sweep_once(&txs, terminal_ttl, hard_ttl, Instant::now());

        assert!(txs.is_empty(), "terminal entry past TTL should be evicted");
    }

    #[test]
    fn test_sweep_once_retains_terminal_within_ttl() {
        let txs = Arc::new(DashMap::new());
        let job_id = JobId::new();
        txs.insert(
            job_id,
            make_entry(Duration::from_secs(60), Some(Duration::from_secs(10))),
        );

        let terminal_ttl = Duration::from_secs(300);
        let hard_ttl = Duration::from_secs(3600);
        sweep_once(&txs, terminal_ttl, hard_ttl, Instant::now());

        assert_eq!(txs.len(), 1, "recent terminal entry should be retained");
    }

    #[test]
    fn test_sweep_once_evicts_hard_ttl_regardless_of_terminal() {
        let txs = Arc::new(DashMap::new());
        let job_id = JobId::new();
        // Non-terminal entry past hard TTL.
        txs.insert(job_id, make_entry(Duration::from_secs(4000), None));

        let terminal_ttl = Duration::from_secs(300);
        let hard_ttl = Duration::from_secs(3600);
        sweep_once(&txs, terminal_ttl, hard_ttl, Instant::now());

        assert!(
            txs.is_empty(),
            "non-terminal entry past hard TTL should be evicted",
        );
    }

    #[test]
    fn test_sweep_once_retains_fresh_non_terminal() {
        let txs = Arc::new(DashMap::new());
        let job_id = JobId::new();
        txs.insert(job_id, make_entry(Duration::from_secs(10), None));

        let terminal_ttl = Duration::from_secs(300);
        let hard_ttl = Duration::from_secs(3600);
        sweep_once(&txs, terminal_ttl, hard_ttl, Instant::now());

        assert_eq!(txs.len(), 1, "fresh non-terminal entry should be retained");
    }
}
