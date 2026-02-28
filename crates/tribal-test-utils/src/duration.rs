//! Centralised test duration constants.
//!
//! Every magic sleep or backdate duration used in integration tests
//! should be defined here with a name that explains *why* the value
//! was chosen, not just how long it is.  This makes it easy to tune
//! timeouts if default test configuration changes or if CI flakes.
//!
//! With the poll-until test pattern these constants serve as
//! **timeout upper bounds** rather than exact sleep durations.  Values
//! include headroom above the theoretical minimum to avoid flakes
//! under CI load.

use std::time::Duration;

// ---------------------------------------------------------------------------
// Worker cycle durations
// ---------------------------------------------------------------------------

/// Timeout for a worker to complete at least one poll-claim-dispatch
/// cycle.
///
/// Theoretical minimum: `poll_interval` (100 ms) + claim + stage
/// dispatch + commit ≈ 400 ms.  Set to 1 s for headroom.
pub const POLL_SETTLE: Duration = Duration::from_secs(1);

/// Timeout for a worker to process multiple tasks across several poll
/// cycles.  Used in concurrency tests that need more than two claim
/// rounds to exercise the concurrency limit.
///
/// Multiple 100 ms poll cycles + dispatch overhead.  Set to 2 s.
pub const MULTI_CYCLE_SETTLE: Duration = Duration::from_secs(2);

/// Timeout for a worker to claim a task and begin dispatching, but
/// before a long-running stage completes.  Used to confirm the
/// provider has been called while a mock delay is still in flight.
///
/// Theoretical minimum: `poll_interval` (100 ms) + claim + DB loads
/// for tag registry and prompt version ≈ 300 ms.  Set to 500 ms for
/// headroom.
pub const CLAIM_SETTLE: Duration = Duration::from_millis(500);

/// Timeout for the heartbeat loop to detect an externally-reclaimed
/// task.
///
/// Given `test_config().heartbeat_interval_millis = 200`, 1 s gives
/// the heartbeat at least four chances to observe the loss.
pub const HEARTBEAT_DETECT: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------
// Simulated staleness
// ---------------------------------------------------------------------------

/// Duration to backdate a task heartbeat to simulate staleness.
///
/// Must exceed `test_config().task_timeout_millis` (`5_000`) by a wide
/// margin to guarantee the reclaim sweep treats the task as stale.
pub const STALE_HEARTBEAT_BACKDATE: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------------------
// Mock provider simulation
// ---------------------------------------------------------------------------

/// Mock provider delay simulating a long-running LLM call.
///
/// Must be long enough that the test can inject external state changes
/// (reclaim, heartbeat loss) before the call completes.
pub const LONG_PROVIDER_DELAY: Duration = Duration::from_secs(5);

/// Upper bound for asserting that an in-flight stage was aborted
/// early.
///
/// Must be well under [`LONG_PROVIDER_DELAY`] to confirm the stage
/// was interrupted rather than running to completion.
pub const EARLY_ABORT_BOUND: Duration = Duration::from_secs(2);

// ---------------------------------------------------------------------------
// Poll intervals
// ---------------------------------------------------------------------------

/// Interval between successive poll attempts in custom `poll_until` calls.
///
/// Matches the interval used internally by `poll_task_status` and
/// `poll_job_status`.  Kept short for fast reactions, long enough to
/// avoid excessive database churn.
pub const POLL_INTERVAL: Duration = Duration::from_millis(50);
