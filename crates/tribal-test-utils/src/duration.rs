//! Centralised test duration constants.
//!
//! Every magic sleep or backdate duration used in integration tests
//! should be defined here with a name that explains *why* the value
//! was chosen, not just how long it is.  This makes it easy to tune
//! timeouts if default test configuration changes or if CI flakes.

use std::time::Duration;

// ---------------------------------------------------------------------------
// Worker cycle durations
// ---------------------------------------------------------------------------

/// Time for a worker to complete at least one poll-claim-dispatch cycle.
///
/// Given `test_config().poll_interval_millis = 100`, 500 ms provides
/// ample room for claim, stage dispatch, and commit.
pub const POLL_SETTLE: Duration = Duration::from_millis(500);

/// Time for a worker to process multiple tasks across several poll
/// cycles.  Used in concurrency tests that need more than two claim
/// rounds to exercise the concurrency limit.
pub const MULTI_CYCLE_SETTLE: Duration = Duration::from_secs(1);

/// Time for a worker to claim a task and begin dispatching, but
/// before a long-running stage completes.  Used to inject external
/// state changes (reclaim, heartbeat loss) while a mock provider
/// delay is still in flight.
pub const CLAIM_SETTLE: Duration = Duration::from_millis(300);

/// Time for the heartbeat loop to detect an externally-reclaimed task.
///
/// Given `test_config().heartbeat_interval_millis = 200`, 600 ms
/// gives the heartbeat at least two chances to observe the loss.
pub const HEARTBEAT_DETECT: Duration = Duration::from_millis(600);

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
