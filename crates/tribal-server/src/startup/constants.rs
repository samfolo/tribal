//! Shared constants for the startup sequence.

use std::time::Duration;

// Pool names re-exported from tribal-common.
pub(crate) use tribal_common::{POOL_NAME_MCP, POOL_NAME_WORKER};

// ---------------------------------------------------------------------------
// Connection retry
// ---------------------------------------------------------------------------

/// Initial backoff duration for pool connection retries.
pub(super) const POOL_RETRY_INITIAL_BACKOFF: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------
// Migration
// ---------------------------------------------------------------------------

/// Maximum number of advisory lock acquisition attempts.
pub(super) const MIGRATION_MAX_ATTEMPTS: u32 = 3;

/// Minimum jittered sleep between migration retry attempts.
pub(super) const MIGRATION_RETRY_SLEEP_MIN: Duration = Duration::from_secs(2);

/// Maximum jittered sleep between migration retry attempts.
pub(super) const MIGRATION_RETRY_SLEEP_MAX: Duration = Duration::from_secs(5);
