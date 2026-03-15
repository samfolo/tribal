//! Shared constants for the startup sequence.

use std::time::Duration;

// ---------------------------------------------------------------------------
// Pool names
// ---------------------------------------------------------------------------

/// Pool name for MCP read-path connections.
pub(crate) const POOL_NAME_MCP: &str = "mcp";

/// Pool name for worker write-path connections.
pub(crate) const POOL_NAME_WORKER: &str = "worker";

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

/// `pg_advisory_lock` timeout per attempt.
pub(super) const MIGRATION_LOCK_TIMEOUT: Duration = Duration::from_secs(10);

/// Minimum jittered sleep between migration retry attempts.
pub(super) const MIGRATION_RETRY_SLEEP_MIN_SECS: u64 = 2;

/// Maximum jittered sleep between migration retry attempts.
pub(super) const MIGRATION_RETRY_SLEEP_MAX_SECS: u64 = 5;

/// Fixed advisory lock identifier derived from hashing `"tribal_migrations"`.
///
/// Chosen as a constant to guarantee every Tribal instance contends on the
/// same lock without relying on runtime hashing.
pub(super) const ADVISORY_LOCK_ID: i64 = 0x7472_6962_616C_6D69; // "tribalmigr" in ASCII bytes
