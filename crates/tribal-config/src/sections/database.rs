//! Database connection pool configuration.
//!
//! [`DatabaseConfig`] is loaded from the application YAML configuration file
//! and consumed by the startup sequence to create the MCP and worker
//! connection pools.

use std::time::Duration;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default maximum connections in the MCP (read-path) pool.
pub const DEFAULT_POOL_MCP_MAX_CONNECTIONS: u32 = 8;

/// Default maximum connections in the worker (write-path) pool.
pub const DEFAULT_POOL_WORKER_MAX_CONNECTIONS: u32 = 16;

/// Default acquire timeout in milliseconds.
pub const DEFAULT_ACQUIRE_TIMEOUT_MS: u64 = 5_000;

/// Default statement timeout in milliseconds for MCP pool connections.
pub const DEFAULT_STATEMENT_TIMEOUT_MCP_MS: u64 = 10_000;

/// Default statement timeout in milliseconds for worker pool connections.
pub const DEFAULT_STATEMENT_TIMEOUT_WORKER_MS: u64 = 60_000;

/// Default maximum connection attempts during startup.
pub const DEFAULT_MAX_CONNECT_ATTEMPTS: u32 = 5;

// ---------------------------------------------------------------------------
// DatabaseConfig
// ---------------------------------------------------------------------------

/// Configuration for the database connection pools.
///
/// Loaded from the application YAML configuration file.  The `url` field is
/// required; all other fields have sensible defaults for local development.
///
/// Two pools are created from this configuration: one for MCP read-path
/// queries and one for worker write-path transactions.  Pool-specific
/// settings (max connections, statement timeout) are selected by the startup
/// sequence based on the pool name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    /// `PostgreSQL` connection URL (e.g.
    /// `postgres://user:pass@localhost:5432/tribal`).  Required; no default.
    pub url: String,

    /// Maximum connections in the MCP (read-path) pool.
    #[serde(default = "default_pool_mcp_max_connections")]
    pub pool_mcp_max_connections: u32,

    /// Maximum connections in the worker (write-path) pool.
    #[serde(default = "default_pool_worker_max_connections")]
    pub pool_worker_max_connections: u32,

    /// Milliseconds to wait when acquiring a connection before returning an
    /// error.
    #[serde(default = "default_acquire_timeout_ms")]
    pub acquire_timeout_ms: u64,

    /// Statement timeout in milliseconds for MCP pool connections.
    #[serde(default = "default_statement_timeout_mcp_ms")]
    pub statement_timeout_mcp_ms: u64,

    /// Statement timeout in milliseconds for worker pool connections.
    #[serde(default = "default_statement_timeout_worker_ms")]
    pub statement_timeout_worker_ms: u64,

    /// Maximum connection attempts during startup (used by the startup
    /// sequence, not by this module directly).
    #[serde(default = "default_max_connect_attempts")]
    pub max_connect_attempts: u32,
}

impl DatabaseConfig {
    /// Returns the acquire timeout as a [`Duration`].
    #[must_use]
    pub fn acquire_timeout(&self) -> Duration {
        Duration::from_millis(self.acquire_timeout_ms)
    }

    /// Returns the MCP statement timeout as a [`Duration`].
    #[must_use]
    pub fn statement_timeout_mcp(&self) -> Duration {
        Duration::from_millis(self.statement_timeout_mcp_ms)
    }

    /// Returns the worker statement timeout as a [`Duration`].
    #[must_use]
    pub fn statement_timeout_worker(&self) -> Duration {
        Duration::from_millis(self.statement_timeout_worker_ms)
    }
}

// ---------------------------------------------------------------------------
// Defaults (serde)
// ---------------------------------------------------------------------------

const fn default_pool_mcp_max_connections() -> u32 {
    DEFAULT_POOL_MCP_MAX_CONNECTIONS
}

const fn default_pool_worker_max_connections() -> u32 {
    DEFAULT_POOL_WORKER_MAX_CONNECTIONS
}

const fn default_acquire_timeout_ms() -> u64 {
    DEFAULT_ACQUIRE_TIMEOUT_MS
}

const fn default_statement_timeout_mcp_ms() -> u64 {
    DEFAULT_STATEMENT_TIMEOUT_MCP_MS
}

const fn default_statement_timeout_worker_ms() -> u64 {
    DEFAULT_STATEMENT_TIMEOUT_WORKER_MS
}

const fn default_max_connect_attempts() -> u32 {
    DEFAULT_MAX_CONNECT_ATTEMPTS
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            pool_mcp_max_connections: DEFAULT_POOL_MCP_MAX_CONNECTIONS,
            pool_worker_max_connections: DEFAULT_POOL_WORKER_MAX_CONNECTIONS,
            acquire_timeout_ms: DEFAULT_ACQUIRE_TIMEOUT_MS,
            statement_timeout_mcp_ms: DEFAULT_STATEMENT_TIMEOUT_MCP_MS,
            statement_timeout_worker_ms: DEFAULT_STATEMENT_TIMEOUT_WORKER_MS,
            max_connect_attempts: DEFAULT_MAX_CONNECT_ATTEMPTS,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = DatabaseConfig::default();
        assert!(config.url.is_empty());
        assert_eq!(
            config.pool_mcp_max_connections,
            DEFAULT_POOL_MCP_MAX_CONNECTIONS
        );
        assert_eq!(
            config.pool_worker_max_connections,
            DEFAULT_POOL_WORKER_MAX_CONNECTIONS
        );
        assert_eq!(config.acquire_timeout_ms, DEFAULT_ACQUIRE_TIMEOUT_MS);
        assert_eq!(
            config.statement_timeout_mcp_ms,
            DEFAULT_STATEMENT_TIMEOUT_MCP_MS
        );
        assert_eq!(
            config.statement_timeout_worker_ms,
            DEFAULT_STATEMENT_TIMEOUT_WORKER_MS
        );
        assert_eq!(config.max_connect_attempts, DEFAULT_MAX_CONNECT_ATTEMPTS);
    }

    #[test]
    fn test_deserialise_with_only_url_applies_defaults() {
        let yaml = "url: postgres://localhost/tribal";
        let config: DatabaseConfig =
            serde_yaml::from_str(yaml).expect("should deserialise with only url");

        assert_eq!(config.url, "postgres://localhost/tribal");
        assert_eq!(
            config.pool_mcp_max_connections,
            DEFAULT_POOL_MCP_MAX_CONNECTIONS
        );
        assert_eq!(
            config.pool_worker_max_connections,
            DEFAULT_POOL_WORKER_MAX_CONNECTIONS
        );
        assert_eq!(config.acquire_timeout_ms, DEFAULT_ACQUIRE_TIMEOUT_MS);
        assert_eq!(
            config.statement_timeout_mcp_ms,
            DEFAULT_STATEMENT_TIMEOUT_MCP_MS
        );
        assert_eq!(
            config.statement_timeout_worker_ms,
            DEFAULT_STATEMENT_TIMEOUT_WORKER_MS
        );
        assert_eq!(config.max_connect_attempts, DEFAULT_MAX_CONNECT_ATTEMPTS);
    }

    #[test]
    fn test_duration_conversions() {
        let config = DatabaseConfig::default();
        assert_eq!(
            config.acquire_timeout(),
            Duration::from_millis(DEFAULT_ACQUIRE_TIMEOUT_MS)
        );
        assert_eq!(
            config.statement_timeout_mcp(),
            Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MCP_MS)
        );
        assert_eq!(
            config.statement_timeout_worker(),
            Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_WORKER_MS)
        );
    }
}
