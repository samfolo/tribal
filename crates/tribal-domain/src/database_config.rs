//! Database connection pool configuration.
//!
//! [`DatabaseConfig`] is loaded from the application YAML configuration file
//! and consumed by the startup sequence to create the MCP and worker
//! connection pools.

use serde::Deserialize;

/// Configuration for the database connection pools.
///
/// Loaded from the application YAML configuration file.  The `url` field is
/// required; all other fields have sensible defaults for local development.
///
/// Two pools are created from this configuration: one for MCP read-path
/// queries and one for worker write-path transactions.  Pool-specific
/// settings (max connections, statement timeout) are selected by the startup
/// sequence based on the pool name.
#[derive(Debug, Clone, PartialEq, Deserialize)]
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

    /// Seconds to wait when acquiring a connection before returning an error.
    #[serde(default = "default_acquire_timeout_seconds")]
    pub acquire_timeout_seconds: u64,

    /// Statement timeout in seconds for MCP pool connections.
    #[serde(default = "default_statement_timeout_mcp_seconds")]
    pub statement_timeout_mcp_seconds: u64,

    /// Statement timeout in seconds for worker pool connections.
    #[serde(default = "default_statement_timeout_worker_seconds")]
    pub statement_timeout_worker_seconds: u64,

    /// Maximum connection attempts during startup (used by the startup
    /// sequence, not by this module directly).
    #[serde(default = "default_max_connect_attempts")]
    pub max_connect_attempts: u32,
}

const fn default_pool_mcp_max_connections() -> u32 {
    8
}

const fn default_pool_worker_max_connections() -> u32 {
    16
}

const fn default_acquire_timeout_seconds() -> u64 {
    5
}

const fn default_statement_timeout_mcp_seconds() -> u64 {
    10
}

const fn default_statement_timeout_worker_seconds() -> u64 {
    60
}

const fn default_max_connect_attempts() -> u32 {
    5
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialise_with_only_url_applies_defaults() {
        let json = r#"{"url": "postgres://localhost/tribal"}"#;
        let config: DatabaseConfig =
            serde_json::from_str(json).expect("should deserialise with only url");

        assert_eq!(config.url, "postgres://localhost/tribal");
        assert_eq!(config.pool_mcp_max_connections, 8);
        assert_eq!(config.pool_worker_max_connections, 16);
        assert_eq!(config.acquire_timeout_seconds, 5);
        assert_eq!(config.statement_timeout_mcp_seconds, 10);
        assert_eq!(config.statement_timeout_worker_seconds, 60);
        assert_eq!(config.max_connect_attempts, 5);
    }
}
