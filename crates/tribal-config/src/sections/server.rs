//! Server transport and connection configuration.

use serde::{Deserialize, Serialize};

use super::transport_kind::TransportKind;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const fn default_shutdown_deadline_ms() -> u64 {
    30_000
}

const fn default_max_connection_age_ms() -> u64 {
    900_000
}

const fn default_idle_timeout_ms() -> u64 {
    300_000
}

const fn default_keepalive_interval_ms() -> u64 {
    30_000
}

// ---------------------------------------------------------------------------
// ServerConfig
// ---------------------------------------------------------------------------

/// Top-level server configuration.
///
/// Controls the transport protocol, bind address, shutdown behaviour,
/// and SSE-specific tuning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Transport protocol for the MCP server.
    #[serde(default)]
    pub transport: TransportKind,

    /// Socket address to bind the HTTP/SSE listener to.
    ///
    /// `None` when using stdio transport.  When transport is HTTP or SSE
    /// and this is `None`, the startup sequence supplies a fallback.
    #[serde(default)]
    pub bind_address: Option<String>,

    /// Graceful shutdown deadline in milliseconds.
    #[serde(default = "default_shutdown_deadline_ms")]
    pub shutdown_deadline_ms: u64,

    /// SSE-specific connection settings.
    #[serde(default)]
    pub sse: SseConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            transport: TransportKind::default(),
            bind_address: None,
            shutdown_deadline_ms: default_shutdown_deadline_ms(),
            sse: SseConfig::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// SseConfig
// ---------------------------------------------------------------------------

/// SSE-specific connection settings.
///
/// Nested under `server.sse` in the configuration file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SseConfig {
    /// Maximum SSE connection lifetime in milliseconds.
    ///
    /// Forces re-authentication after this period.
    #[serde(default = "default_max_connection_age_ms")]
    pub max_connection_age_ms: u64,

    /// Close SSE connection if no real events for this many milliseconds.
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,

    /// SSE comment keepalive interval in milliseconds.
    ///
    /// Prevents proxy timeouts on idle connections.
    #[serde(default = "default_keepalive_interval_ms")]
    pub keepalive_interval_ms: u64,
}

impl Default for SseConfig {
    fn default() -> Self {
        Self {
            max_connection_age_ms: default_max_connection_age_ms(),
            idle_timeout_ms: default_idle_timeout_ms(),
            keepalive_interval_ms: default_keepalive_interval_ms(),
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
    fn test_server_config_default_values() {
        let config = ServerConfig::default();
        assert_eq!(config.transport, TransportKind::Stdio);
        assert_eq!(config.bind_address, None);
        assert_eq!(config.shutdown_deadline_ms, 30_000);
    }

    #[test]
    fn test_sse_config_default_values() {
        let config = SseConfig::default();
        assert_eq!(config.max_connection_age_ms, 900_000);
        assert_eq!(config.idle_timeout_ms, 300_000);
        assert_eq!(config.keepalive_interval_ms, 30_000);
    }
}
