//! Server transport and connection configuration.

use serde::{Deserialize, Serialize};

use super::transport_kind::TransportKind;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default bind address for HTTP and SSE transports.
pub const DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:8725";

/// Default graceful shutdown deadline in milliseconds.
pub const DEFAULT_SHUTDOWN_DEADLINE_MS: u64 = 30_000;

/// Default maximum SSE connection lifetime in milliseconds.
pub const DEFAULT_MAX_CONNECTION_AGE_MS: u64 = 900_000;

/// Default SSE idle timeout in milliseconds.
pub const DEFAULT_IDLE_TIMEOUT_MS: u64 = 300_000;

/// Default SSE keepalive interval in milliseconds.
pub const DEFAULT_KEEPALIVE_INTERVAL_MS: u64 = 30_000;

/// Default terminal watch-entry TTL in seconds.
pub const DEFAULT_JOB_STATE_TTL_SECONDS: u64 = 300;

// Validation bounds

/// Maximum allowed value for SSE lifecycle durations in milliseconds.
///
/// One week.  Prevents overflow when converting to `std::time::Duration`
/// and adding to `tokio::time::Instant`.
pub const MAX_LIFECYCLE_DURATION_MS: u64 = 7 * 24 * 3600 * 1_000;

/// Default hard watch-entry TTL in seconds.
pub const DEFAULT_JOB_STATE_HARD_TTL_SECONDS: u64 = 3_600;

const fn default_shutdown_deadline_ms() -> u64 {
    DEFAULT_SHUTDOWN_DEADLINE_MS
}

const fn default_max_connection_age_ms() -> u64 {
    DEFAULT_MAX_CONNECTION_AGE_MS
}

const fn default_idle_timeout_ms() -> u64 {
    DEFAULT_IDLE_TIMEOUT_MS
}

const fn default_keepalive_interval_ms() -> u64 {
    DEFAULT_KEEPALIVE_INTERVAL_MS
}

const fn default_job_state_ttl_seconds() -> u64 {
    DEFAULT_JOB_STATE_TTL_SECONDS
}

const fn default_job_state_hard_ttl_seconds() -> u64 {
    DEFAULT_JOB_STATE_HARD_TTL_SECONDS
}

// ---------------------------------------------------------------------------
// ServerConfig
// ---------------------------------------------------------------------------

/// Top-level server configuration.
///
/// Controls the transport protocol, bind address, shutdown behaviour,
/// and SSE-specific tuning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Transport protocol for the MCP server.
    #[serde(default)]
    pub transport: TransportKind,

    /// Socket address to bind the HTTP/SSE listener to.
    ///
    /// `None` when using stdio transport.  When transport is HTTP or SSE
    /// and this is `None`, the startup sequence supplies
    /// [`DEFAULT_BIND_ADDRESS`] (`127.0.0.1:8725`) as a fallback.
    #[serde(default)]
    pub bind_address: Option<String>,

    /// Publicly-advertised MCP URL, resolved from the environment at load.
    ///
    /// A reverse-proxied deployment sets it to the routable URL clients
    /// reach while binding loopback, so it is the most public statement of
    /// where the OAuth surface is served. `#[serde(skip)]`: it is an
    /// environment-resolved runtime value, not a configuration-file field,
    /// and is populated after extraction.
    #[serde(skip)]
    pub public_mcp_url: Option<String>,

    /// Graceful shutdown deadline in milliseconds.
    #[serde(default = "default_shutdown_deadline_ms")]
    pub shutdown_deadline_ms: u64,

    /// How long to retain a watch-channel entry after the job reaches a
    /// terminal state, in seconds.
    #[serde(default = "default_job_state_ttl_seconds")]
    pub job_state_ttl_seconds: u64,

    /// Hard upper bound on watch-channel entry lifetime regardless of
    /// terminal state, in seconds.
    #[serde(default = "default_job_state_hard_ttl_seconds")]
    pub job_state_hard_ttl_seconds: u64,

    /// SSE-specific connection settings.
    #[serde(default)]
    pub sse: SseConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            transport: TransportKind::default(),
            bind_address: None,
            public_mcp_url: None,
            shutdown_deadline_ms: default_shutdown_deadline_ms(),
            job_state_ttl_seconds: default_job_state_ttl_seconds(),
            job_state_hard_ttl_seconds: default_job_state_hard_ttl_seconds(),
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
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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
        assert_eq!(config.shutdown_deadline_ms, DEFAULT_SHUTDOWN_DEADLINE_MS);
        assert_eq!(config.job_state_ttl_seconds, DEFAULT_JOB_STATE_TTL_SECONDS);
        assert_eq!(
            config.job_state_hard_ttl_seconds,
            DEFAULT_JOB_STATE_HARD_TTL_SECONDS
        );
    }

    #[test]
    fn test_sse_config_default_values() {
        let config = SseConfig::default();
        assert_eq!(config.max_connection_age_ms, DEFAULT_MAX_CONNECTION_AGE_MS);
        assert_eq!(config.idle_timeout_ms, DEFAULT_IDLE_TIMEOUT_MS);
        assert_eq!(config.keepalive_interval_ms, DEFAULT_KEEPALIVE_INTERVAL_MS);
    }
}
