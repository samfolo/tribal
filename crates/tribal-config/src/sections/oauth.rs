//! OAuth 2.1 authorisation-server configuration.
//!
//! Plumbing for the OAuth flow: issuer and resource URLs, access-token
//! lifetime, CIMD fetcher tuning. URLs are stored as strings here and
//! parsed into `url::Url` at consumer construction time, matching the
//! existing pattern for `server.bind_address`.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

/// Default access-token TTL for OAuth-issued tokens in hours.
pub const DEFAULT_ACCESS_TOKEN_TTL_HOURS: u64 = 24;

/// Default authorisation-code TTL in seconds.
pub const DEFAULT_AUTHORIZATION_CODE_TTL_SECONDS: u64 = 600;

/// Upper bound on authorisation-code TTL accepted by `validate()`.
///
/// Matches the OAuth 2.1 §4.1.3 RECOMMENDED 10-minute upper bound.
pub const MAX_AUTHORIZATION_CODE_TTL_SECONDS: u64 = 600;

/// Lower bound on authorisation-code TTL accepted by `validate()`.
pub const MIN_AUTHORIZATION_CODE_TTL_SECONDS: u64 = 60;

/// Default CIMD response size cap in bytes.
pub const DEFAULT_CIMD_MAX_RESPONSE_BYTES: usize = 5_120;

/// Minimum CIMD response size cap accepted by `validate()`.
pub const MIN_CIMD_MAX_RESPONSE_BYTES: usize = 256;

/// Maximum CIMD response size cap accepted by `validate()`.
pub const MAX_CIMD_MAX_RESPONSE_BYTES: usize = 65_536;

/// Default CIMD fetch timeout in seconds.
pub const DEFAULT_CIMD_FETCH_TIMEOUT_SECONDS: u64 = 5;

/// Maximum CIMD fetch timeout accepted by `validate()`.
pub const MAX_CIMD_FETCH_TIMEOUT_SECONDS: u64 = 30;

/// Default minimum CIMD cache TTL in seconds.
pub const DEFAULT_CIMD_CACHE_MIN_SECONDS: u64 = 60;

/// Default maximum CIMD cache TTL in seconds.
pub const DEFAULT_CIMD_CACHE_MAX_SECONDS: u64 = 3_600;

/// Default CIMD cache entry bound.
pub const DEFAULT_CIMD_MAX_ENTRIES: usize = 256;

/// Maximum CIMD cache entry bound accepted by `validate()`.
pub const MAX_CIMD_MAX_ENTRIES: usize = 4_096;

// ---------------------------------------------------------------------------
// OAuthConfig
// ---------------------------------------------------------------------------

/// OAuth 2.1 authorisation-server configuration.
///
/// `issuer_url` is the canonical authorisation-server identifier;
/// `resource_url` is the canonical resource identifier the AS issues
/// audience-bound tokens for. When either is `None`, the consumer
/// derives it from the server bind address at startup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthConfig {
    /// Canonical authorisation-server issuer URL. Derived from the
    /// server bind address when omitted.
    #[serde(default)]
    pub issuer_url: Option<String>,

    /// Canonical protected-resource URL. Derived from the server bind
    /// address plus `/mcp` when omitted.
    #[serde(default)]
    pub resource_url: Option<String>,

    /// OAuth-issued access-token TTL in hours.
    #[serde(default = "default_access_token_ttl_hours")]
    pub access_token_ttl_hours: u64,

    /// Authorisation-code TTL in seconds.
    #[serde(default = "default_authorization_code_ttl_seconds")]
    pub authorization_code_ttl_seconds: u64,

    /// Whether the DCR `/register` endpoint is enabled.
    #[serde(default = "default_dcr_enabled")]
    pub dcr_enabled: bool,

    /// CIMD fetcher tuning.
    #[serde(default)]
    pub cimd: CimdConfig,
}

const fn default_access_token_ttl_hours() -> u64 {
    DEFAULT_ACCESS_TOKEN_TTL_HOURS
}

const fn default_authorization_code_ttl_seconds() -> u64 {
    DEFAULT_AUTHORIZATION_CODE_TTL_SECONDS
}

const fn default_dcr_enabled() -> bool {
    true
}

impl Default for OAuthConfig {
    fn default() -> Self {
        Self {
            issuer_url: None,
            resource_url: None,
            access_token_ttl_hours: DEFAULT_ACCESS_TOKEN_TTL_HOURS,
            authorization_code_ttl_seconds: DEFAULT_AUTHORIZATION_CODE_TTL_SECONDS,
            dcr_enabled: true,
            cimd: CimdConfig::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// CimdConfig
// ---------------------------------------------------------------------------

/// CIMD fetcher configuration.
///
/// Defaults match the CIMD draft's recommendations: 5 KiB max response,
/// HTTPS-only, no loopback in production, no additional allowlist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CimdConfig {
    /// Maximum CIMD response body size in bytes.
    #[serde(default = "default_cimd_max_response_bytes")]
    pub max_response_bytes: usize,

    /// Request timeout in seconds.
    #[serde(default = "default_cimd_fetch_timeout_seconds")]
    pub fetch_timeout_seconds: u64,

    /// Floor for cache entry TTL in seconds.
    #[serde(default = "default_cimd_cache_min_seconds")]
    pub cache_min_seconds: u64,

    /// Ceiling for cache entry TTL in seconds.
    #[serde(default = "default_cimd_cache_max_seconds")]
    pub cache_max_seconds: u64,

    /// Upper bound on cache entries before LRU eviction kicks in.
    #[serde(default = "default_cimd_max_entries")]
    pub max_entries: usize,

    /// Whether loopback CIMD URLs are allowed (development only).
    #[serde(default)]
    pub allow_loopback_for_dev: bool,

    /// Hosts that bypass the private-range deny list.
    #[serde(default)]
    pub additional_allowlisted_hosts: Vec<String>,
}

const fn default_cimd_max_response_bytes() -> usize {
    DEFAULT_CIMD_MAX_RESPONSE_BYTES
}

const fn default_cimd_fetch_timeout_seconds() -> u64 {
    DEFAULT_CIMD_FETCH_TIMEOUT_SECONDS
}

const fn default_cimd_cache_min_seconds() -> u64 {
    DEFAULT_CIMD_CACHE_MIN_SECONDS
}

const fn default_cimd_cache_max_seconds() -> u64 {
    DEFAULT_CIMD_CACHE_MAX_SECONDS
}

const fn default_cimd_max_entries() -> usize {
    DEFAULT_CIMD_MAX_ENTRIES
}

impl Default for CimdConfig {
    fn default() -> Self {
        Self {
            max_response_bytes: DEFAULT_CIMD_MAX_RESPONSE_BYTES,
            fetch_timeout_seconds: DEFAULT_CIMD_FETCH_TIMEOUT_SECONDS,
            cache_min_seconds: DEFAULT_CIMD_CACHE_MIN_SECONDS,
            cache_max_seconds: DEFAULT_CIMD_CACHE_MAX_SECONDS,
            max_entries: DEFAULT_CIMD_MAX_ENTRIES,
            allow_loopback_for_dev: false,
            additional_allowlisted_hosts: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = OAuthConfig::default();
        assert_eq!(config.access_token_ttl_hours, DEFAULT_ACCESS_TOKEN_TTL_HOURS);
        assert_eq!(
            config.authorization_code_ttl_seconds,
            DEFAULT_AUTHORIZATION_CODE_TTL_SECONDS,
        );
        assert!(config.dcr_enabled);
        assert_eq!(config.cimd.max_response_bytes, DEFAULT_CIMD_MAX_RESPONSE_BYTES);
    }

    #[test]
    fn test_default_deserialises_from_empty_object() {
        let config: OAuthConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(config, OAuthConfig::default());
    }
}
