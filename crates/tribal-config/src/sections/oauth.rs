//! OAuth 2.1 authorisation-server configuration.
//!
//! Plumbing for the OAuth flow: issuer and resource URLs and token
//! lifetimes. URLs are stored as strings here and parsed into
//! `url::Url` at consumer construction time, matching the existing
//! pattern for `server.bind_address`.

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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = OAuthConfig::default();
        assert_eq!(
            config.access_token_ttl_hours,
            DEFAULT_ACCESS_TOKEN_TTL_HOURS
        );
        assert_eq!(
            config.authorization_code_ttl_seconds,
            DEFAULT_AUTHORIZATION_CODE_TTL_SECONDS,
        );
        assert!(config.dcr_enabled);
    }

    #[test]
    fn test_default_deserialises_from_empty_object() {
        let config: OAuthConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(config, OAuthConfig::default());
    }
}
