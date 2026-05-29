//! OAuth 2.1 authorisation-server configuration.
//!
//! Plumbing for the OAuth flow: issuer and resource URLs and token
//! lifetimes. URLs are stored as strings here and parsed into
//! `url::Url` at consumer construction time, matching the existing
//! pattern for `server.bind_address`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

/// Default access-token TTL for OAuth-issued tokens in hours.
pub const DEFAULT_ACCESS_TOKEN_TTL_HOURS: u64 = 24;

/// Upper bound on the OAuth access-token TTL accepted by `validate()`,
/// in hours (30 days).
///
/// A security bound, distinct from the auth-token overflow ceiling: with
/// no refresh tokens, the access token is the whole session, so it needs
/// generous headroom over the 24-hour default, but a multi-year
/// audience-bound bearer is a footgun. 30 days also keeps the
/// `hours * 3600` seconds conversion far inside `u64`.
pub const MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS: u64 = 720;

/// Default authorisation-code TTL in seconds.
pub const DEFAULT_AUTHORIZATION_CODE_TTL_SECONDS: u64 = 600;

/// Upper bound on authorisation-code TTL accepted by `validate()`.
///
/// Matches the OAuth 2.1 §4.1.3 RECOMMENDED 10-minute upper bound.
pub const MAX_AUTHORIZATION_CODE_TTL_SECONDS: u64 = 600;

/// Lower bound on authorisation-code TTL accepted by `validate()`.
pub const MIN_AUTHORIZATION_CODE_TTL_SECONDS: u64 = 60;

// ---------------------------------------------------------------------------
// Host derivation
// ---------------------------------------------------------------------------

/// The host a bind address advertises in an OAuth-derived URL.
///
/// A wildcard bind (`0.0.0.0` or `[::]`) collapses to the IPv4 loopback so
/// a URL derived from it resolves to a reachable address; every other
/// address is returned verbatim.
#[must_use]
pub fn advertised_oauth_host(addr: SocketAddr) -> IpAddr {
    match addr.ip() {
        IpAddr::V4(v4) if v4.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(v6) if v6.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        other => other,
    }
}

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

    #[test]
    fn test_advertised_host_collapses_v4_wildcard_to_loopback() {
        let host = advertised_oauth_host("0.0.0.0:8725".parse().unwrap());
        assert_eq!(host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(host.is_loopback());
    }

    #[test]
    fn test_advertised_host_collapses_v6_wildcard_to_loopback() {
        let host = advertised_oauth_host("[::]:8725".parse().unwrap());
        assert_eq!(host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(host.is_loopback());
    }

    #[test]
    fn test_advertised_host_preserves_routable_address() {
        let host = advertised_oauth_host("10.0.0.5:8725".parse().unwrap());
        assert_eq!(host, "10.0.0.5".parse::<IpAddr>().unwrap());
        assert!(!host.is_loopback());
    }

    #[test]
    fn test_advertised_host_preserves_loopback_address() {
        let host = advertised_oauth_host("127.0.0.1:8725".parse().unwrap());
        assert!(host.is_loopback());
    }
}
