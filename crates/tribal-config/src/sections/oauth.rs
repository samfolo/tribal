//! OAuth 2.1 authorisation-server configuration.
//!
//! Plumbing for the OAuth flow: issuer and resource URLs and token
//! lifetimes. URLs are stored as strings here and parsed into
//! `url::Url` at consumer construction time, matching the existing
//! pattern for `server.bind_address`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::{Deserialize, Serialize};
use tribal_domain::TransportKind;
use url::{Host, Url};

use super::{root::TribalConfig, server::DEFAULT_BIND_ADDRESS};

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
// Surface routability
// ---------------------------------------------------------------------------

/// Returns `true` when the OAuth surface is reachable beyond loopback, the
/// signal that DCR's unauthenticated `/register` would be exposed to remote
/// clients.
///
/// An explicit advertised URL (`server.public_mcp_url`, `oauth.issuer_url`,
/// or `oauth.resource_url`) is the operator's authoritative statement of
/// where clients reach the server: when any is set it alone decides
/// routability, and the bind is not consulted. A loopback advertised URL is
/// therefore the trusted-exposure override that keeps a wildcard-bound
/// server (the Docker host-port-mapping shape) classified as loopback.
///
/// With no advertised URL the bind decides, and a wildcard bind
/// (`0.0.0.0` / `[::]`) is treated as routable rather than collapsed to
/// loopback: from inside the process a public wildcard bind is
/// indistinguishable from one mapped to host loopback, so DCR fails closed
/// unless the operator names an explicit loopback advertised URL.
///
/// Pure over `&TribalConfig`: the advertised URL is resolved into config at
/// load, so every caller shares one judgement that cannot diverge across
/// them, nor depend on the ambient environment.
#[must_use]
pub fn oauth_surface_is_routable(config: &TribalConfig) -> bool {
    let advertised = [
        config.server.public_mcp_url.as_deref(),
        config.oauth.issuer_url.as_deref(),
        config.oauth.resource_url.as_deref(),
    ];

    // An explicit advertised URL is authoritative: when any is set, the
    // surface is routable iff any is non-loopback, and the bind is ignored
    // (a loopback advertised URL is the trusted-exposure override).
    if advertised.iter().any(|url| is_nonempty(*url)) {
        return advertised
            .iter()
            .any(|url| url_is_explicit_non_loopback(*url));
    }

    // No advertised URL: classify from the bind, treating a wildcard or
    // unparseable bind as routable (fail closed).
    bind_is_routable(
        config
            .server
            .bind_address
            .as_deref()
            .unwrap_or(DEFAULT_BIND_ADDRESS),
    )
}

/// Returns `true` when the onboarding hand-off should advertise the URL-only
/// OAuth flow rather than embed a static bearer: only on a loopback surface
/// with DCR enabled, where a fresh harness can register itself on first
/// connect. Every other case (routable, or DCR disabled) needs the static
/// token, so the snippet embeds it and the readiness check requires it.
/// Shared by bootstrap, `mcp-config`, and `valid_token_exists` so the three
/// never disagree.
#[must_use]
pub fn oauth_onboarding_is_url_only(config: &TribalConfig) -> bool {
    matches!(
        client_registration_mode(config),
        ClientRegistrationMode::Automatic
    )
}

/// Whether the configured transport can safely expose automatic client registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientRegistrationMode {
    Automatic,
    NoNetworkTransport,
    RoutableOauthSurface,
}

/// Classifies automatic client registration from transport and routability alone.
#[must_use]
pub fn client_registration_mode(config: &TribalConfig) -> ClientRegistrationMode {
    match config.server.transport {
        TransportKind::Stdio => ClientRegistrationMode::NoNetworkTransport,
        TransportKind::Http | TransportKind::Sse if oauth_surface_is_routable(config) => {
            ClientRegistrationMode::RoutableOauthSurface
        }
        TransportKind::Http | TransportKind::Sse => ClientRegistrationMode::Automatic,
    }
}

/// Whether `value` is a present, non-empty string.
fn is_nonempty(value: Option<&str>) -> bool {
    value.is_some_and(|raw| !raw.is_empty())
}

/// Whether a bind address exposes the listener beyond loopback. A loopback
/// IP is not routable; a routable IP, a wildcard bind (`0.0.0.0` / `[::]`),
/// or an unparseable value all are (fail closed). Distinct from
/// [`advertised_oauth_host`], which collapses a wildcard to loopback for
/// client-facing URL derivation: this is the listener-exposure judgement,
/// where a wildcard must not be assumed loopback-only.
fn bind_is_routable(bind: &str) -> bool {
    match bind.parse::<SocketAddr>() {
        Ok(addr) => !addr.ip().is_loopback(),
        Err(_) => true,
    }
}

/// Returns `true` when `value` advertises the OAuth surface to a
/// non-loopback host, the signal that DCR's `/register` would be reachable
/// by remote clients.
///
/// Fails closed (returns `true`) whenever loopback safety cannot be
/// established: an unparseable value, or a parseable one with no host (a
/// `mailto:`/`file:` style URL). Load-time validation rejects such values
/// first, so this is the defence-in-depth guard for callers that do not
/// validate (e.g. `mcp-config`); it must never let a value it cannot reason
/// about reopen open registration.
fn url_is_explicit_non_loopback(value: Option<&str>) -> bool {
    let Some(raw) = value.filter(|raw| !raw.is_empty()) else {
        return false;
    };
    let Ok(url) = Url::parse(raw) else {
        return true;
    };
    match url.host() {
        Some(Host::Ipv4(ip)) => !ip.is_loopback(),
        Some(Host::Ipv6(ip)) => !ip.is_loopback(),
        Some(Host::Domain(domain)) => domain != "localhost",
        None => true,
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
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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
}

const fn default_access_token_ttl_hours() -> u64 {
    DEFAULT_ACCESS_TOKEN_TTL_HOURS
}

const fn default_authorization_code_ttl_seconds() -> u64 {
    DEFAULT_AUTHORIZATION_CODE_TTL_SECONDS
}

impl Default for OAuthConfig {
    fn default() -> Self {
        Self {
            issuer_url: None,
            resource_url: None,
            access_token_ttl_hours: DEFAULT_ACCESS_TOKEN_TTL_HOURS,
            authorization_code_ttl_seconds: DEFAULT_AUTHORIZATION_CODE_TTL_SECONDS,
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
