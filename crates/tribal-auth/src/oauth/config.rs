//! Runtime OAuth configuration with parsed and canonicalised URLs.
//!
//! Bridges the YAML-driven [`tribal_config::OAuthConfig`] (primitive
//! types) into the strongly-typed runtime values the OAuth handlers
//! consume. URL parsing and canonicalisation happens here so the
//! handlers never re-parse strings.

use std::time::Duration;

use tribal_config::OAuthConfig;
use url::Url;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Failure constructing an [`OAuthRuntimeConfig`].
#[derive(Debug, thiserror::Error)]
pub enum OAuthRuntimeConfigError {
    /// `oauth.issuer_url` was set but failed to parse as a URL.
    #[error("oauth.issuer_url is malformed: {input:?}")]
    IssuerUrlMalformed {
        /// The raw input that failed to parse.
        input: String,
    },
    /// `oauth.resource_url` was set but failed to parse as a URL.
    #[error("oauth.resource_url is malformed: {input:?}")]
    ResourceUrlMalformed {
        /// The raw input that failed to parse.
        input: String,
    },
}

// ---------------------------------------------------------------------------
// OAuthRuntimeConfig
// ---------------------------------------------------------------------------

/// Runtime OAuth configuration with parsed URLs and durations.
#[derive(Debug, Clone)]
pub struct OAuthRuntimeConfig {
    /// Canonical authorisation-server issuer URL.
    pub issuer_url: Url,
    /// Canonical protected-resource URL (bearer audience).
    pub resource_url: Url,
    /// Canonicalised resource URL as a string for byte-equal compares.
    pub canonical_resource: String,
    /// Access-token TTL for OAuth-issued tokens.
    pub access_token_ttl: Duration,
    /// Authorisation-code TTL for OAuth flow codes.
    pub authorization_code_ttl: Duration,
    /// Whether the dynamic client registration `/register` endpoint is
    /// enabled.
    pub dcr_enabled: bool,
}

impl OAuthRuntimeConfig {
    /// Builds a runtime config from the parsed YAML section.
    ///
    /// `fallback_issuer` is used when `config.issuer_url` is `None` and
    /// `fallback_resource` is used when `config.resource_url` is `None`.
    /// Callers supply these from the server's bind address at startup.
    ///
    /// # Errors
    ///
    /// Returns [`OAuthRuntimeConfigError`] if either URL fails to parse.
    pub fn build(
        config: &OAuthConfig,
        fallback_issuer: &Url,
        fallback_resource: &Url,
    ) -> Result<Self, OAuthRuntimeConfigError> {
        let issuer_url = parse_optional_url(config.issuer_url.as_deref(), fallback_issuer)
            .map_err(|input| OAuthRuntimeConfigError::IssuerUrlMalformed { input })?;
        let resource_url = parse_optional_url(config.resource_url.as_deref(), fallback_resource)
            .map_err(|input| OAuthRuntimeConfigError::ResourceUrlMalformed { input })?;

        Ok(Self {
            canonical_resource: canonicalise_resource_url(&resource_url),
            issuer_url,
            resource_url,
            access_token_ttl: Duration::from_secs(config.access_token_ttl_hours * 3_600),
            authorization_code_ttl: Duration::from_secs(config.authorization_code_ttl_seconds),
            dcr_enabled: config.dcr_enabled,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_optional_url(maybe: Option<&str>, fallback: &Url) -> Result<Url, String> {
    match maybe {
        Some(raw) if !raw.is_empty() => Url::parse(raw).map_err(|_| raw.to_owned()),
        _ => Ok(fallback.clone()),
    }
}

/// Canonicalises a resource URL into the byte-exact form used for RFC
/// 8707 audience equality.
///
/// The output is compared byte-for-byte against the canonicalised
/// `resource` indicator a client sends and against the audience recorded
/// on a minted token, so its rules are fixed by what a compliant client
/// transmits, not by convenience:
///
/// - lowercases scheme and host (both case-insensitive per RFC 3986);
/// - removes any fragment (never part of a resource identifier);
/// - trims a single trailing slash so `.../mcp` and `.../mcp/` agree,
///   while a bare `/` collapses to `""` to stay byte-stable across
///   repeated canonicalisation;
/// - does NOT add an explicit default port, because clients send
///   `https://host/mcp`, not `https://host:443/mcp`; and
/// - retains the query string, because a resource indicator may carry
///   one and byte-equality is the contract (Tribal's resource never
///   does).
///
/// This deliberately differs from the registry-key normalisation used
/// to dedupe provider endpoints, which adds explicit ports and drops the
/// query: that answers an identity-of-endpoint question, whereas this
/// answers a security-load-bearing match against exactly what the client
/// transmits.
#[must_use]
pub fn canonicalise_resource_url(url: &Url) -> String {
    let mut clone = url.clone();
    clone.set_fragment(None);
    let scheme = clone.scheme().to_ascii_lowercase();
    let _ = clone.set_scheme(&scheme);
    let host = clone.host_str().map(str::to_ascii_lowercase);
    if let Some(h) = host {
        let _ = clone.set_host(Some(&h));
    }
    let mut s: String = clone.into();
    if s.ends_with('/') && !s.ends_with("//") {
        s.pop();
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).expect("test url parses")
    }

    #[test]
    fn test_canonicalise_lowercases_scheme_and_host() {
        let canonical = canonicalise_resource_url(&url("HTTPS://Example.COM/Mcp"));
        assert_eq!(canonical, "https://example.com/Mcp");
    }

    #[test]
    fn test_canonicalise_trims_single_trailing_slash() {
        let canonical = canonicalise_resource_url(&url("https://example.com/mcp/"));
        assert_eq!(canonical, "https://example.com/mcp");
    }

    #[test]
    fn test_canonicalise_preserves_root_authority() {
        let canonical = canonicalise_resource_url(&url("https://example.com/"));
        assert_eq!(canonical, "https://example.com");
    }

    #[test]
    fn test_canonicalise_drops_fragment() {
        let canonical = canonicalise_resource_url(&url("https://example.com/mcp#frag"));
        assert_eq!(canonical, "https://example.com/mcp");
    }

    // -- Pinned canonical forms for the resource shapes Tribal serves -------
    // These lock the byte-exact audience form. A change here is a change to
    // what every minted token's audience must equal, so it must be
    // deliberate (see Workstream F of the hardening round).

    #[test]
    fn test_canonicalise_pins_bare_host() {
        assert_eq!(
            canonicalise_resource_url(&url("https://example.com")),
            "https://example.com",
        );
    }

    #[test]
    fn test_canonicalise_pins_host_port() {
        assert_eq!(
            canonicalise_resource_url(&url("http://127.0.0.1:8080")),
            "http://127.0.0.1:8080",
        );
    }

    #[test]
    fn test_canonicalise_pins_host_path() {
        assert_eq!(
            canonicalise_resource_url(&url("http://127.0.0.1:8080/mcp")),
            "http://127.0.0.1:8080/mcp",
        );
    }

    #[test]
    fn test_canonicalise_does_not_add_explicit_default_port() {
        // A compliant client sends the audience without :443; adding one
        // would make every token's audience fail to match.
        assert_eq!(
            canonicalise_resource_url(&url("https://example.com/mcp")),
            "https://example.com/mcp",
        );
    }

    #[test]
    fn test_canonicalise_retains_query() {
        assert_eq!(
            canonicalise_resource_url(&url("https://example.com/mcp?tenant=a")),
            "https://example.com/mcp?tenant=a",
        );
    }

    #[test]
    fn test_canonicalise_pins_ipv6_literal() {
        assert_eq!(
            canonicalise_resource_url(&url("http://[::1]:8080/mcp")),
            "http://[::1]:8080/mcp",
        );
    }

    #[test]
    fn test_build_uses_fallbacks_when_unset() {
        let config = OAuthConfig {
            issuer_url: None,
            resource_url: None,
            ..Default::default()
        };
        let issuer = url("http://127.0.0.1:8080");
        let resource = url("http://127.0.0.1:8080/mcp");
        let runtime = OAuthRuntimeConfig::build(&config, &issuer, &resource).unwrap();
        assert_eq!(runtime.issuer_url, issuer);
        assert_eq!(runtime.resource_url, resource);
        assert_eq!(runtime.canonical_resource, "http://127.0.0.1:8080/mcp");
    }

    #[test]
    fn test_build_rejects_malformed_issuer_url() {
        let config = OAuthConfig {
            issuer_url: Some("not a url".to_owned()),
            ..Default::default()
        };
        let issuer = url("http://127.0.0.1:8080");
        let resource = url("http://127.0.0.1:8080/mcp");
        let err = OAuthRuntimeConfig::build(&config, &issuer, &resource).unwrap_err();
        assert!(matches!(
            err,
            OAuthRuntimeConfigError::IssuerUrlMalformed { .. }
        ));
    }
}
