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

/// Canonicalises a resource URL for byte-equal audience comparison.
///
/// Lowercases scheme and host, removes any fragment, and trims a single
/// trailing slash from the path (a bare `/` is preserved as `""` to
/// keep the URL byte-stable across canonicalisations).
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
