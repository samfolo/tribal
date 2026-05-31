//! Canonicalisation of provider endpoint URLs.
//!
//! [`normalise_endpoint_url`] answers an identity-of-endpoint question (do two
//! base URLs name the same provider endpoint?) by parsing the URL, lower-casing
//! the scheme and host, adding the explicit default port, retaining the path
//! without a trailing slash, and dropping the query and fragment. Folding
//! equivalent spellings together lets the provider registry share one semaphore
//! per endpoint and lets the credentials catalogue key resolution on a stable
//! string.
//!
//! It is intentionally not an RFC 8707 audience canonicalisation, which keeps
//! the query and omits the explicit port to match a client's bytes exactly.

use url::{Host, Url};

/// A base URL could not be canonicalised into an endpoint identity.
#[derive(Debug, thiserror::Error)]
#[error("unparseable endpoint URL {url:?}: {reason}")]
pub struct EndpointUrlError {
    /// The URL string that failed to parse or canonicalise.
    pub url: String,
    /// A description of why it failed.
    pub reason: String,
}

/// Normalises a provider base URL into a stable endpoint identity.
///
/// Steps:
/// 1. Parse with [`url::Url`]
/// 2. Lower-case scheme and host (handled by the `url` crate)
/// 3. Include the explicit port (default 80 for HTTP, 443 for HTTPS)
/// 4. Retain the path, stripped of a trailing slash
/// 5. Drop the query string and fragment
///
/// # Errors
///
/// Returns [`EndpointUrlError`] when `raw` is not a valid URL or lacks a host
/// or a port derivable from its scheme.
pub fn normalise_endpoint_url(raw: &str) -> Result<String, EndpointUrlError> {
    let parsed = Url::parse(raw).map_err(|e| EndpointUrlError {
        url: raw.to_owned(),
        reason: e.to_string(),
    })?;

    let scheme = parsed.scheme();

    let host = parsed.host().ok_or_else(|| EndpointUrlError {
        url: raw.to_owned(),
        reason: "missing host".to_owned(),
    })?;

    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| EndpointUrlError {
            url: raw.to_owned(),
            reason: "unknown port for scheme".to_owned(),
        })?;

    let path = parsed.path().trim_end_matches('/');

    // `Host::Display` formats IPv6 with brackets (e.g. `[::1]`); domains and
    // IPv4 addresses pass through unchanged.
    match host {
        Host::Ipv6(addr) => Ok(format!("{scheme}://[{addr}]:{port}{path}")),
        _ => Ok(format!("{scheme}://{host}:{port}{path}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strips_trailing_slash() {
        assert_eq!(
            normalise_endpoint_url("http://localhost:11434/").unwrap(),
            "http://localhost:11434",
        );
    }

    #[test]
    fn test_lowercases_scheme_and_host_and_adds_https_port() {
        assert_eq!(
            normalise_endpoint_url("HTTPS://API.OpenAI.com/v1").unwrap(),
            "https://api.openai.com:443/v1",
        );
    }

    #[test]
    fn test_includes_default_http_port() {
        assert_eq!(
            normalise_endpoint_url("http://localhost").unwrap(),
            "http://localhost:80",
        );
    }

    #[test]
    fn test_retains_explicit_port() {
        assert_eq!(
            normalise_endpoint_url("http://localhost:11434").unwrap(),
            "http://localhost:11434",
        );
    }

    #[test]
    fn test_drops_query_and_fragment() {
        assert_eq!(
            normalise_endpoint_url("http://localhost:11434/api?key=value#frag").unwrap(),
            "http://localhost:11434/api",
        );
    }

    #[test]
    fn test_ipv6_host_brackets_preserved() {
        assert_eq!(
            normalise_endpoint_url("http://[::1]:11434/api").unwrap(),
            "http://[::1]:11434/api",
        );
    }

    #[test]
    fn test_distinguishes_different_paths() {
        let a = normalise_endpoint_url("http://localhost:11434/api/v1").unwrap();
        let b = normalise_endpoint_url("http://localhost:11434/api/v2").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn test_unparseable_returns_error() {
        let err = normalise_endpoint_url("not a url").unwrap_err();
        assert_eq!(err.url, "not a url");
        assert!(!err.reason.is_empty());
    }

    #[test]
    fn test_missing_host_returns_error() {
        let err = normalise_endpoint_url("mailto:someone@example.com").unwrap_err();
        assert_eq!(err.reason, "missing host");
    }
}
