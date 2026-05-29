//! Redirect URI matching per RFC 8252 §7.3.
//!
//! Exact match on scheme, host, path, and query for all redirect URIs
//! except loopback HTTP, which allows any port. The widening to
//! `localhost` (alongside `127.0.0.1` and `[::1]`) is a deliberate
//! ecosystem-compatibility choice that follows the deployed reality
//! while satisfying the spec's intent.

use url::Url;

use crate::oauth::common::is_loopback_host;

/// Returns `true` when `incoming` is an acceptable match for the
/// `registered` redirect URI.
#[must_use]
pub fn matches_redirect_uri(registered: &Url, incoming: &Url) -> bool {
    if registered.scheme() != incoming.scheme() {
        return false;
    }
    if registered.host_str() != incoming.host_str() {
        return false;
    }
    if registered.path() != incoming.path() {
        return false;
    }
    if registered.query() != incoming.query() {
        return false;
    }
    if incoming.fragment().is_some() {
        return false;
    }

    let is_loopback =
        registered.scheme() == "http" && registered.host_str().is_some_and(is_loopback_host);

    if !is_loopback && registered.port_or_known_default() != incoming.port_or_known_default() {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).expect("test url parses")
    }

    #[test]
    fn test_loopback_http_accepts_any_port() {
        let registered = url("http://127.0.0.1/cb");
        let incoming = url("http://127.0.0.1:53076/cb");
        assert!(matches_redirect_uri(&registered, &incoming));
    }

    #[test]
    fn test_localhost_widening() {
        let registered = url("http://localhost/cb");
        let incoming = url("http://localhost:53076/cb");
        assert!(matches_redirect_uri(&registered, &incoming));
    }

    #[test]
    fn test_loopback_with_registered_port_still_accepts_other_port() {
        let registered = url("http://127.0.0.1:3118/cb");
        let incoming = url("http://127.0.0.1:53076/cb");
        assert!(matches_redirect_uri(&registered, &incoming));
    }

    #[test]
    fn test_ipv6_loopback_accepts_any_port() {
        // The IPv6 loopback literal gets the same any-port flexibility as
        // 127.0.0.1: `host_str` returns it bracketed, which the shared
        // loopback resolver unwraps.
        let registered = url("http://[::1]/cb");
        let incoming = url("http://[::1]:53076/cb");
        assert!(matches_redirect_uri(&registered, &incoming));
    }

    #[test]
    fn test_non_loopback_https_requires_exact_port() {
        let registered = url("https://example.com/cb");
        let incoming = url("https://example.com:8443/cb");
        assert!(!matches_redirect_uri(&registered, &incoming));
    }

    #[test]
    fn test_paths_must_match() {
        let registered = url("http://127.0.0.1/cb");
        let incoming = url("http://127.0.0.1/cb2");
        assert!(!matches_redirect_uri(&registered, &incoming));
    }

    #[test]
    fn test_fragment_rejected() {
        let registered = url("https://example.com/cb");
        let incoming = url("https://example.com/cb#frag");
        assert!(!matches_redirect_uri(&registered, &incoming));
    }

    #[test]
    fn test_query_must_match() {
        let registered = url("https://example.com/cb?x=1");
        let incoming = url("https://example.com/cb?x=2");
        assert!(!matches_redirect_uri(&registered, &incoming));
    }
}
