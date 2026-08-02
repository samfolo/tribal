//! Shared low-level constants and helpers for the authorisation-server
//! surface.

use std::net::Ipv6Addr;

/// Sole supported `response_type` (authorisation code flow).
pub(crate) const RESPONSE_TYPE_CODE: &str = "code";

/// Sole supported `grant_type` (no refresh tokens).
pub(crate) const GRANT_TYPE_AUTHORIZATION_CODE: &str = "authorization_code";

/// Loopback host literals.
///
/// `127.0.0.1` and `::1` are the RFC 8252 the cluster contract.3 loopback IP literals;
/// `localhost` is included for ecosystem interoperability (many native
/// MCP clients send it). The redirect-URI matcher, the DCR redirect-URI
/// validator, and the consent-page loopback warning all resolve against
/// this single definition, via [`is_loopback_host`], so they cannot
/// drift apart.
const LOOPBACK_HOSTS: &[&str] = &["127.0.0.1", "::1", "localhost"];

/// Returns `true` when `host` is one of the [`LOOPBACK_HOSTS`] literals.
///
/// `Url::host_str` serialises an IPv6 literal in brackets (`[::1]`). The
/// brackets are unwrapped before matching, but only when their contents
/// parse as an IPv6 address, so `[::1]` resolves like `::1` while a
/// bracketed IPv4 or name (`[127.0.0.1]`, `[localhost]` — both malformed
/// authorities that URL parsing rejects upstream) is not silently
/// accepted. This is the single resolution point shared by the
/// redirect-URI matcher, the DCR redirect-URI validator, and the
/// consent-page loopback warning.
pub(crate) fn is_loopback_host(host: &str) -> bool {
    let candidate = host
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .filter(|inner| inner.parse::<Ipv6Addr>().is_ok())
        .unwrap_or(host);
    LOOPBACK_HOSTS.contains(&candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_loopback_host_matches_literals() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("::1"));
    }

    #[test]
    fn test_is_loopback_host_unwraps_ipv6_brackets() {
        assert!(is_loopback_host("[::1]"));
    }

    #[test]
    fn test_is_loopback_host_rejects_bracketed_non_ipv6() {
        // Brackets wrap IPv6 literals only; a bracketed IPv4 or name is a
        // malformed authority (rejected by URL parsing upstream) and must
        // not be accepted as loopback if it reaches the helper directly.
        assert!(!is_loopback_host("[127.0.0.1]"));
        assert!(!is_loopback_host("[localhost]"));
    }

    #[test]
    fn test_is_loopback_host_rejects_non_loopback() {
        assert!(!is_loopback_host("example.com"));
        assert!(!is_loopback_host("10.0.0.1"));
    }
}
