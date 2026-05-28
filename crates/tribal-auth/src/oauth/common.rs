//! Shared low-level constants for the authorisation-server surface.

/// Sole supported `response_type` (authorisation code flow).
pub(crate) const RESPONSE_TYPE_CODE: &str = "code";

/// Sole supported `grant_type` (no refresh tokens).
pub(crate) const GRANT_TYPE_AUTHORIZATION_CODE: &str = "authorization_code";

/// Sole supported PKCE `code_challenge_method` (OAuth 2.1 forbids plain).
pub(crate) const CODE_CHALLENGE_METHOD_S256: &str = "S256";

/// Loopback host literals.
///
/// `127.0.0.1` and `::1` are the RFC 8252 §7.3 loopback IP literals;
/// `localhost` is included for ecosystem interoperability (many native
/// MCP clients send it). The redirect-URI matcher, the DCR redirect-URI
/// validator, and the consent-page loopback warning all resolve against
/// this single definition so they cannot drift apart.
pub(crate) const LOOPBACK_HOSTS: &[&str] = &["127.0.0.1", "::1", "localhost"];
