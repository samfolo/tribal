//! Shared low-level constants for the authorisation-server surface.

/// Loopback host literals.
///
/// `127.0.0.1` and `::1` are the RFC 8252 §7.3 loopback IP literals;
/// `localhost` is included for ecosystem interoperability (many native
/// MCP clients send it). The redirect-URI matcher, the DCR redirect-URI
/// validator, and the consent-page loopback warning all resolve against
/// this single definition so they cannot drift apart.
pub(crate) const LOOPBACK_HOSTS: &[&str] = &["127.0.0.1", "::1", "localhost"];
