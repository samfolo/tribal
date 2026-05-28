//! Resolves the runtime OAuth configuration from the YAML-driven config
//! plus the server bind address.
//!
//! The OAuth surface needs canonical issuer and resource URLs. When the
//! operator has set them in `tribal.yaml`, those values win unchanged.
//! When unset, the values are derived from the server's bind address
//! and the MCP resource path, with the host transformed to a loopback
//! literal when the bind address itself is unspecified (`0.0.0.0` or
//! `[::]`) so the URLs land somewhere a client can actually reach.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tribal_auth::oauth::{OAuthRuntimeConfig, OAuthRuntimeConfigError};
use tribal_config::{DEFAULT_BIND_ADDRESS, TribalConfig};
use url::Url;

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Path appended to the issuer URL when deriving the resource URL.
const MCP_RESOURCE_PATH: &str = "/mcp";

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

/// Builds the runtime OAuth configuration from the loaded YAML config.
///
/// Falls back to a loopback-host URL derived from `server.bind_address`
/// for any URL the operator did not set explicitly. Wildcard bind
/// addresses (`0.0.0.0`, `[::]`) are rewritten to `127.0.0.1` so the
/// derived URL is reachable from the same machine.
///
/// # Errors
///
/// Returns [`AppError::ConfigInvariant`] when the operator-supplied
/// issuer or resource URL fails to parse.
pub fn resolve_oauth_runtime(config: &TribalConfig) -> Result<OAuthRuntimeConfig, AppError> {
    let bind_addr: SocketAddr = config
        .server
        .bind_address
        .as_deref()
        .unwrap_or(DEFAULT_BIND_ADDRESS)
        .parse()
        .expect("bind address validated during config validation");

    let (host, port) = canonical_host_and_port(bind_addr);
    let fallback_issuer = Url::parse(&format!("http://{host}:{port}"))
        .expect("loopback issuer URL is well-formed by construction");
    let fallback_resource = Url::parse(&format!("http://{host}:{port}{MCP_RESOURCE_PATH}"))
        .expect("loopback resource URL is well-formed by construction");

    OAuthRuntimeConfig::build(&config.oauth, &fallback_issuer, &fallback_resource).map_err(
        |source| match source {
            OAuthRuntimeConfigError::IssuerUrlMalformed { input } => AppError::ConfigInvariant {
                reason: format!("oauth.issuer_url is not a valid URL: {input:?}"),
            },
            OAuthRuntimeConfigError::ResourceUrlMalformed { input } => AppError::ConfigInvariant {
                reason: format!("oauth.resource_url is not a valid URL: {input:?}"),
            },
        },
    )
}

/// Translates a bind address to a host/port pair suitable for embedding
/// in a public URL.
///
/// Wildcard addresses (`0.0.0.0`, `[::]`) are rewritten to the IPv4
/// loopback literal `127.0.0.1` so the derived URL is reachable. All
/// other addresses are preserved verbatim.
fn canonical_host_and_port(addr: SocketAddr) -> (String, u16) {
    let host = match addr.ip() {
        IpAddr::V4(v4) if v4.is_unspecified() => Ipv4Addr::LOCALHOST.to_string(),
        IpAddr::V6(v6) if v6.is_unspecified() => Ipv4Addr::LOCALHOST.to_string(),
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("[{v6}]"),
    };
    (host, addr.port())
}

#[cfg(test)]
mod tests {
    use tribal_config::TribalConfig;

    use super::*;

    #[test]
    fn test_canonical_host_rewrites_v4_wildcard_to_loopback() {
        let addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
        let (host, port) = canonical_host_and_port(addr);
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 8080);
    }

    #[test]
    fn test_canonical_host_rewrites_v6_wildcard_to_loopback() {
        let addr: SocketAddr = "[::]:8080".parse().unwrap();
        let (host, _port) = canonical_host_and_port(addr);
        assert_eq!(host, "127.0.0.1");
    }

    #[test]
    fn test_canonical_host_preserves_specific_v4() {
        let addr: SocketAddr = "10.0.0.5:8080".parse().unwrap();
        let (host, _) = canonical_host_and_port(addr);
        assert_eq!(host, "10.0.0.5");
    }

    #[test]
    fn test_resolve_uses_bind_address_when_oauth_config_empty() {
        let mut config = TribalConfig::default();
        config.server.bind_address = Some("127.0.0.1:8725".to_owned());
        let runtime = resolve_oauth_runtime(&config).unwrap();
        assert_eq!(runtime.issuer_url.as_str(), "http://127.0.0.1:8725/");
        assert_eq!(runtime.resource_url.as_str(), "http://127.0.0.1:8725/mcp");
    }
}
