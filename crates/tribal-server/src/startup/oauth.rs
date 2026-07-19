//! Resolves the runtime OAuth configuration from the YAML-driven config
//! plus the server bind address.
//!
//! The OAuth surface needs canonical issuer and resource URLs. When the
//! operator has set them in `tribal.yaml`, those values win unchanged.
//! When unset, the values are derived from the server's bind address
//! and the MCP resource path, with the host transformed to a loopback
//! literal when the bind address itself is unspecified (`0.0.0.0` or
//! `[::]`) so the URLs land somewhere a client can actually reach.

use std::net::{IpAddr, SocketAddr};

use tribal_auth::oauth::{OAuthRuntimeConfig, OAuthRuntimeConfigError};
use tribal_config::{DEFAULT_BIND_ADDRESS, TribalConfig, advertised_oauth_host};
use tribal_domain::TransportKind;
use tribal_wire::management::{NetworkTransport, RuntimeDataPlane};
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
    let (host, port) = canonical_host_and_port(bound_socket_address(config));
    let fallback_issuer = Url::parse(&format!("http://{host}:{port}"))
        .expect("loopback issuer URL is well-formed by construction");
    let fallback_resource = Url::parse(&format!("http://{host}:{port}{MCP_RESOURCE_PATH}"))
        .expect("loopback resource URL is well-formed by construction");

    let dcr_enabled = matches!(
        tribal_config::client_registration_mode(config),
        tribal_config::ClientRegistrationMode::Automatic,
    );
    OAuthRuntimeConfig::build(
        &config.oauth,
        &fallback_issuer,
        &fallback_resource,
        dcr_enabled,
    )
    .map_err(|source| match source {
        OAuthRuntimeConfigError::IssuerUrlMalformed { input } => AppError::ConfigInvariant {
            reason: format!("oauth.issuer_url is not a valid URL: {input:?}"),
        },
        OAuthRuntimeConfigError::ResourceUrlMalformed { input } => AppError::ConfigInvariant {
            reason: format!("oauth.resource_url is not a valid URL: {input:?}"),
        },
    })
}

/// The bearer-token audience a token must carry to be accepted under
/// `transport`.
///
/// Network transports bind the canonical resource (RFC 8707 audience
/// restriction); stdio carries no audience. This is the single
/// definition the network auth middleware and the CLI validation paths
/// (`tribal check`, `tribal project register`) all resolve against, so
/// what the live server enforces and what the validators assert cannot
/// drift apart.
#[must_use]
pub fn expected_token_audience(
    transport: TransportKind,
    runtime: &OAuthRuntimeConfig,
) -> Option<String> {
    match transport {
        TransportKind::Http | TransportKind::Sse => Some(runtime.canonical_resource.clone()),
        TransportKind::Stdio => None,
    }
}

/// The data plane a managed runtime reports for its loaded
/// configuration: the bind-derived MCP URL and the audience its
/// authenticator enforces. `None` for stdio, which has no network
/// data plane.
#[must_use]
pub fn runtime_data_plane(
    config: &TribalConfig,
    runtime: &OAuthRuntimeConfig,
) -> Option<RuntimeDataPlane> {
    let transport = match config.server.transport {
        TransportKind::Http => NetworkTransport::Http,
        TransportKind::Sse => NetworkTransport::Sse,
        TransportKind::Stdio => return None,
    };
    let (host, port) = canonical_host_and_port(bound_socket_address(config));
    Some(RuntimeDataPlane {
        transport,
        mcp_url: format!("http://{host}:{port}{MCP_RESOURCE_PATH}"),
        canonical_resource: runtime.canonical_resource.clone(),
    })
}

/// The socket address the transport runner binds.
pub(crate) fn bound_socket_address(config: &TribalConfig) -> SocketAddr {
    config
        .server
        .bind_address
        .as_deref()
        .unwrap_or(DEFAULT_BIND_ADDRESS)
        .parse()
        .expect("bind address validated during config validation")
}

/// Formats the host [`advertised_oauth_host`] derives for `addr`, with the
/// port, for embedding in a URL: an IPv6 host is wrapped in brackets.
fn canonical_host_and_port(addr: SocketAddr) -> (String, u16) {
    let host = match advertised_oauth_host(addr) {
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
