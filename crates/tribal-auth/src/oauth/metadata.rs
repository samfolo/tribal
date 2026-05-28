//! Authorisation server and protected-resource metadata documents.
//!
//! Both documents are served as 200 OK `application/json` bodies. The
//! protected-resource metadata document follows RFC 9728; the
//! authorisation-server metadata document follows RFC 8414 extended
//! with MCP 2025-11-25's `client_id_metadata_document_supported`
//! flag.

use serde::{Deserialize, Serialize};
use url::Url;

use crate::oauth::config::OAuthRuntimeConfig;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Catalogue of permission scopes advertised in metadata documents.
pub const SCOPES_CATALOGUE: &[&str] = &[
    "tribal:read",
    "tribal:write",
    "tribal.knowledge:read",
    "tribal.knowledge:write",
    "tribal.jobs:read",
    "tribal.jobs:write",
];

/// Sole supported code-challenge method for OAuth 2.1.
pub const CODE_CHALLENGE_METHOD_S256: &str = "S256";

/// Sole supported response type.
pub const RESPONSE_TYPE_CODE: &str = "code";

/// Sole supported grant type advertised in v1 (no refresh tokens).
pub const GRANT_TYPE_AUTHORIZATION_CODE: &str = "authorization_code";

/// Token endpoint auth method advertised for CIMD public clients.
pub const AUTH_METHOD_NONE: &str = "none";

/// Token endpoint auth method advertised for DCR confidential clients.
pub const AUTH_METHOD_CLIENT_SECRET_BASIC: &str = "client_secret_basic";

/// Bearer methods supported by the MCP transport.
pub const BEARER_METHOD_HEADER: &str = "header";

/// Path of the protected-resource metadata document on the root form.
pub const PATH_PROTECTED_RESOURCE_METADATA: &str = "/.well-known/oauth-protected-resource";

/// Path of the authorisation-server metadata document.
pub const PATH_AUTHORIZATION_SERVER_METADATA: &str = "/.well-known/oauth-authorization-server";

/// Path of the authorisation endpoint.
pub const PATH_AUTHORIZE: &str = "/authorize";

/// Path of the token endpoint.
pub const PATH_TOKEN: &str = "/token";

/// Path of the DCR registration endpoint.
pub const PATH_REGISTER: &str = "/register";

// ---------------------------------------------------------------------------
// ProtectedResourceMetadata
// ---------------------------------------------------------------------------

/// RFC 9728 Protected Resource Metadata document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtectedResourceMetadata {
    /// Canonical protected-resource identifier.
    pub resource: String,
    /// Authorisation server issuer URLs.
    pub authorization_servers: Vec<String>,
    /// Scopes recognised by the resource.
    pub scopes_supported: Vec<String>,
    /// Bearer transport methods accepted (header only).
    pub bearer_methods_supported: Vec<String>,
}

/// Builds the protected-resource metadata document for the runtime config.
#[must_use]
pub fn protected_resource_metadata(runtime: &OAuthRuntimeConfig) -> ProtectedResourceMetadata {
    ProtectedResourceMetadata {
        resource: runtime.canonical_resource.clone(),
        authorization_servers: vec![runtime.issuer_url.to_string()],
        scopes_supported: SCOPES_CATALOGUE.iter().map(|s| (*s).to_owned()).collect(),
        bearer_methods_supported: vec![BEARER_METHOD_HEADER.to_owned()],
    }
}

// ---------------------------------------------------------------------------
// AuthorizationServerMetadata
// ---------------------------------------------------------------------------

/// RFC 8414 Authorization Server Metadata document, extended with the
/// MCP 2025-11-25 CIMD flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationServerMetadata {
    /// Canonical authorisation-server issuer URL.
    pub issuer: String,
    /// Authorisation endpoint URL.
    pub authorization_endpoint: String,
    /// Token endpoint URL.
    pub token_endpoint: String,
    /// DCR registration endpoint URL. Omitted when DCR is disabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registration_endpoint: Option<String>,
    /// Supported response types.
    pub response_types_supported: Vec<String>,
    /// Supported grant types.
    pub grant_types_supported: Vec<String>,
    /// Supported PKCE methods (S256 only).
    pub code_challenge_methods_supported: Vec<String>,
    /// Token endpoint auth methods supported.
    pub token_endpoint_auth_methods_supported: Vec<String>,
    /// Whether CIMD discovery is supported.
    pub client_id_metadata_document_supported: bool,
    /// Catalogue of supported scopes.
    pub scopes_supported: Vec<String>,
}

/// Builds the authorisation-server metadata document for the runtime config.
#[must_use]
pub fn authorization_server_metadata(runtime: &OAuthRuntimeConfig) -> AuthorizationServerMetadata {
    let token_endpoint_auth_methods_supported = if runtime.dcr_enabled {
        vec![
            AUTH_METHOD_NONE.to_owned(),
            AUTH_METHOD_CLIENT_SECRET_BASIC.to_owned(),
        ]
    } else {
        vec![AUTH_METHOD_NONE.to_owned()]
    };

    AuthorizationServerMetadata {
        issuer: runtime.issuer_url.to_string(),
        authorization_endpoint: join_issuer(&runtime.issuer_url, PATH_AUTHORIZE),
        token_endpoint: join_issuer(&runtime.issuer_url, PATH_TOKEN),
        registration_endpoint: runtime
            .dcr_enabled
            .then(|| join_issuer(&runtime.issuer_url, PATH_REGISTER)),
        response_types_supported: vec![RESPONSE_TYPE_CODE.to_owned()],
        grant_types_supported: vec![GRANT_TYPE_AUTHORIZATION_CODE.to_owned()],
        code_challenge_methods_supported: vec![CODE_CHALLENGE_METHOD_S256.to_owned()],
        token_endpoint_auth_methods_supported,
        client_id_metadata_document_supported: true,
        scopes_supported: SCOPES_CATALOGUE.iter().map(|s| (*s).to_owned()).collect(),
    }
}

/// Joins a path onto the issuer URL.
///
/// Strips a single trailing slash on the issuer (RFC 8414 §3.1 rule)
/// before appending.
fn join_issuer(issuer: &Url, path: &str) -> String {
    let mut base = issuer.to_string();
    if base.ends_with('/') {
        base.pop();
    }
    let mut out = String::with_capacity(base.len() + path.len());
    out.push_str(&base);
    out.push_str(path);
    out
}

#[cfg(test)]
mod tests {
    use tribal_config::OAuthConfig;

    use super::*;

    fn runtime() -> OAuthRuntimeConfig {
        let issuer = Url::parse("http://127.0.0.1:8080").unwrap();
        let resource = Url::parse("http://127.0.0.1:8080/mcp").unwrap();
        OAuthRuntimeConfig::build(&OAuthConfig::default(), &issuer, &resource).unwrap()
    }

    #[test]
    fn test_protected_resource_metadata_carries_canonical_resource() {
        let doc = protected_resource_metadata(&runtime());
        assert_eq!(doc.resource, "http://127.0.0.1:8080/mcp");
        assert_eq!(doc.authorization_servers.len(), 1);
        assert_eq!(doc.bearer_methods_supported, vec!["header".to_owned()]);
        assert!(doc.scopes_supported.contains(&"tribal:read".to_owned()));
    }

    #[test]
    fn test_authorization_server_metadata_advertises_s256_and_cimd_support() {
        let doc = authorization_server_metadata(&runtime());
        assert_eq!(doc.code_challenge_methods_supported, vec!["S256".to_owned()]);
        assert!(doc.client_id_metadata_document_supported);
        assert_eq!(doc.response_types_supported, vec!["code".to_owned()]);
        assert_eq!(doc.grant_types_supported, vec!["authorization_code".to_owned()]);
    }

    #[test]
    fn test_authorization_server_metadata_includes_registration_when_dcr_enabled() {
        let doc = authorization_server_metadata(&runtime());
        assert_eq!(
            doc.registration_endpoint,
            Some("http://127.0.0.1:8080/register".to_owned()),
        );
        assert!(
            doc.token_endpoint_auth_methods_supported
                .contains(&"client_secret_basic".to_owned()),
        );
    }

    #[test]
    fn test_authorization_server_metadata_omits_registration_when_dcr_disabled() {
        let mut config = OAuthConfig::default();
        config.dcr_enabled = false;
        let runtime = OAuthRuntimeConfig::build(
            &config,
            &Url::parse("http://127.0.0.1:8080").unwrap(),
            &Url::parse("http://127.0.0.1:8080/mcp").unwrap(),
        )
        .unwrap();
        let doc = authorization_server_metadata(&runtime);
        assert!(doc.registration_endpoint.is_none());
        assert_eq!(
            doc.token_endpoint_auth_methods_supported,
            vec!["none".to_owned()],
        );
    }

    #[test]
    fn test_join_issuer_drops_trailing_slash_on_issuer() {
        let url = Url::parse("https://example.com/").unwrap();
        assert_eq!(join_issuer(&url, "/authorize"), "https://example.com/authorize");
    }
}
