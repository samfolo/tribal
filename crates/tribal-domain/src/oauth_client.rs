//! OAuth 2.0 Dynamic Client Registration record.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

/// Token endpoint authentication method declared by the client at
/// registration time, per RFC 7591 §2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenEndpointAuthMethod {
    /// Public client; no client credentials presented to /token.
    None,
    /// Confidential client; secret presented via HTTP Basic.
    ClientSecretBasic,
    /// Confidential client; secret presented in the form body.
    ClientSecretPost,
}

impl TokenEndpointAuthMethod {
    /// Returns the canonical RFC 7591 string for this method.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ClientSecretBasic => "client_secret_basic",
            Self::ClientSecretPost => "client_secret_post",
        }
    }

    /// Returns true when the method requires a client secret to be
    /// generated and stored.
    #[must_use]
    pub fn requires_secret(self) -> bool {
        matches!(self, Self::ClientSecretBasic | Self::ClientSecretPost)
    }
}

/// Application type declared by the client at registration time, per
/// RFC 7591 §2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationType {
    /// Web application served over HTTPS.
    Web,
    /// Native application running on a user device.
    Native,
}

impl ApplicationType {
    /// Returns the canonical RFC 7591 string for this application type.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Web => "web",
            Self::Native => "native",
        }
    }
}

/// A DCR-registered OAuth client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TypedBuilder)]
pub struct OauthClient {
    /// Opaque identifier returned at registration.
    client_id: String,
    /// SHA-256 hex digest of the issued client secret. `None` for
    /// public clients.
    #[builder(default)]
    client_secret_hash: Option<String>,
    /// Human-readable client name.
    #[builder(default)]
    client_name: Option<String>,
    /// Registered redirect URIs.
    redirect_uris: Vec<String>,
    /// Grant types declared at registration. Defaults to
    /// `authorization_code`.
    grant_types: Vec<String>,
    /// Response types declared at registration. Defaults to `code`.
    response_types: Vec<String>,
    /// Token endpoint auth method.
    token_endpoint_auth_method: TokenEndpointAuthMethod,
    /// Optional declared scope.
    #[builder(default)]
    scope: Option<String>,
    /// Optional declared application type.
    #[builder(default)]
    application_type: Option<ApplicationType>,
    /// Creation timestamp.
    created_at: DateTime<Utc>,
    /// Client secret expiry timestamp, when applicable.
    #[builder(default)]
    client_secret_expires_at: Option<DateTime<Utc>>,
}

impl OauthClient {
    /// Returns the opaque client identifier.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the SHA-256 digest of the client secret.
    #[must_use]
    pub fn client_secret_hash(&self) -> Option<&str> {
        self.client_secret_hash.as_deref()
    }

    /// Returns the human-readable client name.
    #[must_use]
    pub fn client_name(&self) -> Option<&str> {
        self.client_name.as_deref()
    }

    /// Returns the registered redirect URIs.
    #[must_use]
    pub fn redirect_uris(&self) -> &[String] {
        &self.redirect_uris
    }

    /// Returns the registered grant types.
    #[must_use]
    pub fn grant_types(&self) -> &[String] {
        &self.grant_types
    }

    /// Returns the registered response types.
    #[must_use]
    pub fn response_types(&self) -> &[String] {
        &self.response_types
    }

    /// Returns the token endpoint auth method.
    #[must_use]
    pub fn token_endpoint_auth_method(&self) -> TokenEndpointAuthMethod {
        self.token_endpoint_auth_method
    }

    /// Returns the optional declared scope.
    #[must_use]
    pub fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }

    /// Returns the optional declared application type.
    #[must_use]
    pub fn application_type(&self) -> Option<ApplicationType> {
        self.application_type
    }

    /// Returns the creation timestamp.
    #[must_use]
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns the client secret expiry timestamp, when applicable.
    #[must_use]
    pub fn client_secret_expires_at(&self) -> Option<DateTime<Utc>> {
        self.client_secret_expires_at
    }
}
