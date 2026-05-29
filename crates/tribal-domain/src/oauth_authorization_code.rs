//! OAuth 2.1 authorisation-code record.
//!
//! Codes are single-use, short-lived, and bound to the PKCE challenge
//! captured at /authorize time. The raw code is delivered via redirect
//! and never persisted; only its SHA-256 hex digest is stored.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::PrincipalId;

/// A persisted authorisation code awaiting exchange at /token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TypedBuilder)]
pub struct OauthAuthorizationCode {
    /// SHA-256 hex digest of the raw code.
    code_hash: String,
    /// Client identifier the code was issued to.
    client_id: String,
    /// Redirect URI the code is bound to.
    redirect_uri: String,
    /// S256 code challenge captured at /authorize.
    code_challenge: String,
    /// Optional declared scope (space-separated).
    #[builder(default)]
    scope: Option<String>,
    /// Optional declared resource indicator (RFC 8707).
    #[builder(default)]
    resource: Option<String>,
    /// Principal the resulting token will represent.
    principal_id: PrincipalId,
    /// Timestamp the code was issued at.
    issued_at: DateTime<Utc>,
    /// Timestamp the code expires.
    expires_at: DateTime<Utc>,
    /// Timestamp the code was exchanged, if applicable.
    #[builder(default)]
    consumed_at: Option<DateTime<Utc>>,
}

impl OauthAuthorizationCode {
    /// Sole supported PKCE `code_challenge_method` (OAuth 2.1 forbids
    /// `plain`).
    ///
    /// Owned here because the challenge method is intrinsic to the code.
    /// The auth layer reads it for the inbound-request comparison and the
    /// metadata advertisement, and the DB layer binds it as the stored
    /// value, so both reference this one definition. The SQL `CHECK`
    /// constraint repeats the literal by necessity, since it cannot
    /// reference a Rust constant.
    pub const CODE_CHALLENGE_METHOD_S256: &'static str = "S256";

    /// Returns the SHA-256 hex digest of the raw code.
    #[must_use]
    pub fn code_hash(&self) -> &str {
        &self.code_hash
    }

    /// Returns the client identifier.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the redirect URI.
    #[must_use]
    pub fn redirect_uri(&self) -> &str {
        &self.redirect_uri
    }

    /// Returns the S256 code challenge.
    #[must_use]
    pub fn code_challenge(&self) -> &str {
        &self.code_challenge
    }

    /// Returns the optional declared scope.
    #[must_use]
    pub fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }

    /// Returns the optional declared resource indicator.
    #[must_use]
    pub fn resource(&self) -> Option<&str> {
        self.resource.as_deref()
    }

    /// Returns the principal identifier.
    #[must_use]
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Returns the issued-at timestamp.
    #[must_use]
    pub fn issued_at(&self) -> DateTime<Utc> {
        self.issued_at
    }

    /// Returns the expiry timestamp.
    #[must_use]
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    /// Returns the consumed-at timestamp, when applicable.
    #[must_use]
    pub fn consumed_at(&self) -> Option<DateTime<Utc>> {
        self.consumed_at
    }
}
