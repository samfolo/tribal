//! Authentication token entity.
//!
//! Bearer tokens for HTTP/SSE authentication. Tokens are 32 bytes random,
//! base64url-encoded, displayed once to the user, and stored as SHA-256
//! hashes. Revocation is soft — `revoked_at` is set, the row persists.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{AuthTokenId, PrincipalId, Scope};

/// An authentication token for a principal.
///
/// The system stores only the SHA-256 hash of the raw token. The raw
/// token is displayed once at creation and never retrievable afterwards.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
pub struct AuthToken {
    /// Unique identifier with `at_` prefix.
    id: AuthTokenId,
    /// SHA-256 hash of the raw bearer token (hex-encoded, 64 chars).
    token_hash: String,
    /// The principal this token authenticates.
    principal_id: PrincipalId,
    /// Permission scopes granted to this token.
    scopes: Vec<Scope>,
    /// Canonical resource URL this token's audience is bound to
    /// (RFC 8707). Every mint path supplies it; the bearer middleware
    /// rejects a token whose audience does not match the running
    /// server's canonical resource.
    audience: String,
    /// When this token expires.
    expires_at: DateTime<Utc>,
    /// When this token was created.
    created_at: DateTime<Utc>,
    /// When this token was revoked, if applicable.
    #[builder(default)]
    revoked_at: Option<DateTime<Utc>>,
}

impl AuthToken {
    /// Returns the token identifier.
    pub fn id(&self) -> AuthTokenId {
        self.id
    }

    /// Returns the token hash.
    pub fn token_hash(&self) -> &str {
        &self.token_hash
    }

    /// Returns the principal this token authenticates.
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Returns the permission scopes granted to this token.
    pub fn scopes(&self) -> &[Scope] {
        &self.scopes
    }

    /// Returns the canonical resource URL this token is audience-bound to.
    pub fn audience(&self) -> &str {
        &self.audience
    }

    /// Returns when this token expires.
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    /// Returns when this token was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns when this token was revoked, if applicable.
    pub fn revoked_at(&self) -> Option<DateTime<Utc>> {
        self.revoked_at
    }
}
