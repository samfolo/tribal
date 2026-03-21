//! Authentication types and token verification.
//!
//! Provides bearer token verification against the database and stdio
//! transport bypass via the local principal key. The
//! [`AuthenticatedPrincipal`] type is a verification-proof wrapper — its
//! existence guarantees the holder passed either token verification or
//! the stdio bypass path.

use std::sync::Arc;

use chrono::Utc;
use sqlx::PgConnection;
use tracing::{debug, error, warn};
use tribal_common::sha256_hex;
use tribal_db::{AuthTokenRepository, DbError, PrincipalRepository};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, PrincipalId, Scope, full_access_scopes, is_authorised};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const AUTH_FAILURE_REASON_INVALID: &str = "invalid";
const AUTH_FAILURE_REASON_REVOKED: &str = "revoked";
const AUTH_FAILURE_REASON_EXPIRED: &str = "expired";

const DISPLAY_INVALID_TOKEN: &str = "invalid token";
const DISPLAY_TOKEN_REVOKED: &str = "token revoked";
const DISPLAY_TOKEN_EXPIRED: &str = "token expired";

const EXPECT_VALID_TOOL_SCOPE: &str = "invariant: tool-registry scopes are compile-time valid";

// ---------------------------------------------------------------------------
// AuthenticatedPrincipal
// ---------------------------------------------------------------------------

/// Verification-proof principal identity.
///
/// Carries the resolved [`PrincipalId`] and human-readable principal key.
/// Its existence guarantees the holder passed either token verification
/// or the stdio bypass path.
#[derive(Debug)]
pub struct AuthenticatedPrincipal {
    principal_id: PrincipalId,
    principal_key: String,
    scopes: Vec<Scope>,
}

impl AuthenticatedPrincipal {
    /// Returns the principal's database identifier.
    #[must_use]
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Returns the human-readable principal key.
    #[must_use]
    pub fn principal_key(&self) -> &str {
        &self.principal_key
    }

    /// Returns the permission scopes granted to this principal.
    #[must_use]
    pub fn scopes(&self) -> &[Scope] {
        &self.scopes
    }
}

#[cfg(any(test, feature = "test-helpers"))]
impl AuthenticatedPrincipal {
    /// Test-only constructor that bypasses verification.
    ///
    /// The caller supplies the [`PrincipalId`] and scopes directly. Use
    /// a random ID for mock-backed tests; use the real ID from the
    /// database for tests that hit FK-constrained tables.
    #[must_use]
    pub fn for_test(
        principal_id: PrincipalId,
        principal_key: &str,
        scopes: Vec<Scope>,
    ) -> Self {
        Self {
            principal_id,
            principal_key: principal_key.to_owned(),
            scopes,
        }
    }
}

// ---------------------------------------------------------------------------
// AuthError
// ---------------------------------------------------------------------------

/// Errors produced by the authentication layer.
///
/// Variants carrying `token_hash` hold the SHA-256 hex digest, not the
/// raw token. The raw token is never stored or logged.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// No token row matched the computed hash.
    #[error("{DISPLAY_INVALID_TOKEN}")]
    InvalidToken {
        /// SHA-256 hex digest of the raw token.
        token_hash: String,
    },

    /// The token has been revoked (`revoked_at` is set).
    #[error("{DISPLAY_TOKEN_REVOKED}")]
    TokenRevoked {
        /// SHA-256 hex digest of the raw token.
        token_hash: String,
    },

    /// The token has expired (`expires_at` is in the past).
    #[error("{DISPLAY_TOKEN_EXPIRED}")]
    TokenExpired {
        /// SHA-256 hex digest of the raw token.
        token_hash: String,
    },

    /// The token is valid but its `principal_id` does not resolve.
    ///
    /// A defensive check — the FK constraint on `auth_tokens` prevents
    /// this under normal operations. Logged at ERROR level.
    #[error("principal not found for token: {principal_id}")]
    PrincipalNotFound {
        /// The `principal_id` from the valid token row.
        principal_id: PrincipalId,
    },

    /// The local principal record is missing from the database.
    ///
    /// Occurs when the stdio transport attempts to resolve the local
    /// principal identity but setup has not been run.
    #[error("{principal_key} not found — run `tribal setup` to create it")]
    LocalPrincipalMissing {
        /// The principal key that was not found.
        principal_key: String,
    },

    /// A database operation failed during authentication.
    #[error("database unavailable: {context}")]
    DatabaseUnavailable {
        /// Description of which operation failed.
        context: String,
        /// The underlying error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The token is valid but lacks the required scope.
    #[error("insufficient scope: requires {required_scope}")]
    InsufficientScope {
        /// The scope the operation requires.
        required_scope: String,
        /// The scopes the token was granted.
        granted_scopes: Vec<String>,
    },
}

// ---------------------------------------------------------------------------
// AuthContext
// ---------------------------------------------------------------------------

/// Authentication context carried on the handler.
///
/// Constructed once at handler creation from the resolved principal
/// identity and stored as a field on the handler.
pub struct AuthContext {
    principal: AuthenticatedPrincipal,
}

impl AuthContext {
    /// Creates a new authentication context from a verified principal.
    #[must_use]
    pub fn new(principal: AuthenticatedPrincipal) -> Self {
        Self { principal }
    }

    /// Returns the authenticated principal.
    #[must_use]
    pub fn principal(&self) -> &AuthenticatedPrincipal {
        &self.principal
    }

    /// Checks whether the authenticated principal has the required scope.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::InsufficientScope`] if no granted scope
    /// satisfies the required scope.
    ///
    /// # Panics
    ///
    /// Panics if `scope` is not a valid scope string. All tool-registry
    /// scopes are compile-time valid, so this is an invariant violation.
    pub fn require_scope(&self, scope: &str) -> Result<(), AuthError> {
        let required = Scope::parse(scope).expect(EXPECT_VALID_TOOL_SCOPE);
        if is_authorised(self.principal.scopes(), &required) {
            Ok(())
        } else {
            Err(AuthError::InsufficientScope {
                required_scope: scope.to_owned(),
                granted_scopes: self
                    .principal
                    .scopes()
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Authenticator
// ---------------------------------------------------------------------------

/// Holds repository references needed for token verification and
/// principal resolution.
///
/// Constructed once and shared. Methods accept a `&mut PgConnection`
/// matching the repository method convention.
pub struct Authenticator {
    auth_token: Arc<dyn AuthTokenRepository + Send + Sync>,
    principal: Arc<dyn PrincipalRepository + Send + Sync>,
}

impl Authenticator {
    /// Creates a new authenticator with the given repository
    /// implementations.
    #[must_use]
    pub fn new(
        auth_token: Arc<dyn AuthTokenRepository + Send + Sync>,
        principal: Arc<dyn PrincipalRepository + Send + Sync>,
    ) -> Self {
        Self {
            auth_token,
            principal,
        }
    }

    /// Verifies a raw bearer token against the database.
    ///
    /// Computes the SHA-256 hash, looks up the token row, checks
    /// revocation then expiry, and resolves the principal. Returns an
    /// [`AuthenticatedPrincipal`] on success.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] with a distinct variant for each failure
    /// mode. See the variant docs for details.
    pub async fn verify_token(
        &self,
        conn: &mut PgConnection,
        raw_token: &str,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        let token_hash = sha256_hex(raw_token);

        debug!("verifying token");

        // Look up by hash — always fetch the full row.
        let token = self
            .auth_token
            .find_by_hash(conn, &token_hash)
            .await
            .map_err(|e| AuthError::DatabaseUnavailable {
                context: "finding auth token by hash".to_owned(),
                source: Box::new(e),
            })?;

        let Some(token) = token else {
            warn!(
                auth_failure_reason = AUTH_FAILURE_REASON_INVALID,
                "token verification failed: no matching token",
            );
            return Err(AuthError::InvalidToken { token_hash });
        };

        // Check revocation before expiry — revocation is terminal
        // regardless of expiry state.
        if token.revoked_at().is_some() {
            warn!(
                auth_failure_reason = AUTH_FAILURE_REASON_REVOKED,
                "token verification failed: token revoked",
            );
            return Err(AuthError::TokenRevoked { token_hash });
        }

        if token.expires_at() < Utc::now() {
            warn!(
                auth_failure_reason = AUTH_FAILURE_REASON_EXPIRED,
                "token verification failed: token expired",
            );
            return Err(AuthError::TokenExpired { token_hash });
        }

        // Resolve the principal.
        let principal = match self.principal.find_by_id(conn, token.principal_id()).await {
            Ok(p) => p,
            Err(DbError::NotFound { .. }) => {
                error!(
                    principal_id = %token.principal_id(),
                    "token verification failed: principal not found (data integrity violation)",
                );
                return Err(AuthError::PrincipalNotFound {
                    principal_id: token.principal_id(),
                });
            }
            Err(e) => {
                return Err(AuthError::DatabaseUnavailable {
                    context: "finding principal by id".to_owned(),
                    source: Box::new(e),
                });
            }
        };

        Ok(AuthenticatedPrincipal {
            principal_id: principal.id(),
            principal_key: principal.principal_key().to_owned(),
        })
    }

    /// Resolves the local principal identity for the stdio transport.
    ///
    /// Called once at handler creation. A missing local principal
    /// (because `tribal setup` has not been run) is a fatal startup
    /// error.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::LocalPrincipalMissing`] if the local
    /// principal does not exist. Returns
    /// [`AuthError::DatabaseUnavailable`] on database errors.
    pub async fn resolve_stdio_principal(
        &self,
        conn: &mut PgConnection,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        debug!("resolving stdio principal");

        let principal = self
            .principal
            .find_by_key(conn, LOCAL_PRINCIPAL_KEY)
            .await
            .map_err(|e| AuthError::DatabaseUnavailable {
                context: "finding local principal by key".to_owned(),
                source: Box::new(e),
            })?;

        match principal {
            Some(p) => Ok(AuthenticatedPrincipal {
                principal_id: p.id(),
                principal_key: p.principal_key().to_owned(),
            }),
            None => Err(AuthError::LocalPrincipalMissing {
                principal_key: LOCAL_PRINCIPAL_KEY.to_owned(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::io;

    use tribal_domain::PrincipalId;

    use super::*;

    #[test]
    fn test_authenticated_principal_accessors() {
        let id = PrincipalId::new();
        let principal = AuthenticatedPrincipal::for_test(id, "user:sam");

        assert_eq!(principal.principal_id(), id);
        assert_eq!(principal.principal_key(), "user:sam");
    }

    #[test]
    fn test_auth_context_require_scope_always_succeeds() {
        let auth = AuthContext::new(AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            "user:test",
        ));

        assert!(auth.require_scope("read").is_ok());
        assert!(auth.require_scope("write").is_ok());
        assert!(auth.require_scope("admin").is_ok());
    }

    #[test]
    fn test_auth_error_display_variants() {
        let invalid = AuthError::InvalidToken {
            token_hash: "abc".into(),
        };
        assert_eq!(invalid.to_string(), DISPLAY_INVALID_TOKEN);

        let revoked = AuthError::TokenRevoked {
            token_hash: "abc".into(),
        };
        assert_eq!(revoked.to_string(), DISPLAY_TOKEN_REVOKED);

        let expired = AuthError::TokenExpired {
            token_hash: "abc".into(),
        };
        assert_eq!(expired.to_string(), DISPLAY_TOKEN_EXPIRED);

        let id = PrincipalId::new();
        let not_found = AuthError::PrincipalNotFound { principal_id: id };
        assert_eq!(
            not_found.to_string(),
            format!("principal not found for token: {id}"),
        );

        let missing = AuthError::LocalPrincipalMissing {
            principal_key: LOCAL_PRINCIPAL_KEY.to_owned(),
        };
        assert!(missing.to_string().contains("tribal setup"));

        let db_err = AuthError::DatabaseUnavailable {
            context: "test op".into(),
            source: Box::new(io::Error::other("boom")),
        };
        assert_eq!(db_err.to_string(), "database unavailable: test op");
    }
}
