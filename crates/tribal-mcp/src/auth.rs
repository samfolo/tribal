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

pub(crate) const AUTH_FAILURE_REASON_MISSING: &str = "missing";
pub(crate) const AUTH_FAILURE_REASON_INVALID: &str = "invalid";
pub(crate) const AUTH_FAILURE_REASON_REVOKED: &str = "revoked";
pub(crate) const AUTH_FAILURE_REASON_EXPIRED: &str = "expired";

/// Display string for requests with no bearer token.
pub const DISPLAY_MISSING_TOKEN: &str = "missing bearer token";

/// Display string for invalid or unrecognised tokens.
pub const DISPLAY_INVALID_TOKEN: &str = "invalid token";

/// Display string for revoked tokens.
pub const DISPLAY_TOKEN_REVOKED: &str = "token revoked";

/// Display string for expired tokens.
pub const DISPLAY_TOKEN_EXPIRED: &str = "token expired";

// ---------------------------------------------------------------------------
// AuthenticatedPrincipal
// ---------------------------------------------------------------------------

/// Verification-proof principal identity.
///
/// Carries the resolved [`PrincipalId`] and human-readable principal key.
/// Its existence guarantees the holder passed either token verification
/// or the stdio bypass path.
#[derive(Debug, Clone)]
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
    pub fn for_test(principal_id: PrincipalId, principal_key: &str, scopes: Vec<Scope>) -> Self {
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
        required_scope: Scope,
        /// The scopes the token was granted.
        granted_scopes: Vec<Scope>,
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
    pub fn require_scope(&self, scope: &Scope) -> Result<(), AuthError> {
        if is_authorised(self.principal.scopes(), scope) {
            Ok(())
        } else {
            Err(AuthError::InsufficientScope {
                required_scope: scope.clone(),
                granted_scopes: self.principal.scopes().to_vec(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// TransportAuth
// ---------------------------------------------------------------------------

/// How the transport layer provides authentication to the handler.
///
/// New transports pick the variant that matches their auth model without
/// requiring changes to handler code.  Per-connection transports (e.g.
/// stdio) supply [`AtCreation`](Self::AtCreation); per-request transports
/// (e.g. Streamable HTTP) supply [`PerRequest`](Self::PerRequest) and
/// inject the principal into request context extensions via middleware.
pub enum TransportAuthStrategy {
    /// Principal resolved once at handler creation (e.g. stdio).
    AtCreation(AuthContext),

    /// Principal injected per-request into request context extensions
    /// by the transport middleware (e.g. Streamable HTTP).
    PerRequest,
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
            scopes: token.scopes().to_vec(),
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
                scopes: full_access_scopes(),
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
        let principal = AuthenticatedPrincipal::for_test(id, "user:sam", full_access_scopes());

        assert_eq!(principal.principal_id(), id);
        assert_eq!(principal.principal_key(), "user:sam");
        assert_eq!(principal.scopes().len(), 2);
    }

    #[test]
    fn test_require_scope_full_access_accepts_all_tool_scopes() {
        let auth = AuthContext::new(AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            "user:test",
            full_access_scopes(),
        ));

        let tool_scopes: Vec<Scope> = [
            "tribal:write",
            "tribal.knowledge:read",
            "tribal.knowledge:write",
            "tribal.jobs:read",
        ]
        .iter()
        .map(|s| Scope::parse(s).unwrap())
        .collect();

        for scope in &tool_scopes {
            assert!(
                auth.require_scope(scope).is_ok(),
                "expected {scope} to pass"
            );
        }
    }

    #[test]
    fn test_require_scope_insufficient_scope() {
        let scopes = vec![Scope::parse("tribal.knowledge:read").unwrap()];
        let auth = AuthContext::new(AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            "user:test",
            scopes,
        ));

        let required = Scope::parse("tribal:write").unwrap();
        let err = auth
            .require_scope(&required)
            .expect_err("should reject missing scope");

        assert!(matches!(err, AuthError::InsufficientScope { .. }));
    }

    #[test]
    fn test_require_scope_prefix_satisfaction() {
        let scopes = vec![Scope::parse("tribal:read").unwrap()];
        let auth = AuthContext::new(AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            "user:test",
            scopes,
        ));

        let required = Scope::parse("tribal.knowledge:read").unwrap();
        assert!(auth.require_scope(&required).is_ok());
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

        let insufficient = AuthError::InsufficientScope {
            required_scope: Scope::parse("tribal:write").unwrap(),
            granted_scopes: vec![Scope::parse("tribal.knowledge:read").unwrap()],
        };
        assert_eq!(
            insufficient.to_string(),
            "insufficient scope: requires tribal:write",
        );
    }
}
