//! Bearer-token verification and stdio principal resolution.

use std::sync::Arc;

use chrono::Utc;
use sqlx::PgConnection;
use tracing::{debug, error, warn};
use tribal_common::sha256_hex;
use tribal_db::{AuthTokenRepository, DbError, PrincipalRepository};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, full_access_scopes};

use crate::{
    error::{
        AUTH_FAILURE_REASON_EXPIRED, AUTH_FAILURE_REASON_INVALID, AUTH_FAILURE_REASON_REVOKED,
        AuthError,
    },
    principal::AuthenticatedPrincipal,
};

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

        // Look up by hash; always fetch the full row.
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

        // Check revocation before expiry; revocation is terminal regardless
        // of expiry state.
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

        Ok(AuthenticatedPrincipal::new(
            principal.id(),
            principal.principal_key().to_owned(),
            token.scopes().to_vec(),
        ))
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
            Some(p) => Ok(AuthenticatedPrincipal::new(
                p.id(),
                p.principal_key().to_owned(),
                full_access_scopes(),
            )),
            None => Err(AuthError::LocalPrincipalMissing {
                principal_key: LOCAL_PRINCIPAL_KEY.to_owned(),
            }),
        }
    }
}
