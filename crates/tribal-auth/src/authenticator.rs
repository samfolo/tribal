//! Bearer-token verification and stdio principal resolution.

use std::sync::Arc;

use chrono::Utc;
use sqlx::PgConnection;
use tracing::{debug, error, warn};
use tribal_common::sha256_hex;
use tribal_db::{AuthTokenRepository, DbError, PrincipalRepository};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, stdio_principal_scopes};

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
/// matching the repository method convention. When `expected_audience`
/// is set, every verified token's audience must equal it exactly (RFC
/// 8707 audience binding); a token minted for a different resource is
/// rejected. Callers with no canonical resource URL in scope (CLI
/// subcommands that touch the token store without running the MCP
/// transport) construct the authenticator with no expected audience and
/// the check is skipped.
pub struct Authenticator {
    auth_token: Arc<dyn AuthTokenRepository + Send + Sync>,
    principal: Arc<dyn PrincipalRepository + Send + Sync>,
    expected_audience: Option<String>,
}

impl Authenticator {
    /// Creates a new authenticator with no audience binding.
    ///
    /// Equivalent to [`Self::with_audience`] called with `None`.
    /// Reserved for callers that have no canonical resource URL in
    /// scope (e.g. CLI subcommands that touch the token store without
    /// running the MCP transport).
    #[must_use]
    pub fn new(
        auth_token: Arc<dyn AuthTokenRepository + Send + Sync>,
        principal: Arc<dyn PrincipalRepository + Send + Sync>,
    ) -> Self {
        Self::with_audience(auth_token, principal, None)
    }

    /// Creates a new authenticator binding token verification to a
    /// specific audience.
    #[must_use]
    pub fn with_audience(
        auth_token: Arc<dyn AuthTokenRepository + Send + Sync>,
        principal: Arc<dyn PrincipalRepository + Send + Sync>,
        expected_audience: Option<String>,
    ) -> Self {
        Self {
            auth_token,
            principal,
            expected_audience,
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

        // Audience binding (RFC 8707). When an expected audience is
        // configured, the token's audience must match it exactly; a token
        // minted for a different resource is rejected.
        if let Some(expected) = &self.expected_audience
            && token.audience() != expected
        {
            warn!(
                auth_failure_reason = AUTH_FAILURE_REASON_INVALID,
                "token verification failed: audience mismatch",
            );
            return Err(AuthError::AudienceMismatch {
                expected: expected.clone(),
                found: token.audience().to_owned(),
            });
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
    /// (because database initialisation has not run) is a fatal startup
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
                stdio_principal_scopes(),
            )),
            None => Err(AuthError::LocalPrincipalMissing {
                principal_key: LOCAL_PRINCIPAL_KEY.to_owned(),
            }),
        }
    }
}
