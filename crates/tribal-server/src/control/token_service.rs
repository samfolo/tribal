//! One token-metadata application service for local control transports.

use std::sync::Arc;

use sqlx::PgPool;
use tribal_auth::{AuthenticatedPrincipal, Authenticator};
use tribal_db::{AuthTokenRepository, DbError, PgAuthTokenRepository, PgPrincipalRepository};
use tribal_domain::AuthToken;
use tribal_wire::control::{TokenInfo, TokenList};

/// Failure resolving or reading the local principal's token metadata.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TokenMetadataError {
    #[error("the local principal is unavailable")]
    PrincipalUnavailable,
    #[error("acquiring the token metadata database connection: {source}")]
    Acquire {
        #[source]
        source: sqlx::Error,
    },
    #[error("reading token metadata: {source}")]
    Read {
        #[source]
        source: DbError,
    },
}

/// Resolves the configured local principal without making it a socket gate.
pub(crate) async fn resolve_local_principal(pool: &PgPool) -> Option<AuthenticatedPrincipal> {
    let authenticator = Authenticator::new(
        Arc::new(PgAuthTokenRepository),
        Arc::new(PgPrincipalRepository),
    );
    let mut connection = pool.acquire().await.ok()?;
    authenticator
        .resolve_stdio_principal(&mut connection)
        .await
        .ok()
}

/// Reads non-secret metadata for an already-resolved local principal.
pub(crate) async fn list_token_metadata(
    pool: &PgPool,
    principal: Option<&AuthenticatedPrincipal>,
) -> Result<TokenList, TokenMetadataError> {
    let principal = principal.ok_or(TokenMetadataError::PrincipalUnavailable)?;
    let mut connection = pool
        .acquire()
        .await
        .map_err(|source| TokenMetadataError::Acquire { source })?;
    let tokens = PgAuthTokenRepository
        .find_by_principal_id(&mut connection, principal.principal_id())
        .await
        .map_err(|source| TokenMetadataError::Read { source })?;
    Ok(TokenList {
        tokens: tokens
            .iter()
            .map(|token| token_info(principal.principal_key(), token))
            .collect(),
    })
}

/// Resolves the local principal and reads its non-secret token metadata.
pub(crate) async fn list_local_token_metadata(
    pool: &PgPool,
) -> Result<TokenList, TokenMetadataError> {
    let principal = resolve_local_principal(pool).await;
    list_token_metadata(pool, principal.as_ref()).await
}

pub(super) fn token_info(principal_key: &str, token: &AuthToken) -> TokenInfo {
    TokenInfo {
        principal: principal_key.to_owned(),
        scopes: token
            .scopes()
            .iter()
            .map(|scope| scope.as_str().to_owned())
            .collect(),
        created_at: token.created_at(),
        expires_at: Some(token.expires_at()),
    }
}
