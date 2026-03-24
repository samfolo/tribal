//! Core revoke-all flow: entry point and async orchestration.

use chrono::Utc;
use tribal_config::{DatabaseConfig, load_config};
use tribal_db::{
    AuthTokenRepository, PgAuthTokenRepository, PgPrincipalRepository, PrincipalRepository,
};

use super::output;
use crate::{
    cli::TokenRevokeAllArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
    },
    error::AppError,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Pool name for the revoke-all connection.
const POOL_NAME: &str = "token-revoke-all";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal token revoke-all` flow.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, database connection, principal
/// resolution, or revocation fails.
pub(crate) fn run(config_path: &str, args: TokenRevokeAllArgs) -> Result<(), AppError> {
    let TokenRevokeAllArgs {
        principal,
        database,
    } = args;

    let cli_overrides = database.into_cli_overrides();
    let config = load_config(
        config_path,
        Some(cli_overrides),
        Some(&DATABASE_COMMAND_DEFAULTS),
    )?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    rt.block_on(run_async(&config.database, principal.as_deref()))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Batch-revokes tokens, optionally filtered by principal.
async fn run_async(
    db_config: &DatabaseConfig,
    principal_filter: Option<&str>,
) -> Result<(), AppError> {
    let pool = tribal_db::create_pool(
        db_config,
        POOL_NAME,
        COMMAND_POOL_MAX_CONNECTIONS,
        COMMAND_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| AppError::Database { source })?;

    let mut conn = pool
        .acquire()
        .await
        .map_err(|err| AppError::pool_acquire(POOL_NAME, "acquiring revoke-all connection", err))?;

    let principal_id = if let Some(key) = principal_filter {
        let principal = PgPrincipalRepository
            .find_by_key(&mut conn, key)
            .await
            .map_err(|source| AppError::Database { source })?
            .ok_or_else(|| AppError::TokenOperation {
                reason: format!("{}: '{key}'", output::PRINCIPAL_NOT_FOUND),
            })?;
        Some(principal.id())
    } else {
        None
    };

    let count = PgAuthTokenRepository
        .revoke_all(&mut conn, principal_id, Utc::now())
        .await
        .map_err(|source| AppError::Database { source })?;

    output::tokens_revoked(count);

    Ok(())
}
