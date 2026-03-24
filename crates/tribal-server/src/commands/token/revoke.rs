//! Core revoke flow: entry point and async orchestration.

use chrono::Utc;
use tribal_config::{DatabaseConfig, load_config};
use tribal_db::{AuthTokenRepository, PgAuthTokenRepository};

use super::output;
use crate::{
    cli::TokenRevokeArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
    },
    error::AppError,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Pool name for the revoke connection.
const POOL_NAME: &str = "token-revoke";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal token revoke` flow.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, database connection,
/// prefix resolution, or revocation fails.
pub(crate) fn run(config_path: &str, args: TokenRevokeArgs) -> Result<(), AppError> {
    let TokenRevokeArgs { prefix, database } = args;

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

    rt.block_on(run_async(&config.database, &prefix))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Resolves the prefix to a unique token and revokes it.
async fn run_async(db_config: &DatabaseConfig, prefix: &str) -> Result<(), AppError> {
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
        .map_err(|err| AppError::pool_acquire(POOL_NAME, "acquiring revoke connection", err))?;

    let matches = PgAuthTokenRepository
        .find_by_hash_prefix(&mut conn, prefix)
        .await
        .map_err(|source| AppError::Database { source })?;

    let token = match matches.len() {
        0 => {
            return Err(AppError::TokenOperation {
                reason: format!("{}: '{prefix}'", output::NO_MATCHING_TOKEN),
            });
        }
        1 => &matches[0],
        _ => {
            return Err(AppError::TokenOperation {
                reason: format!("{}: '{prefix}'", output::AMBIGUOUS_PREFIX),
            });
        }
    };

    // The user's input prefix may be shorter or longer than the canonical
    // display length. Use the stored hash to produce a consistent prefix.
    let display_prefix = token
        .token_hash()
        .get(..output::HASH_PREFIX_LENGTH)
        .expect("token hash is always 64 hex chars");

    if token.revoked_at().is_some() {
        output::token_already_revoked(display_prefix);
        return Ok(());
    }

    PgAuthTokenRepository
        .revoke(&mut conn, token.id(), Utc::now())
        .await
        .map_err(|source| AppError::Database { source })?;

    output::token_revoked(display_prefix);

    Ok(())
}
