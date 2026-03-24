//! Core list flow: entry point and async orchestration.

use std::collections::HashMap;

use tribal_config::{DatabaseConfig, load_config};
use tribal_db::{
    AuthTokenRepository, PgAuthTokenRepository, PgPrincipalRepository, PrincipalRepository,
};
use tribal_domain::{AuthToken, Principal, PrincipalId};

use super::output;
use crate::{
    cli::TokenListArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
    },
    error::AppError,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Pool name for the list connection.
const POOL_NAME: &str = "token-list";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal token list` flow.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, database connection,
/// or the query fails.
pub(crate) fn run(config_path: &str, args: TokenListArgs) -> Result<(), AppError> {
    let cli_overrides = args.into_cli_overrides();
    let config = load_config(
        config_path,
        Some(cli_overrides),
        Some(&DATABASE_COMMAND_DEFAULTS),
    )?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    rt.block_on(run_async(&config.database))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Fetches all tokens, resolves their principals, and prints the table.
async fn run_async(db_config: &DatabaseConfig) -> Result<(), AppError> {
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
        .map_err(|err| AppError::pool_acquire(POOL_NAME, "acquiring list connection", err))?;

    let tokens = PgAuthTokenRepository
        .find_all(&mut conn)
        .await
        .map_err(|source| AppError::Database { source })?;

    if tokens.is_empty() {
        output::no_tokens();
        return Ok(());
    }

    let principals = resolve_principals(&mut conn, &tokens).await?;
    output::token_table(&tokens, &principals);

    Ok(())
}

/// Batch-resolves principal keys for the given tokens.
async fn resolve_principals(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    tokens: &[AuthToken],
) -> Result<HashMap<PrincipalId, Principal>, AppError> {
    let ids: Vec<PrincipalId> = tokens
        .iter()
        .map(AuthToken::principal_id)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let principals = PgPrincipalRepository
        .find_by_ids(conn, &ids)
        .await
        .map_err(|source| AppError::Database { source })?;

    let map: HashMap<PrincipalId, Principal> =
        principals.into_iter().map(|p| (p.id(), p)).collect();

    // Every token's principal must be resolvable — the FK constraint makes
    // orphans impossible under normal operation. A missing principal indicates
    // data corruption and should be surfaced, not masked.
    for id in &ids {
        if !map.contains_key(id) {
            return Err(AppError::TokenOperation {
                reason: format!("{}: {id}", output::ORPHANED_TOKEN),
            });
        }
    }

    Ok(map)
}
