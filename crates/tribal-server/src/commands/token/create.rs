//! Core create flow: entry point and async orchestration.

use chrono::{DateTime, Utc};
use tribal_common::sha256_hex;
use tribal_config::{DatabaseConfig, load_config};
use tribal_db::{AuthTokenRepository, NewAuthToken, PgAuthTokenRepository};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, full_access_scopes};

use super::output;
use crate::{
    cli::TokenCreateArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
        TIMESTAMP_FORMAT, find_or_create_principal, generate_raw_token, ttl_to_delta,
    },
    error::AppError,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Pool name for the create connection.
const POOL_NAME: &str = "token-create";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal token create` flow.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, database connection, principal
/// resolution, or token insertion fails.
pub(crate) fn run(config_path: &str, mut args: TokenCreateArgs) -> Result<(), AppError> {
    let principal = args.principal.take();
    let ttl = args.ttl;
    let cli_overrides = args.into_cli_overrides();
    let config = load_config(
        config_path,
        Some(cli_overrides),
        Some(&DATABASE_COMMAND_DEFAULTS),
    )?;

    if ttl == Some(0) {
        return Err(AppError::TokenOperation {
            reason: output::TTL_MUST_BE_POSITIVE.into(),
        });
    }
    let resolved_ttl = ttl.unwrap_or(config.auth.token_ttl_hours);
    let expires_at = Utc::now() + ttl_to_delta(resolved_ttl)?;
    let principal_key = principal.unwrap_or_else(|| LOCAL_PRINCIPAL_KEY.to_owned());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    rt.block_on(run_async(&config.database, &principal_key, expires_at))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Creates a new auth token for the resolved principal.
async fn run_async(
    db_config: &DatabaseConfig,
    principal_key: &str,
    expires_at: DateTime<Utc>,
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
        .map_err(|err| AppError::pool_acquire(POOL_NAME, "acquiring create connection", err))?;

    let principal = find_or_create_principal(&mut conn, principal_key).await?;
    output::principal_resolved(principal.principal_key());

    let raw_token = generate_raw_token();
    let token_hash = sha256_hex(&raw_token);

    let new_token = NewAuthToken::builder()
        .token_hash(token_hash)
        .principal_id(principal.id())
        .scopes(full_access_scopes())
        .expires_at(expires_at)
        .build();

    PgAuthTokenRepository
        .insert(&mut conn, &new_token)
        .await
        .map_err(|source| AppError::Database { source })?;

    output::raw_token(&raw_token);
    output::token_created(&expires_at.format(TIMESTAMP_FORMAT).to_string());

    Ok(())
}
