//! Core setup flow: entry point and async orchestration.

use std::path::Path;

use chrono::{DateTime, Utc};
use tribal_common::sha256_hex;
use tribal_config::{TribalConfig, load_config};
use tribal_db::{AuthTokenRepository, NewAuthToken, PgAuthTokenRepository};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, full_access_scopes};

use super::{config_file, output};
use crate::{
    cli::SetupArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, DATABASE_COMMAND_DEFAULTS, TIMESTAMP_FORMAT,
        find_or_create_principal, generate_raw_token, ttl_to_delta,
    },
    error::AppError,
    startup::{ensure_prompt_files, run_migrations},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Pool name for the single setup connection.
const POOL_NAME_SETUP: &str = "setup";

/// Statement timeout for setup operations (migrations + inserts).
///
/// Longer than the shared `COMMAND_STATEMENT_TIMEOUT_MS` because setup
/// runs migrations in addition to inserts.
const SETUP_STATEMENT_TIMEOUT_MS: u64 = 60_000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal setup` bootstrap sequence and exits.
///
/// # Errors
///
/// Returns an [`AppError`] if any phase of the setup fails.
pub(crate) fn run(config_path: &str, args: SetupArgs) -> Result<(), AppError> {
    let cli_overrides = args.into_cli_overrides();
    let config = load_config(
        config_path,
        Some(cli_overrides),
        Some(&DATABASE_COMMAND_DEFAULTS),
    )?;
    let expires_at = Utc::now() + ttl_to_delta(config.auth.token_ttl_hours)?;

    let expanded_config_path = shellexpand::tilde(config_path).into_owned();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    rt.block_on(run_async(&config, &expanded_config_path, expires_at))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Executes the setup steps asynchronously.
async fn run_async(
    config: &TribalConfig,
    config_path: &str,
    expires_at: DateTime<Utc>,
) -> Result<(), AppError> {
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(config_dir)
        .await
        .map_err(|source| AppError::SetupIo {
            context: format!("create config directory {}", config_dir.display()),
            source,
        })?;
    output::config_directory(&config_dir.to_string_lossy());

    let prompts_dir = Path::new(&config.prompts.directory);
    ensure_prompt_files(prompts_dir).await?;
    output::prompt_files(&prompts_dir.to_string_lossy());

    let pool = tribal_db::create_pool(
        &config.database,
        POOL_NAME_SETUP,
        COMMAND_POOL_MAX_CONNECTIONS,
        SETUP_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| {
        output::database_unreachable();
        AppError::Database { source }
    })?;
    output::database_connected();

    run_migrations(&pool).await?;
    output::migrations_complete();

    let mut conn = pool.acquire().await.map_err(|err| {
        AppError::pool_acquire(POOL_NAME_SETUP, "acquiring setup connection", err)
    })?;

    let principal = find_or_create_principal(&mut conn, LOCAL_PRINCIPAL_KEY).await?;
    output::principal(principal.principal_key());

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
    output::token_created(&expires_at.format(TIMESTAMP_FORMAT).to_string());

    drop(conn);

    let outcome = config_file::write_if_absent(config_path, config).await?;
    output::config_file(&outcome);

    output::instructions(&raw_token);

    Ok(())
}
