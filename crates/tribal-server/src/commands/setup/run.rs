//! Core setup flow: entry point and async orchestration.

use std::path::Path;

use chrono::{DateTime, TimeDelta, Utc};
use sqlx::{Postgres, pool::PoolConnection};
use tribal_common::sha256_hex;
use tribal_config::{ConfigError, TribalConfig, load_config};
use tribal_db::{
    AuthTokenRepository, DbError, NewAuthToken, NewPrincipal, PgAuthTokenRepository,
    PgPrincipalRepository, PrincipalRepository,
};
use tribal_domain::Principal;

use super::{config_file, output, token};
use crate::{
    cli::SetupArgs,
    error::AppError,
    startup::{ensure_prompt_files, run_migrations},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Principal key for the local stdio transport identity.
const LOCAL_PRINCIPAL_KEY: &str = "principal:local";

/// Default database URL used when no other source provides one.
///
/// Injected as a command-defaults-layer value so that the figment cascade
/// still respects YAML, env vars, and CLI overrides.
const DEFAULT_DATABASE_URL: &str = "postgresql://tribal@localhost:5432/tribal";

/// Pool name for the single setup connection.
const POOL_NAME_SETUP: &str = "setup";

/// Maximum connections for the setup pool.
const SETUP_POOL_MAX_CONNECTIONS: u32 = 1;

/// Statement timeout for setup operations (migrations + inserts).
const SETUP_STATEMENT_TIMEOUT_MS: u64 = 60_000;

/// Error message for a token TTL that exceeds the representable range.
const ERR_TTL_OUT_OF_RANGE: &str = "auth.token_ttl_hours value is too large";

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
    let command_defaults = [("database.url", DEFAULT_DATABASE_URL)];

    let config = load_config(config_path, Some(cli_overrides), Some(&command_defaults))?;

    if config.auth.token_ttl_hours == 0 {
        return Err(AppError::Config {
            source: ConfigError::ValidationFailed {
                errors: vec!["auth.token_ttl_hours must be greater than zero".into()],
            },
        });
    }

    let expanded_config_path = shellexpand::tilde(config_path).into_owned();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    rt.block_on(run_async(&config, &expanded_config_path))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Executes the setup steps asynchronously.
async fn run_async(config: &TribalConfig, config_path: &str) -> Result<(), AppError> {
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
        SETUP_POOL_MAX_CONNECTIONS,
        SETUP_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| AppError::Database { source })?;
    output::database_connected();

    run_migrations(&pool).await?;
    output::migrations_complete();

    let mut conn = pool.acquire().await.map_err(|err| {
        AppError::pool_acquire(POOL_NAME_SETUP, "acquiring setup connection", err)
    })?;

    let principal = find_or_create_principal(&mut conn).await?;
    output::principal(principal.principal_key());

    let raw_token = token::generate_raw_token();
    let token_hash = sha256_hex(&raw_token);
    let expires_at = compute_expiry(config.auth.token_ttl_hours)?;

    let new_token = NewAuthToken::builder()
        .token_hash(token_hash)
        .principal_id(principal.id())
        .expires_at(expires_at)
        .build();

    PgAuthTokenRepository
        .insert(&mut *conn, &new_token)
        .await
        .map_err(|source| AppError::Database { source })?;
    output::token_created(&expires_at.format("%Y-%m-%d %H:%M:%S UTC").to_string());

    drop(conn);

    let outcome = config_file::write_if_absent(config_path, config).await?;
    output::config_file(&outcome);

    output::instructions(&raw_token);

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Finds the `principal:local` principal or creates it if absent.
///
/// Handles the TOCTOU race from concurrent setup processes by catching
/// `UniqueViolation` on insert and falling back to a second lookup.
async fn find_or_create_principal(
    conn: &mut PoolConnection<Postgres>,
) -> Result<Principal, AppError> {
    if let Some(existing) = PgPrincipalRepository
        .find_by_key(&mut *conn, LOCAL_PRINCIPAL_KEY)
        .await
        .map_err(|source| AppError::Database { source })?
    {
        return Ok(existing);
    }

    let new = NewPrincipal::builder()
        .principal_key(LOCAL_PRINCIPAL_KEY.to_owned())
        .build();

    match PgPrincipalRepository.insert(&mut *conn, &new).await {
        Ok(principal) => Ok(principal),
        Err(DbError::UniqueViolation { .. }) => PgPrincipalRepository
            .find_by_key(&mut *conn, LOCAL_PRINCIPAL_KEY)
            .await
            .map_err(|source| AppError::Database { source })?
            .ok_or_else(|| AppError::Database {
                source: DbError::NotFound {
                    entity: "principal",
                    id: LOCAL_PRINCIPAL_KEY.into(),
                },
            }),
        Err(source) => Err(AppError::Database { source }),
    }
}

/// Computes the token expiry timestamp from the configured TTL.
///
/// # Errors
///
/// Returns [`AppError::Config`] if the TTL value exceeds the representable
/// range for `TimeDelta`.
fn compute_expiry(ttl_hours: u64) -> Result<DateTime<Utc>, AppError> {
    let hours = i64::try_from(ttl_hours).map_err(|_| AppError::Config {
        source: ConfigError::ValidationFailed {
            errors: vec![ERR_TTL_OUT_OF_RANGE.into()],
        },
    })?;

    let delta = TimeDelta::try_hours(hours).ok_or_else(|| AppError::Config {
        source: ConfigError::ValidationFailed {
            errors: vec![ERR_TTL_OUT_OF_RANGE.into()],
        },
    })?;

    Ok(Utc::now() + delta)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Expiry computation -------------------------------------------------

    #[test]
    fn test_compute_expiry_is_in_future() {
        let before = Utc::now();
        let expires_at = compute_expiry(8760).unwrap();
        assert!(expires_at > before);
    }

    #[test]
    fn test_compute_expiry_rejects_overflow() {
        let result = compute_expiry(u64::MAX);
        assert!(result.is_err());
    }
}
