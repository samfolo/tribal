//! Core setup flow: entry point and async orchestration.

use std::{
    io::{self, Write},
    path::Path,
};

use chrono::{DateTime, Utc};
use tribal_common::sha256_hex;
use tribal_config::{PromptSource, TribalConfig, load_config};
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

    let mut stderr = io::stderr().lock();
    rt.block_on(run_async(
        &config,
        &expanded_config_path,
        expires_at,
        &mut stderr,
    ))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Executes the setup steps asynchronously.
async fn run_async(
    config: &TribalConfig,
    config_path: &str,
    expires_at: DateTime<Utc>,
    out: &mut dyn Write,
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
    output::config_directory(out, &config_dir.to_string_lossy());

    if let PromptSource::Disk { directory, .. } = &config.prompts.source {
        let prompts_dir = Path::new(directory);
        ensure_prompt_files(prompts_dir).await?;
        output::prompt_files(out, &prompts_dir.to_string_lossy());
    }

    let pool = tribal_db::create_pool(
        &config.database,
        POOL_NAME_SETUP,
        COMMAND_POOL_MAX_CONNECTIONS,
        SETUP_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| {
        output::database_unreachable(out);
        AppError::Database { source }
    })?;
    output::database_connected(out);

    run_migrations(&pool).await?;
    output::migrations_complete(out);

    let mut conn = pool.acquire().await.map_err(|err| {
        AppError::pool_acquire(POOL_NAME_SETUP, "acquiring setup connection", err)
    })?;

    let principal = find_or_create_principal(&mut conn, LOCAL_PRINCIPAL_KEY).await?;
    output::principal(out, principal.principal_key());

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
    output::token_created(out, &expires_at.format(TIMESTAMP_FORMAT).to_string());

    drop(conn);

    let outcome = config_file::write_if_absent(config_path, config).await?;
    output::config_file(out, &outcome);

    output::instructions(out, &raw_token).map_err(|source| AppError::SetupIo {
        context: "writing bearer token output".into(),
        source,
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_test_utils::{
        count_prompt_versions, serial_lock, test_context, truncate_all_tables,
    };

    use super::*;

    #[tokio::test]
    async fn test_setup_embedded_omits_prompts_directory_line() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("create pool");

        let mut conn = pool.acquire().await.expect("acquire connection");
        truncate_all_tables(&mut conn).await;
        drop(conn);

        let config_dir = tempfile::tempdir().expect("config dir");
        let config_path = config_dir.path().join("tribal.yaml");

        let mut config = TribalConfig::default();
        config.database.url = ctx.database_url().to_owned();
        config.database.max_connect_attempts = 1;
        // Default `PromptSource::Embedded` — no `prompts.source` override
        // needed.
        let expires_at = Utc::now() + chrono::Duration::hours(1);

        let mut buf: Vec<u8> = Vec::new();
        run_async(&config, config_path.to_str().unwrap(), expires_at, &mut buf)
            .await
            .expect("setup succeeds");

        let captured = String::from_utf8(buf).expect("utf8");
        assert!(
            !captured.contains("prompt files:"),
            "embedded mode must not emit the prompts-directory line, got:\n{captured}",
        );

        // Setup does not upsert prompts — that responsibility belongs to
        // `serve`.
        let mut conn = pool.acquire().await.expect("acquire connection");
        assert_eq!(count_prompt_versions(&mut conn).await, 0);
    }

    #[tokio::test]
    async fn test_setup_disk_emits_prompts_directory_line_and_writes_files() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("create pool");

        let mut conn = pool.acquire().await.expect("acquire connection");
        truncate_all_tables(&mut conn).await;
        drop(conn);

        let prompts_dir = tempfile::tempdir().expect("prompts dir");
        let config_dir = tempfile::tempdir().expect("config dir");
        let config_path = config_dir.path().join("tribal.yaml");

        let mut config = TribalConfig::default();
        config.database.url = ctx.database_url().to_owned();
        config.database.max_connect_attempts = 1;
        config.prompts.source = PromptSource::Disk {
            directory: prompts_dir.path().to_string_lossy().into_owned(),
            hot_reload: false,
        };
        let expires_at = Utc::now() + chrono::Duration::hours(1);

        let mut buf: Vec<u8> = Vec::new();
        run_async(&config, config_path.to_str().unwrap(), expires_at, &mut buf)
            .await
            .expect("setup succeeds");

        let captured = String::from_utf8(buf).expect("utf8");
        let expected = format!("prompt files: {}", prompts_dir.path().display());
        assert!(
            captured.contains(&expected),
            "disk mode must emit the prompts-directory line, got:\n{captured}",
        );

        // The six embedded defaults must have been written to disk.
        for stage in ["extraction", "triage", "relation"] {
            for role in ["system.tera", "user.tera"] {
                let file = prompts_dir.path().join(stage).join(role);
                assert!(file.exists(), "missing prompt file: {}", file.display());
            }
        }

        let mut conn = pool.acquire().await.expect("acquire connection");
        assert_eq!(count_prompt_versions(&mut conn).await, 0);
    }
}
