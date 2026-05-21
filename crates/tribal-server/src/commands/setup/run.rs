//! Core setup flow: entry point and async orchestration.

use std::{
    io::{self, Write},
    path::Path,
};

use chrono::{DateTime, Utc};
use tribal_common::sha256_hex;
use tribal_config::{ConfigPersistence, PromptSource, TribalConfig};
use tribal_db::{AuthTokenRepository, NewAuthToken, PgAuthTokenRepository};
use tribal_domain::{BearerToken, LOCAL_PRINCIPAL_KEY, full_access_scopes};

use super::{config_file, outcome::SetupOutcome, output};
use crate::{
    cli::SetupArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, CredentialsPersistOutcome, DATABASE_COMMAND_DEFAULTS,
        TIMESTAMP_FORMAT, find_or_create_principal, generate_raw_token, persist_credentials,
        prepare_config, resolve_absolute_config_path, resolve_ttl,
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
pub(crate) fn run(config_path: &str, mut args: SetupArgs) -> Result<(), AppError> {
    let principal = args.principal.take();
    let ttl = args.ttl;
    let cli_overrides = args.into_cli_overrides();
    let config = prepare_config(config_path, cli_overrides, &DATABASE_COMMAND_DEFAULTS)?;

    let expires_at = Utc::now() + resolve_ttl(ttl, config.auth.token_ttl_hours)?;
    let absolute_config_path = resolve_absolute_config_path(config_path)?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let mut stderr = io::stderr().lock();
    rt.block_on(run_async(
        &config,
        &absolute_config_path,
        principal.as_deref(),
        expires_at,
        ConfigPersistence::Minimal,
        &mut stderr,
    ))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Executes the setup steps asynchronously.
///
/// `principal_key` is the user-supplied `--principal` override. When
/// `Some(key)` and `key != LOCAL_PRINCIPAL_KEY`, the token is issued
/// against that principal; `principal:local` is still ensured to satisfy
/// the stdio-transport invariant in [`tribal_mcp::auth::Authenticator`].
///
/// # Errors
///
/// Returns an [`AppError`] if database connection, migrations,
/// principal resolution, config-file writing, or token insertion
/// fails.
pub async fn run_async(
    config: &TribalConfig,
    config_path: &Path,
    principal_key: Option<&str>,
    expires_at: DateTime<Utc>,
    persistence: ConfigPersistence<'_>,
    out: &mut dyn Write,
) -> Result<SetupOutcome, AppError> {
    let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(config_dir)
        .await
        .map_err(|source| AppError::Io {
            context: format!("create config directory {}", config_dir.display()),
            source,
        })?;
    output::config_directory(out, &config_dir.to_string_lossy());

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

    // Disk prompt-file IO runs after migrations so a setup that fails
    // at the database or migration step does not leave a half-written
    // prompts directory or emit the prompts-directory line.
    if let PromptSource::Disk { directory, .. } = &config.prompts.source {
        let prompts_dir = Path::new(directory);
        ensure_prompt_files(prompts_dir).await?;
        output::prompt_files(out, &prompts_dir.to_string_lossy());
    }

    let mut conn = pool.acquire().await.map_err(|err| {
        AppError::pool_acquire(POOL_NAME_SETUP, "acquiring setup connection", err)
    })?;

    let local_principal = find_or_create_principal(&mut conn, LOCAL_PRINCIPAL_KEY).await?;
    output::principal(out, local_principal.principal_key());

    let token_principal = match principal_key {
        None | Some(LOCAL_PRINCIPAL_KEY) => local_principal,
        Some(other_key) => {
            let principal = find_or_create_principal(&mut conn, other_key).await?;
            output::principal(out, principal.principal_key());
            principal
        }
    };

    // Compute and write the config file BEFORE the token insert so a
    // render or filesystem failure leaves no DB token row behind. After
    // the insert succeeds the only remaining fallible step is the
    // instructions writeln; if that fails, the user can rerun
    // `tribal token create` against an already-valid config file.
    let content = persistence
        .render(config)
        .map_err(|source| AppError::Config { source })?;
    let config_file = config_file::write_if_absent(config_path, config, &content).await?;

    let raw_token = generate_raw_token();
    let token_hash = sha256_hex(&raw_token);

    let new_token = NewAuthToken::builder()
        .token_hash(token_hash)
        .principal_id(token_principal.id())
        .scopes(full_access_scopes())
        .expires_at(expires_at)
        .build();

    PgAuthTokenRepository
        .insert(&mut conn, &new_token)
        .await
        .map_err(|source| AppError::Database { source })?;

    drop(conn);

    // Persist credentials.json before any post-insert output so the
    // file remains a recoverable artefact if a later writeln fails.
    let bearer_token: BearerToken =
        raw_token
            .parse()
            .map_err(|source| AppError::TokenVerification {
                reason: "generated bearer token failed parse validation".into(),
                source: Box::new(source),
            })?;
    let credentials = persist_credentials(&bearer_token);

    output::config_file(out, &config_file);
    output::token_created(out, &expires_at.format(TIMESTAMP_FORMAT).to_string());
    let instructions_result = output::instructions(out, bearer_token.as_str());

    // Emit the credentials warning best-effort regardless of whether
    // instructions succeeded — otherwise a broken stderr would
    // suppress the only signal that credentials.json wasn't written.
    if let CredentialsPersistOutcome::Failed { warning } = &credentials {
        let _ = writeln!(out, "{warning}");
    }

    instructions_result.map_err(|source| AppError::Io {
        context: "writing bearer token output".into(),
        source,
    })?;

    Ok(SetupOutcome {
        bearer_token,
        principal_key: token_principal.principal_key().to_owned(),
        principal_id: token_principal.id(),
        config_file,
        credentials,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
// `Jail::expect_with` closures return `Result<(), figment::Error>` (208 bytes),
// which we cannot reduce without wrapping an upstream type.
#[allow(clippy::result_large_err)]
mod tests {
    use figment::Jail;
    use tribal_test_utils::{
        assert_text_snapshot, count_prompt_versions, serial_lock, test_context, truncate_all_tables,
    };

    use super::*;

    /// Builds a current-thread runtime for tests that run inside a sync
    /// `figment::Jail` closure but still need to drive async setup work.
    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test runtime")
    }

    /// Normalises the run-time-dynamic substrings in setup's captured
    /// stderr so the result is suitable for snapshot comparison: the
    /// tempdir path, the expiry timestamp, and the bearer token.
    fn normalise_setup_stderr(
        captured: &str,
        config_dir: &Path,
        expires_at: DateTime<Utc>,
        bearer_token: &BearerToken,
    ) -> String {
        captured
            .replace(&config_dir.display().to_string(), "<CONFIG_DIR>")
            .replace(
                &expires_at.format(TIMESTAMP_FORMAT).to_string(),
                "<TIMESTAMP>",
            )
            .replace(bearer_token.as_str(), "<TOKEN>")
    }

    #[test]
    fn test_setup_embedded_omits_prompts_directory_line() {
        let rt = test_runtime();
        let _serial_guard = rt.block_on(serial_lock());
        Jail::expect_with(|jail| {
            let xdg = tempfile::tempdir().expect("xdg tempdir");
            jail.set_env("XDG_CONFIG_HOME", xdg.path().to_str().expect("utf8 xdg"));
            rt.block_on(async {
                let ctx = test_context().await;
                let pool = ctx.create_pool().await.expect("create pool");

                let mut conn = pool.acquire().await.expect("acquire connection");
                truncate_all_tables(&mut conn).await;
                drop(conn);

                let config_dir = tempfile::tempdir().expect("config dir");
                let config_path = config_dir.path().join("tribal.yaml");

                let mut config = TribalConfig::minimum_valid(ctx.database_url());
                config.database.max_connect_attempts = 1;
                // Default `PromptSource::Embedded` — no `prompts.source`
                // override needed.
                let expires_at = Utc::now() + chrono::Duration::hours(1);

                let mut buf: Vec<u8> = Vec::new();
                run_async(
                    &config,
                    &config_path,
                    None,
                    expires_at,
                    ConfigPersistence::Minimal,
                    &mut buf,
                )
                .await
                .expect("setup succeeds");

                let captured = String::from_utf8(buf).expect("utf8");
                assert!(
                    !captured.contains("prompt files:"),
                    "embedded mode must not emit the prompts-directory line, got:\n{captured}",
                );

                // Credentials must land inside the tempdir, not the
                // developer's real `~/.config/tribal/credentials.json`.
                assert!(
                    xdg.path().join("tribal").join("credentials.json").exists(),
                    "credentials.json must be written under the tempdir XDG",
                );

                // Setup does not upsert prompts — that responsibility
                // belongs to `serve`.
                let mut conn = pool.acquire().await.expect("acquire connection");
                assert_eq!(count_prompt_versions(&mut conn).await, 0);
            });
            Ok(())
        });
    }

    #[test]
    fn test_setup_disk_emits_prompts_directory_line_and_writes_files() {
        let rt = test_runtime();
        let _serial_guard = rt.block_on(serial_lock());
        Jail::expect_with(|jail| {
            let xdg = tempfile::tempdir().expect("xdg tempdir");
            jail.set_env("XDG_CONFIG_HOME", xdg.path().to_str().expect("utf8 xdg"));
            rt.block_on(async {
                let ctx = test_context().await;
                let pool = ctx.create_pool().await.expect("create pool");

                let mut conn = pool.acquire().await.expect("acquire connection");
                truncate_all_tables(&mut conn).await;
                drop(conn);

                let prompts_dir = tempfile::tempdir().expect("prompts dir");
                let config_dir = tempfile::tempdir().expect("config dir");
                let config_path = config_dir.path().join("tribal.yaml");

                let mut config = TribalConfig::minimum_valid(ctx.database_url());
                config.database.max_connect_attempts = 1;
                config.prompts.source = PromptSource::Disk {
                    directory: prompts_dir.path().to_string_lossy().into_owned(),
                    hot_reload: false,
                };
                let expires_at = Utc::now() + chrono::Duration::hours(1);

                let mut buf: Vec<u8> = Vec::new();
                run_async(
                    &config,
                    &config_path,
                    None,
                    expires_at,
                    ConfigPersistence::Minimal,
                    &mut buf,
                )
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

                assert!(
                    xdg.path().join("tribal").join("credentials.json").exists(),
                    "credentials.json must be written under the tempdir XDG",
                );

                let mut conn = pool.acquire().await.expect("acquire connection");
                assert_eq!(count_prompt_versions(&mut conn).await, 0);
            });
            Ok(())
        });
    }

    #[tokio::test]
    async fn test_setup_disk_writes_no_prompts_when_database_unreachable() {
        // Port 1 is privileged on every supported platform, so nothing
        // listens there; `create_pool` fails fast with
        // `max_connect_attempts = 1`. No testcontainers handle is needed.
        let prompts_dir = tempfile::tempdir().expect("prompts dir");
        let config_dir = tempfile::tempdir().expect("config dir");
        let config_path = config_dir.path().join("tribal.yaml");

        let mut config = TribalConfig::minimum_valid("postgres://invalid:invalid@127.0.0.1:1/x");
        config.database.max_connect_attempts = 1;
        config.prompts.source = PromptSource::Disk {
            directory: prompts_dir.path().to_string_lossy().into_owned(),
            hot_reload: false,
        };
        let expires_at = Utc::now() + chrono::Duration::hours(1);

        let mut buf: Vec<u8> = Vec::new();
        let result = run_async(
            &config,
            &config_path,
            None,
            expires_at,
            ConfigPersistence::Minimal,
            &mut buf,
        )
        .await;
        assert!(
            result.is_err(),
            "setup must fail when the database is unreachable",
        );

        let captured = String::from_utf8(buf).expect("utf8");
        assert!(
            !captured.contains("prompt files:"),
            "no prompts-directory line should appear when the database fails first, got:\n{captured}",
        );

        let entries: Vec<_> = std::fs::read_dir(prompts_dir.path())
            .expect("read prompts dir")
            .collect();
        assert!(
            entries.is_empty(),
            "prompts dir must remain empty when the database fails first, got {} entries",
            entries.len(),
        );
    }

    // -- Snapshot locks -----------------------------------------------------

    #[test]
    fn test_setup_stderr_default_principal_matches_snapshot() {
        let rt = test_runtime();
        let _serial_guard = rt.block_on(serial_lock());
        Jail::expect_with(|jail| {
            let xdg = tempfile::tempdir().expect("xdg tempdir");
            jail.set_env("XDG_CONFIG_HOME", xdg.path().to_str().expect("utf8 xdg"));
            rt.block_on(async {
                let ctx = test_context().await;
                let pool = ctx.create_pool().await.expect("create pool");

                let mut conn = pool.acquire().await.expect("acquire connection");
                truncate_all_tables(&mut conn).await;
                drop(conn);

                let config_dir = tempfile::tempdir().expect("config dir");
                let config_path = config_dir.path().join("tribal.yaml");

                let mut config = TribalConfig::minimum_valid(ctx.database_url());
                config.database.max_connect_attempts = 1;
                let expires_at = Utc::now() + chrono::Duration::hours(1);

                let mut buf: Vec<u8> = Vec::new();
                let outcome = run_async(
                    &config,
                    &config_path,
                    None,
                    expires_at,
                    ConfigPersistence::Minimal,
                    &mut buf,
                )
                .await
                .expect("setup succeeds");

                assert!(
                    xdg.path().join("tribal").join("credentials.json").exists(),
                    "credentials.json must be written under the tempdir XDG",
                );

                let captured = String::from_utf8(buf).expect("utf8");
                let normalised = normalise_setup_stderr(
                    &captured,
                    config_dir.path(),
                    expires_at,
                    &outcome.bearer_token,
                );

                assert_text_snapshot!(
                    &normalised,
                    "src/commands/setup/snapshots/stderr-default-principal.txt"
                );
            });
            Ok(())
        });
    }

    #[test]
    fn test_setup_stderr_explicit_principal_matches_snapshot() {
        let rt = test_runtime();
        let _serial_guard = rt.block_on(serial_lock());
        Jail::expect_with(|jail| {
            let xdg = tempfile::tempdir().expect("xdg tempdir");
            jail.set_env("XDG_CONFIG_HOME", xdg.path().to_str().expect("utf8 xdg"));
            rt.block_on(async {
                let ctx = test_context().await;
                let pool = ctx.create_pool().await.expect("create pool");

                let mut conn = pool.acquire().await.expect("acquire connection");
                truncate_all_tables(&mut conn).await;
                drop(conn);

                let config_dir = tempfile::tempdir().expect("config dir");
                let config_path = config_dir.path().join("tribal.yaml");

                let mut config = TribalConfig::minimum_valid(ctx.database_url());
                config.database.max_connect_attempts = 1;
                let expires_at = Utc::now() + chrono::Duration::hours(1);

                let mut buf: Vec<u8> = Vec::new();
                let outcome = run_async(
                    &config,
                    &config_path,
                    Some("user:alice"),
                    expires_at,
                    ConfigPersistence::Minimal,
                    &mut buf,
                )
                .await
                .expect("setup succeeds");

                assert!(
                    xdg.path().join("tribal").join("credentials.json").exists(),
                    "credentials.json must be written under the tempdir XDG",
                );

                let captured = String::from_utf8(buf).expect("utf8");
                let normalised = normalise_setup_stderr(
                    &captured,
                    config_dir.path(),
                    expires_at,
                    &outcome.bearer_token,
                );

                assert_text_snapshot!(
                    &normalised,
                    "src/commands/setup/snapshots/stderr-explicit-principal.txt"
                );
            });
            Ok(())
        });
    }
}
