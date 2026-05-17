//! Shared fixtures and helpers for the bootstrap / mcp-config / credentials
//! integration suite.

use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};

use chrono::{Duration, Utc};
use sqlx::PgPool;
use tempfile::TempDir;
use tribal::{
    AppError, BootstrapOptions, McpConfigOptions, SetupOutcome, bootstrap_async, mcp_config_async,
    setup_async, token_create_async,
};
use tribal_config::{CliOverrides, ConfigPersistence, TransportKind, TribalConfig, load_config};
use tribal_domain::{BearerToken, GitRemote, LOCAL_PRINCIPAL_KEY};
use tribal_test_utils::{TestContext, truncate_all_tables};
use tribal_ui::Theme;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Fixture project name used across the suite.
pub(crate) const PROJECT_NAME: &str = "widgets";

/// Fixture token TTL — long enough that no test should hit expiry.
pub(crate) const TEST_TTL_HOURS: i64 = 24;

// ---------------------------------------------------------------------------
// Env-var lifecycle guard
// ---------------------------------------------------------------------------

/// Restores a process-global env var on drop.
///
/// Callers must hold [`tribal_test_utils::serial_lock`] for the lifetime
/// of the guard so concurrent tests do not observe the mutation. The
/// `set_var` / `remove_var` calls are wrapped in `unsafe` since Rust
/// 1.81 because the POSIX `setenv`/`getenv` pair is not thread-safe;
/// the serial lock is what makes the call sound here.
///
/// `#[must_use]` guards against the common mistake of constructing
/// the guard without binding — that would mutate the env then drop
/// the guard immediately, leaving the test running against restored
/// (i.e. unrelated) state.
#[must_use = "binding the guard is what scopes the env mutation"]
pub(crate) struct EnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
    /// Sets `key=value` for the lifetime of the guard.
    pub(crate) fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
        let previous = std::env::var_os(key);
        // SAFETY: callers hold `serial_lock`; the test binary serialises
        // mutations and no foreign thread is reading these vars during
        // the guarded scope.
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }

    /// Removes `key` for the lifetime of the guard.
    pub(crate) fn remove(key: &'static str) -> Self {
        let previous = std::env::var_os(key);
        // SAFETY: see `set`.
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            // SAFETY: invariants identical to `set` / `remove`.
            Some(v) => unsafe { std::env::set_var(self.key, v) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

/// Restores the process current directory on drop.
///
/// Used to force `detect_git_remote_from(".")` into the failure path
/// (tests can point cwd at a tempdir that has no `.git`). Callers
/// must hold [`tribal_test_utils::serial_lock`] since cwd is
/// process-global.
#[must_use = "binding the guard is what scopes the cwd mutation"]
pub(crate) struct CwdGuard {
    previous: PathBuf,
}

impl CwdGuard {
    /// Sets the process cwd to `dir` for the lifetime of the guard.
    pub(crate) fn set(dir: &Path) -> Self {
        let previous = std::env::current_dir().expect("current_dir");
        std::env::set_current_dir(dir).expect("set_current_dir");
        Self { previous }
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.previous);
    }
}

// ---------------------------------------------------------------------------
// Filesystem fixtures
// ---------------------------------------------------------------------------

/// Bundle of temp directories + env guards a test commonly needs:
/// an isolated `XDG_CONFIG_HOME`, a writable `config_dir`, and the
/// resolved `tribal.yaml` path inside it.
///
/// Fields are declared so guards drop before the temp directories
/// they referenced: env restoration is observed against the original
/// process state, and the now-pointer-dangling paths are then cleaned
/// up.
pub(crate) struct TestEnv {
    _xdg_guard: EnvGuard,
    _project_guard: EnvGuard,
    _config_guard: EnvGuard,
    pub(crate) xdg_dir: TempDir,
    // Held only so the config-file tempdir survives until the
    // [`TestEnv`] is dropped — accessed via `config_path` below.
    _config_dir: TempDir,
    pub(crate) config_path: PathBuf,
}

impl TestEnv {
    /// Constructs a fresh isolated environment. Caller must hold
    /// [`tribal_test_utils::serial_lock`].
    pub(crate) fn new() -> Self {
        let xdg_dir = tempfile::tempdir().expect("xdg tempdir");
        let config_dir = tempfile::tempdir().expect("config tempdir");
        let config_path = config_dir.path().join("tribal.yaml");

        let xdg_guard = EnvGuard::set("XDG_CONFIG_HOME", xdg_dir.path());
        // Clear any caller-supplied project / config env that could
        // bypass the cascade tests want to exercise.
        let project_guard = EnvGuard::remove("TRIBAL_PROJECT_ID");
        let config_guard = EnvGuard::remove("TRIBAL_CONFIG_PATH");

        Self {
            _xdg_guard: xdg_guard,
            _project_guard: project_guard,
            _config_guard: config_guard,
            xdg_dir,
            _config_dir: config_dir,
            config_path,
        }
    }

    /// Path the credentials.json file resolves to under this XDG home.
    pub(crate) fn credentials_path(&self) -> PathBuf {
        self.xdg_dir.path().join("tribal").join("credentials.json")
    }
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

/// Drains all rows from the testcontainer DB so the test starts from
/// a known-empty state. Runs through a pool acquired from the shared
/// context.
pub(crate) async fn fresh_db(ctx: &TestContext) -> PgPool {
    let pool = ctx.create_pool().await.expect("create pool");
    let mut conn = pool.acquire().await.expect("acquire");
    truncate_all_tables(&mut conn).await;
    drop(conn);
    pool
}

// ---------------------------------------------------------------------------
// Bootstrap / mcp-config drivers
// ---------------------------------------------------------------------------

/// Drives the full bootstrap pipeline — `load_config` →
/// `bootstrap_async` — mirroring the synchronous CLI wrapper. The
/// `overrides` argument is what `BootstrapArgs::into_cli_overrides`
/// would produce in production; the helper threads it through both
/// the figment cascade (so config-derived fields end up populated)
/// and the persistence renderer (so the YAML reflects user-supplied
/// flags). The testcontainer URL is injected via the command-defaults
/// layer so `cli_overrides.database` stays untouched and tests can
/// model `--database-url` independently. Validation runs inside
/// `setup::run_async`, so the helper does not call it here.
pub(crate) async fn run_bootstrap(
    ctx: &TestContext,
    config_path: &Path,
    overrides: CliOverrides,
    principal_key: Option<&str>,
    transport: TransportKind,
    json: bool,
) -> Result<(Vec<u8>, Vec<u8>), AppError> {
    let merged = load_test_config(ctx, config_path, overrides.clone())?;
    let expires_at = Utc::now() + Duration::hours(TEST_TTL_HOURS);
    let git_remote = fixture_git_remote();
    let theme = Theme::default_dark();
    let mut stdout = Vec::<u8>::new();
    let mut stderr = Vec::<u8>::new();
    bootstrap_async(
        BootstrapOptions {
            config: &merged,
            config_path,
            principal_key,
            expires_at,
            persisted_overrides: &overrides,
            git_remote: &git_remote,
            project_name: PROJECT_NAME,
            transport,
            json,
            theme: &theme,
        },
        &mut stdout,
        &mut stderr,
    )
    .await?;
    Ok((stdout, stderr))
}

/// Drives the full mcp-config pipeline — `load_config` →
/// `mcp_config_async` — with the same cascade fidelity as
/// [`run_bootstrap`]. mcp-config is a pure renderer and does not call
/// `validate`, so the helper skips that step too.
pub(crate) async fn run_mcp_config(
    ctx: &TestContext,
    config_path: &Path,
    overrides: CliOverrides,
    project_override: Option<String>,
    transport: TransportKind,
    explicit_token: Option<String>,
) -> Result<(Vec<u8>, Vec<u8>), AppError> {
    let merged = load_test_config(ctx, config_path, overrides)?;
    let mut stdout = Vec::<u8>::new();
    let mut stderr = Vec::<u8>::new();
    mcp_config_async(
        McpConfigOptions {
            config: &merged,
            config_path,
            project_override,
            transport,
            explicit_token,
        },
        &mut stdout,
        &mut stderr,
    )
    .await?;
    Ok((stdout, stderr))
}

/// Feeds `overrides` through `load_config` with the testcontainer URL
/// injected as a command-defaults layer. The merged config matches what
/// the production wrappers compute before entering `*_async`.
fn load_test_config(
    ctx: &TestContext,
    config_path: &Path,
    overrides: CliOverrides,
) -> Result<TribalConfig, AppError> {
    let db_defaults: [(&str, &str); 1] = [("database.url", ctx.database_url())];
    let path = config_path.to_str().expect("utf8 config path");
    Ok(load_config(path, Some(overrides), Some(&db_defaults))?)
}

/// Drives the full setup pipeline — `load_config` → `setup_async` —
/// mirroring the synchronous CLI wrapper. `setup_async` emits the
/// warn-and-success literal internally on its provided stderr.
pub(crate) async fn run_setup(
    ctx: &TestContext,
    config_path: &Path,
    principal_key: Option<&str>,
) -> Result<(SetupOutcome, Vec<u8>), AppError> {
    let merged = load_test_config(ctx, config_path, CliOverrides::default())?;
    let expires_at = Utc::now() + Duration::hours(TEST_TTL_HOURS);
    let mut stderr = Vec::<u8>::new();
    let outcome = setup_async(
        &merged,
        config_path,
        principal_key,
        expires_at,
        ConfigPersistence::Minimal,
        &mut stderr,
    )
    .await?;
    Ok((outcome, stderr))
}

/// Drives the full token-create pipeline — `load_config` →
/// `token_create_async` — mirroring the synchronous CLI wrapper.
/// token-create does not call `validate` in production, so the helper
/// skips it too. `token_create_async` emits the warn-and-success literal
/// internally on its provided stderr.
pub(crate) async fn run_token_create(
    ctx: &TestContext,
    config_path: &Path,
    principal_key: Option<&str>,
) -> Result<(BearerToken, Vec<u8>), AppError> {
    let merged = load_test_config(ctx, config_path, CliOverrides::default())?;
    let expires_at = Utc::now() + Duration::hours(TEST_TTL_HOURS);
    let mut stdout = Vec::<u8>::new();
    let mut stderr = Vec::<u8>::new();
    let outcome = token_create_async(
        &merged.database,
        principal_key.unwrap_or(LOCAL_PRINCIPAL_KEY),
        expires_at,
        &mut stdout,
        &mut stderr,
    )
    .await?;
    Ok((outcome.bearer_token, stderr))
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

/// Standard fixture git remote used across the suite.
pub(crate) fn fixture_git_remote() -> GitRemote {
    GitRemote::from_parts("github.com", "acme/widgets", None)
}

/// Parses a JSON byte slice or panics with the captured payload for
/// debugging.
pub(crate) fn parse_json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap_or_else(|err| {
        let preview = String::from_utf8_lossy(bytes);
        panic!("expected valid JSON, got error {err}: {preview}");
    })
}
