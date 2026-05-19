//! Shared fixtures and helpers for the `tribal check` integration suite.

use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};

use sqlx::PgPool;
use tempfile::TempDir;
use tribal::{AppError, CheckOptions, check_async};
use tribal_config::TribalConfig;
use tribal_test_utils::{TestContext, truncate_all_tables};
use tribal_ui::Theme;

// ---------------------------------------------------------------------------
// Env-var lifecycle guard
// ---------------------------------------------------------------------------

#[must_use = "binding the guard is what scopes the env mutation"]
pub(crate) struct EnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
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

// ---------------------------------------------------------------------------
// Filesystem fixtures
// ---------------------------------------------------------------------------

/// Bundle of temp directories + env guards covering the env knobs that
/// would otherwise bleed across tests.  Fields are declared so guards
/// drop before the temp directories they referenced.
pub(crate) struct TestEnv {
    _xdg_guard: EnvGuard,
    _project_guard: EnvGuard,
    _token_guard: EnvGuard,
    _config_guard: EnvGuard,
    _xdg_dir: TempDir,
    _config_dir: TempDir,
    pub(crate) config_path: PathBuf,
}

impl TestEnv {
    /// Constructs a fresh isolated environment.  Caller must hold
    /// [`tribal_test_utils::serial_lock`].
    pub(crate) fn new() -> Self {
        let xdg_dir = tempfile::tempdir().expect("xdg tempdir");
        let config_dir = tempfile::tempdir().expect("config tempdir");
        let config_path = config_dir.path().join("tribal.yaml");

        let xdg_guard = EnvGuard::set("XDG_CONFIG_HOME", xdg_dir.path());
        let project_guard = EnvGuard::remove("TRIBAL_PROJECT_ID");
        let token_guard = EnvGuard::remove("TRIBAL_AUTH_TOKEN");
        let config_guard = EnvGuard::remove("TRIBAL_CONFIG_PATH");

        Self {
            _xdg_guard: xdg_guard,
            _project_guard: project_guard,
            _token_guard: token_guard,
            _config_guard: config_guard,
            _xdg_dir: xdg_dir,
            _config_dir: config_dir,
            config_path,
        }
    }
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

/// Drains all rows from the testcontainer DB so the test starts from a
/// known-empty state.
pub(crate) async fn fresh_db(ctx: &TestContext) -> PgPool {
    let pool = ctx.create_pool().await.expect("create pool");
    let mut conn = pool.acquire().await.expect("acquire");
    truncate_all_tables(&mut conn).await;
    drop(conn);
    pool
}

// ---------------------------------------------------------------------------
// Config-fixture helpers
// ---------------------------------------------------------------------------

/// Serialises `config` to YAML and writes it to `config_path`.  Tests
/// build the [`TribalConfig`] inline (via [`TribalConfig::minimum_valid`]
/// plus any per-test mutations) so the invariant under test reads
/// alongside the assertion that depends on it.
pub(crate) fn write_config(config_path: &Path, config: &TribalConfig) {
    let yaml = config.to_yaml().expect("serialise config");
    std::fs::write(config_path, yaml).expect("write config");
}

// ---------------------------------------------------------------------------
// Check driver
// ---------------------------------------------------------------------------

/// Bundle of inputs the integration suite repeatedly threads in.
pub(crate) struct CheckRun<'a> {
    pub config_path: &'a Path,
    pub json: bool,
    pub providers: bool,
    pub project: Option<&'a str>,
    pub token: Option<&'a str>,
}

/// Drives `check_async` with captured stdout and stderr.  Returns the
/// captured streams so callers can assert on either form independently.
pub(crate) async fn run_check(run: CheckRun<'_>) -> Result<(Vec<u8>, Vec<u8>), AppError> {
    let theme = Theme::default_dark();
    let mut stdout = Vec::<u8>::new();
    let mut stderr = Vec::<u8>::new();
    check_async(
        CheckOptions {
            config_path: run.config_path,
            json: run.json,
            providers: run.providers,
            project: run.project,
            token: run.token,
            theme: &theme,
        },
        &mut stdout,
        &mut stderr,
    )
    .await?;
    Ok((stdout, stderr))
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

/// Parses a JSON byte slice or panics with the captured payload for
/// debugging.
pub(crate) fn parse_json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap_or_else(|err| {
        let preview = String::from_utf8_lossy(bytes);
        panic!("expected valid JSON, got error {err}: {preview}");
    })
}

/// Returns the `status` strings in the order they appear in `output`.
pub(crate) fn statuses(output: &serde_json::Value) -> Vec<String> {
    output["checks"]
        .as_array()
        .expect("checks is array")
        .iter()
        .map(|c| c["status"].as_str().expect("status is string").to_owned())
        .collect()
}

/// Returns the `name` strings in the order they appear in `output`.
pub(crate) fn names(output: &serde_json::Value) -> Vec<String> {
    output["checks"]
        .as_array()
        .expect("checks is array")
        .iter()
        .map(|c| c["name"].as_str().expect("name is string").to_owned())
        .collect()
}

/// Returns the `(name, status)` of the first row whose name matches.
pub(crate) fn row_status<'a>(output: &'a serde_json::Value, name: &str) -> Option<&'a str> {
    output["checks"]
        .as_array()?
        .iter()
        .find(|c| c["name"].as_str() == Some(name))
        .and_then(|c| c["status"].as_str())
}
