//! Shared fixtures and helpers for the `tribal check` integration suite.

use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};

use sqlx::PgPool;
use tempfile::TempDir;
use tribal::{AppError, CheckOptions, CheckOutput, check_async};
use tribal_config::TribalConfig;
use tribal_test_utils::TestDb;
use tribal_ui::Theme;

// ---------------------------------------------------------------------------
// Env-var lifecycle guard
// ---------------------------------------------------------------------------

/// Restores a process-global env var on drop. The `unsafe` `set_var` /
/// `remove_var` are sound only while the test holds `env_lock`, which
/// serialises every env-touching test in this binary.
#[must_use = "binding the guard is what scopes the env mutation"]
pub(crate) struct EnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
    pub(crate) fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
        let previous = std::env::var_os(key);
        // SAFETY: `setenv` is not thread-safe, but every test in this binary
        // that touches the process environment holds `env_lock` for its whole
        // body, serialising them. So no other thread reads or writes the
        // environment during the guarded scope.
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
    /// Constructs a fresh isolated environment scoped to a single test.
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

/// Builds a connection pool against this test's isolated database, which
/// starts empty.
pub(crate) async fn fresh_db(ctx: &TestDb) -> PgPool {
    ctx.create_pool().await.expect("create pool")
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
/// captured streams plus the assembled [`CheckOutput`] so callers can
/// assert on either form independently (and on `output.ok` without
/// re-parsing the JSON payload).
pub(crate) async fn run_check(
    run: CheckRun<'_>,
) -> Result<(Vec<u8>, Vec<u8>, CheckOutput), AppError> {
    let theme = Theme::default_dark();
    let mut stdout = Vec::<u8>::new();
    let mut stderr = Vec::<u8>::new();
    let output = check_async(
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
    Ok((stdout, stderr, output))
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
