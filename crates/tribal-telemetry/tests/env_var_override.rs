//! Integration test: `TRIBAL_LOG` environment variable overrides the config
//! level directive.
//!
//! This test lives in `tests/` (separate binary) because it installs a
//! global subscriber. It uses a subprocess re-exec pattern to set the
//! environment variable safely — avoiding `unsafe` calls to
//! `std::env::set_var` which are unsound in multi-threaded programs.

use tribal_telemetry::{LogFormat, LogOutput, LoggingConfig};

#[test]
fn test_tribal_log_env_var_overrides_config_level() {
    // When the sentinel is set we are the child process — run the real test.
    if std::env::var("__TRIBAL_ENV_OVERRIDE_CHILD").is_ok() {
        run_env_override_test();
        return;
    }

    // Parent: spawn ourselves as a subprocess with TRIBAL_LOG=debug.
    let exe = std::env::current_exe().expect("should resolve test binary path");
    let output = std::process::Command::new(exe)
        .arg("test_tribal_log_env_var_overrides_config_level")
        .arg("--exact")
        .arg("--nocapture")
        .env("TRIBAL_LOG", "debug")
        .env("__TRIBAL_ENV_OVERRIDE_CHILD", "1")
        .output()
        .expect("should spawn subprocess");

    assert!(
        output.status.success(),
        "child process should succeed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Actual test logic — runs inside the child process where `TRIBAL_LOG=debug`
/// is already set safely via `Command::env()`.
fn run_env_override_test() {
    let dir = tempfile::tempdir().expect("should create temp dir");
    let log_path = dir.path().join("test.log");

    // Config requests "error" level, but the inherited TRIBAL_LOG=debug
    // should override it.
    let config = LoggingConfig {
        level: "error".to_owned(),
        format: LogFormat::Json,
        output: LogOutput::File,
        file_path: Some(log_path.display().to_string()),
        include_llm_content: false,
    };

    let guard = tribal_telemetry::init_subscriber(config).expect("init should succeed");

    // Emit a debug-level event — should appear because TRIBAL_LOG=debug
    // overrides config level "error".
    tracing::debug!(target: "test_target", "debug message from env override test");

    // Dropping the guard joins the non-blocking writer thread and flushes
    // all pending writes — no sleep needed.
    drop(guard);

    let output = std::fs::read_to_string(&log_path).expect("should read log file");
    assert!(
        output.contains("debug message from env override test"),
        "debug event should appear when TRIBAL_LOG=debug overrides config level 'error', \
         but log output was:\n{output}",
    );
}
