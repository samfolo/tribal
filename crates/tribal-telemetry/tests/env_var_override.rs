//! Integration test: `TRIBAL_LOG` environment variable overrides the config
//! level directive.
//!
//! This test lives in `tests/` (separate binary) because it installs a
//! global subscriber and sets an environment variable that must not leak
//! into other tests.

use tribal_telemetry::{LogFormat, LogOutput, LoggingConfig};

#[test]
fn test_tribal_log_env_var_overrides_config_level() {
    // Configure for file output so we can capture and inspect the output.
    let dir = tempfile::tempdir().expect("should create temp dir");
    let log_path = dir.path().join("test.log");

    // Config requests "error" level, but env var sets "debug".
    let config = LoggingConfig {
        level: "error".to_owned(),
        format: LogFormat::Json,
        output: LogOutput::File,
        file_path: Some(log_path.display().to_string()),
        include_llm_content: false,
    };

    // Set the env var before initialising the subscriber.
    unsafe { std::env::set_var("TRIBAL_LOG", "debug") };

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

    // Clean up env var.
    unsafe { std::env::remove_var("TRIBAL_LOG") };
}
