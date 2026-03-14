//! Integration test: per-target filter directives selectively enable levels.
//!
//! This test lives in `tests/` (separate binary) because it installs a
//! global subscriber.

use tribal_telemetry::{LogFormat, LogOutput, LoggingConfig};

#[test]
fn test_per_target_filter_directive() {
    let dir = tempfile::tempdir().expect("should create temp dir");
    let log_path = dir.path().join("filter.log");

    // Only enable debug for `allowed_target`; everything else stays at error.
    let config = LoggingConfig {
        level: "error,allowed_target=debug".to_owned(),
        format: LogFormat::Json,
        output: LogOutput::File,
        file_path: Some(log_path.display().to_string()),
        include_llm_content: false,
    };

    let guard = tribal_telemetry::init_subscriber(&config).expect("init should succeed");

    // This debug event targets `allowed_target` — should appear.
    tracing::debug!(target: "allowed_target", "allowed debug message");

    // This debug event targets `blocked_target` — should NOT appear
    // because the default level is error.
    tracing::debug!(target: "blocked_target", "blocked debug message");

    // Dropping the guard joins the non-blocking writer thread and flushes
    // all pending writes.
    drop(guard);

    let output = std::fs::read_to_string(&log_path).expect("should read log file");

    assert!(
        output.contains("allowed debug message"),
        "debug event for 'allowed_target' should appear, but log was:\n{output}",
    );
    assert!(
        !output.contains("blocked debug message"),
        "debug event for 'blocked_target' should be filtered out, but log was:\n{output}",
    );
}
