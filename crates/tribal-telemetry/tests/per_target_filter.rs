//! Integration test: per-target filter directives selectively enable levels.
//!
//! This test lives in `tests/` (separate binary) because it installs a
//! global subscriber.

use tribal_config::{LogFormat, LogOutput, LoggingConfig, TelemetryConfig};

#[test]
fn test_per_target_filter_directive() {
    let dir = tempfile::tempdir().expect("should create temp dir");

    // Only enable debug for `allowed_target`; everything else stays at error.
    let config = LoggingConfig {
        level: "error,allowed_target=debug".to_owned(),
        format: LogFormat::Json,
        output: LogOutput::File,
        file_directory: dir.path().display().to_string(),
        ..LoggingConfig::default()
    };

    let (guard, _metrics) =
        tribal_telemetry::init_subscriber(&config, &TelemetryConfig::default())
            .expect("init should succeed");

    // This debug event targets `allowed_target` — should appear.
    tracing::debug!(target: "allowed_target", "allowed debug message");

    // This debug event targets `blocked_target` — should NOT appear
    // because the default level is error.
    tracing::debug!(target: "blocked_target", "blocked debug message");

    // Dropping the guard joins the non-blocking writer thread and flushes
    // all pending writes.
    drop(guard);

    // Read whichever file was created by the rolling appender.
    let output: String = std::fs::read_dir(dir.path())
        .expect("should read dir")
        .filter_map(Result::ok)
        .map(|e| std::fs::read_to_string(e.path()).unwrap_or_default())
        .collect();

    assert!(
        output.contains("allowed debug message"),
        "debug event for 'allowed_target' should appear, but log was:\n{output}",
    );
    assert!(
        !output.contains("blocked debug message"),
        "debug event for 'blocked_target' should be filtered out, but log was:\n{output}",
    );
}
