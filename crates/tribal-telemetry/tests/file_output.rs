//! Integration test: file output writes log events to the specified path.
//!
//! This test lives in `tests/` (separate binary) because it installs a
//! global subscriber.

use tribal_config::{LogFormat, LogOutput, LoggingConfig};

#[test]
fn test_file_output_writes_to_specified_path() {
    let dir = tempfile::tempdir().expect("should create temp dir");
    let log_path = dir.path().join("tribal.log");

    let config = LoggingConfig {
        level: "info".to_owned(),
        format: LogFormat::Json,
        output: LogOutput::File,
        file_path: Some(log_path.display().to_string()),
        include_llm_content: false,
    };

    let guard = tribal_telemetry::init_subscriber(&config).expect("init should succeed");

    tracing::info!(target: "file_output_test", "hello from file output test");

    // Dropping the guard joins the non-blocking writer thread and flushes
    // all pending writes.
    drop(guard);

    let output = std::fs::read_to_string(&log_path).expect("should read log file");
    assert!(
        output.contains("hello from file output test"),
        "log file should contain the emitted event, but contents were:\n{output}",
    );
}
