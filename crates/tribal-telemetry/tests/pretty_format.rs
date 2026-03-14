//! Integration test: pretty format produces human-readable (non-JSON) output.
//!
//! This test lives in `tests/` (separate binary) because it installs a
//! global subscriber.

use tribal_telemetry::{LogFormat, LogOutput, LoggingConfig};

#[test]
fn test_pretty_format_produces_non_json_output() {
    let dir = tempfile::tempdir().expect("should create temp dir");
    let log_path = dir.path().join("pretty.log");

    let config = LoggingConfig {
        level: "info".to_owned(),
        format: LogFormat::Pretty,
        output: LogOutput::File,
        file_path: Some(log_path.display().to_string()),
        include_llm_content: false,
    };

    let guard = tribal_telemetry::init_subscriber(&config).expect("init should succeed");

    tracing::info!(target: "pretty_test", "human readable event");

    // Dropping the guard joins the non-blocking writer thread and flushes
    // all pending writes.
    drop(guard);

    let output = std::fs::read_to_string(&log_path).expect("should read log file");

    assert!(
        output.contains("human readable event"),
        "pretty output should contain the event message, but got:\n{output}",
    );

    // Pretty output should not be valid JSON — each line is human-readable.
    for line in output.lines().filter(|l| !l.trim().is_empty()) {
        let is_json = serde_json::from_str::<serde_json::Value>(line).is_ok();
        assert!(
            !is_json,
            "pretty format should not produce JSON lines, but got valid JSON:\n{line}",
        );
    }
}
