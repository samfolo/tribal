//! Integration test: pretty format produces human-readable (non-JSON) output.
//!
//! This test lives in `tests/` (separate binary) because it installs a
//! global subscriber.

use tribal_config::{LogFormat, LogOutput, LoggingConfig, TelemetryConfig};

#[test]
fn test_pretty_format_produces_non_json_output() {
    let dir = tempfile::tempdir().expect("should create temp dir");

    let config = LoggingConfig {
        level: "info".to_owned(),
        format: LogFormat::Pretty,
        output: LogOutput::File,
        file_directory: dir.path().display().to_string(),
        ..LoggingConfig::default()
    };

    let (guard, _metrics) = tribal_telemetry::init_subscriber(&config, &TelemetryConfig::default())
        .expect("init should succeed");

    tracing::info!(target: "pretty_test", "human readable event");

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
        output.contains("human readable event"),
        "pretty output should contain the event message, but got:\n{output}",
    );

    // Pretty format should produce files with a .log suffix.
    let filenames: Vec<_> = std::fs::read_dir(dir.path())
        .expect("should read dir")
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    for name in &filenames {
        assert!(
            name.ends_with(".log"),
            "Pretty format should produce .log files, got: {name}",
        );
    }

    // Pretty output should not be valid JSON — each line is human-readable.
    for line in output.lines().filter(|l| !l.trim().is_empty()) {
        let is_json = serde_json::from_str::<serde_json::Value>(line).is_ok();
        assert!(
            !is_json,
            "pretty format should not produce JSON lines, but got valid JSON:\n{line}",
        );
    }
}
