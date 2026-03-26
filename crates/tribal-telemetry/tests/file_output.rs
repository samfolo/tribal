//! Integration test: file output writes log events to the specified directory.
//!
//! This test lives in `tests/` (separate binary) because it installs a
//! global subscriber.

use tribal_config::{LogFormat, LogOutput, LoggingConfig, TelemetryConfig};

#[test]
fn test_file_output_writes_to_specified_directory() {
    let dir = tempfile::tempdir().expect("should create temp dir");

    let config = LoggingConfig {
        level: "info".to_owned(),
        format: LogFormat::Json,
        output: LogOutput::File,
        file_directory: dir.path().display().to_string(),
        ..LoggingConfig::default()
    };

    let (guard, _metrics) = tribal_telemetry::init_subscriber(&config, &TelemetryConfig::default())
        .expect("init should succeed");

    tracing::info!(target: "file_output_test", "hello from file output test");

    // Dropping the guard joins the non-blocking writer thread and flushes
    // all pending writes.
    drop(guard);

    // The rolling appender creates files in the directory — find and read
    // whichever file was created.
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .expect("should read dir")
        .filter_map(Result::ok)
        .collect();

    assert!(
        !entries.is_empty(),
        "at least one log file should be created in the directory"
    );

    let mut found = false;
    for entry in &entries {
        let content = std::fs::read_to_string(entry.path()).unwrap_or_default();
        if content.contains("hello from file output test") {
            found = true;
            break;
        }
    }

    assert!(found, "log file should contain the emitted event");

    // JSON format should produce files with a .jsonl suffix.
    for entry in &entries {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        assert!(
            name.ends_with(".jsonl"),
            "JSON format should produce .jsonl files, got: {name}",
        );
    }
}
