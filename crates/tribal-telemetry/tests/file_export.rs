//! Integration test: file export creates trace files.
//!
//! Verifies that `init_subscriber` succeeds when `file_export` is enabled
//! and that a `.jsonl` trace file is created in the configured directory.
//!
//! Requires a tokio runtime because `with_batch_exporter` spawns a
//! background task via `tokio::spawn`.

use tribal_config::{LogFormat, LoggingConfig, TelemetryConfig};

#[tokio::test]
async fn test_file_export_creates_trace_file() {
    let dir = tempfile::tempdir().expect("should create temp dir");

    let logging = LoggingConfig {
        format: LogFormat::Pretty,
        ..LoggingConfig::default()
    };
    let telemetry = TelemetryConfig {
        enabled: true,
        console_export: false,
        file_export: true,
        file_directory: dir.path().display().to_string(),
        otlp_endpoint: None,
        ..TelemetryConfig::default()
    };

    {
        let (_guard, _) =
            tribal_telemetry::init_subscriber(&logging, &telemetry).expect("init should succeed");

        // Emit a span so the batch exporter has something to flush.
        tracing::info_span!("test-file-export").in_scope(|| {
            tracing::info!("trace file test");
        });

        // Guard drops here, triggering provider shutdown which flushes
        // all batch processors synchronously.
    }

    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .expect("should read dir")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "jsonl"))
        .collect();

    assert!(
        !entries.is_empty(),
        "expected at least one .jsonl file in {}",
        dir.path().display(),
    );

    // Verify the file contains exported span data, not just that the
    // appender created an empty file.
    let content = std::fs::read_to_string(entries[0].path()).expect("should read trace file");
    assert!(
        content.contains("test-file-export"),
        "trace file should contain the exported span name",
    );
}
