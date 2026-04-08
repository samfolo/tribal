//! Integration test: all three exporters coexist.
//!
//! Verifies that OTLP, console, and file exporters can all be active
//! simultaneously, process spans, and flush on shutdown.
//!
//! Requires a tokio runtime because the OTLP gRPC exporter sets up a
//! tonic channel at init time.

use tribal_config::{LogFormat, LoggingConfig, TelemetryConfig};

#[tokio::test]
async fn test_all_exporters_coexist() {
    let dir = tempfile::tempdir().expect("should create temp dir");

    let logging = LoggingConfig {
        format: LogFormat::Pretty,
        ..LoggingConfig::default()
    };
    let telemetry = TelemetryConfig {
        enabled: true,
        console_export: true,
        file_export: true,
        file_directory: dir.path().display().to_string(),
        otlp_endpoint: Some("http://localhost:19999".to_owned()),
        ..TelemetryConfig::default()
    };

    {
        let (_guard, _) =
            tribal_telemetry::init_subscriber(&logging, &telemetry).expect("init should succeed");

        tracing::info_span!("all-exporters-span").in_scope(|| {
            tracing::info!("coexistence test");
        });

        // Guard drops here, triggering provider shutdown which flushes
        // all batch processors synchronously.
    }

    let content = std::fs::read_dir(dir.path())
        .expect("should read dir")
        .filter_map(Result::ok)
        .find(|e| e.path().extension().is_some_and(|ext| ext == "jsonl"))
        .map(|e| std::fs::read_to_string(e.path()).expect("should read trace file"));

    assert!(
        content.is_some_and(|c| c.contains("all-exporters-span")),
        "trace file should contain the exported span name",
    );
}
