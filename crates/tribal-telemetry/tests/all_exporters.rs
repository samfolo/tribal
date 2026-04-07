//! Integration test: all three exporters coexist.
//!
//! Verifies that OTLP, console, and file exporters can all be active
//! simultaneously without error.
//!
//! Requires a tokio runtime because the OTLP gRPC exporter and batch
//! processors spawn background tasks via `tokio::spawn`.

use tribal_config::{LogFormat, LoggingConfig, TelemetryConfig};

#[tokio::test]
async fn test_all_exporters_initialise() {
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

    let _ = tribal_telemetry::init_subscriber(&logging, &telemetry).expect("init should succeed");
}
