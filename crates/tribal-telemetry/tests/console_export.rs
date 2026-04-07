//! Integration test: console export initialises without error.
//!
//! Verifies that `init_subscriber` succeeds when `console_export` is
//! enabled.  Spans are written to stderr as OTLP JSON lines via the
//! `WriterSpanExporter` batch pipeline.

use tribal_config::{LogFormat, LoggingConfig, TelemetryConfig};

#[test]
fn test_console_export_initialises() {
    let logging = LoggingConfig {
        format: LogFormat::Pretty,
        ..LoggingConfig::default()
    };
    let telemetry = TelemetryConfig {
        enabled: true,
        console_export: true,
        file_export: false,
        otlp_endpoint: None,
        ..TelemetryConfig::default()
    };

    let _ = tribal_telemetry::init_subscriber(&logging, &telemetry).expect("init should succeed");
}
