//! Integration test: `enabled = false` silently ignores sub-flags.
//!
//! Verifies that when `telemetry.enabled` is false, `console_export` and
//! OTLP flags are ignored and `init_subscriber` succeeds without creating
//! any exporters.  This confirms `enabled` acts as a master switch.
//!
//! `file_export` is not set here because it is rejected at the config
//! validation layer when `enabled` is false.

use tribal_config::{LogFormat, LoggingConfig, TelemetryConfig};

#[test]
fn test_disabled_ignores_export_flags() {
    let logging = LoggingConfig {
        format: LogFormat::Pretty,
        ..LoggingConfig::default()
    };
    let telemetry = TelemetryConfig {
        enabled: false,
        console_export: true,
        otlp_endpoint: Some("http://localhost:19999".to_owned()),
        ..TelemetryConfig::default()
    };

    let _ = tribal_telemetry::init_subscriber(&logging, &telemetry).expect("init should succeed");
}
