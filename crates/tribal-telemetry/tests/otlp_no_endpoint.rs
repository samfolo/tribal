//! Integration test: telemetry enabled but no endpoint returns no-op metrics.
//!
//! When `otlp_endpoint` is `None`, no OTLP exporter is installed and
//! no external connection is attempted.

use tribal_config::{LogFormat, LoggingConfig, TelemetryConfig};

#[test]
fn test_otlp_no_endpoint_returns_noop_metrics() {
    let logging = LoggingConfig {
        format: LogFormat::Pretty,
        ..LoggingConfig::default()
    };
    let telemetry = TelemetryConfig {
        enabled: true,
        otlp_endpoint: None,
        ..TelemetryConfig::default()
    };

    let (_guard, metrics) =
        tribal_telemetry::init_subscriber(&logging, &telemetry).expect("init should succeed");

    // No-op instruments accept recordings without panic.
    metrics.tasks_completed.add(1, &[]);
    metrics.pool_acquire_wait_ms.record(42.0, &[]);
}
