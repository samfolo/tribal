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

    // No-op recorder methods accept recordings without panic.
    metrics.record_task_completed("test", 0.0);
    metrics.record_pool_acquire("test", std::time::Duration::from_millis(42));
}
