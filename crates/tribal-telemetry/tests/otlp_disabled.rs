//! Integration test: telemetry disabled returns no-op metrics.
//!
//! When `telemetry.enabled` is false, no OTLP trace or metrics layer
//! is added regardless of other settings.

use tribal_config::{LogFormat, LoggingConfig, TelemetryConfig};

#[test]
fn test_otlp_disabled_returns_noop_metrics() {
    let logging = LoggingConfig {
        format: LogFormat::Pretty,
        ..LoggingConfig::default()
    };
    let telemetry = TelemetryConfig {
        enabled: false,
        otlp_endpoint: Some("http://localhost:4317".to_owned()),
        ..TelemetryConfig::default()
    };

    let (_guard, metrics) =
        tribal_telemetry::init_subscriber(&logging, &telemetry).expect("init should succeed");

    // No-op instruments accept recordings without panic.
    metrics.tasks_completed.add(1, &[]);
    metrics.pool_acquire_wait_ms.record(42.0, &[]);
    metrics.tasks_queued.record(5, &[]);
}
