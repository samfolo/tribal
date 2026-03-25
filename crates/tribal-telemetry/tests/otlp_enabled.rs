//! Integration test: OTLP pipeline initialises without panicking.
//!
//! Verifies that `init_subscriber` succeeds when `otlp_endpoint` is set
//! to a non-listening address.  The pipeline initialises, but export
//! silently fails — confirming no hard dependency on a running collector.

use tribal_config::{LogFormat, LoggingConfig, TelemetryConfig};

#[test]
fn test_otlp_enabled_with_unreachable_endpoint() {
    let logging = LoggingConfig {
        format: LogFormat::Pretty,
        ..LoggingConfig::default()
    };
    let telemetry = TelemetryConfig {
        enabled: true,
        otlp_endpoint: Some("http://localhost:19999".to_owned()),
        ..TelemetryConfig::default()
    };

    let (_guard, metrics) =
        tribal_telemetry::init_subscriber(&logging, &telemetry).expect("init should succeed");

    // Instruments accept recordings without panic even when the endpoint
    // is unreachable — export failures are handled internally.
    metrics.tasks_completed.add(1, &[]);
    metrics.pool_acquire_wait_ms.record(42.0, &[]);
    metrics.tasks_queued.record(5, &[]);
}
