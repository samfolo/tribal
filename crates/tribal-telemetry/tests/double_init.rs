//! Integration test: calling `init_subscriber` twice returns an error.
//!
//! This test lives in `tests/` (separate binary) because
//! `set_global_default` is process-global and cannot be reset between
//! inline unit tests.

use tribal_telemetry::{LogFormat, LoggingConfig, TelemetryError};

#[test]
fn test_init_subscriber_twice_returns_already_initialised() {
    let config = LoggingConfig {
        format: LogFormat::Pretty,
        ..LoggingConfig::default()
    };

    let _guard = tribal_telemetry::init_subscriber(&config).expect("first init should succeed");

    let result = tribal_telemetry::init_subscriber(&config);
    assert!(
        matches!(result, Err(TelemetryError::SubscriberAlreadyInitialised)),
        "second init should return SubscriberAlreadyInitialised, got {result:?}",
    );
}
