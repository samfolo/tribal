//! Integration test: the handle returned by subscriber init reloads the level
//! filter of the installed subscriber, live.
//!
//! This test lives in `tests/` (separate binary) because it installs a
//! global subscriber.

use tokio::sync::broadcast;
use tribal_config::{LoggingConfig, TelemetryConfig};

#[test]
fn test_the_returned_handle_reloads_the_installed_subscriber_live() {
    let (events, _) = broadcast::channel(8);
    // Scoped to this test's target so background threads cannot add lines to
    // the ring between the reload and the read.
    let config = LoggingConfig {
        level: "error,level_reload=info".to_owned(),
        ..LoggingConfig::default()
    };

    let (_guard, _metrics, ring, log_filter) = tribal_telemetry::init_subscriber_with_log_bridge(
        &config,
        &TelemetryConfig::default(),
        events,
    )
    .expect("init succeeds");

    // Below and above the threshold on both sides of the reload.
    tracing::debug!("below before");
    tracing::info!("above before");
    log_filter
        .reload("error,level_reload=debug")
        .expect("the reload succeeds");
    tracing::debug!("below after");

    let messages: Vec<String> = ring
        .tail(usize::MAX)
        .into_iter()
        .map(|line| line.message)
        .collect();
    assert_eq!(
        messages,
        vec!["above before", "below after"],
        "debug is filtered before the reload and admitted after",
    );
}
