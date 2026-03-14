//! Integration test: JSON format produces structured JSONL output with the
//! expected fields.
//!
//! This test lives in `tests/` (separate binary) because it installs a
//! global subscriber.

use tribal_config::{LogFormat, LogOutput, LoggingConfig};

#[test]
fn test_json_format_produces_structured_output() {
    let dir = tempfile::tempdir().expect("should create temp dir");

    let config = LoggingConfig {
        level: "info".to_owned(),
        format: LogFormat::Json,
        output: LogOutput::File,
        file_directory: dir.path().display().to_string(),
        ..LoggingConfig::default()
    };

    let guard = tribal_telemetry::init_subscriber(&config).expect("init should succeed");

    // Emit an event inside a span so we can verify span context is included.
    let span = tracing::info_span!("test_span");
    let _enter = span.enter();
    tracing::info!(target: "json_format_test", "structured event");
    drop(_enter);

    // Dropping the guard joins the non-blocking writer thread and flushes
    // all pending writes.
    drop(guard);

    // Read whichever file was created by the rolling appender.
    let output: String = std::fs::read_dir(dir.path())
        .expect("should read dir")
        .filter_map(Result::ok)
        .map(|e| std::fs::read_to_string(e.path()).unwrap_or_default())
        .collect();

    // Each line should be valid JSON.
    let mut found_event = false;
    for line in output.lines() {
        let value: serde_json::Value =
            serde_json::from_str(line).expect("each line should be valid JSON");

        if let Some(fields) = value.get("fields")
            && let Some(message) = fields.get("message")
            && message.as_str() == Some("structured event")
        {
            found_event = true;

            // Verify required structural fields.
            assert!(
                value.get("level").is_some(),
                "JSON line should contain 'level'"
            );
            assert!(
                value.get("target").is_some(),
                "JSON line should contain 'target'"
            );

            // Verify span context is present.
            assert!(
                value.get("span").is_some() || value.get("spans").is_some(),
                "JSON line should contain span context ('span' or 'spans'), got: {value}",
            );
        }
    }

    assert!(
        found_event,
        "should find the 'structured event' in JSON output, but log was:\n{output}",
    );
}
