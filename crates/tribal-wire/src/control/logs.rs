//! The `logs.*` crossings: a bounded tail of recent lines, and the line shape
//! the live `logs.line` event also carries.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Log lines
// ---------------------------------------------------------------------------

/// A log line's severity. The closed `tracing` level set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    /// The most verbose level.
    Trace,
    /// Debug-level detail.
    Debug,
    /// Ordinary operational information.
    Info,
    /// A recoverable concern.
    Warn,
    /// A failure.
    Error,
}

/// One captured log line, as the ring buffer holds it and the `logs.line` event
/// streams it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LogLine {
    /// When the line was emitted.
    pub at: DateTime<Utc>,
    /// Its severity.
    pub level: LogLevel,
    /// The emitting module target.
    pub target: String,
    /// The rendered message.
    pub message: String,
}

// ---------------------------------------------------------------------------
// logs.tail
// ---------------------------------------------------------------------------

/// Parameters for `logs.tail`: how many trailing lines to return.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LogsTailRequest {
    /// The number of most-recent lines to return, capped by the ring's size.
    pub lines: u32,
}

/// The trailing lines `logs.tail` returns, oldest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LogLines {
    /// The lines, in emission order.
    pub lines: Vec<LogLine>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_log_line_round_trips() {
        let line = LogLine {
            at: DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp"),
            level: LogLevel::Warn,
            target: "tribal_server::control".to_owned(),
            message: "peer credential mismatch refused".to_owned(),
        };
        let parsed: LogLine = serde_json::from_str(&serde_json::to_string(&line).unwrap()).unwrap();
        assert_eq!(parsed, line);
    }

    #[test]
    fn test_an_unknown_log_level_is_rejected() {
        assert!(
            serde_json::from_value::<LogLevel>(serde_json::json!("verbose")).is_err(),
            "an unknown level must be rejected",
        );
    }
}
