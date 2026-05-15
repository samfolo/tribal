//! Time-shape vocabulary and pure formatting functions.
//!
//! [`Precision`] is the elapsed-time precision axis the theme picks
//! defaults for and individual presenters override per-call.
//! [`format_elapsed`] is the only place the elapsed-string shape
//! lives — both [`TimeDisplay::Elapsed`](crate::time_display::TimeDisplay)
//! and naked-text callers route through it so the user sees one rule.
//!
//! Output is the canonical form, never leading-padded for column
//! alignment. Consumers that want column alignment (the status-line
//! timing column) pad the result themselves.

use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};

/// Canonical absolute-time render: RFC 3339 with second precision
/// and the trailing `Z` for UTC. Pairs with [`format_elapsed`] as the
/// other canonical shape function in this module.
#[must_use]
pub fn format_absolute(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Precision {
    Seconds,
    #[default]
    Tenths,
    Milliseconds,
}

/// Format `duration` as a canonical elapsed string with no leading
/// whitespace and no zero-padding on the trailing component.
/// Variant boundaries (1 min, 1 h) keep the result short enough to
/// fit `Dimensions::timing_column`. Runs over 100 hours overflow
/// that budget by one character; a multi-day run is itself a signal
/// that something is wrong.
#[must_use]
pub fn format_elapsed(duration: Duration, precision: Precision) -> String {
    if duration < Duration::from_mins(1) {
        let ms = duration.as_millis();
        match precision {
            Precision::Seconds => format!("{}s", ms / 1000),
            Precision::Tenths => {
                let tenths = ms / 100;
                format!("{}.{}s", tenths / 10, tenths % 10)
            }
            Precision::Milliseconds => format!("{}.{:03}s", ms / 1000, ms % 1000),
        }
    } else if duration < Duration::from_hours(1) {
        let secs = duration.as_secs();
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        let secs = duration.as_secs();
        format!("{}h {}m", secs / 3600, (secs / 60) % 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_elapsed_under_minute_tenths() {
        assert_eq!(
            format_elapsed(Duration::from_millis(1234), Precision::Tenths),
            "1.2s",
        );
    }

    #[test]
    fn test_format_elapsed_under_minute_seconds() {
        assert_eq!(
            format_elapsed(Duration::from_millis(12340), Precision::Seconds),
            "12s",
        );
    }

    #[test]
    fn test_format_elapsed_under_minute_milliseconds() {
        assert_eq!(
            format_elapsed(Duration::from_millis(1234), Precision::Milliseconds),
            "1.234s",
        );
    }

    #[test]
    fn test_format_elapsed_minutes_and_seconds() {
        assert_eq!(
            format_elapsed(Duration::from_secs(563), Precision::Tenths),
            "9m 23s",
        );
    }

    #[test]
    fn test_format_elapsed_minutes_and_seconds_omits_zero_pad_on_seconds() {
        assert_eq!(
            format_elapsed(Duration::from_secs(63), Precision::Tenths),
            "1m 3s",
        );
    }

    #[test]
    fn test_format_elapsed_hours_and_minutes() {
        assert_eq!(
            format_elapsed(Duration::from_mins(83), Precision::Tenths),
            "1h 23m",
        );
    }
}
