//! Time-display component: elapsed, absolute, and relative times.
//!
//! Elapsed time delegates to [`crate::format::time::format_elapsed`]
//! — the canonical shape function — and pads the result for column
//! alignment via [`Dimensions::timing_column`](
//! crate::theme::Dimensions). Absolute time renders as RFC 3339 in
//! UTC. Relative time delegates to `chrono-humanize`'s `Display`
//! impl, which derives past/future phrasing from the signed
//! duration's sign internally (via `HumanTime::tense`).

use std::{io, time::Duration};

use chrono::{DateTime, Utc};
use chrono_humanize::HumanTime;
use unicode_width::UnicodeWidthStr;

use crate::{
    component::{Component, InlineComponent},
    components::Text,
    format::time::{Precision, format_absolute, format_elapsed},
    render_ctx::RenderCtx,
};

#[derive(Debug, Clone, Copy)]
pub enum TimeDisplay {
    Elapsed {
        duration: Duration,
        precision: Precision,
    },
    Absolute {
        datetime: DateTime<Utc>,
    },
    Relative {
        target: DateTime<Utc>,
        reference: DateTime<Utc>,
    },
}

impl Component for TimeDisplay {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let formatted = match *self {
            Self::Elapsed {
                duration,
                precision,
            } => format_elapsed(duration, precision),
            Self::Absolute { datetime } => format_absolute(datetime),
            Self::Relative { target, reference } => {
                let delta = target.signed_duration_since(reference);
                HumanTime::from(delta).to_string()
            }
        };
        let timing_column = ctx.theme().dimensions.timing_column;
        let pad_left = timing_column.saturating_sub(UnicodeWidthStr::width(formatted.as_str()));
        Text::new(formatted).with_pad_left(pad_left).render(ctx)
    }
}

impl InlineComponent for TimeDisplay {}

#[cfg(test)]
mod tests {
    use unicode_width::UnicodeWidthStr;

    use super::*;
    use crate::{test_support::render_to_string, theme::Theme};

    // -----------------------------------------------------------------
    // absolute and relative rendering
    // -----------------------------------------------------------------

    #[test]
    fn test_absolute_renders_rfc3339_utc_with_z() {
        let dt = DateTime::parse_from_rfc3339("2026-04-24T10:30:00Z")
            .expect("parse fixed datetime")
            .with_timezone(&Utc);
        let out = render_to_string(&TimeDisplay::Absolute { datetime: dt });
        assert_eq!(out, "2026-04-24T10:30:00Z");
    }

    #[test]
    fn test_relative_renders_humanised_text() {
        let now = DateTime::parse_from_rfc3339("2026-04-24T12:00:00Z")
            .expect("parse fixed datetime")
            .with_timezone(&Utc);
        let earlier = now - chrono::Duration::hours(4);
        let out = render_to_string(&TimeDisplay::Relative {
            target: earlier,
            reference: now,
        });
        assert!(out.contains("ago"), "expected past tense, got {out:?}");
    }

    // -----------------------------------------------------------------
    // column alignment
    // -----------------------------------------------------------------

    /// Short elapsed values right-align into `theme.dimensions.timing_column`
    /// so adjacent column edges line up cleanly.
    #[test]
    fn test_elapsed_render_pads_to_timing_column_width() {
        let timing_column = Theme::default_dark().dimensions.timing_column;
        let out = render_to_string(&TimeDisplay::Elapsed {
            duration: Duration::from_millis(1234),
            precision: Precision::Tenths,
        });
        assert_eq!(
            UnicodeWidthStr::width(out.as_str()),
            timing_column,
            "TimeDisplay::Elapsed should pad to timing_column ({timing_column}); got {out:?}",
        );
    }

    // -----------------------------------------------------------------
    // newline discipline
    // -----------------------------------------------------------------

    #[test]
    fn test_render_does_not_emit_trailing_newline() {
        let dt = DateTime::parse_from_rfc3339("2026-04-24T10:30:00Z")
            .expect("parse fixed datetime")
            .with_timezone(&Utc);
        let out = render_to_string(&TimeDisplay::Absolute { datetime: dt });
        assert!(!out.ends_with('\n'));
    }
}
