//! Load-bearing display widths.
//!
//! Computed once at CLI startup using saturating arithmetic so narrow
//! terminals and non-TTY fallback never underflow. Terminal width is
//! supplied by the caller — the dispatcher probes the host terminal
//! at startup and feeds the result here. The
//! intra-column gutter (extra whitespace inside the name column to
//! keep adjacent column edges from visually colliding) reads from the
//! supplied `SpacingRamp` so a "compact" theme that tightens
//! `Spacing::Sm` shrinks the gutter consistently with the rest of the
//! layout.

use super::spacing::{Spacing, SpacingRamp};

/// Width consumed by glyph + timing column + padding around content.
pub const RESERVED_COLUMNS: usize = 30;

/// Minimum permissible name column width.
pub const MIN_NAME_WIDTH: usize = 20;

/// Upper bound on the name column width on any terminal.
pub const MAX_NAME_CEILING: usize = 72;

/// Fallback width used when no terminal is attached.
pub const NON_TTY_WIDTH: usize = 100;

/// Width of the right-aligned timing column. Sized to fit every
/// duration variant up to 100 hours with one column of separation.
pub const TIMING_COLUMN_WIDTH: usize = 8;

/// Default name-column width baked into [`Dimensions::default`].
/// Sized to fit realistic primary names with headroom so unit tests
/// and toy callers don't truncate. Production paths override this
/// through [`Dimensions::compute`] with the consumer's actual extents.
pub const DEFAULT_NAME_WIDTH: usize = 30;

/// Load-bearing column widths for a single output stream.
///
/// `name_column` is the primary identifier column (e.g. `tribal
/// check` populates it with check-phase names); `timing_column` is
/// the fixed right-hand duration column; `max_name` is the ceiling
/// any name column is clamped against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dimensions {
    pub name_column: usize,
    pub timing_column: usize,
    pub max_name: usize,
}

impl Dimensions {
    /// Compute load-bearing widths from terminal width and the
    /// consumer's name extents. Pass `NON_TTY_WIDTH` for
    /// `terminal_width` when no terminal is attached.
    /// `name_width_override` (when `Some`) short-circuits the
    /// terminal-derived `max_name` calculation — the operator-supplied
    /// value wins. The intra-column gutter is read from `spacing` via
    /// `Spacing::Sm`.
    #[must_use]
    pub fn compute(
        terminal_width: usize,
        max_name_width: usize,
        name_width_override: Option<usize>,
        spacing: &SpacingRamp,
    ) -> Self {
        let gutter = spacing.chars(Spacing::Sm);
        let computed_max = terminal_width
            .saturating_sub(RESERVED_COLUMNS)
            .clamp(MIN_NAME_WIDTH, MAX_NAME_CEILING);
        let max_name = name_width_override.unwrap_or(computed_max);
        Self {
            name_column: max_name_width.saturating_add(gutter).min(max_name),
            timing_column: TIMING_COLUMN_WIDTH,
            max_name,
        }
    }
}

impl Default for Dimensions {
    /// Baseline reflecting an average consumer shape — `name_column`
    /// sized via [`DEFAULT_NAME_WIDTH`] so the resulting
    /// [`Dimensions`] is usable in unit tests and toy callers without
    /// further configuration. Production paths still call
    /// [`Dimensions::compute`] with the consumer's exact extents.
    fn default() -> Self {
        Self::compute(
            NON_TTY_WIDTH,
            DEFAULT_NAME_WIDTH,
            None,
            &SpacingRamp::default(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ramp() -> SpacingRamp {
        SpacingRamp::default()
    }

    // -- saturation ---------------------------------------------------------

    #[test]
    fn test_narrow_terminal_clamps_to_min_name() {
        let d = Dimensions::compute(40, 100, None, &ramp());
        assert_eq!(d.max_name, MIN_NAME_WIDTH);
    }

    #[test]
    fn test_wide_terminal_clamps_to_max_name_ceiling() {
        let d = Dimensions::compute(500, 1000, None, &ramp());
        assert_eq!(d.max_name, MAX_NAME_CEILING);
    }

    #[test]
    fn test_zero_terminal_width_does_not_underflow() {
        let d = Dimensions::compute(0, 0, None, &ramp());
        assert_eq!(d.max_name, MIN_NAME_WIDTH);
    }

    // -- column ceilings ----------------------------------------------------

    #[test]
    fn test_name_column_capped_at_max_name() {
        let d = Dimensions::compute(80, 200, None, &ramp());
        assert_eq!(d.name_column, d.max_name);
    }

    #[test]
    fn test_timing_column_is_fixed() {
        let d = Dimensions::compute(80, 0, None, &ramp());
        assert_eq!(d.timing_column, TIMING_COLUMN_WIDTH);
    }

    // -- name-width override -----------------------------------------------

    #[test]
    fn test_name_width_override_replaces_terminal_derived_max() {
        let d = Dimensions::compute(120, 0, Some(40), &ramp());
        assert_eq!(d.max_name, 40);
    }

    #[test]
    fn test_name_width_override_clamps_name_column() {
        let d = Dimensions::compute(200, 60, Some(30), &ramp());
        assert_eq!(d.name_column, 30);
    }

    // -- gutter sourced from spacing ---------------------------------------

    #[test]
    fn test_gutter_tracks_spacing_sm() {
        let tight = SpacingRamp {
            sm: 1,
            ..SpacingRamp::default()
        };
        let wide = SpacingRamp {
            sm: 6,
            ..SpacingRamp::default()
        };
        let d_tight = Dimensions::compute(120, 10, None, &tight);
        let d_wide = Dimensions::compute(120, 10, None, &wide);
        assert_eq!(d_tight.name_column, 10 + tight.sm);
        assert_eq!(d_wide.name_column, 10 + wide.sm);
    }
}
