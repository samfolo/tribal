//! Glyph set: status and structural characters.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlyphSet {
    /// Outcome glyph for a passed case.
    pub pass: char,
    /// Outcome glyph for a failed case.
    pub fail: char,
    /// Outcome glyph for an errored case.
    pub error: char,
    /// Outcome glyph for a warning.
    pub warning: char,
    /// Outcome glyph for a skipped case.
    pub skipped: char,
    /// Status glyph for an in-progress case.
    pub active: char,
    /// Status glyph for a planned-but-not-started case.
    pub pending: char,
    /// Static placeholder for the running state when animation is off.
    pub running_static: char,
    /// Truncation suffix for fitted text that exceeds its column.
    pub ellipsis: char,
    /// Leading marker for items in a bullet list.
    pub bullet: char,
    /// Inline-cell separator (the default `HStack` separator).
    pub middot: char,
    /// Leading marker for a child line nested under a parent.
    pub nested: char,
    /// Vertical-bar tree connector for a continuation row.
    pub vertical_bar: char,
    /// Up-and-right corner tree connector — sits flush below
    /// `vertical_bar` to mark a last-child row.
    pub tree_corner: char,
    /// Horizontal-rule character for section dividers.
    pub section_rule: char,
    /// Spinner template for any in-progress indicator.
    pub running: SpinnerTemplate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpinnerTemplate {
    pub frames: &'static [&'static str],
    pub interval: Duration,
}

const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Spinner cadence: fast enough to feel responsive, slow enough that
/// individual frames remain readable rather than blurring together.
pub const SPINNER_INTERVAL: Duration = Duration::from_millis(80);

impl Default for GlyphSet {
    fn default() -> Self {
        Self {
            pass: '✓',
            fail: '×',
            error: '◬',
            warning: '◬',
            skipped: '⊝',
            active: '●',
            pending: '◌',
            running_static: '⋯',
            ellipsis: '…',
            bullet: '•',
            middot: '·',
            nested: '↳',
            vertical_bar: '│',
            tree_corner: '└',
            section_rule: '─',
            running: SpinnerTemplate {
                frames: SPINNER_FRAMES,
                interval: SPINNER_INTERVAL,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use unicode_width::UnicodeWidthChar;

    use super::*;

    // -----------------------------------------------------------------
    // glyph metrics
    // -----------------------------------------------------------------

    #[test]
    fn test_default_glyphs_are_single_width() {
        let g = GlyphSet::default();
        for c in [
            g.pass,
            g.fail,
            g.error,
            g.warning,
            g.skipped,
            g.active,
            g.pending,
            g.running_static,
            g.ellipsis,
            g.bullet,
            g.middot,
            g.nested,
            g.vertical_bar,
            g.tree_corner,
            g.section_rule,
        ] {
            assert_eq!(
                c.width().expect("glyphs must have a defined width"),
                1,
                "glyph {c:?} (U+{:04X}) is not single-width",
                u32::from(c),
            );
        }
    }

    #[test]
    fn test_default_spinner_frames_are_single_width() {
        let g = GlyphSet::default();
        for frame in g.running.frames {
            let chars: Vec<char> = frame.chars().collect();
            assert_eq!(
                chars.len(),
                1,
                "spinner frame {frame:?} should be exactly one char",
            );
            assert_eq!(
                chars[0]
                    .width()
                    .expect("spinner frame must have a defined width"),
                1,
                "spinner frame {frame:?} is not single-width",
            );
        }
    }
}
