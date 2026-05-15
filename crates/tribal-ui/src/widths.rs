//! Display-width helpers. Measurement uses
//! `unicode_width::UnicodeWidthStr::width`; truncation is
//! grapheme-safe via `unicode-segmentation`.

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

/// Left-align `text` within a column of `width` display columns,
/// padding with spaces on the right. If `text` already exceeds the
/// width, return it unchanged — callers truncate explicitly.
#[must_use]
pub fn pad_right(text: &str, width: usize) -> String {
    let actual = UnicodeWidthStr::width(text);
    if actual >= width {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len() + (width - actual));
    out.push_str(text);
    out.push_str(&" ".repeat(width - actual));
    out
}

/// Right-align `text` within a column of `width` display columns,
/// padding with spaces on the left.
#[must_use]
pub fn pad_left(text: &str, width: usize) -> String {
    let actual = UnicodeWidthStr::width(text);
    if actual >= width {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len() + (width - actual));
    out.push_str(&" ".repeat(width - actual));
    out.push_str(text);
    out
}

/// Take the leading `width` display columns of `text` without
/// appending an ellipsis. Grapheme-safe. If `text` already fits,
/// return it unchanged. Use [`truncate`] when an ellipsis is wanted.
#[must_use]
pub fn preview(text: &str, width: usize) -> String {
    let actual = UnicodeWidthStr::width(text);
    if actual <= width {
        return text.to_owned();
    }
    let mut acc: usize = 0;
    let mut out = String::with_capacity(text.len());
    for cluster in text.graphemes(true) {
        let cw = UnicodeWidthStr::width(cluster);
        if acc + cw > width {
            break;
        }
        out.push_str(cluster);
        acc += cw;
    }
    out
}

/// Truncate `text` to fit within `width` display columns, appending
/// `ellipsis` at the cut point. Grapheme-safe. If `text` already
/// fits, return it unchanged. If `width` cannot fit even the
/// ellipsis, return the ellipsis alone.
#[must_use]
pub fn truncate(text: &str, width: usize, ellipsis: char) -> String {
    if UnicodeWidthStr::width(text) <= width {
        return text.to_owned();
    }
    let ellipsis_width = UnicodeWidthChar::width(ellipsis).unwrap_or(1);
    if width <= ellipsis_width {
        return ellipsis.to_string();
    }
    let mut out = preview(text, width - ellipsis_width);
    out.push(ellipsis);
    out
}

/// Truncate then pad — fit `text` into exactly `width` display
/// columns: truncated with `ellipsis` if too long, right-padded with
/// spaces if too short.
#[must_use]
pub fn fit(text: &str, width: usize, ellipsis: char) -> String {
    let truncated = truncate(text, width, ellipsis);
    pad_right(&truncated, width)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------
    // padding
    // -----------------------------------------------------------------

    #[test]
    fn test_pad_right_ascii_pads_with_spaces() {
        assert_eq!(pad_right("ab", 5), "ab   ");
    }

    #[test]
    fn test_pad_right_double_width_uses_display_columns() {
        // "中" is double-width, so pad to 5 columns adds 3 spaces.
        assert_eq!(pad_right("中", 5), "中   ");
    }

    #[test]
    fn test_pad_right_passes_through_when_already_wider() {
        assert_eq!(pad_right("abcdef", 4), "abcdef");
    }

    #[test]
    fn test_pad_left_ascii_pads_on_the_left() {
        assert_eq!(pad_left("ab", 5), "   ab");
    }

    // -----------------------------------------------------------------
    // truncation
    // -----------------------------------------------------------------

    #[test]
    fn test_truncate_returns_unchanged_when_fits() {
        assert_eq!(truncate("abc", 5, '…'), "abc");
    }

    #[test]
    fn test_truncate_appends_ellipsis_when_over() {
        assert_eq!(truncate("abcdef", 4, '…'), "abc…");
    }

    #[test]
    fn test_truncate_keeps_combining_sequence_intact() {
        let text = "e\u{0301}xtra";
        assert_eq!(truncate(text, 3, '…'), "e\u{0301}x…");
    }

    #[test]
    fn test_truncate_emits_only_ellipsis_when_width_too_small() {
        assert_eq!(truncate("abcdef", 1, '…'), "…");
    }

    // -----------------------------------------------------------------
    // fit
    // -----------------------------------------------------------------

    #[test]
    fn test_fit_truncates_long_input() {
        assert_eq!(fit("abcdef", 4, '…'), "abc…");
    }

    #[test]
    fn test_fit_pads_short_input() {
        assert_eq!(fit("ab", 5, '…'), "ab   ");
    }
}
