//! Horizontal divider with an optional label.
//!
//! Rule width is read from the theme's `Dimensions` plus a `Spacing::Md`
//! gutter on each side — the rule never extends beyond the active
//! content column block.

use std::io::{self, Write};

use anstyle::Style;
use unicode_width::UnicodeWidthStr;

use crate::{
    component::{Component, InlineComponent},
    components::Text,
    render_ctx::RenderCtx,
    theme::Spacing,
    widths::truncate,
};

/// Where the label sits within a labelled rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelAlign {
    /// Equal glyph runs on both sides of the label.
    Center,
    /// Two leading glyphs, then label, then glyphs out to the rule width.
    Left,
}

#[derive(Debug, Clone)]
pub struct SectionRule {
    pub label: Option<String>,
    pub align: LabelAlign,
    pub style: Style,
}

impl SectionRule {
    #[must_use]
    pub fn plain() -> Self {
        Self {
            label: None,
            align: LabelAlign::Center,
            style: Style::new(),
        }
    }

    #[must_use]
    pub fn labelled(label: impl Into<String>) -> Self {
        Self {
            label: Some(label.into()),
            align: LabelAlign::Center,
            style: Style::new(),
        }
    }

    /// Override the rule's label alignment. No-op on a [`Self::plain`]
    /// rule (alignment only affects labelled rules).
    #[must_use]
    pub fn with_alignment(mut self, align: LabelAlign) -> Self {
        self.align = align;
        self
    }

    /// Override the rule's foreground style. The default is
    /// `Style::new()` (no styling); callers that want the theme's
    /// separator colour pass it explicitly so the component never
    /// has to know about themes.
    #[must_use]
    pub fn with_style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }
}

impl Component for SectionRule {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let (indent_prefix, glyph, width) = {
            let theme = ctx.theme();
            (
                theme.indentation.prefix(ctx.indent()),
                theme.glyphs.section_rule,
                rule_width(ctx),
            )
        };
        write!(ctx, "{indent_prefix}")?;
        let theme = ctx.theme();
        let content = match &self.label {
            None => std::iter::repeat_n(glyph, width).collect::<String>(),
            Some(label) => {
                let max_label_width = width.saturating_sub(2);
                let label_text = truncate(label, max_label_width, theme.glyphs.ellipsis);
                let label_with_padding_width = UnicodeWidthStr::width(label_text.as_str()) + 2;
                let total_glyphs = width.saturating_sub(label_with_padding_width);
                match self.align {
                    LabelAlign::Center => {
                        let left = total_glyphs / 2;
                        let right = total_glyphs - left;
                        let left_rule: String = std::iter::repeat_n(glyph, left).collect();
                        let right_rule: String = std::iter::repeat_n(glyph, right).collect();
                        format!("{left_rule} {label_text} {right_rule}")
                    }
                    LabelAlign::Left => {
                        const LEADING_GLYPHS: usize = 2;
                        let leading: String = std::iter::repeat_n(glyph, LEADING_GLYPHS).collect();
                        let trailing_glyphs = total_glyphs.saturating_sub(LEADING_GLYPHS);
                        let trailing: String =
                            std::iter::repeat_n(glyph, trailing_glyphs).collect();
                        format!("{leading} {label_text} {trailing}")
                    }
                }
            }
        };
        Text::new(content).with_style(self.style).render(ctx)
    }
}

impl InlineComponent for SectionRule {}

fn rule_width(ctx: &RenderCtx) -> usize {
    let theme = ctx.theme();
    let gutter = theme.spacing.chars(Spacing::Md);
    let indent_chars = theme.indentation.chars(ctx.indent());
    theme
        .dimensions
        .max_name
        .saturating_add(theme.dimensions.timing_column)
        .saturating_add(gutter.saturating_mul(2))
        .saturating_sub(indent_chars)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test_support::render_to_string, theme::Theme};

    #[test]
    fn test_plain_rule_contains_section_glyph() {
        let out = render_to_string(&SectionRule::plain());
        assert!(out.contains('─'));
    }

    #[test]
    fn test_labelled_rule_contains_label_and_glyphs() {
        let out = render_to_string(&SectionRule::labelled("Failures"));
        assert!(out.contains("Failures"));
        assert!(out.contains('─'));
    }

    #[test]
    fn test_render_does_not_emit_trailing_newline() {
        let out = render_to_string(&SectionRule::plain());
        assert!(!out.ends_with('\n'));
    }

    /// Two labels with the same display width must produce rules of
    /// equal display width — verifies that label measurement uses
    /// `UnicodeWidthStr::width`, not codepoint count. The CJK label
    /// has 2 codepoints but display width 4; without the fix, this
    /// test would fail because `chars().count()` measures 2.
    #[test]
    fn test_labelled_rule_uses_display_width_for_cjk() {
        let cjk = render_to_string(&SectionRule::labelled("中文"));
        let ascii = render_to_string(&SectionRule::labelled("XXXX"));
        assert_eq!(
            UnicodeWidthStr::width(cjk.as_str()),
            UnicodeWidthStr::width(ascii.as_str()),
            "rule with CJK label '中文' (display width 4) should match rule with 'XXXX' (display width 4)",
        );
    }

    /// A label longer than the configured rule width gets truncated
    /// so the line still fits. Both rules wear the same `Style`, so
    /// the SGR overhead cancels in the comparison. The label size
    /// is anchored to `theme.dimensions.max_name * 4` — comfortably
    /// larger than any `rule_width` the default dimensions can
    /// produce (`max_name + timing_column + 2 * gutter`).
    #[test]
    fn test_labelled_rule_truncates_label_longer_than_rule_width() {
        let label_size = Theme::default_dark().dimensions.max_name * 4;
        let long = render_to_string(&SectionRule::labelled("X".repeat(label_size)));
        let plain = render_to_string(&SectionRule::plain());
        assert_eq!(
            UnicodeWidthStr::width(long.as_str()),
            UnicodeWidthStr::width(plain.as_str()),
            "rule with an overflowing label should truncate to match the plain rule's width",
        );
    }
}
