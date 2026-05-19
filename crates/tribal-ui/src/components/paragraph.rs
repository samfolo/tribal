//! Wrapped block of body text.
//!
//! Word-aware line breaks via [`textwrap`]; each emitted line carries
//! the active render indentation and the supplied style.  The final
//! line emits no trailing newline so callers control vertical spacing.

use std::io::{self, Write};

use anstyle::Style;

use crate::{component::Component, components::Text, render_ctx::RenderCtx};

pub struct Paragraph<'a> {
    text: &'a str,
    width: usize,
    style: Option<Style>,
}

impl<'a> Paragraph<'a> {
    /// Default wrap width — the classic typographic legibility ceiling
    /// for body text.  Overridable per call via [`Self::with_width`].
    pub const DEFAULT_WIDTH: usize = 80;

    /// Construct a paragraph wrapping `text` at the default width
    /// ([`Self::DEFAULT_WIDTH`]).
    #[must_use]
    pub fn new(text: &'a str) -> Self {
        Self {
            text,
            width: Self::DEFAULT_WIDTH,
            style: None,
        }
    }

    /// Override the wrap width.  Pass the total visible-line budget
    /// including the indent prefix; the renderer subtracts the active
    /// indent so wrapped content fits the declared width even at
    /// deeper nesting.
    #[must_use]
    pub fn with_width(mut self, width: usize) -> Self {
        self.width = width;
        self
    }

    /// Override the per-line text style.  Defaults to the theme's body
    /// typography.
    #[must_use]
    pub fn with_style(mut self, style: Style) -> Self {
        self.style = Some(style);
        self
    }
}

impl Component for Paragraph<'_> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        if self.text.is_empty() {
            return Ok(());
        }
        let style = self.style.unwrap_or(ctx.theme().typography.body);
        let indent_chars = ctx.theme().indentation.chars(ctx.indent());
        let indent = ctx.theme().indentation.prefix(ctx.indent());
        let body_width = self.width.saturating_sub(indent_chars).max(1);
        let lines = textwrap::wrap(self.text, body_width);
        let last = lines.len().saturating_sub(1);
        for (i, line) in lines.iter().enumerate() {
            write!(ctx, "{indent}")?;
            Text::new(line.as_ref()).with_style(style).render(ctx)?;
            if i < last {
                writeln!(ctx)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use unicode_width::UnicodeWidthStr;

    use super::*;
    use crate::{
        test_support::{render_to_string, render_with_theme},
        theme::{Capability, Mode, Theme, ThemeSelection},
    };

    fn monochrome_theme() -> Theme {
        Theme::for_selection(ThemeSelection {
            capability: Capability::None,
            is_tty: false,
            mode: Mode::Dark,
        })
    }

    #[test]
    fn test_short_text_renders_on_one_line() {
        let out = render_with_theme(&Paragraph::new("hello world"), &monochrome_theme());
        assert_eq!(out, "hello world");
    }

    #[test]
    fn test_long_text_wraps_at_overridden_width() {
        let text = "the quick brown fox jumps over the lazy dog";
        let out = render_with_theme(&Paragraph::new(text).with_width(20), &monochrome_theme());
        assert!(out.contains('\n'), "expected wrapping in: {out:?}");
        for line in out.lines() {
            assert!(
                UnicodeWidthStr::width(line) <= 20,
                "line exceeds 20 cols: {line:?}",
            );
        }
    }

    #[test]
    fn test_empty_text_renders_nothing() {
        let out = render_to_string(&Paragraph::new(""));
        assert!(out.is_empty(), "expected empty render, got {out:?}");
    }

    #[test]
    fn test_render_does_not_emit_trailing_newline() {
        let out = render_to_string(
            &Paragraph::new("the quick brown fox jumps over the lazy dog").with_width(20),
        );
        assert!(!out.ends_with('\n'));
    }
}
