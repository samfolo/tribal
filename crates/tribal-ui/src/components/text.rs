//! Styled-text primitive. Inline component that owns the `style +
//! content + reset` SGR triple so callers never write it by hand.

use std::{
    borrow::Cow,
    io::{self, Write},
};

use anstyle::{Reset, Style};

use crate::{
    component::{Component, InlineComponent},
    render_ctx::RenderCtx,
};

pub struct Text<'a> {
    content: Cow<'a, str>,
    style: Style,
    pad_left: usize,
    pad_right: usize,
}

impl<'a> Text<'a> {
    #[must_use]
    pub fn new(content: impl Into<Cow<'a, str>>) -> Self {
        Self {
            content: content.into(),
            style: Style::new(),
            pad_left: 0,
            pad_right: 0,
        }
    }

    #[must_use]
    pub fn with_style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }

    /// Add `n` spaces of padding on the left, outside the SGR.
    #[must_use]
    pub fn with_pad_left(mut self, n: usize) -> Self {
        self.pad_left = n;
        self
    }

    /// Add `n` spaces of padding on the right, outside the SGR.
    #[must_use]
    pub fn with_pad_right(mut self, n: usize) -> Self {
        self.pad_right = n;
        self
    }

    /// Add `n` spaces of padding on both sides. Equivalent to
    /// `.with_pad_left(n).with_pad_right(n)`; a subsequent
    /// `with_pad_left` or `with_pad_right` overrides only the
    /// matching side.
    #[must_use]
    pub fn with_pad_x(mut self, n: usize) -> Self {
        self.pad_left = n;
        self.pad_right = n;
        self
    }
}

impl Component for Text<'_> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        if self.pad_left > 0 {
            write!(ctx, "{}", " ".repeat(self.pad_left))?;
        }
        if self.style == Style::new() {
            write!(ctx, "{}", self.content)?;
        } else {
            write!(
                ctx,
                "{}{}{}",
                self.style.render(),
                self.content,
                Reset.render(),
            )?;
        }
        if self.pad_right > 0 {
            write!(ctx, "{}", " ".repeat(self.pad_right))?;
        }
        Ok(())
    }
}

impl Text<'_> {
    /// Yield a `Text<'static>` for each input item with `style`
    /// applied. Returns an iterator rather than a `Vec` so call sites
    /// that pipe straight into another `IntoIterator` consumer don't
    /// pay for a transient allocation.
    pub fn style_iter<I, T>(items: I, style: Style) -> impl Iterator<Item = Text<'static>>
    where
        I: IntoIterator<Item = T>,
        T: ToString,
    {
        items
            .into_iter()
            .map(move |t| Text::new(t.to_string()).with_style(style))
    }

    /// Render this `Text` as a complete line: emit the current
    /// indent prefix, render the styled content, then a trailing
    /// newline. Mirrors the `writeln!` vs `write!` convention. Use
    /// when a `Text` stands as its own line (variant labels,
    /// sentinel messages); not for components that prepend their
    /// own indent.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`io::Error`] from the writer.
    pub fn renderln(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let indent = ctx.theme().indentation.prefix(ctx.indent());
        write!(ctx, "{indent}")?;
        self.render(ctx)?;
        writeln!(ctx)
    }
}

impl InlineComponent for Text<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::render_to_string;

    #[test]
    fn test_text_renders_content() {
        let out = render_to_string(&Text::new("hello"));
        assert!(out.contains("hello"));
    }

    #[test]
    fn test_styled_text_emits_reset() {
        let style = Style::new().bold();
        let out = render_to_string(&Text::new("hello").with_style(style));
        assert!(out.ends_with("\x1b[0m"));
    }

    #[test]
    fn test_render_does_not_emit_trailing_newline() {
        let out = render_to_string(&Text::new("hello"));
        assert!(!out.ends_with('\n'));
    }
}
