//! Single-line status: status glyph + name + optional detail + optional timing.

use std::io::{self, Write};

use anstyle::Style;

use crate::{
    component::{Component, ThemedComponent},
    components::{Status, Text},
    render_ctx::RenderCtx,
    theme::Theme,
    time_display::TimeDisplay,
    widths::fit,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusLineStyles {
    pub label: Style,
    pub detail: Style,
    pub name_column: usize,
    pub ellipsis: char,
}

pub struct StatusLine<'a> {
    status: Status,
    name: &'a str,
    detail: Option<&'a str>,
    timing: Option<TimeDisplay>,
}

impl<'a> StatusLine<'a> {
    #[must_use]
    pub fn new(status: Status, name: &'a str) -> Self {
        Self {
            status,
            name,
            detail: None,
            timing: None,
        }
    }

    #[must_use]
    pub fn with_detail(mut self, detail: &'a str) -> Self {
        self.detail = Some(detail);
        self
    }

    #[must_use]
    pub fn with_timing(mut self, timing: TimeDisplay) -> Self {
        self.timing = Some(timing);
        self
    }
}

impl ThemedComponent for StatusLine<'_> {
    type Styles = StatusLineStyles;

    fn resolve_styles(theme: &Theme) -> Self::Styles {
        StatusLineStyles {
            label: theme.typography.body,
            detail: theme.typography.dim_label,
            name_column: theme.dimensions.name_column,
            ellipsis: theme.glyphs.ellipsis,
        }
    }
}

impl Component for StatusLine<'_> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let theme = ctx.theme();
        let styles = Self::resolve_styles(theme);
        let glyph_style = self.status.style(&theme.palette, &theme.typography);
        let glyph = self.status.glyph(&theme.glyphs);
        let indent = theme.indentation.prefix(ctx.indent());
        let detail_indent = theme.indentation.prefix(ctx.indent().saturating_next());

        write!(ctx, "{indent}")?;
        Text::new(glyph.to_string())
            .with_style(glyph_style)
            .render(ctx)?;
        write!(ctx, " ")?;
        // The padded slot is `name_column - 2` to account for the
        // glyph and its space already emitted; `fit` handles the rest.
        let padded_name = fit(
            self.name,
            styles.name_column.saturating_sub(2),
            styles.ellipsis,
        );
        Text::new(padded_name)
            .with_style(styles.label)
            .render(ctx)?;
        if let Some(timing) = &self.timing {
            write!(ctx, " ")?;
            timing.render(ctx)?;
        }
        if let Some(detail) = self.detail {
            writeln!(ctx)?;
            write!(ctx, "{detail_indent}")?;
            Text::new(detail).with_style(styles.detail).render(ctx)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::render_to_string;

    #[test]
    fn test_status_line_contains_glyph_and_name() {
        let out = render_to_string(&StatusLine::new(Status::Pass, "check/foo"));
        assert!(out.contains('✓'));
        assert!(out.contains("check/foo"));
    }

    #[test]
    fn test_status_line_with_detail_includes_detail() {
        let out =
            render_to_string(&StatusLine::new(Status::Pass, "check/foo").with_detail("scoring"));
        assert!(out.contains("scoring"));
    }

    #[test]
    fn test_status_line_with_detail_renders_detail_on_a_new_line() {
        let out =
            render_to_string(&StatusLine::new(Status::Pass, "check/foo").with_detail("scoring"));
        assert!(
            out.contains('\n'),
            "with_detail variant must break onto a new line: {out:?}",
        );
    }

    #[test]
    fn test_render_does_not_emit_trailing_newline() {
        let out = render_to_string(&StatusLine::new(Status::Pass, "check"));
        assert!(!out.ends_with('\n'));
    }
}
