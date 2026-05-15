//! Status badge: glyph + coloured label.

use std::io;

use anstyle::Style;

use crate::{
    component::{Component, InlineComponent},
    components::Text,
    render_ctx::RenderCtx,
    theme::{GlyphSet, Palette, Typography},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Pass,
    Fail,
    Error,
    Skipped,
    Warning,
}

impl Status {
    /// Every variant in declaration order.
    pub const ALL: &[Self] = &[
        Self::Pass,
        Self::Fail,
        Self::Error,
        Self::Skipped,
        Self::Warning,
    ];

    pub(crate) fn glyph(self, glyphs: &GlyphSet) -> char {
        match self {
            Self::Pass => glyphs.pass,
            Self::Fail => glyphs.fail,
            Self::Error => glyphs.error,
            Self::Skipped => glyphs.skipped,
            Self::Warning => glyphs.warning,
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Error => "error",
            Self::Skipped => "skipped",
            Self::Warning => "warning",
        }
    }

    pub(crate) fn style(self, palette: &Palette, typography: &Typography) -> Style {
        match self {
            Self::Pass => Style::new().fg_color(palette.success.base),
            Self::Fail => Style::new().fg_color(palette.failure.base),
            Self::Error => Style::new().fg_color(palette.error.base).bold(),
            Self::Skipped => typography.dim_label,
            Self::Warning => Style::new().fg_color(palette.warning.base),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Badge {
    pub status: Status,
}

impl Badge {
    #[must_use]
    pub fn new(status: Status) -> Self {
        Self { status }
    }
}

impl Component for Badge {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let theme = ctx.theme();
        let style = self.status.style(&theme.palette, &theme.typography);
        let content = format!(
            "{} {}",
            self.status.glyph(&theme.glyphs),
            self.status.label()
        );
        Text::new(content).with_style(style).render(ctx)
    }
}

impl InlineComponent for Badge {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_support::{render_no_sgr, render_to_string, render_with_theme},
        theme::{Capability, Mode, Theme, ThemeSelection},
    };

    #[test]
    fn test_pass_badge_contains_pass_glyph_and_label() {
        let out = render_to_string(&Badge::new(Status::Pass));
        assert!(out.contains('✓'));
        assert!(out.contains("pass"));
    }

    #[test]
    fn test_render_does_not_emit_trailing_newline() {
        let out = render_to_string(&Badge::new(Status::Pass));
        assert!(!out.ends_with('\n'));
    }

    /// Under `(Capability::None, is_tty: true)` — i.e. `NO_COLOR=1`
    /// on a TTY — the writer is `ColorChoice::Always` so weight
    /// modifiers survive, but the theme must not emit any
    /// foreground colour SGR. The `Error` variant chains `.bold()`
    /// on top of the colour, so we can assert both halves: no
    /// fg-colour escape, and bold (`\x1b[1m`) is still present.
    #[test]
    fn test_monochrome_theme_emits_no_fg_colour_but_keeps_bold() {
        let theme = Theme::for_selection(ThemeSelection {
            capability: Capability::None,
            is_tty: true,
            mode: Mode::Dark,
        });
        let out = render_with_theme(&Badge::new(Status::Error), &theme);

        // ANSI foreground SGR is `\x1b[3Xm` (basic 0-7), `\x1b[9Xm`
        // (bright 8-15), or `\x1b[38;...m` (extended). None should
        // appear under the `WeightOnly` render class.
        assert!(
            !out.contains("\x1b[3") && !out.contains("\x1b[9"),
            "monochrome theme leaked fg-colour SGR: {out:?}",
        );
        assert!(
            out.contains("\x1b[1m"),
            "bold modifier should survive under WeightOnly: {out:?}",
        );
    }

    /// Phase 1 acceptance: when the writer is configured for the
    /// `NoSgr` render class (non-TTY / redirected output), the
    /// rendered bytes contain no ANSI escape sequences at all —
    /// the ESC byte (`\x1b`, 0x1B) must not appear.
    #[test]
    fn test_no_sgr_render_emits_no_ansi_escape() {
        let out = render_no_sgr(&Badge::new(Status::Pass));
        assert!(
            !out.contains('\x1b'),
            "NoSgr render leaked an ANSI escape byte: {out:?}",
        );
        // Content survives — only the SGR escapes drop.
        assert!(out.contains("pass"));
        assert!(out.contains('✓'));
    }
}
