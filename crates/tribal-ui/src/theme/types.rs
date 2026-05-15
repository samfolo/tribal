//! Theme assembly and selection axes.

use super::{
    dark::default_dark, dimensions::Dimensions, glyphs::GlyphSet, indentation::IndentRamp,
    light::default_light, palette::Palette, spacing::SpacingRamp, time_format::TimeFormat,
    timings::Timings, typography::Typography,
};

/// Resolved appearance configuration: every palette, glyph,
/// typography, dimension, spacing/indent ramp, timing, and
/// time-format default the renderer needs. Constructed once per
/// output stream via [`Theme::for_selection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Theme {
    pub palette: Palette,
    pub glyphs: GlyphSet,
    pub typography: Typography,
    pub dimensions: Dimensions,
    pub spacing: SpacingRamp,
    pub indentation: IndentRamp,
    pub timings: Timings,
    pub time_format: TimeFormat,
}

/// Terminal colour-capability tier, detected from
/// `supports-color`'s `ColorLevel` and threaded through every
/// per-stream theme decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// 24-bit RGB.
    TrueColour,
    /// 256-colour indexed palette.
    Ansi256,
    /// 8/16-colour ANSI palette.
    Basic,
    /// No colour — `NO_COLOR=1` or the writer is not a TTY.
    None,
}

/// Light-mode vs dark-mode selection. [`Auto`](Self::Auto) is the
/// dispatcher's input; the dispatcher resolves it against the
/// terminal background before invoking [`Theme::for_selection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Dark,
    Light,
    Auto,
}

/// Per-stream input bundle for [`Theme::for_selection`] —
/// `(capability, is_tty, mode)` is the single probe that drives the
/// theme, the writer's [`anstream::ColorChoice`], and the miette
/// bridge so all three layers agree on what to emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThemeSelection {
    pub capability: Capability,
    pub is_tty: bool,
    pub mode: Mode,
}

/// Render-class selector: discriminates the three SGR-emission cases
/// (full colour, weight-only, no SGR). Computed once at startup from
/// the same `(capability, is_tty)` probe that drives
/// `Theme::for_selection`; threaded into the writer choice and the
/// miette bridge so all three layers agree on what to emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThemeRender {
    /// Full SGR — colour and weight modifiers both emitted.
    Coloured,
    /// Weight modifiers only — bold/dim survive, foreground colour
    /// suppressed. The `NO_COLOR=1` on a TTY case.
    WeightOnly,
    /// No SGR — output is plain text. The non-TTY / redirected case.
    NoSgr,
}

impl Theme {
    /// Resolve a `Theme` for the given selection. `Mode::Auto`
    /// resolves to `Dark` here; the dispatcher is expected to have
    /// already collapsed `Auto` against `COLORFGBG` before calling.
    #[must_use]
    pub fn for_selection(selection: ThemeSelection) -> Self {
        match selection.mode {
            Mode::Light => default_light(selection.capability),
            Mode::Dark | Mode::Auto => default_dark(selection.capability),
        }
    }

    /// Returns a copy of the theme with `dimensions` substituted.
    #[must_use]
    pub fn with_dimensions(mut self, dimensions: Dimensions) -> Self {
        self.dimensions = dimensions;
        self
    }

    /// Convenience constructor — dark mode at the top colour tier.
    /// Used by tests and call sites that have no stream-specific
    /// selection to pass.
    #[must_use]
    pub fn default_dark() -> Self {
        default_dark(Capability::TrueColour)
    }

    /// Convenience constructor — light mode at the top colour tier.
    #[must_use]
    pub fn default_light() -> Self {
        default_light(Capability::TrueColour)
    }
}

impl ThemeSelection {
    /// Project the selection's `(capability, is_tty)` axes onto the
    /// three-way render class. The dispatcher uses this single helper
    /// to feed the writer, the theme, and the miette bridge so all
    /// three layers agree on what to emit.
    #[must_use]
    pub const fn render(self) -> ThemeRender {
        match (self.capability, self.is_tty) {
            (_, false) => ThemeRender::NoSgr,
            (Capability::None, true) => ThemeRender::WeightOnly,
            (_, true) => ThemeRender::Coloured,
        }
    }
}

impl ThemeRender {
    /// Project the render class onto the concrete
    /// [`anstream::ColorChoice`] fed to
    /// [`AutoStream::new`](anstream::AutoStream::new).
    ///
    /// The three-way mapping mirrors the three render classes so the
    /// writer never strips SGR weight on a no-colour TTY (the
    /// [`WeightOnly`](ThemeRender::WeightOnly) case under
    /// `NO_COLOR=1`). `AlwaysAnsi` is preferred over `Always` for
    /// the coloured branch because the two diverge on Windows —
    /// `Always` invokes the legacy console API while `AlwaysAnsi`
    /// always emits ANSI escape sequences, matching the theme's
    /// expectation.
    #[must_use]
    pub const fn color_choice(self) -> anstream::ColorChoice {
        match self {
            Self::Coloured => anstream::ColorChoice::AlwaysAnsi,
            Self::WeightOnly => anstream::ColorChoice::Always,
            Self::NoSgr => anstream::ColorChoice::Never,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------
    // render-class projection
    // -----------------------------------------------------------------

    #[test]
    fn test_render_class_for_coloured_tty() {
        let s = ThemeSelection {
            capability: Capability::TrueColour,
            is_tty: true,
            mode: Mode::Dark,
        };
        assert_eq!(s.render(), ThemeRender::Coloured);
    }

    #[test]
    fn test_render_class_for_no_color_on_tty() {
        let s = ThemeSelection {
            capability: Capability::None,
            is_tty: true,
            mode: Mode::Dark,
        };
        assert_eq!(s.render(), ThemeRender::WeightOnly);
    }

    #[test]
    fn test_render_class_for_redirected_output() {
        let s = ThemeSelection {
            capability: Capability::TrueColour,
            is_tty: false,
            mode: Mode::Dark,
        };
        assert_eq!(s.render(), ThemeRender::NoSgr);
    }

    // -----------------------------------------------------------------
    // mode dispatch
    // -----------------------------------------------------------------

    #[test]
    fn test_mode_auto_resolves_to_dark() {
        let auto = Theme::for_selection(ThemeSelection {
            capability: Capability::TrueColour,
            is_tty: true,
            mode: Mode::Auto,
        });
        let dark = Theme::default_dark();
        assert_eq!(auto.palette, dark.palette);
    }

    #[test]
    fn test_light_mode_differs_from_dark() {
        let light = Theme::default_light();
        let dark = Theme::default_dark();
        assert_ne!(light.palette, dark.palette);
    }

    // -----------------------------------------------------------------
    // ThemeRender → anstream::ColorChoice projection
    // -----------------------------------------------------------------

    #[test]
    fn test_color_choice_is_always_ansi_for_coloured_render() {
        assert_eq!(
            ThemeRender::Coloured.color_choice(),
            anstream::ColorChoice::AlwaysAnsi,
        );
    }

    /// `WeightOnly` is the design's "TTY but `NO_COLOR=1`" case. SGR
    /// weight (bold, dim) must survive the writer; only colour
    /// codes drop, and the theme handles that by not emitting them.
    #[test]
    fn test_color_choice_is_always_for_weight_only_render() {
        assert_eq!(
            ThemeRender::WeightOnly.color_choice(),
            anstream::ColorChoice::Always,
        );
    }

    #[test]
    fn test_color_choice_is_never_for_no_sgr_render() {
        assert_eq!(
            ThemeRender::NoSgr.color_choice(),
            anstream::ColorChoice::Never,
        );
    }
}
