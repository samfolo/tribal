//! Per-stream colour, theme, and background-mode probing.
//!
//! Computes the render class once per output stream from
//! `supports_color::on_cached(stream)` plus
//! [`std::io::IsTerminal::is_terminal`], and feeds the same
//! `(capability, is_tty, mode)` tuple to [`Theme::for_selection`],
//! [`anstream::AutoStream`]'s [`ColorChoice`](anstream::ColorChoice),
//! and any other surface that must agree on what to emit.
//!
//! [`Mode::Auto`] is resolved here too, via `termbg`'s OSC 11 query.

use std::time::Duration;

use supports_color::{ColorLevel, Stream};

use crate::theme::{Capability, Mode, Theme, ThemeRender, ThemeSelection};

/// Maximum time the OSC 11 background-colour probe may block on the
/// terminal's response before the harness falls back to dark.
pub const BACKGROUND_PROBE_TIMEOUT: Duration = Duration::from_millis(100);

/// Theme and colour state for a single output stream. Every field
/// derives from one `(capability, is_tty)` probe.
#[derive(Debug, Clone, Copy)]
pub struct StreamThemeContext {
    /// Selection axes that drive every colour decision.
    pub selection: ThemeSelection,
    /// Pre-computed theme for `selection`.
    pub theme: Theme,
    /// Concrete `anstream` colour policy fed to
    /// [`AutoStream::new`](anstream::AutoStream::new). `AutoStream::auto`
    /// is not used — its env-var precedence diverges from
    /// `supports-color`'s.
    pub color_choice: anstream::ColorChoice,
}

impl StreamThemeContext {
    /// Probe `stream` for colour capability and assemble the matching
    /// theme. `is_tty` is supplied by the caller via
    /// [`IsTerminal::is_terminal`](std::io::IsTerminal::is_terminal)
    /// on the concrete stream handle so this module stays free of
    /// `std::io::stdout`/`std::io::stderr` coupling.
    #[must_use]
    pub fn probe(stream: Stream, is_tty: bool, mode: Mode) -> Self {
        let capability = Capability::from(supports_color::on_cached(stream));
        let selection = ThemeSelection {
            capability,
            is_tty,
            mode,
        };
        Self {
            selection,
            theme: Theme::for_selection(selection),
            color_choice: selection.render().color_choice(),
        }
    }

    /// Render-class projection used by any surface that needs to know
    /// whether to emit SGR sequences and at what fidelity.
    #[must_use]
    pub fn render(&self) -> ThemeRender {
        self.selection.render()
    }
}

// ---------------------------------------------------------------------------
// supports-color → Capability bridge
// ---------------------------------------------------------------------------

impl From<ColorLevel> for Capability {
    fn from(level: ColorLevel) -> Self {
        // `supports-color` reports cumulative support (a truecolour
        // terminal also sets `has_256` and `has_basic`); the highest
        // available tier wins.
        if level.has_16m {
            Self::TrueColour
        } else if level.has_256 {
            Self::Ansi256
        } else if level.has_basic {
            Self::Basic
        } else {
            Self::None
        }
    }
}

impl From<Option<ColorLevel>> for Capability {
    fn from(level: Option<ColorLevel>) -> Self {
        level.map_or(Self::None, Self::from)
    }
}

// ---------------------------------------------------------------------------
// Mode resolution
// ---------------------------------------------------------------------------

/// Resolves [`Mode::Auto`] via `termbg`'s OSC 11 query, falling back
/// to dark on timeout. Explicit [`Mode::Light`]/[`Mode::Dark`]
/// passes through unchanged.
#[must_use]
pub fn resolve_mode(mode: Mode) -> Mode {
    match mode {
        Mode::Auto => detect_background_mode().unwrap_or(Mode::Dark),
        Mode::Dark | Mode::Light => mode,
    }
}

fn detect_background_mode() -> Option<Mode> {
    match termbg::theme(BACKGROUND_PROBE_TIMEOUT) {
        Ok(termbg::Theme::Light) => Some(Mode::Light),
        Ok(termbg::Theme::Dark) => Some(Mode::Dark),
        Err(_) => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capability_is_none_when_supports_color_returns_none() {
        assert_eq!(Capability::from(None), Capability::None);
    }

    /// Phase 1 acceptance: the `NoSgr` render class (non-TTY /
    /// redirected output) must map to `anstream::ColorChoice::Never`
    /// so the writer strips every SGR escape — colour and weight —
    /// at its boundary, leaving plain text for downstream consumers
    /// (pagers, log files, structured pipelines).
    #[test]
    fn test_nosgr_render_class_maps_to_never_color_choice() {
        let selection = ThemeSelection {
            capability: Capability::None,
            is_tty: false,
            mode: Mode::Dark,
        };
        assert_eq!(selection.render(), ThemeRender::NoSgr);
        assert_eq!(
            selection.render().color_choice(),
            anstream::ColorChoice::Never,
        );
    }
}
