//! Light-mode preset.

use anstyle::{AnsiColor, Color, RgbColor};

use super::{
    dimensions::Dimensions,
    glyphs::GlyphSet,
    indentation::IndentRamp,
    palette::{Palette, Shade},
    spacing::SpacingRamp,
    time_format::TimeFormat,
    timings::Timings,
    types::{Capability, Theme},
    typography::Typography,
};

pub(super) fn default_light(capability: Capability) -> Theme {
    let palette = palette_for(capability);
    let typography = Typography::resolve(&palette);
    Theme {
        palette,
        glyphs: GlyphSet::default(),
        typography,
        dimensions: Dimensions::default(),
        spacing: SpacingRamp::default(),
        indentation: IndentRamp::default(),
        timings: Timings::default(),
        time_format: TimeFormat::default(),
    }
}

fn palette_for(capability: Capability) -> Palette {
    match capability {
        Capability::TrueColour => true_colour(),
        Capability::Ansi256 => ansi256(),
        Capability::Basic => basic(),
        Capability::None => Palette::monochrome(),
    }
}

fn true_colour() -> Palette {
    Palette {
        success: ramp_rgb(88, 117, 57),
        failure: ramp_rgb(245, 42, 101),
        error: ramp_rgb(177, 92, 0),
        warning: ramp_rgb(140, 108, 62),
        info: ramp_rgb(46, 125, 233),
        pending: ramp_rgb(132, 140, 181),
        separator: ramp_rgb(209, 213, 230),
        text: ramp_rgb(55, 96, 191),
    }
}

fn ansi256() -> Palette {
    Palette {
        success: ramp_ansi256(28),
        failure: ramp_ansi256(124),
        error: ramp_ansi256(166),
        warning: ramp_ansi256(136),
        info: ramp_ansi256(25),
        pending: ramp_ansi256(244),
        separator: ramp_ansi256(252),
        text: ramp_ansi256(238),
    }
}

fn basic() -> Palette {
    Palette {
        success: Shade::uniform(Color::Ansi(AnsiColor::Green)),
        failure: Shade::uniform(Color::Ansi(AnsiColor::Red)),
        error: Shade::uniform(Color::Ansi(AnsiColor::Red)),
        warning: Shade::uniform(Color::Ansi(AnsiColor::Yellow)),
        info: Shade::uniform(Color::Ansi(AnsiColor::Blue)),
        pending: Shade::uniform(Color::Ansi(AnsiColor::BrightBlack)),
        separator: Shade::uniform(Color::Ansi(AnsiColor::White)),
        text: Shade::uniform(Color::Ansi(AnsiColor::Black)),
    }
}

/// Step the base colour darker for emphasis (more contrast against a
/// light background) and lighter for muted (less contrast). Mirrors
/// the dark-mode ramp's intent inverted along the brightness axis.
fn ramp_rgb(r: u8, g: u8, b: u8) -> Shade {
    Shade {
        base: Some(Color::Rgb(RgbColor(r, g, b))),
        emphasised: Some(Color::Rgb(RgbColor(
            r.saturating_sub(30),
            g.saturating_sub(30),
            b.saturating_sub(30),
        ))),
        muted: Some(Color::Rgb(RgbColor(
            r.saturating_add(40),
            g.saturating_add(40),
            b.saturating_add(40),
        ))),
    }
}

/// Step the ANSI-256 index ∓2 along the closest perceptual band.
/// Smaller step than dark mode: light backgrounds need a tighter ramp
/// to stay legible.
fn ramp_ansi256(idx: u8) -> Shade {
    Shade {
        base: Some(Color::Ansi256(idx.into())),
        emphasised: Some(Color::Ansi256(idx.saturating_sub(2).into())),
        muted: Some(Color::Ansi256(idx.saturating_add(2).into())),
    }
}
