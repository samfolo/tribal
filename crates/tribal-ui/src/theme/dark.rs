//! Dark-mode preset.

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

pub(super) fn default_dark(capability: Capability) -> Theme {
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
        success: ramp_rgb(158, 206, 106),
        failure: ramp_rgb(247, 118, 142),
        error: ramp_rgb(255, 158, 100),
        warning: ramp_rgb(224, 175, 104),
        info: ramp_rgb(122, 162, 247),
        pending: ramp_rgb(86, 95, 137),
        separator: ramp_rgb(59, 66, 97),
        text: ramp_rgb(192, 202, 245),
    }
}

fn ansi256() -> Palette {
    Palette {
        success: ramp_ansi256(34),
        failure: ramp_ansi256(160),
        error: ramp_ansi256(166),
        warning: ramp_ansi256(178),
        info: ramp_ansi256(33),
        pending: ramp_ansi256(244),
        separator: ramp_ansi256(238),
        text: ramp_ansi256(250),
    }
}

fn basic() -> Palette {
    Palette {
        success: Shade::uniform(Color::Ansi(AnsiColor::Green)),
        failure: Shade::uniform(Color::Ansi(AnsiColor::Red)),
        error: Shade::uniform(Color::Ansi(AnsiColor::BrightRed)),
        warning: Shade::uniform(Color::Ansi(AnsiColor::Yellow)),
        info: Shade::uniform(Color::Ansi(AnsiColor::Blue)),
        pending: Shade::uniform(Color::Ansi(AnsiColor::BrightBlack)),
        separator: Shade::uniform(Color::Ansi(AnsiColor::BrightBlack)),
        text: Shade::uniform(Color::Ansi(AnsiColor::White)),
    }
}

/// Step the base colour ±30 along each channel for the emphasis /
/// muted stops. Saturating arithmetic keeps the ramp legal at the
/// 0/255 boundaries.
fn ramp_rgb(r: u8, g: u8, b: u8) -> Shade {
    Shade {
        base: Some(Color::Rgb(RgbColor(r, g, b))),
        emphasised: Some(Color::Rgb(RgbColor(
            r.saturating_add(30),
            g.saturating_add(30),
            b.saturating_add(30),
        ))),
        muted: Some(Color::Rgb(RgbColor(
            r.saturating_sub(60),
            g.saturating_sub(60),
            b.saturating_sub(60),
        ))),
    }
}

/// Step the ANSI-256 index ±2 along the closest perceptual band for
/// the emphasis / muted stops. A wider step risks clamping muted
/// stops with low base indices to ANSI black.
fn ramp_ansi256(idx: u8) -> Shade {
    Shade {
        base: Some(Color::Ansi256(idx.into())),
        emphasised: Some(Color::Ansi256(idx.saturating_add(2).into())),
        muted: Some(Color::Ansi256(idx.saturating_sub(2).into())),
    }
}
