//! Theme: every appearance decision in one place.
//!
//! Each design-system concern owns its own sibling module so iteration
//! on one axis does not touch unrelated concerns. The per-tier preset
//! constructors invoked by `Theme::for_selection` live in `dark` and
//! `light`.

mod dark;
mod dimensions;
mod glyphs;
mod indentation;
mod light;
mod palette;
mod spacing;
mod time_format;
mod timings;
mod types;
mod typography;

pub use dimensions::{Dimensions, NARROW_TERMINAL_THRESHOLD, NON_TTY_WIDTH};
pub use glyphs::{GlyphSet, SpinnerTemplate};
pub use indentation::{Indent, IndentRamp};
pub use palette::{Palette, Shade};
pub use spacing::{Spacing, SpacingRamp};
pub use time_format::{
    AbsoluteFormat, AbsoluteStyle, ElapsedFormat, RelativeAccuracy, RelativeFormat, TimeFormat,
};
pub use timings::Timings;
pub use types::{Capability, Mode, Theme, ThemeRender, ThemeSelection};
pub use typography::Typography;
