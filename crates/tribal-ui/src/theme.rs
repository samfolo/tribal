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

pub use glyphs::GlyphSet;
pub use indentation::Indent;
pub use palette::Palette;
pub use spacing::Spacing;
pub use types::{Capability, Mode, Theme, ThemeRender, ThemeSelection};
pub use typography::Typography;
