//! `tribal-ui` — design-system primitives for Tribal's CLI surface.
//!
//! Every appearance decision in the Tribal CLI routes through this
//! crate: the theme system, the render context, the component trait
//! and catalogue, the width helpers, and per-stream colour and theme
//! probing.

#![deny(warnings)]
#![warn(clippy::pedantic)]

mod component;
mod components;
mod format;
mod probe;
mod render_ctx;
mod theme;
mod time_display;
mod widths;

#[cfg(test)]
mod test_support;

pub use component::{Component, InlineComponent, ThemedComponent};
pub use components::{
    Badge, Decimal, Header, KeyValueGrid, OrderedList, OrderedListMarker, Paragraph, SectionRule,
    Status, StatusLine, Text,
};
pub use format::time::{Precision, format_absolute, format_elapsed};
pub use probe::{StreamThemeContext, resolve_mode};
pub use render_ctx::RenderCtx;
pub use theme::{Capability, Mode, Theme, ThemeRender, ThemeSelection};
pub use time_display::TimeDisplay;
