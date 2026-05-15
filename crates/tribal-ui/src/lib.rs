//! `tribal-ui` — design-system primitives for Tribal's CLI surface.
//!
//! Every appearance decision in the Tribal CLI routes through this
//! crate: the theme system, the render context, the component trait
//! and catalogue, the width helpers, and per-stream colour and theme
//! probing.

#![deny(warnings)]
#![warn(clippy::pedantic)]

pub mod component;
pub mod components;
pub mod format;
pub mod probe;
pub mod render_ctx;
pub mod theme;
pub mod time_display;
pub mod widths;
pub mod wrap;

#[cfg(test)]
pub(crate) mod test_support;

pub use component::{Component, InlineComponent, ThemedComponent};
pub use components::{Badge, Header, SectionRule, Status, StatusLine, Text};
pub use probe::StreamThemeContext;
pub use render_ctx::RenderCtx;
pub use theme::{Capability, Mode, Theme, ThemeRender, ThemeSelection};
