//! Palette: colour semantics only — one keyed shade per status role,
//! each a three-stop ramp.
//!
//! Each [`Shade`]'s stops are [`Option<Color>`] rather than
//! [`Color`] so the [`Capability::None`](super::types::Capability)
//! tier can deliver a genuinely monochrome theme: components feed
//! the slot directly into [`Style::fg_color`](anstyle::Style::fg_color),
//! and a `None` slot produces no foreground SGR while leaving
//! weight modifiers (bold, dimmed) untouched.

use anstyle::Color;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Palette {
    pub success: Shade,
    pub failure: Shade,
    pub error: Shade,
    pub warning: Shade,
    pub info: Shade,
    pub pending: Shade,
    pub separator: Shade,
    pub text: Shade,
}

impl Palette {
    /// Constructs a palette with no foreground colour at any
    /// shade stop. Used for the `Capability::None` tier and the
    /// `WeightOnly` render class so SGR colour escapes never
    /// leave the writer; weight modifiers applied on top of the
    /// resulting [`Style`](anstyle::Style) survive.
    #[must_use]
    pub const fn monochrome() -> Self {
        Self {
            success: Shade::none(),
            failure: Shade::none(),
            error: Shade::none(),
            warning: Shade::none(),
            info: Shade::none(),
            pending: Shade::none(),
            separator: Shade::none(),
            text: Shade::none(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shade {
    pub base: Option<Color>,
    pub emphasised: Option<Color>,
    pub muted: Option<Color>,
}

impl Shade {
    /// Three-stop ramp where every stop holds the same colour.
    #[must_use]
    pub const fn uniform(color: Color) -> Self {
        Self {
            base: Some(color),
            emphasised: Some(color),
            muted: Some(color),
        }
    }

    /// Three-stop ramp with no colour at any stop. Components feed
    /// the resulting `None` directly into
    /// [`Style::fg_color`](anstyle::Style::fg_color), which omits
    /// the foreground SGR sequence entirely.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            base: None,
            emphasised: None,
            muted: None,
        }
    }
}
