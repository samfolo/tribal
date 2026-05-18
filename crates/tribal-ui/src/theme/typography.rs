//! Typography: tier-2 generic semantic tokens — visual roles that
//! apply across components. Tier-3 component-specific recipes live in
//! `recipes` (see `KeyValueGridTheme`, `HeaderTheme`, `TableTheme`).

use anstyle::Style;

use super::palette::Palette;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Typography {
    pub body: Style,
    pub emphasis: Style,
    pub dim_label: Style,
    pub active: Style,
    pub negative: Style,
    pub subtle: Style,
}

impl Typography {
    /// Resolve typography from a palette.
    #[must_use]
    pub fn resolve(palette: &Palette) -> Self {
        Self {
            body: Style::new().fg_color(palette.text.base),
            emphasis: Style::new().bold(),
            dim_label: Style::new().fg_color(palette.text.muted),
            active: Style::new().fg_color(palette.info.emphasised).bold(),
            negative: Style::new().fg_color(palette.failure.base),
            subtle: Style::new().fg_color(palette.text.muted),
        }
    }
}
