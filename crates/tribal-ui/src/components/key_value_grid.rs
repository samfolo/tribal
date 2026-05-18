//! Aligned `(label, value)` grid.
//!
//! Label width is auto-computed from the widest label plus a
//! `Spacing::Sm` gutter; pass `with_label_width` when two grids must
//! line up against each other.

use std::io::{self, Write};

use anstyle::Style;
use unicode_width::UnicodeWidthStr;

use crate::{
    component::{Component, ThemedComponent},
    components::Text,
    render_ctx::RenderCtx,
    theme::{Spacing, Theme},
    widths::pad_right,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyValueGridStyles {
    pub label: Style,
    pub value: Style,
}

pub struct KeyValueGrid {
    pairs: Vec<(String, String)>,
    label_width: Option<usize>,
}

impl KeyValueGrid {
    #[must_use]
    pub fn new(pairs: Vec<(String, String)>) -> Self {
        Self {
            pairs,
            label_width: None,
        }
    }

    /// Override the auto-computed label-column width. Use when two
    /// grids on the same screen must align against each other.
    #[must_use]
    pub fn with_label_width(mut self, width: usize) -> Self {
        self.label_width = Some(width);
        self
    }
}

impl ThemedComponent for KeyValueGrid {
    type Styles = KeyValueGridStyles;

    fn resolve_styles(theme: &Theme) -> Self::Styles {
        KeyValueGridStyles {
            label: Style::new().fg_color(theme.palette.text.muted),
            value: Style::new().fg_color(theme.palette.info.base),
        }
    }
}

impl Component for KeyValueGrid {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let label_width = self.label_width.unwrap_or_else(|| {
            let widest = self
                .pairs
                .iter()
                .map(|(label, _)| UnicodeWidthStr::width(label.as_str()))
                .max()
                .unwrap_or(0);
            widest + ctx.theme().spacing.chars(Spacing::Sm)
        });
        let styles = Self::resolve_styles(ctx.theme());
        let indent = ctx.theme().indentation.prefix(ctx.indent());

        let last = self.pairs.len().saturating_sub(1);
        for (i, (label, value)) in self.pairs.iter().enumerate() {
            write!(ctx, "{indent}")?;
            Text::new(pad_right(label, label_width))
                .with_style(styles.label)
                .render(ctx)?;
            Text::new(value.as_str())
                .with_style(styles.value)
                .render(ctx)?;
            if i < last {
                writeln!(ctx)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{render_no_sgr, render_to_string};

    #[test]
    fn test_grid_contains_each_label_and_value() {
        let out = render_to_string(&KeyValueGrid::new(vec![
            ("status".to_owned(), "ok".to_owned()),
            ("count".to_owned(), "12".to_owned()),
        ]));
        assert!(out.contains("status"));
        assert!(out.contains("ok"));
        assert!(out.contains("count"));
        assert!(out.contains("12"));
    }

    #[test]
    fn test_grid_separates_pairs_with_newline() {
        let out = render_to_string(&KeyValueGrid::new(vec![
            ("a".to_owned(), "1".to_owned()),
            ("b".to_owned(), "2".to_owned()),
        ]));
        assert_eq!(out.matches('\n').count(), 1);
    }

    #[test]
    fn test_render_does_not_emit_trailing_newline() {
        let out = render_to_string(&KeyValueGrid::new(vec![
            ("a".to_owned(), "1".to_owned()),
            ("b".to_owned(), "2".to_owned()),
        ]));
        assert!(!out.ends_with('\n'));
    }

    #[test]
    fn test_explicit_label_width_aligns_values_uniformly() {
        let rendered = render_no_sgr(
            &KeyValueGrid::new(vec![
                ("short".to_owned(), "v1".to_owned()),
                ("longer_label".to_owned(), "v2".to_owned()),
            ])
            .with_label_width(16),
        );
        for line in rendered.lines() {
            if let Some(value_at) = line.find('v') {
                assert_eq!(
                    value_at, 16,
                    "value 'v' should land at column 16 in {line:?}"
                );
            }
        }
    }
}
