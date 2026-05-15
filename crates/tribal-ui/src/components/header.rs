//! Section title styled with the theme's header typography.

use std::io;

use anstyle::Style;

use crate::{
    component::{Component, InlineComponent, ThemedComponent},
    components::Text,
    render_ctx::RenderCtx,
    theme::Theme,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderStyles {
    pub title: Style,
}

pub struct Header {
    title: String,
}

impl Header {
    #[must_use]
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
        }
    }
}

impl ThemedComponent for Header {
    type Styles = HeaderStyles;

    fn resolve_styles(theme: &Theme) -> Self::Styles {
        HeaderStyles {
            title: theme.typography.active,
        }
    }
}

impl Component for Header {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let styles = Self::resolve_styles(ctx.theme());
        Text::new(&self.title).with_style(styles.title).render(ctx)
    }
}

impl InlineComponent for Header {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::render_to_string;

    #[test]
    fn test_header_contains_title() {
        let out = render_to_string(&Header::new("tribal check"));
        assert!(out.contains("tribal check"));
    }

    #[test]
    fn test_render_does_not_emit_trailing_newline() {
        let out = render_to_string(&Header::new("solo title"));
        assert!(!out.ends_with('\n'));
    }
}
