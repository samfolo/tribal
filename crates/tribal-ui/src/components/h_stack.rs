//! Horizontal layout container.
//!
//! Composes inline children left-to-right with a separator glyph
//! between adjacent items.  The separator defaults to the theme's
//! middot; spacing controls per-side padding around it.  The
//! [`InlineComponent`] bound is what makes block components a compile
//! error rather than a runtime layout bug.

use std::io::{self, Write};

use crate::{
    component::{Component, InlineComponent},
    render_ctx::RenderCtx,
    theme::Spacing,
};

pub struct HStack<C: InlineComponent> {
    items: Vec<C>,
    separator: Option<char>,
    spacing: Spacing,
}

impl<C: InlineComponent> HStack<C> {
    #[must_use]
    pub fn new(items: Vec<C>) -> Self {
        Self {
            items,
            separator: None,
            spacing: Spacing::Sm,
        }
    }

    #[must_use]
    pub fn with_separator(mut self, sep: char) -> Self {
        self.separator = Some(sep);
        self
    }

    #[must_use]
    pub fn with_spacing(mut self, spacing: Spacing) -> Self {
        self.spacing = spacing;
        self
    }
}

impl<C: InlineComponent> Component for HStack<C> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let mut iter = self.items.iter();
        let Some(first) = iter.next() else {
            return Ok(());
        };
        let (indent_prefix, sep, pad) = {
            let theme = ctx.theme();
            (
                theme.indentation.prefix(ctx.indent()),
                self.separator.unwrap_or(theme.glyphs.middot),
                " ".repeat(theme.spacing.chars(self.spacing)),
            )
        };
        write!(ctx, "{indent_prefix}")?;
        first.render(ctx)?;
        for item in iter {
            write!(ctx, "{pad}{sep}{pad}")?;
            item.render(ctx)?;
        }
        Ok(())
    }
}

impl<C: InlineComponent> InlineComponent for HStack<C> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        components::{Badge, Status, Text},
        test_support::render_to_string,
    };

    #[test]
    fn test_accepts_inline_components() {
        let stack = HStack::new(vec![Badge::new(Status::Pass), Badge::new(Status::Fail)]);
        let out = render_to_string(&stack);
        assert!(out.contains('·'));
        assert!(out.contains("pass"));
        assert!(out.contains("fail"));
    }

    #[test]
    fn test_single_item_emits_no_separator() {
        let stack: HStack<Badge> = HStack::new(vec![Badge::new(Status::Pass)]);
        let out = render_to_string(&stack);
        assert!(!out.contains('·'));
    }

    #[test]
    fn test_empty_stack_emits_nothing() {
        let stack: HStack<Badge> = HStack::new(vec![]);
        let out = render_to_string(&stack);
        assert!(out.is_empty());
    }

    #[test]
    fn test_render_does_not_emit_trailing_newline() {
        let stack = HStack::new(vec![Badge::new(Status::Pass), Badge::new(Status::Fail)]);
        let out = render_to_string(&stack);
        assert!(!out.ends_with('\n'));
    }

    #[test]
    fn test_custom_separator_replaces_default_middot() {
        let stack = HStack::new(vec![Badge::new(Status::Pass), Badge::new(Status::Fail)])
            .with_separator('—');
        let out = render_to_string(&stack);
        assert!(out.contains('—'));
        assert!(!out.contains('·'));
    }

    /// Heterogeneous inline composition via boxed trait objects.
    /// Only compiles because [`Component`] and [`InlineComponent`] are
    /// forwarded through `Box<T: ?Sized>` in `component.rs`.
    #[test]
    fn test_heterogeneous_box_dyn_inline_layout_renders() {
        let items: Vec<Box<dyn InlineComponent>> = vec![
            Box::new(Badge::new(Status::Pass)),
            Box::new(Text::new("tally")),
        ];
        let stack = HStack::new(items);
        let out = render_to_string(&stack);
        assert!(out.contains("pass"));
        assert!(out.contains("tally"));
        assert!(out.contains('·'));
    }
}
