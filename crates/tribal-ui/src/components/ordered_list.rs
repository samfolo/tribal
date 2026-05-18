//! Numbered-list primitive with pluggable marker styles.
//!
//! `OrderedList` owns the marker glyph, the trailing `". "`, and the
//! continuation indent that aligns body lines under the marker. Items
//! are arbitrary [`Component`]s — they write to a [`RenderCtx`] that
//! transparently prepends the continuation indent after every newline,
//! so multi-line items stay flush without the item having to know
//! about the marker width.

use std::io::{self, Write};

use crate::{component::Component, render_ctx::RenderCtx};

// ---------------------------------------------------------------------------
// Markers
// ---------------------------------------------------------------------------

/// Strategy for formatting the marker glyph rendered before each item.
///
/// Implementors return the marker for the n-th item (1-based); the
/// list appends a trailing `". "` itself so layout stays uniform
/// across marker styles.
pub trait OrderedListMarker {
    /// Format the marker for the n-th item.
    fn format(&self, n: u32) -> String;
}

/// Arabic-numeral markers: `1`, `2`, `3`, …
#[derive(Debug, Default, Clone, Copy)]
pub struct Decimal;

impl OrderedListMarker for Decimal {
    fn format(&self, n: u32) -> String {
        n.to_string()
    }
}

// ---------------------------------------------------------------------------
// Continuation-indent writer
// ---------------------------------------------------------------------------

/// `Write` adaptor that injects `prefix` at the start of each
/// continuation line. The first byte written after construction is
/// emitted as-is so callers can place the marker manually before
/// handing the writer to the item's render call; every line after a
/// `\n` then receives `prefix` automatically. Empty lines stay empty
/// — the prefix is omitted when the chunk is just a newline.
struct PrefixedLines<'inner, 'prefix, W: Write + ?Sized> {
    inner: &'inner mut W,
    prefix: &'prefix str,
    at_line_start: bool,
}

impl<'inner, 'prefix, W: Write + ?Sized> PrefixedLines<'inner, 'prefix, W> {
    fn new(inner: &'inner mut W, prefix: &'prefix str) -> Self {
        Self {
            inner,
            prefix,
            at_line_start: false,
        }
    }
}

impl<W: Write + ?Sized> Write for PrefixedLines<'_, '_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        for chunk in buf.split_inclusive(|&b| b == b'\n') {
            let is_just_newline = chunk == b"\n";
            if self.at_line_start && !is_just_newline {
                self.inner.write_all(self.prefix.as_bytes())?;
            }
            self.inner.write_all(chunk)?;
            self.at_line_start = chunk.last() == Some(&b'\n');
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

/// Numbered list of [`Component`] items.
///
/// Layout: each item is preceded by `<ambient><marker>. ` where
/// `<ambient>` is the writer's current indent prefix. The item then
/// renders into a writer that injects the continuation indent after
/// every newline, so every body line — including any newlines the
/// item itself emits — aligns flush under the post-marker column. A
/// blank line separates items.
///
/// Marker defaults to [`Decimal`]; switch with [`Self::with_marker`].
/// The first item is numbered `1`; offset with
/// [`Self::with_starting_number`].
pub struct OrderedList<'a, C: Component, M: OrderedListMarker = Decimal> {
    items: &'a [C],
    marker: M,
    starting_number: u32,
}

impl<'a, C: Component> OrderedList<'a, C, Decimal> {
    #[must_use]
    pub fn new(items: &'a [C]) -> Self {
        Self {
            items,
            marker: Decimal,
            starting_number: 1,
        }
    }
}

impl<'a, C: Component, M: OrderedListMarker> OrderedList<'a, C, M> {
    /// Switch the marker style. Returns a new list parameterised over
    /// the new marker type.
    #[must_use]
    pub fn with_marker<M2: OrderedListMarker>(self, marker: M2) -> OrderedList<'a, C, M2> {
        OrderedList {
            items: self.items,
            marker,
            starting_number: self.starting_number,
        }
    }

    /// Offset the starting number of the first item. Useful when a
    /// list resumes a previous one or starts at a non-`1` index.
    #[must_use]
    pub fn with_starting_number(mut self, n: u32) -> Self {
        self.starting_number = n;
        self
    }
}

impl<C: Component, M: OrderedListMarker> Component for OrderedList<'_, C, M> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let theme = *ctx.theme();
        let indent = ctx.indent();
        let ambient = theme.indentation.prefix(indent);
        let last = self.items.len().saturating_sub(1);
        for (idx, item) in self.items.iter().enumerate() {
            let number = self
                .starting_number
                .saturating_add(u32::try_from(idx).unwrap_or(u32::MAX));
            let marker_str = self.marker.format(number);
            let prefix = format!("{ambient}{marker_str}. ");
            let body_indent = " ".repeat(prefix.len());

            write!(ctx, "{prefix}")?;
            {
                let mut prefixed = PrefixedLines::new(ctx, &body_indent);
                let mut item_ctx = RenderCtx::new(&mut prefixed, &theme).with_indent(indent);
                item.render(&mut item_ctx)?;
            }
            if idx != last {
                // Items follow the no-trailing-newline convention, so
                // one writeln terminates the item's last line and a
                // second emits the blank separator line.
                writeln!(ctx)?;
                writeln!(ctx)?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use anstream::{AutoStream, ColorChoice};

    use super::*;
    use crate::{
        components::Text,
        test_support::render_no_sgr,
        theme::{Indent, Theme},
    };

    /// Fixture component: writes its content verbatim. Multi-line
    /// content exercises the `PrefixedLines` wrapper's continuation
    /// indent — the item itself stays unaware of the marker width.
    struct Lines(&'static str);

    impl Component for Lines {
        fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
            write!(ctx, "{}", self.0)
        }
    }

    #[test]
    fn test_decimal_marker_numbers_items_starting_at_one() {
        let items = [Lines("alpha"), Lines("beta")];
        let out = render_no_sgr(&OrderedList::new(&items));
        assert_eq!(out, "1. alpha\n\n2. beta");
    }

    #[test]
    fn test_with_starting_number_offsets_first_marker() {
        let items = [Lines("x")];
        let out = render_no_sgr(&OrderedList::new(&items).with_starting_number(7));
        assert_eq!(out, "7. x");
    }

    #[test]
    fn test_multi_line_item_aligns_continuation_under_marker_column() {
        let items = [Lines("head\nBODY")];
        let out = render_no_sgr(&OrderedList::new(&items));
        // "1. " is 3 chars wide; the second line aligns at column 3.
        assert_eq!(out, "1. head\n   BODY");
    }

    #[test]
    fn test_blank_line_separates_items() {
        let items = [Lines("a\nab"), Lines("b\nbb")];
        let out = render_no_sgr(&OrderedList::new(&items));
        assert_eq!(out, "1. a\n   ab\n\n2. b\n   bb");
    }

    #[test]
    fn test_empty_lines_within_an_item_stay_empty() {
        let items = [Lines("first\n\nthird")];
        let out = render_no_sgr(&OrderedList::new(&items));
        assert_eq!(out, "1. first\n\n   third");
    }

    #[test]
    fn test_respects_ambient_indent() {
        let theme = Theme::default_dark();
        let items = [Lines("nested")];
        let list = OrderedList::new(&items);
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = AutoStream::new(&mut buf, ColorChoice::Never);
            let mut ctx = RenderCtx::new(&mut writer, &theme).with_indent(Indent::L2);
            list.render(&mut ctx).expect("render");
        }
        let out = String::from_utf8(buf).expect("utf8");
        let l2 = theme.indentation.prefix(Indent::L2);
        assert_eq!(out, format!("{l2}1. nested"));
    }

    #[test]
    fn test_with_marker_switches_marker_style() {
        // Custom marker exercising the trait extension point. The
        // built-in `Decimal` is covered by the other tests.
        struct Stars;
        impl OrderedListMarker for Stars {
            fn format(&self, n: u32) -> String {
                "*".repeat(n as usize)
            }
        }
        let items = [Lines("alpha"), Lines("beta")];
        let out = render_no_sgr(&OrderedList::new(&items).with_marker(Stars));
        assert_eq!(out, "*. alpha\n\n**. beta");
    }

    #[test]
    fn test_styled_text_item_renders_with_marker() {
        // Any `Component` is a valid item — here a styled `Text`.
        // `render_no_sgr` strips the SGR escapes at the writer
        // boundary, so the assertion targets plain content.
        let bold = anstyle::Style::new().bold();
        let items = [Text::new("greetings").with_style(bold)];
        let out = render_no_sgr(&OrderedList::new(&items));
        assert_eq!(out, "1. greetings");
    }
}
