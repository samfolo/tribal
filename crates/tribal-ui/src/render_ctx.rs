//! `RenderCtx`: writer + theme + indent threaded through the render
//! tree.
//!
//! Mirrors `std::fmt::Formatter` — one bundled argument carries every
//! rendering concern, recursive composition stays clean, and growing
//! the context (a debug flag, a cursor tracker) never breaks the
//! `Component::render` signature.

use std::{
    io::{self, Write},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
};

use crate::theme::{Indent, Theme};

pub struct RenderCtx<'a> {
    writer: &'a mut dyn Write,
    theme: &'a Theme,
    indent: Indent,
}

impl<'a> RenderCtx<'a> {
    /// Construct a context starting at the outermost indent (`L1`).
    pub fn new(writer: &'a mut dyn Write, theme: &'a Theme) -> Self {
        Self {
            writer,
            theme,
            indent: Indent::L1,
        }
    }

    /// Override the starting indent. Use at construction time when
    /// the consumer composes a component into a region whose implicit
    /// nesting differs from `L1`. Mid-life indent changes go through
    /// [`Self::indented`] so the push/pop scoping stays honest.
    #[must_use]
    pub fn with_indent(mut self, indent: Indent) -> Self {
        self.indent = indent;
        self
    }

    #[must_use]
    pub fn theme(&self) -> &Theme {
        self.theme
    }

    #[must_use]
    pub fn indent(&self) -> Indent {
        self.indent
    }

    /// Run a closure with the indent stepped one level deeper, then
    /// restore the prior indent unconditionally — including on panic
    /// unwind. The indent field is the only state restored; the
    /// writer is the caller's responsibility (a half-emitted SGR
    /// escape will leak into subsequent writes if a closure panics
    /// between opening and closing escapes).
    pub fn indented<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut RenderCtx<'_>) -> R,
    {
        let prior = self.indent;
        self.indent = prior.saturating_next();
        let result = catch_unwind(AssertUnwindSafe(|| f(self)));
        self.indent = prior;
        match result {
            Ok(value) => value,
            Err(payload) => resume_unwind(payload),
        }
    }
}

impl Write for RenderCtx<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;

    fn theme() -> Theme {
        Theme::default_dark()
    }

    // -----------------------------------------------------------------
    // construction defaults
    // -----------------------------------------------------------------

    #[test]
    fn test_initial_indent_is_l1() {
        let mut buf: Vec<u8> = Vec::new();
        let theme = theme();
        let ctx = RenderCtx::new(&mut buf, &theme);
        assert_eq!(ctx.indent(), Indent::L1);
    }

    // -----------------------------------------------------------------
    // indent scoping
    // -----------------------------------------------------------------

    #[test]
    fn test_indented_restores_after_normal_return() {
        let mut buf: Vec<u8> = Vec::new();
        let theme = theme();
        let mut ctx = RenderCtx::new(&mut buf, &theme);
        ctx.indented(|inner| {
            assert_eq!(inner.indent(), Indent::L2);
        });
        assert_eq!(ctx.indent(), Indent::L1);
    }

    #[test]
    fn test_indented_restores_after_panic_unwind() {
        let mut buf: Vec<u8> = Vec::new();
        let theme = theme();
        let mut ctx = RenderCtx::new(&mut buf, &theme);
        let result = catch_unwind(AssertUnwindSafe(|| {
            ctx.indented(|_inner| panic!("propagation under test"));
        }));
        assert!(result.is_err(), "panic must propagate to the caller");
        assert_eq!(ctx.indent(), Indent::L1, "indent must restore");
    }

    #[test]
    fn test_indented_nests_two_levels() {
        let mut buf: Vec<u8> = Vec::new();
        let theme = theme();
        let mut ctx = RenderCtx::new(&mut buf, &theme);
        ctx.indented(|inner| {
            assert_eq!(inner.indent(), Indent::L2);
            inner.indented(|deeper| {
                assert_eq!(deeper.indent(), Indent::L3);
            });
            assert_eq!(inner.indent(), Indent::L2);
        });
        assert_eq!(ctx.indent(), Indent::L1);
    }

    // -----------------------------------------------------------------
    // writer pass-through
    // -----------------------------------------------------------------

    #[test]
    fn test_writeln_through_ctx_reaches_writer() {
        let mut buf: Vec<u8> = Vec::new();
        let theme = theme();
        let mut ctx = RenderCtx::new(&mut buf, &theme);
        writeln!(ctx, "hello {n}", n = 1).expect("writeln succeeds");
        assert_eq!(buf, b"hello 1\n");
    }
}
