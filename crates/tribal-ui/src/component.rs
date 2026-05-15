//! `Component` trait and the inline-vs-block marker.
//!
//! Every catalogued component implements `Component`. Components
//! whose rendered output is a single line with no embedded `\n`
//! additionally implement `InlineComponent`; that marker is what
//! `HStack` requires of its children, so a multi-line component
//! composed inside an `HStack` is a compile error rather than a
//! layout corruption at runtime.

use std::io;

use crate::{render_ctx::RenderCtx, theme::Theme};

/// Single-method rendering contract.
///
/// **Newline discipline.** A component's `render` writes the
/// component's own bytes and stops. Parents that compose children
/// vertically write an explicit `\n` (or `writeln!`) between them;
/// the final child gets no trailing newline. This rule keeps
/// `HStack` well-defined and `TreeNode` recursion clean.
pub trait Component {
    /// Render this component to the writer in `ctx`.
    ///
    /// # Errors
    ///
    /// Returns any I/O error reported by the underlying writer.
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()>;

    /// Render this component to a `String` against the supplied
    /// theme, at the outermost indent (`Indent::L1`).
    ///
    /// # Panics
    ///
    /// Panics if the component emits non-UTF-8 bytes — components in
    /// this crate compose only `&str` and `char` content, so the
    /// invariant holds in practice.
    fn render_to_string(&self, theme: &Theme) -> String {
        let mut buf: Vec<u8> = Vec::new();
        let mut ctx = RenderCtx::new(&mut buf, theme);
        self.render(&mut ctx)
            .expect("rendering into a Vec<u8> never fails");
        String::from_utf8(buf).expect("components emit UTF-8")
    }
}

/// Marker for components whose rendered output is a single line with
/// no embedded `\n`. Used as a bound on `HStack` so heterogeneous
/// horizontal composition is type-safe.
pub trait InlineComponent: Component {}

/// Opt-in capability for components that own a tier-3 style recipe
/// derived from the theme. Implementors declare a `Styles` type and
/// a `resolve_styles` constructor that maps the theme's tier-1/tier-2
/// substrate (palette, typography) onto component-specific styles.
/// Components without theme-derived styling (e.g. `Text` takes its
/// style as a builder argument; `Badge` derives from `Status`) do
/// not implement this trait.
pub trait ThemedComponent: Component {
    /// The component's resolved style bundle.
    type Styles;

    /// Derive the styles bundle from `theme`. Pure projection from
    /// palette + tier-2 tokens — no I/O, no allocations beyond what
    /// the styles type itself owns.
    fn resolve_styles(theme: &Theme) -> Self::Styles;
}

// Forwarding impls for `Box<T>` so heterogeneous inline layouts can
// be composed as `HStack<Box<dyn InlineComponent>>`.

impl<T: Component + ?Sized> Component for Box<T> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        self.as_ref().render(ctx)
    }
}

impl<T: InlineComponent + ?Sized> InlineComponent for Box<T> {}
