//! Shared test helpers for the UI primitives. `cfg(test)`-only — not
//! shipped in production builds.

use anstream::{AutoStream, ColorChoice};

use crate::{component::Component, render_ctx::RenderCtx, theme::Theme};

/// Render a component against the default dark theme into a `String`.
/// Takes `&dyn Component` so the helper handles both Copy and non-Copy
/// component types uniformly.
pub(crate) fn render_to_string(component: &dyn Component) -> String {
    render_with_theme(component, &Theme::default_dark())
}

/// Render a component against an explicit theme. Used by tests that
/// need to exercise non-default theme configurations (e.g.
/// `Capability::None` for the no-colour render class).
pub(crate) fn render_with_theme(component: &dyn Component, theme: &Theme) -> String {
    let mut buf: Vec<u8> = Vec::new();
    let mut ctx = RenderCtx::new(&mut buf, theme);
    component
        .render(&mut ctx)
        .expect("write to Vec never fails");
    String::from_utf8(buf).expect("utf8 output")
}

/// Render a component with the writer wrapped in
/// `AutoStream::new(_, ColorChoice::Never)` — the same `NoSgr`
/// render-class wiring used in production for non-TTY output. Both
/// colour and weight escapes are stripped at the writer boundary, so
/// assertions can target plain textual content. Use this when you
/// care about what was rendered, not how it was styled.
pub(crate) fn render_no_sgr(component: &dyn Component) -> String {
    let mut buf: Vec<u8> = Vec::new();
    let theme = Theme::default_dark();
    {
        let mut writer = AutoStream::new(&mut buf, ColorChoice::Never);
        let mut ctx = RenderCtx::new(&mut writer, &theme);
        component
            .render(&mut ctx)
            .expect("write to AutoStream over Vec never fails");
    }
    String::from_utf8(buf).expect("utf8 output")
}
