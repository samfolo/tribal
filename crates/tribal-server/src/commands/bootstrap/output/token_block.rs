//! Bearer-token blocks framed by the bootstrap hand-off.

use std::io::{self, Write};

use tribal_config::default_credentials_file_path;
use tribal_domain::BearerToken;
use tribal_ui::{Component, RenderCtx, Text};

/// Extra indent applied to the token line so it stands out from the
/// heading above it.
const TOKEN_INDENT: usize = 2;

/// Stdio variant: trailing stash-for-later block. The transport
/// authenticates as `principal:local` at runtime, so the token is a
/// recover-it-later artefact rather than an activation step.
pub(super) struct StdioTokenBlock<'a> {
    pub token: &'a BearerToken,
}

impl Component for StdioTokenBlock<'_> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let style = ctx.theme().typography.body;
        Text::new(format!(
            "Bearer token (also saved to {}):",
            default_credentials_file_path(),
        ))
        .with_style(style)
        .renderln(ctx)?;
        writeln!(ctx)?;
        Text::new(self.token.as_str())
            .with_style(style)
            .with_pad_left(TOKEN_INDENT)
            .render(ctx)
    }
}

/// HTTP/SSE variant: leading block, rendered before the action list
/// because step 1 (`export TRIBAL_AUTH_TOKEN=…`) references the token.
pub(super) struct HttpSseTokenBlock<'a> {
    pub token: &'a BearerToken,
}

impl Component for HttpSseTokenBlock<'_> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let style = ctx.theme().typography.body;
        Text::new("Bearer token (save this — it will not be shown again):")
            .with_style(style)
            .renderln(ctx)?;
        writeln!(ctx)?;
        Text::new(self.token.as_str())
            .with_style(style)
            .with_pad_left(TOKEN_INDENT)
            .render(ctx)
    }
}
