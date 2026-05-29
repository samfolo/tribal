//! Bearer-token blocks framed by the bootstrap hand-off.

use std::io::{self, Write};

use tribal_domain::BearerToken;
use tribal_ui::{Component, RenderCtx, Text};

use crate::commands::common::CredentialsPersistOutcome;

/// Extra indent applied to the token line so it stands out from the
/// heading above it.
const TOKEN_INDENT: usize = 2;

/// Stdio variant: trailing stash-for-later block. The transport
/// authenticates as `principal:local` at runtime, so the token is a
/// recover-it-later artefact rather than an activation step. The
/// heading reflects the actual persistence outcome — the resolved
/// path on success, or a copy-this-manually hint on failure — so the
/// message never claims a credentials file that was not written.
pub(super) struct StdioTokenBlock<'a> {
    pub token: &'a BearerToken,
    pub credentials: &'a CredentialsPersistOutcome,
}

impl Component for StdioTokenBlock<'_> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let style = ctx.theme().typography.body;
        let heading = match self.credentials {
            CredentialsPersistOutcome::Persisted { path } => {
                format!("Bearer token (also saved to {}):", path.display())
            }
            CredentialsPersistOutcome::Failed { .. } => {
                "Bearer token (credentials.json was NOT written — copy this manually):".to_owned()
            }
        };
        Text::new(heading).with_style(style).renderln(ctx)?;
        writeln!(ctx)?;
        Text::new(self.token.as_str())
            .with_style(style)
            .with_pad_left(TOKEN_INDENT)
            .render(ctx)
    }
}

/// HTTP/SSE last-resort recovery block: rendered only when
/// `credentials.json` could not be written, so a freshly minted token is
/// not lost. On the success path the token stays in `credentials.json`
/// (retrievable through `tribal mcp-config --static-token`) and is never
/// printed, keeping it out of terminal transcripts.
pub(super) struct HttpSseTokenBlock<'a> {
    pub token: &'a BearerToken,
}

impl Component for HttpSseTokenBlock<'_> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let style = ctx.theme().typography.body;
        Text::new("Bearer token (credentials.json was NOT written, copy it manually):")
            .with_style(style)
            .renderln(ctx)?;
        writeln!(ctx)?;
        Text::new(self.token.as_str())
            .with_style(style)
            .with_pad_left(TOKEN_INDENT)
            .render(ctx)
    }
}
