//! Indented `Project / Git remote / Transport` block that opens the
//! bootstrap hand-off.

use std::io::{self, Write};

use tribal_config::TransportKind;
use tribal_domain::{GitRemote, ProjectId};
use tribal_ui::{Component, KeyValueGrid, RenderCtx};

/// Inputs the status block needs to render.
pub(super) struct StatusBlock<'a> {
    pub project_id: ProjectId,
    pub project_name: &'a str,
    pub git_remote: &'a GitRemote,
    pub transport: TransportKind,
    pub advertised_url: &'a str,
}

impl Component for StatusBlock<'_> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let mut pairs = vec![
            (
                "Project:".to_owned(),
                format!("{} ({})", self.project_name, self.project_id),
            ),
            (
                "Git remote:".to_owned(),
                self.git_remote.as_str().to_owned(),
            ),
        ];
        match self.transport {
            TransportKind::Stdio => {
                pairs.push((
                    "Transport:".to_owned(),
                    "stdio (no bearer token required at runtime)".to_owned(),
                ));
            }
            TransportKind::Http | TransportKind::Sse => {
                pairs.push(("Transport:".to_owned(), self.transport.to_string()));
                pairs.push(("MCP URL:".to_owned(), self.advertised_url.to_owned()));
            }
        }
        KeyValueGrid::new(pairs).render(ctx)?;
        writeln!(ctx)
    }
}
