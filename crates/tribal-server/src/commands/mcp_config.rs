//! `tribal mcp-config` — print the active project's MCP wire-up snippet.

mod run;

pub(crate) use run::run;
pub use run::{McpConfigOptions, run_async};
