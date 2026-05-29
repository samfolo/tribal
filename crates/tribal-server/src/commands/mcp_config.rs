//! `tribal mcp-config` — print the active project's MCP wire-up snippet.

mod run;

pub(crate) use run::run;
#[cfg(feature = "test-helpers")]
pub use run::{McpConfigOptions, TokenStrategy, run_async};
