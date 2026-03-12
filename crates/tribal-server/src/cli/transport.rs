//! MCP transport protocol selection.

use clap::ValueEnum;

/// MCP transport protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Transport {
    /// Standard input/output (stdin/stdout).
    Stdio,
    /// Streamable HTTP.
    Http,
    /// Server-sent events.
    Sse,
}
