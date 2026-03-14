//! MCP transport protocol selection.

use clap::ValueEnum;
use tribal_config::TransportKind;

/// MCP transport protocol.
///
/// This enum exists separately from [`TransportKind`] because the orphan rule
/// prevents deriving [`ValueEnum`] on a foreign type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Transport {
    /// Standard input/output (stdin/stdout).
    Stdio,
    /// Streamable HTTP.
    Http,
    /// Server-sent events.
    Sse,
}

impl From<Transport> for TransportKind {
    fn from(transport: Transport) -> Self {
        match transport {
            Transport::Stdio => Self::Stdio,
            Transport::Http => Self::Http,
            Transport::Sse => Self::Sse,
        }
    }
}
