//! Transport protocol for the MCP server.
//!
//! [`TransportKind`] is the config-layer equivalent of the clap
//! `Transport` enum in `tribal-server`.  `tribal-config` does not depend
//! on `tribal-server`, so a separate type is needed.

use serde::{Deserialize, Serialize};

/// Transport protocol for the MCP server.
///
/// Determines how the server communicates with clients.  The HTTP/SSE
/// startup path supplies `127.0.0.1:7077` as a fallback when
/// `bind_address` is `None` and transport is not `Stdio`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportKind {
    /// Communicate over stdin/stdout.
    #[default]
    Stdio,

    /// Streamable HTTP transport.
    Http,

    /// Server-sent events transport.
    Sse,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enum_serde_tests;

    enum_serde_tests!(test_transport_kind_serde_roundtrip, TransportKind {
        TransportKind::Stdio => "stdio",
        TransportKind::Http => "http",
        TransportKind::Sse => "sse",
    });

    #[test]
    fn test_default_is_stdio() {
        assert_eq!(TransportKind::default(), TransportKind::Stdio);
    }
}
