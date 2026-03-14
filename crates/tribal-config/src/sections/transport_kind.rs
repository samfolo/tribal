//! Transport protocol for the MCP server.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// Transport protocol for the MCP server.
///
/// Determines how the server communicates with clients.  The HTTP/SSE
/// startup path supplies `127.0.0.1:7077` as a fallback when
/// `bind_address` is `None` and transport is not `Stdio`.
///
/// Implements [`FromStr`] so that clap can parse the type natively
/// without a mirror enum.
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

impl fmt::Display for TransportKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Stdio => "stdio",
            Self::Http => "http",
            Self::Sse => "sse",
        };
        f.write_str(s)
    }
}

impl FromStr for TransportKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "stdio" => Ok(Self::Stdio),
            "http" => Ok(Self::Http),
            "sse" => Ok(Self::Sse),
            other => Err(format!(
                "unknown transport: {other} (expected stdio, http, or sse)"
            )),
        }
    }
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

    #[test]
    fn test_from_str_valid() {
        assert_eq!(
            "stdio".parse::<TransportKind>().unwrap(),
            TransportKind::Stdio
        );
        assert_eq!(
            "http".parse::<TransportKind>().unwrap(),
            TransportKind::Http
        );
        assert_eq!("sse".parse::<TransportKind>().unwrap(), TransportKind::Sse);
    }

    #[test]
    fn test_from_str_invalid() {
        let err = "grpc".parse::<TransportKind>().unwrap_err();
        assert!(err.contains("unknown transport"));
    }

    #[test]
    fn test_display() {
        assert_eq!(TransportKind::Stdio.to_string(), "stdio");
        assert_eq!(TransportKind::Http.to_string(), "http");
        assert_eq!(TransportKind::Sse.to_string(), "sse");
    }
}
