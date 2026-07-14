//! MCP transport identity shared across configuration and management.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// Protocol used to serve MCP traffic.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum TransportKind {
    #[default]
    Stdio,
    Http,
    Sse,
}

impl fmt::Display for TransportKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Stdio => "stdio",
            Self::Http => "http",
            Self::Sse => "sse",
        })
    }
}

impl FromStr for TransportKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
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
    fn test_transport_kind_text_roundtrip() {
        for kind in [
            TransportKind::Stdio,
            TransportKind::Http,
            TransportKind::Sse,
        ] {
            assert_eq!(kind.to_string().parse::<TransportKind>(), Ok(kind));
        }
        assert!("grpc".parse::<TransportKind>().is_err());
    }
}
