use serde::{Deserialize, Serialize};

/// Application-level error codes aligned with Google's `google.rpc.Code`
/// taxonomy. Serialises to lowercase `snake_case` strings in MCP JSON
/// responses.
///
/// The mapping from `McpErrorCode` to `tonic::Code` is a boundary concern
/// implemented in `tribal-mcp`, not here, to avoid pulling `tonic` into the
/// leaf domain crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpErrorCode {
    /// Requested entity does not exist.
    NotFound,
    /// No valid credentials provided.
    Unauthenticated,
    /// Valid credentials but insufficient scope.
    PermissionDenied,
    /// Caller provided an invalid parameter value.
    InvalidArgument,
    /// System state prevents the operation.
    FailedPrecondition,
    /// Rate limit or quota exceeded.
    ResourceExhausted,
    /// Unexpected server error.
    Internal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enum_serde_tests;

    enum_serde_tests!(test_mcp_error_code_serde_roundtrip, McpErrorCode {
        McpErrorCode::NotFound => "not_found",
        McpErrorCode::Unauthenticated => "unauthenticated",
        McpErrorCode::PermissionDenied => "permission_denied",
        McpErrorCode::InvalidArgument => "invalid_argument",
        McpErrorCode::FailedPrecondition => "failed_precondition",
        McpErrorCode::ResourceExhausted => "resource_exhausted",
        McpErrorCode::Internal => "internal",
    });
}
