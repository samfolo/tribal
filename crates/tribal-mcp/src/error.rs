use rmcp::model::{
    CallToolResult, Content, ErrorCode,
    ErrorData as McpError,
};
use serde::Serialize;
use tribal_domain::McpErrorCode;

// ---------------------------------------------------------------------------
// McpToolError
// ---------------------------------------------------------------------------

/// Application-level error returned by tool handlers.
///
/// Maps domain failures to a structured error shape. The `code` field uses
/// `McpErrorCode` from `tribal-domain`; `details` is always present
/// (defaults to `{}`).
#[derive(Debug, Serialize)]
pub struct McpToolError {
    pub code: McpErrorCode,
    pub message: String,
    pub details: serde_json::Value,
}

impl std::fmt::Display for McpToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for McpToolError {}

// ---------------------------------------------------------------------------
// IntoMcpError
// ---------------------------------------------------------------------------

/// Converts a domain error into an `McpToolError`.
///
/// Defined here; implementations for domain error types live in the
/// mapping layer.
pub trait IntoMcpError {
    fn into_mcp_error(self) -> McpToolError;
}

// ---------------------------------------------------------------------------
// IntoCallToolResult
// ---------------------------------------------------------------------------

/// Converts a value into an rmcp `CallToolResult` following the structured
/// content convention: every response includes both a `content` text block
/// and a `structured_content` JSON value.
pub trait IntoCallToolResult {
    fn into_call_tool_result(self) -> CallToolResult;
}

impl IntoCallToolResult for McpToolError {
    fn into_call_tool_result(self) -> CallToolResult {
        let structured = serde_json::json!({
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        });

        CallToolResult {
            content: vec![Content::text(&self.message)],
            structured_content: Some(structured),
            is_error: Some(true),
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Protocol error helpers
// ---------------------------------------------------------------------------

/// Produces a JSON-RPC method-not-found error for an unknown tool name.
pub fn method_not_found(name: &str) -> McpError {
    McpError::new(
        ErrorCode::METHOD_NOT_FOUND,
        format!("Unknown tool: {name}"),
        None,
    )
}

/// Produces a JSON-RPC invalid-params error for malformed arguments.
pub fn invalid_argument(message: impl Into<String>) -> McpError {
    McpError::invalid_params(message.into(), None)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_error() -> McpToolError {
        McpToolError {
            code: McpErrorCode::NotFound,
            message: "No job found with ID job_abc".into(),
            details: serde_json::json!({}),
        }
    }

    #[test]
    fn test_mcp_tool_error_display() {
        let err = sample_error();
        assert_eq!(err.to_string(), "No job found with ID job_abc");
    }

    #[test]
    fn test_mcp_tool_error_error_trait() {
        let err = sample_error();
        let as_error: &dyn std::error::Error = &err;
        assert!(as_error.source().is_none());
    }

    #[test]
    fn test_mcp_tool_error_serialisation() {
        let err = sample_error();
        let json = serde_json::to_value(&err).expect("serialises");
        assert_eq!(json["code"], "not_found");
        assert_eq!(json["message"], "No job found with ID job_abc");
        assert_eq!(json["details"], serde_json::json!({}));
    }

    #[test]
    fn test_mcp_tool_error_into_call_tool_result() {
        let err = sample_error();
        let result = err.into_call_tool_result();

        assert_eq!(result.is_error, Some(true));
        assert_eq!(result.content.len(), 1);

        let structured = result
            .structured_content
            .expect("structured_content is present");
        assert_eq!(structured["error"]["code"], "not_found");
        assert_eq!(
            structured["error"]["message"],
            "No job found with ID job_abc"
        );
    }

    #[test]
    fn test_method_not_found_error() {
        let err = method_not_found("tribal_nonexistent");
        assert_eq!(err.code, ErrorCode::METHOD_NOT_FOUND);
        assert!(err.message.contains("tribal_nonexistent"));
    }

    #[test]
    fn test_invalid_argument_error() {
        let err = invalid_argument("bad param");
        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
        assert!(err.message.contains("bad param"));
    }
}
