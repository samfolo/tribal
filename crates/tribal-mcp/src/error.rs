use rmcp::model::{CallToolResult, Content, ErrorCode, ErrorData as McpError, Meta};
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
#[derive(Debug, Serialize, thiserror::Error)]
#[error("{message}")]
pub struct McpToolError {
    pub code: McpErrorCode,
    pub message: String,
    pub details: serde_json::Value,
}

impl McpToolError {
    /// Converts this application error into an rmcp protocol error.
    ///
    /// Maps application error codes to JSON-RPC error codes at the
    /// `call_tool` dispatch boundary.
    #[must_use]
    pub fn into_protocol_error(self) -> McpError {
        let code = match self.code {
            McpErrorCode::Unauthenticated | McpErrorCode::PermissionDenied => {
                ErrorCode::INVALID_REQUEST
            }
            McpErrorCode::InvalidArgument | McpErrorCode::NotFound => ErrorCode::INVALID_PARAMS,
            McpErrorCode::Conflict
            | McpErrorCode::FailedPrecondition
            | McpErrorCode::ResourceExhausted
            | McpErrorCode::Unavailable
            | McpErrorCode::Internal => ErrorCode::INTERNAL_ERROR,
        };
        let data = serde_json::json!({
            "code": self.code,
            "details": self.details,
        });

        McpError::new(code, self.message, Some(data))
    }
}

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

/// Converts a value into an rmcp `CallToolResult`.
///
/// A success mapping includes a `content` text summary and a `structured_content`
/// value conforming to the tool's output schema. An error mapping
/// ([`McpToolError`]) carries the message in the `content` text block with
/// `is_error` set and no `structured_content`, because a tool's output schema
/// describes only the success shape and a strict harness validates any returned
/// structured content against it.
pub trait IntoCallToolResult {
    fn into_call_tool_result(self) -> CallToolResult;
}

/// The `_meta` member carrying the typed tool error on error results.
pub const ERROR_META_KEY: &str = "com.tribal/error";

impl IntoCallToolResult for McpToolError {
    fn into_call_tool_result(self) -> CallToolResult {
        // Error results carry the message in the text content and `is_error`
        // only; they deliberately omit `structured_content`. A tool declares a
        // success output schema, and a strict harness validates ANY returned
        // structured content against it, so an error-shaped payload would fail
        // that validation and mask the real message. The typed error rides
        // result `_meta` instead, where a first-party client reads it without
        // parsing prose and every other client is free to ignore it.
        let mut meta = Meta::default();
        meta.insert(
            ERROR_META_KEY.to_owned(),
            // An enum code, a string, and an already-parsed value cannot
            // fail to serialise; the empty object keeps the text channel
            // authoritative rather than masking the error with a panic.
            serde_json::to_value(&self).unwrap_or_else(|_| serde_json::json!({})),
        );
        CallToolResult::error(vec![Content::text(&self.message)]).with_meta(Some(meta))
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
    fn test_an_error_result_carries_the_typed_error_in_meta() {
        let result = McpToolError {
            code: McpErrorCode::Conflict,
            message: "idempotency key reused with a different project or content".into(),
            details: serde_json::json!({}),
        }
        .into_call_tool_result();

        assert_eq!(result.is_error, Some(true));
        assert!(result.structured_content.is_none());
        let meta = result.meta.expect("error results carry meta");
        let typed = meta.get(ERROR_META_KEY).expect("the typed error member");
        assert_eq!(typed["code"], "conflict");
        assert_eq!(
            typed["message"],
            "idempotency key reused with a different project or content",
        );
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

        // The message lives in the text content; it is the only client-visible
        // error payload, so it must carry the error verbatim.
        assert_eq!(
            crate::test_utils::first_text_content(&result),
            "No job found with ID job_abc"
        );

        // Error results must omit structured content: a strict harness validates
        // any structured content against the tool's success output schema, so an
        // error-shaped payload there would mask the real message.
        assert!(
            result.structured_content.is_none(),
            "error results must not carry structured content"
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

    #[test]
    fn test_into_protocol_error_permission_denied() {
        let err = McpToolError {
            code: McpErrorCode::PermissionDenied,
            message: "insufficient scope".into(),
            details: serde_json::json!({}),
        };
        let protocol = err.into_protocol_error();
        assert_eq!(protocol.code, ErrorCode::INVALID_REQUEST);
        assert_eq!(protocol.message, "insufficient scope");

        let data = protocol.data.expect("data should be present");
        assert_eq!(data["code"], "permission_denied");
    }

    #[test]
    fn test_into_protocol_error_internal() {
        let err = McpToolError {
            code: McpErrorCode::Internal,
            message: "db failure".into(),
            details: serde_json::json!({}),
        };
        let protocol = err.into_protocol_error();
        assert_eq!(protocol.code, ErrorCode::INTERNAL_ERROR);

        let data = protocol.data.expect("data should be present");
        assert_eq!(data["code"], "internal");
    }
}
