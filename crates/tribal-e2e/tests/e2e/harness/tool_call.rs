use rmcp::{
    model::{CallToolRequestParams, CallToolResult, RawContent},
    service::{Peer, RoleClient},
};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Tool call helper
// ---------------------------------------------------------------------------

/// Calls a tool via JSON-RPC and returns the result.
///
/// # Panics
///
/// Panics on protocol-level transport errors or if `arguments` is not a
/// JSON object or null.
pub async fn call_tool(client: &Peer<RoleClient>, name: &str, arguments: Value) -> CallToolResult {
    let params = match arguments {
        Value::Object(map) => CallToolRequestParams::new(name.to_owned()).with_arguments(map),
        Value::Null => CallToolRequestParams::new(name.to_owned()),
        other => panic!("call_tool arguments must be a JSON object, got {other}"),
    };
    client
        .call_tool(params)
        .await
        .expect("tool call failed at protocol level")
}

// ---------------------------------------------------------------------------
// Response extraction
// ---------------------------------------------------------------------------

/// Extracts the text content from the first content block.
///
/// # Panics
///
/// Panics if the result contains no content blocks or the first block is
/// not text.
#[must_use]
pub fn tool_result_text(result: &CallToolResult) -> String {
    result
        .content
        .first()
        .and_then(|c| match &c.raw {
            RawContent::Text(t) => Some(t.text.clone()),
            _ => None,
        })
        .expect("expected text content in tool result")
}

/// Returns the structured JSON content of a tool result.
///
/// Prefers `structured_content` (machine-readable) over parsing the text
/// content block.
///
/// # Panics
///
/// Panics if neither `structured_content` nor parseable JSON text is
/// present.
#[must_use]
pub fn tool_result_json(result: &CallToolResult) -> Value {
    if let Some(structured) = &result.structured_content {
        return structured.clone();
    }
    let text = tool_result_text(result);
    serde_json::from_str(&text)
        .expect("tool result has no structured_content and text is not valid JSON")
}

/// Asserts the tool result does not indicate an error.
pub fn assert_success(result: &CallToolResult) {
    assert!(
        !result.is_error.unwrap_or(false),
        "expected tool success, got error: {}",
        tool_result_text(result),
    );
}

/// Asserts the tool result indicates an error.
pub fn assert_error(result: &CallToolResult) {
    assert!(
        result.is_error.unwrap_or(false),
        "expected tool error, got success: {}",
        tool_result_text(result),
    );
}
