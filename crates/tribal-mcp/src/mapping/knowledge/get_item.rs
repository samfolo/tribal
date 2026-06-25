//! MCP server glue for `tribal_get_item`.

use std::fmt::Write;

use rmcp::model::{CallToolResult, Content};
use tribal_wire::McpGetItemResponse;

use crate::error::IntoCallToolResult;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERIALISE_GET_ITEM_RESPONSE: &str = "McpGetItemResponse should always serialise successfully";

// ---------------------------------------------------------------------------
// IntoCallToolResult
// ---------------------------------------------------------------------------

impl IntoCallToolResult for McpGetItemResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let found = self.found_count();
        let requested = self.requested_count();
        let not_found_count = self.not_found_ids.len();

        let item_word = if requested == 1 { "item" } else { "items" };
        let mut text = format!("Retrieved {found} of {requested} requested {item_word}.");

        if not_found_count > 0 {
            let plural = if not_found_count == 1 { "" } else { "s" };
            let _ = write!(text, " {not_found_count} ID{plural} not found: ");

            for (i, id) in self.not_found_ids.iter().enumerate() {
                if i > 0 {
                    text.push_str(", ");
                }
                text.push_str(id);
            }
        }

        let structured = serde_json::to_value(&self).expect(SERIALISE_GET_ITEM_RESPONSE);
        let mut result = CallToolResult::success(vec![Content::text(text)]);
        result.structured_content = Some(structured);
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use rmcp::model::RawContent;

    use super::*;

    #[test]
    fn test_get_item_response_into_call_tool_result_partial() {
        let mut items = serde_json::Map::new();
        items.insert("ki_found".into(), serde_json::json!({"item": {}}));
        items.insert("ki_missing".into(), serde_json::Value::Null);

        let resp = McpGetItemResponse {
            items,
            not_found_ids: vec!["ki_missing".into()],
        };
        let result = resp.into_call_tool_result();
        assert_eq!(result.is_error, Some(false));

        let RawContent::Text(t) = &result.content[0].raw else {
            panic!("expected text content");
        };
        let text = &t.text;
        assert!(
            text.contains("Retrieved 1 of 2 requested items."),
            "unexpected text: {text}"
        );
        assert!(
            text.contains("1 ID not found: ki_missing"),
            "unexpected text: {text}"
        );
    }

    #[test]
    fn test_get_item_response_all_found_text() {
        let mut items = serde_json::Map::new();
        items.insert("ki_a".into(), serde_json::json!({"item": {}}));
        items.insert("ki_b".into(), serde_json::json!({"item": {}}));

        let resp = McpGetItemResponse {
            items,
            not_found_ids: vec![],
        };
        let result = resp.into_call_tool_result();

        let RawContent::Text(t) = &result.content[0].raw else {
            panic!("expected text content");
        };
        let text = &t.text;
        assert_eq!(text, "Retrieved 2 of 2 requested items.");
    }

    #[test]
    fn test_get_item_response_all_not_found_text() {
        let mut items = serde_json::Map::new();
        items.insert("ki_a".into(), serde_json::Value::Null);
        items.insert("ki_b".into(), serde_json::Value::Null);

        let resp = McpGetItemResponse {
            items,
            not_found_ids: vec!["ki_a".into(), "ki_b".into()],
        };
        let result = resp.into_call_tool_result();

        let RawContent::Text(t) = &result.content[0].raw else {
            panic!("expected text content");
        };
        let text = &t.text;
        assert!(
            text.contains("Retrieved 0 of 2 requested items."),
            "unexpected text: {text}"
        );
        assert!(
            text.contains("2 IDs not found: ki_a, ki_b"),
            "unexpected text: {text}"
        );
    }
}
