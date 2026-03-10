//! MCP request and response types for `tribal_get_item`.

use std::fmt::Write;

use rmcp::model::{CallToolResult, Content, RawContent};
use serde::{Deserialize, Serialize};

use super::common::{McpKnowledgeItem, McpReference, McpStanding};
use crate::error::IntoCallToolResult;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERIALISE_GET_ITEM_RESPONSE: &str = "McpGetItemResponse should always serialise successfully";

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_get_item` input.
#[derive(Debug, Deserialize)]
pub(crate) struct McpGetItemRequest {
    pub(crate) item_ids: Vec<String>,
    pub(crate) include_standing: Option<bool>,
    pub(crate) include_references: Option<bool>,
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

/// Response for `tribal_get_item`.
///
/// The `items` field is a JSON object keyed by prefixed item ID. Values
/// are either a serialised `McpGetItemEntry` or JSON `null` for IDs that
/// were not found.
#[derive(Debug, Serialize)]
pub(crate) struct McpGetItemResponse {
    pub(crate) items: serde_json::Map<String, serde_json::Value>,
    #[serde(skip)]
    pub(crate) not_found_ids: Vec<String>,
}

/// A single found item with optional computed fields.
#[derive(Debug, Serialize)]
pub(crate) struct McpGetItemEntry {
    pub(crate) item: McpKnowledgeItem,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) standing: Option<McpStanding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) references: Option<Vec<McpReference>>,
}

impl McpGetItemResponse {
    /// Returns the number of non-null entries in the response.
    #[must_use]
    pub(crate) fn found_count(&self) -> usize {
        self.items.values().filter(|v| !v.is_null()).count()
    }

    /// Returns the total number of requested entries.
    #[must_use]
    pub(crate) fn requested_count(&self) -> usize {
        self.items.len()
    }
}

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
    use super::{
        super::common::{McpSourceContext, McpSourceType},
        *,
    };

    #[test]
    fn test_get_item_request_deserialises() {
        let json = serde_json::json!({
            "item_ids": ["ki_abc", "ki_def"],
            "include_standing": true,
        });
        let req: McpGetItemRequest = serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.item_ids.len(), 2);
        assert_eq!(req.include_standing, Some(true));
        assert!(req.include_references.is_none());
    }

    #[test]
    fn test_get_item_response_serialises_null_entry() {
        let mut items = serde_json::Map::new();
        items.insert("ki_missing".into(), serde_json::Value::Null);

        let resp = McpGetItemResponse {
            items,
            not_found_ids: vec!["ki_missing".into()],
        };
        let json = serde_json::to_value(&resp).expect("serialises");
        assert!(json["items"]["ki_missing"].is_null());
    }

    #[test]
    fn test_get_item_response_serialises_present_entry() {
        let entry = McpGetItemEntry {
            item: McpKnowledgeItem {
                id: "ki_abc".into(),
                project_id: "proj_def".into(),
                principal_key: "user:sam".into(),
                kind: tribal_domain::KnowledgeKind::Fact,
                content: "test content".into(),
                tags: vec!["auth".into()],
                confidence: tribal_domain::Confidence::Verified,
                source_context: McpSourceContext {
                    source_type: McpSourceType::Manual,
                    provider: None,
                    model: None,
                },
                episode_id: None,
                capture_commit: None,
                capture_branch: None,
                created_at: chrono::Utc::now(),
            },
            standing: None,
            references: None,
        };
        let entry_json = serde_json::to_value(&entry).expect("serialises entry");

        let mut items = serde_json::Map::new();
        items.insert("ki_abc".into(), entry_json);

        let resp = McpGetItemResponse {
            items,
            not_found_ids: vec![],
        };
        let json = serde_json::to_value(&resp).expect("serialises");
        assert!(json["items"]["ki_abc"]["item"]["id"] == "ki_abc");
    }

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
