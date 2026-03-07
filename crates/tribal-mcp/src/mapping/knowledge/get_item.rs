//! MCP request and response types for `tribal_get_item`.

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
    pub(crate) fn found_count(&self) -> usize {
        self.items.values().filter(|v| !v.is_null()).count()
    }

    /// Returns the total number of requested entries.
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
        let text = format!("Retrieved {found} of {requested} item(s)");

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
    use super::*;

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

        let resp = McpGetItemResponse { items };
        let json = serde_json::to_value(&resp).expect("serialises");
        assert!(json["items"]["ki_missing"].is_null());
    }

    #[test]
    fn test_get_item_response_serialises_present_entry() {
        use super::super::common::{McpSourceContext, McpSourceType};

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

        let resp = McpGetItemResponse { items };
        let json = serde_json::to_value(&resp).expect("serialises");
        assert!(json["items"]["ki_abc"]["item"]["id"] == "ki_abc");
    }

    #[test]
    fn test_get_item_response_into_call_tool_result() {
        let mut items = serde_json::Map::new();
        items.insert("ki_found".into(), serde_json::json!({"item": {}}));
        items.insert("ki_missing".into(), serde_json::Value::Null);

        let resp = McpGetItemResponse { items };
        let result = resp.into_call_tool_result();
        assert_eq!(result.is_error, Some(false));

        let RawContent::Text(t) = &result.content[0].raw else {
            panic!("expected text content");
        };
        let text = &t.text;
        assert!(text.contains("Retrieved 1 of 2 item(s)"));
    }
}
