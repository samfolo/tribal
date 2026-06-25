//! MCP request and response types for `tribal_get_item`.

use serde::{Deserialize, Serialize};

use super::common::{McpKnowledgeItem, McpReference, McpStanding};

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_get_item` input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpGetItemRequest {
    pub item_ids: Vec<String>,
    pub include_standing: Option<bool>,
    pub include_references: Option<bool>,
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

/// Response for `tribal_get_item`.
///
/// The `items` field is a JSON object keyed by prefixed item ID. Values
/// are either a serialised `McpGetItemEntry` or JSON `null` for IDs that
/// were not found.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpGetItemResponse {
    pub items: serde_json::Map<String, serde_json::Value>,
    #[serde(skip)]
    pub not_found_ids: Vec<String>,
}

/// A single found item with optional computed fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpGetItemEntry {
    pub item: McpKnowledgeItem,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub standing: Option<McpStanding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<McpReference>>,
}

impl McpGetItemResponse {
    /// Returns the number of non-null entries in the response.
    #[must_use]
    pub fn found_count(&self) -> usize {
        self.items.values().filter(|v| !v.is_null()).count()
    }

    /// Returns the total number of requested entries.
    #[must_use]
    pub fn requested_count(&self) -> usize {
        self.items.len()
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
}
