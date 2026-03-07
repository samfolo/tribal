//! MCP request and response types for `tribal_explore`.

use chrono::{DateTime, Utc};
use rmcp::model::{CallToolResult, Content};
use serde::{Deserialize, Serialize};
use tribal_domain::RelationKind;

use super::common::{McpKnowledgeItem, McpReference, McpStanding};
use crate::error::IntoCallToolResult;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERIALISE_EXPLORE_RESPONSE: &str =
    "McpExploreResponse should always serialise successfully";

// ---------------------------------------------------------------------------
// McpRelationDirection
// ---------------------------------------------------------------------------

/// Direction of a relationship edge relative to the exploration anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum McpRelationDirection {
    /// This item asserts something about the anchor.
    Inbound,
    /// The anchor asserts something about this item.
    Outbound,
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_explore` input.
#[derive(Debug, Deserialize)]
pub(crate) struct McpExploreRequest {
    pub(crate) item_id: String,
    pub(crate) session_trace_id: Option<String>,
    pub(crate) direction: Option<String>,
    pub(crate) relation_types: Option<Vec<String>>,
    pub(crate) depth: Option<u32>,
    pub(crate) include_standing: Option<bool>,
    pub(crate) include_references: Option<bool>,
    pub(crate) limit: Option<u32>,
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

/// Response for `tribal_explore`.
#[derive(Debug, Serialize)]
pub(crate) struct McpExploreResponse {
    /// The full knowledge item for the exploration anchor.
    pub(crate) anchor: McpKnowledgeItem,
    /// Evidential profile of the anchor — always present.
    pub(crate) anchor_standing: McpStanding,
    pub(crate) related_items: Vec<McpExplorationResult>,
    pub(crate) trace_id: String,
    pub(crate) exact: bool,
}

/// A single exploration result with relationship metadata.
#[derive(Debug, Serialize)]
pub(crate) struct McpExplorationResult {
    pub(crate) item: McpKnowledgeItem,
    pub(crate) relation_type: RelationKind,
    pub(crate) relation_direction: McpRelationDirection,
    pub(crate) depth: u32,
    pub(crate) relation_created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) standing: Option<McpStanding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) references: Option<Vec<McpReference>>,
}

// ---------------------------------------------------------------------------
// IntoCallToolResult
// ---------------------------------------------------------------------------

impl IntoCallToolResult for McpExploreResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let count = self.related_items.len();
        let anchor_id = &self.anchor.id;

        let mut text = format!(
            "Explored {anchor_id}: {count} related item(s) (exact: {})",
            self.exact,
        );

        // Append supersession notice when an inbound supersedes relation
        // is present in the results.
        for result in &self.related_items {
            if result.relation_type == RelationKind::Supersedes
                && result.relation_direction == McpRelationDirection::Inbound
            {
                text.push_str(&format!(
                    "\nThis item has been superseded by {} (newer understanding)",
                    result.item.id,
                ));
                break;
            }
        }

        let structured =
            serde_json::to_value(&self).expect(SERIALISE_EXPLORE_RESPONSE);
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
    use tribal_domain::StandingBuilder;

    fn sample_item(id: &str) -> McpKnowledgeItem {
        use super::super::common::{McpSourceContext, McpSourceType};
        McpKnowledgeItem {
            id: id.to_owned(),
            project_id: "proj_00000000-0000-0000-0000-000000000000".into(),
            principal_key: "user:test".into(),
            kind: tribal_domain::KnowledgeKind::Fact,
            content: "test".into(),
            tags: vec![],
            confidence: tribal_domain::Confidence::Inferred,
            source_context: McpSourceContext {
                source_type: McpSourceType::Manual,
                provider: None,
                model: None,
            },
            episode_id: None,
            capture_commit: None,
            capture_branch: None,
            created_at: chrono::Utc::now(),
        }
    }

    fn sample_standing() -> McpStanding {
        McpStanding::from(
            &StandingBuilder::default()
                .supporting_count(0)
                .contradicting_count(0)
                .observation_count(0)
                .supporting_episode_count(0)
                .supporting_project_count(0)
                .build(),
        )
    }

    #[test]
    fn test_explore_request_deserialises_minimal() {
        let json = serde_json::json!({"item_id": "ki_abc"});
        let req: McpExploreRequest =
            serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.item_id, "ki_abc");
        assert!(req.direction.is_none());
    }

    #[test]
    fn test_explore_request_deserialises_full() {
        let json = serde_json::json!({
            "item_id": "ki_abc",
            "session_trace_id": "trace1",
            "direction": "both",
            "relation_types": ["supports", "contradicts"],
            "depth": 2,
            "include_standing": true,
            "include_references": false,
            "limit": 50
        });
        let req: McpExploreRequest =
            serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.depth, Some(2));
        assert_eq!(req.relation_types.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_explore_response_serialises_anchor_standing_required() {
        let resp = McpExploreResponse {
            anchor: sample_item("ki_anchor"),
            anchor_standing: sample_standing(),
            related_items: vec![],
            trace_id: "t1".into(),
            exact: true,
        };
        let json = serde_json::to_value(&resp).expect("serialises");
        assert!(json.get("anchor_standing").is_some());
        assert!(json["anchor_standing"].is_object());
    }

    #[test]
    fn test_explore_response_into_call_tool_result_with_supersession() {
        let resp = McpExploreResponse {
            anchor: sample_item("ki_anchor"),
            anchor_standing: sample_standing(),
            related_items: vec![McpExplorationResult {
                item: sample_item("ki_superseder"),
                relation_type: RelationKind::Supersedes,
                relation_direction: McpRelationDirection::Inbound,
                depth: 1,
                relation_created_at: chrono::Utc::now(),
                standing: None,
                references: None,
            }],
            trace_id: "t1".into(),
            exact: true,
        };
        let result = resp.into_call_tool_result();
        assert!(result.is_error.is_none());

        let text = match &result.content[0] {
            Content::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        assert!(text.contains("superseded by ki_superseder"));
    }
}
