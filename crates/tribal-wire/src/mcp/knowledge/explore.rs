//! Wire request and response types for `tribal_explore`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tribal_domain::RelationKind;

use super::common::{McpKnowledgeItem, McpReference, McpStanding};

// ---------------------------------------------------------------------------
// McpRelationDirection
// ---------------------------------------------------------------------------

/// Direction of a relationship edge relative to the exploration anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpRelationDirection {
    /// This item asserts something about the anchor.
    Inbound,
    /// The anchor asserts something about this item.
    Outbound,
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_explore` input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpExploreRequest {
    pub item_id: String,
    pub session_trace_id: Option<String>,
    pub direction: Option<String>,
    pub relation_types: Option<Vec<String>>,
    pub depth: Option<u32>,
    pub include_standing: Option<bool>,
    pub include_references: Option<bool>,
    pub limit: Option<u32>,
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

/// Response for `tribal_explore`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpExploreResponse {
    /// The full knowledge item for the exploration anchor.
    pub anchor: McpKnowledgeItem,
    /// Evidential profile of the anchor — always present.
    pub anchor_standing: McpStanding,
    pub related_items: Vec<McpExplorationResult>,
    pub trace_id: String,
    pub exact: bool,
}

/// A single exploration result with relationship metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpExplorationResult {
    pub item: McpKnowledgeItem,
    pub relation_type: RelationKind,
    pub relation_direction: McpRelationDirection,
    pub depth: u32,
    pub relation_created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub standing: Option<McpStanding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<McpReference>>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::{ProjectId, Standing};

    use super::{
        super::common::{McpSourceContext, McpSourceType},
        *,
    };

    fn sample_item(id: &str) -> McpKnowledgeItem {
        McpKnowledgeItem {
            id: id.to_owned(),
            project_id: ProjectId::new().to_string(),
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
            &Standing::builder()
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
        let req: McpExploreRequest = serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.item_id, "ki_abc");
        assert!(req.direction.is_none());
    }

    #[test]
    fn test_explore_request_deserialises_full() {
        let json = serde_json::json!({
            "item_id": "ki_abc",
            "session_trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
            "direction": "both",
            "relation_types": ["supports", "contradicts"],
            "depth": 2,
            "include_standing": true,
            "include_references": false,
            "limit": 50
        });
        let req: McpExploreRequest = serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.depth, Some(2));
        assert_eq!(req.relation_types.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_explore_response_serialises_anchor_standing_required() {
        let resp = McpExploreResponse {
            anchor: sample_item("ki_anchor"),
            anchor_standing: sample_standing(),
            related_items: vec![],
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".into(),
            exact: true,
        };
        let json = serde_json::to_value(&resp).expect("serialises");
        assert!(json.get("anchor_standing").is_some());
        assert!(json["anchor_standing"].is_object());
    }
}
