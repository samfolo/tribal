//! MCP server glue for `tribal_explore`: result rendering and DB conversions.

use std::fmt::Write;

use rmcp::model::{CallToolResult, Content};
use tribal_db::TraversalDirection;
use tribal_domain::RelationKind;
use tribal_wire::mcp::{McpExploreResponse, McpRelationDirection};

use crate::error::IntoCallToolResult;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERIALISE_EXPLORE_RESPONSE: &str = "McpExploreResponse should always serialise successfully";

// ---------------------------------------------------------------------------
// McpRelationDirection
// ---------------------------------------------------------------------------

/// Map a database traversal direction onto its wire representation.
pub(crate) fn relation_direction_from_db(
    direction: tribal_db::TraversalDirection,
) -> tribal_wire::mcp::McpRelationDirection {
    match direction {
        TraversalDirection::Inbound => McpRelationDirection::Inbound,
        TraversalDirection::Outbound => McpRelationDirection::Outbound,
    }
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
                let _ = write!(
                    text,
                    "\nThis item has been superseded by {} (newer understanding)",
                    result.item.id,
                );
                break;
            }
        }

        let structured = serde_json::to_value(&self).expect(SERIALISE_EXPLORE_RESPONSE);
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
    use tribal_domain::{ProjectId, Standing};
    use tribal_wire::mcp::{
        McpExplorationResult, McpKnowledgeItem, McpSourceContext, McpSourceType, McpStanding,
    };

    use super::*;

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
    fn test_relation_direction_from_db() {
        assert_eq!(
            relation_direction_from_db(TraversalDirection::Inbound),
            McpRelationDirection::Inbound,
        );
        assert_eq!(
            relation_direction_from_db(TraversalDirection::Outbound),
            McpRelationDirection::Outbound,
        );
    }

    #[test]
    fn test_explore_response_into_call_tool_result_without_supersession() {
        let resp = McpExploreResponse {
            anchor: sample_item("ki_anchor"),
            anchor_standing: sample_standing(),
            related_items: vec![McpExplorationResult {
                item: sample_item("ki_related"),
                relation_type: RelationKind::Supports,
                relation_direction: McpRelationDirection::Inbound,
                depth: 1,
                relation_created_at: chrono::Utc::now(),
                standing: None,
                references: None,
            }],
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".into(),
            exact: true,
        };
        let result = resp.into_call_tool_result();
        assert_eq!(result.is_error, Some(false));
        assert!(result.structured_content.is_some());

        let RawContent::Text(t) = &result.content[0].raw else {
            panic!("expected text content");
        };
        let text = &t.text;
        assert!(text.contains("ki_anchor"));
        assert!(text.contains("1 related item(s)"));
        assert!(!text.contains("superseded"));
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
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".into(),
            exact: true,
        };
        let result = resp.into_call_tool_result();
        assert_eq!(result.is_error, Some(false));

        let RawContent::Text(t) = &result.content[0].raw else {
            panic!("expected text content");
        };
        let text = &t.text;
        assert!(text.contains("superseded by ki_superseder"));
    }
}
