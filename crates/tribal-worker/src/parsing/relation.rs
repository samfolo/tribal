//! Relation response parsing and LLM response types.

use serde::Deserialize;
use tribal_domain::{KnowledgeItemId, RelationKind};
use tribal_inference::CompletionResponse;

use crate::error::StageError;

// ---------------------------------------------------------------------------
// RelationOutput
// ---------------------------------------------------------------------------

/// The deserialised output from the relation LLM call.
///
/// Lenient serde — unknown fields are silently ignored so the LLM
/// can return extra keys without breaking parsing.
#[derive(Debug, Clone, PartialEq, Deserialize, schemars::JsonSchema)]
pub(crate) struct RelationOutput {
    /// The complete set of relations to create for this job.
    pub relations: Vec<RelationEdge>,
}

/// A single directed relationship edge to create.
#[derive(Debug, Clone, PartialEq, Deserialize, schemars::JsonSchema)]
pub(crate) struct RelationEdge {
    /// The source item (the item asserting the relationship).
    pub source: RelationTarget,
    /// The target item.
    pub target: RelationTarget,
    /// The relationship type.
    pub relation_type: RelationKind,
    /// The agent's reasoning for this relationship.
    #[serde(default)]
    pub justification: Option<String>,
}

/// Identifies one end of a relationship edge.
///
/// The relation agent may reference items by their batch index (for
/// candidates created in this episode) or by their `KnowledgeItemId`
/// (for existing items found during triage similarity search).
/// The worker resolves batch indices to `KnowledgeItemId`s via triage
/// results before persisting.
///
/// Uses `#[serde(tag = "kind")]` (internally tagged) for explicit
/// discrimination.
#[derive(Debug, Clone, PartialEq, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind")]
pub(crate) enum RelationTarget {
    /// A candidate from the current episode, identified by its
    /// position in the extraction candidates array.
    /// Wire format: `{"kind": "batch_index", "batch_index": 2}`
    #[serde(rename = "batch_index")]
    BatchIndex { batch_index: u32 },
    /// An existing knowledge item, identified by ID.
    /// Wire format: `{"kind": "item_id", "item_id": "ki_..."}`
    #[serde(rename = "item_id")]
    ItemId { item_id: KnowledgeItemId },
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parses a completion response into a [`RelationOutput`].
///
/// # Errors
///
/// Returns [`StageError::Parse`] if the response text cannot be
/// deserialised into [`RelationOutput`], with an operator-safe
/// `context` and the full `raw_response` for debugging.
pub(crate) fn parse_relation_response(
    response: &CompletionResponse,
) -> Result<RelationOutput, StageError> {
    serde_json::from_str::<RelationOutput>(&response.text).map_err(|e| StageError::Parse {
        context: format!("deserialising relation output: {e}"),
        raw_response: Some(response.text.clone()),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tribal_domain::{KnowledgeItemId, RelationKind};
    use tribal_inference::CompletionUsage;

    use super::*;

    fn mock_response(text: &str) -> CompletionResponse {
        CompletionResponse {
            text: text.to_owned(),
            usage: CompletionUsage {
                provider: "test".into(),
                model: "test-model".into(),
                input_tokens: 0,
                output_tokens: 0,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                total_tokens: 0,
                latency: Duration::ZERO,
            },
        }
    }

    #[test]
    fn test_parse_valid_empty_relations() {
        let json = r#"{"relations": []}"#;
        let response = mock_response(json);
        let result = parse_relation_response(&response).unwrap();
        assert!(result.relations.is_empty());
    }

    #[test]
    fn test_parse_valid_relations_with_batch_index_and_item_id() {
        let ki_id: KnowledgeItemId = "ki_550e8400-e29b-41d4-a716-446655440000".parse().unwrap();

        let json = r#"{
            "relations": [
                {
                    "source": {"kind": "batch_index", "batch_index": 0},
                    "target": {"kind": "batch_index", "batch_index": 1},
                    "relation_type": "supports",
                    "justification": "Both describe memory safety"
                },
                {
                    "source": {"kind": "batch_index", "batch_index": 0},
                    "target": {"kind": "item_id", "item_id": "ki_550e8400-e29b-41d4-a716-446655440000"},
                    "relation_type": "contradicts"
                }
            ]
        }"#;
        let response = mock_response(json);
        let result = parse_relation_response(&response).unwrap();

        assert_eq!(
            result.relations,
            vec![
                RelationEdge {
                    source: RelationTarget::BatchIndex { batch_index: 0 },
                    target: RelationTarget::BatchIndex { batch_index: 1 },
                    relation_type: RelationKind::Supports,
                    justification: Some("Both describe memory safety".into()),
                },
                RelationEdge {
                    source: RelationTarget::BatchIndex { batch_index: 0 },
                    target: RelationTarget::ItemId { item_id: ki_id },
                    relation_type: RelationKind::Contradicts,
                    justification: None,
                },
            ]
        );
    }

    #[test]
    fn test_parse_invalid_json_returns_parse_error() {
        let response = mock_response("not json at all");
        let result = parse_relation_response(&response);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            StageError::Parse { raw_response, .. } => {
                assert_eq!(raw_response.as_deref(), Some("not json at all"));
            }
            other => panic!("expected StageError::Parse, got {other}"),
        }
    }

    #[test]
    fn test_parse_unknown_fields_are_ignored() {
        let json = r#"{"relations": [], "extra_field": true}"#;
        let response = mock_response(json);
        let result = parse_relation_response(&response);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_relation_with_justification() {
        let json = r#"{
            "relations": [
                {
                    "source": {"kind": "item_id", "item_id": "ki_550e8400-e29b-41d4-a716-446655440000"},
                    "target": {"kind": "item_id", "item_id": "ki_660e8400-e29b-41d4-a716-446655440000"},
                    "relation_type": "derived_from",
                    "justification": "The conclusion follows from the premise"
                }
            ]
        }"#;
        let response = mock_response(json);
        let result = parse_relation_response(&response).unwrap();
        assert_eq!(result.relations.len(), 1);
        assert_eq!(
            result.relations[0].justification.as_deref(),
            Some("The conclusion follows from the premise")
        );
    }
}
