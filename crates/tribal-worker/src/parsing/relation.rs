//! Relation response parsing and LLM response types.

use std::fmt;

use serde::Deserialize;
use tribal_domain::RelationKind;
use tribal_inference::CompletionResponse;

use super::deserialise;
use crate::error::StageError;

// ---------------------------------------------------------------------------
// IngestionRelationKind
// ---------------------------------------------------------------------------

/// Relation types permitted during ingestion.
///
/// A subset of [`RelationKind`] that excludes `Supersedes`, which is
/// a system-determined relationship and must not be produced by
/// inference. By omitting the variant from the ingestion schema,
/// any `supersedes` (or other unknown) relation types returned by the
/// model will be rejected during deserialisation, rather than filtered
/// out post-hoc or constrained only via prompt instructions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(description = "How the source claim relates to the target claim.")]
pub(crate) enum IngestionRelationKind {
    /// Source provides evidence for, reinforces, or adds supporting
    /// context to target.
    Supports,
    /// Source conflicts with, corrects, or undermines target.
    Contradicts,
    /// Source was logically derived from or builds upon target.
    DerivedFrom,
}

impl IngestionRelationKind {
    /// All variants in display order.
    pub(crate) const ALL: &[Self] = &[Self::Supports, Self::Contradicts, Self::DerivedFrom];
}

impl From<IngestionRelationKind> for RelationKind {
    fn from(kind: IngestionRelationKind) -> Self {
        match kind {
            IngestionRelationKind::Supports => Self::Supports,
            IngestionRelationKind::Contradicts => Self::Contradicts,
            IngestionRelationKind::DerivedFrom => Self::DerivedFrom,
        }
    }
}

impl fmt::Display for IngestionRelationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        RelationKind::from(*self).fmt(f)
    }
}

// ---------------------------------------------------------------------------
// RelationOutput
// ---------------------------------------------------------------------------

/// The deserialised output from the relation LLM call.
///
/// Lenient serde — unknown fields are silently ignored so the LLM
/// can return extra keys without breaking parsing.
#[derive(Debug, Clone, PartialEq, Deserialize, schemars::JsonSchema)]
#[schemars(
    description = "The relationship edges to create. Empty when no genuine relationship \
    holds between the claims."
)]
pub(crate) struct RelationOutput {
    /// The complete set of relations to create for this job.
    #[schemars(
        description = "Directed edges, each connecting a source claim to a target claim. \
        Only edges grounded in the content of both claims."
    )]
    pub relations: Vec<RelationEdge>,
}

/// A single directed relationship edge to create.
#[derive(Debug, Clone, PartialEq, Deserialize, schemars::JsonSchema)]
#[schemars(description = "A directed relationship between two claims.")]
pub(crate) struct RelationEdge {
    /// The source claim (the claim asserting the relationship).
    #[schemars(description = "The claim asserting the relationship.")]
    pub source: RelationTarget,
    /// The target claim.
    #[schemars(description = "The claim the relationship is asserted about.")]
    pub target: RelationTarget,
    /// The relationship type.
    #[schemars(description = "How the source relates to the target.")]
    pub relation_type: IngestionRelationKind,
    /// The agent's reasoning for this relationship.
    #[serde(default)]
    #[schemars(
        description = "Why this relationship holds, grounded in the content of both claims."
    )]
    pub justification: Option<String>,
}

/// Identifies one end of a relationship edge.
///
/// All items in the relation prompt context — both extraction
/// candidates and existing items from similar-item decisions — are
/// assigned a sequential index. The model references items
/// exclusively by this index; the worker resolves indices to
/// `KnowledgeItemId`s via a unified lookup table before persisting.
///
/// Uses `#[serde(tag = "kind")]` (internally tagged) for explicit
/// discrimination and forward compatibility.
#[derive(Debug, Clone, PartialEq, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind")]
#[schemars(description = "A reference to a knowledge item by its context index.")]
pub(crate) enum RelationTarget {
    /// An item from the prompt context, identified by its zero-based
    /// position in the numbered item list.
    /// Wire format: `{"kind": "context_index", "context_index": 2}`
    #[serde(rename = "context_index")]
    #[schemars(description = "An item from the prompt context, referenced by its \
        zero-based index in the numbered item list. The index must be one of those shown in the prompt.")]
    ContextIndex { context_index: u32 },
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parses a completion response into a [`RelationOutput`].
///
/// Individual edges that fail to deserialise (e.g. unknown
/// `relation_type` values) are skipped with a warning rather than
/// failing the entire response. The outer structure must be valid.
///
/// # Errors
///
/// Returns [`StageError::Parse`] if the outer response structure
/// cannot be deserialised, with an operator-safe `context` and the
/// full `raw_response` for debugging.
pub(crate) fn parse_relation_response(
    response: &CompletionResponse,
) -> Result<RelationOutput, StageError> {
    let raw: RawRelationOutput =
        deserialise::tolerant(&response.text).map_err(|e| StageError::Parse {
            context: format!("deserialising relation output: {e}"),
            raw_response: Some(response.text.clone()),
        })?;

    let relations = raw
        .relations
        .into_iter()
        .enumerate()
        .filter_map(
            |(index, edge_value)| match serde_json::from_value::<RelationEdge>(edge_value) {
                Ok(edge) => Some(edge),
                Err(error) => {
                    tracing::warn!(%error, index, "skipping relation edge with unrecognised structure");
                    None
                }
            },
        )
        .collect();

    Ok(RelationOutput { relations })
}

/// Intermediate type for lenient edge parsing. The outer structure
/// must be valid JSON; individual edges are parsed independently.
#[derive(Deserialize)]
struct RawRelationOutput {
    relations: Vec<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
    fn test_parse_valid_relations_with_context_index() {
        let json = r#"{
            "relations": [
                {
                    "source": {"kind": "context_index", "context_index": 0},
                    "target": {"kind": "context_index", "context_index": 1},
                    "relation_type": "supports",
                    "justification": "Both describe memory safety"
                },
                {
                    "source": {"kind": "context_index", "context_index": 0},
                    "target": {"kind": "context_index", "context_index": 3},
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
                    source: RelationTarget::ContextIndex { context_index: 0 },
                    target: RelationTarget::ContextIndex { context_index: 1 },
                    relation_type: IngestionRelationKind::Supports,
                    justification: Some("Both describe memory safety".into()),
                },
                RelationEdge {
                    source: RelationTarget::ContextIndex { context_index: 0 },
                    target: RelationTarget::ContextIndex { context_index: 3 },
                    relation_type: IngestionRelationKind::Contradicts,
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
                    "source": {"kind": "context_index", "context_index": 0},
                    "target": {"kind": "context_index", "context_index": 4},
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

    #[test]
    fn test_parse_skips_edges_with_unknown_relation_type() {
        let json = r#"{
            "relations": [
                {
                    "source": {"kind": "context_index", "context_index": 0},
                    "target": {"kind": "context_index", "context_index": 1},
                    "relation_type": "supports"
                },
                {
                    "source": {"kind": "context_index", "context_index": 0},
                    "target": {"kind": "context_index", "context_index": 2},
                    "relation_type": "supersedes"
                },
                {
                    "source": {"kind": "context_index", "context_index": 1},
                    "target": {"kind": "context_index", "context_index": 2},
                    "relation_type": "contradicts"
                }
            ]
        }"#;
        let response = mock_response(json);
        let result = parse_relation_response(&response).unwrap();
        assert_eq!(
            result.relations.len(),
            2,
            "supersedes edge should be skipped, got: {:?}",
            result.relations,
        );
        assert_eq!(
            result.relations[0].relation_type,
            IngestionRelationKind::Supports,
        );
        assert_eq!(
            result.relations[1].relation_type,
            IngestionRelationKind::Contradicts,
        );
    }
}
