//! The agentic relation submission: the `submit_result` tool's schema.
//!
//! ID-referencing where the one-shot relation output is index-referencing:
//! the loop's model reads item identifiers from its rendered batch and its
//! committed tool results, and the submission copies them back. The
//! validator pipeline resolves every endpoint id against what the thread
//! actually saw, so the schema needs no positional lookup table.

use serde::{Deserialize, Serialize};

use super::IngestionRelationKind;

/// The upper bound on one edge's justification, in characters. The
/// justification is the agent's reasoning for the edge, not a transcript;
/// the validator bounces anything longer.
pub(crate) const RELATION_JUSTIFICATION_MAX_CHARS: usize = 2_000;

/// The agentic relation submission, as `submit_result` accepts it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(
    description = "The relationship edges to commit for this batch. Empty when no genuine \
    relationship holds between the claims you examined."
)]
pub(crate) struct RelationSubmission {
    /// The directed edges to commit, each between two item ids.
    #[serde(default)]
    #[schemars(
        description = "Directed edges, each connecting a source claim to a target claim by their \
        exact item ids. Only edges grounded in the content of both claims."
    )]
    pub relations: Vec<RelationSubmissionEdge>,
}

/// One directed relationship edge, referencing both endpoints by item id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "A directed relationship between two claims, referenced by item id.")]
pub(crate) struct RelationSubmissionEdge {
    /// The source claim's item id, the claim asserting the relationship.
    #[schemars(
        description = "The source claim's item id, copied exactly as shown in your batch or a \
        tool result: the claim asserting the relationship."
    )]
    pub source_id: String,
    /// The target claim's item id.
    #[schemars(
        description = "The target claim's item id, copied exactly as shown in your batch or a \
        tool result: the claim the relationship is asserted about."
    )]
    pub target_id: String,
    /// The relationship type.
    #[schemars(description = "How the source relates to the target.")]
    pub relation_type: IngestionRelationKind,
    /// The agent's reasoning for this edge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(max = 2_000),
        description = "Why this relationship holds, grounded in the content of both claims. At \
        most 2000 characters."
    )]
    pub justification: Option<String>,
}

/// The relation `submit_result` tool's input schema — a binding-hash
/// input, so its stability is part of the binding's identity.
pub(crate) fn relation_submission_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(RelationSubmission))
        .expect("schema_for! produces serialisable output")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_submission_parses_the_wire_shape() {
        let submission: RelationSubmission = serde_json::from_value(serde_json::json!({
            "relations": [{
                "source_id": "ki_00000000-0000-0000-0000-000000000001",
                "target_id": "ki_00000000-0000-0000-0000-000000000002",
                "relation_type": "supports",
                "justification": "the first reinforces the second",
            }],
        }))
        .expect("parses");
        assert_eq!(submission.relations.len(), 1);
        assert_eq!(
            submission.relations[0].relation_type,
            IngestionRelationKind::Supports,
        );
    }

    #[test]
    fn test_an_empty_submission_is_valid() {
        // The honest outcome when nothing relates: an empty edge set, not a
        // refusal to submit.
        let submission: RelationSubmission =
            serde_json::from_value(serde_json::json!({ "relations": [] })).expect("parses");
        assert!(submission.relations.is_empty());
    }

    #[test]
    fn test_unknown_fields_are_rejected() {
        let result = serde_json::from_value::<RelationSubmission>(serde_json::json!({
            "relations": [],
            "edges": [],
        }));
        assert!(result.is_err());
    }

    #[test]
    fn test_supersedes_is_not_a_submittable_relation_type() {
        // The ingestion relation kind excludes the system-determined
        // supersedes edge, so the schema rejects it at parse time.
        let result = serde_json::from_value::<RelationSubmission>(serde_json::json!({
            "relations": [{
                "source_id": "ki_00000000-0000-0000-0000-000000000001",
                "target_id": "ki_00000000-0000-0000-0000-000000000002",
                "relation_type": "supersedes",
            }],
        }));
        assert!(result.is_err());
    }

    #[test]
    fn test_the_schema_names_the_id_endpoints() {
        let schema = relation_submission_schema();
        let rendered = schema.to_string();
        assert!(rendered.contains("source_id"));
        assert!(rendered.contains("target_id"));
        assert!(rendered.contains("relation_type"));
    }
}
