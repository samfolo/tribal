//! The agentic extraction submission: the `submit_result` tool's schema.
//!
//! The same candidate-and-hints shape the one-shot returns as structured
//! output. The submission envelope is strict: an unknown field beside
//! `candidates` or `relation_hints` is rejected, since the loop's model
//! submits it as a tool call the validator can bounce. That strictness is
//! the envelope's own, not recursive into the candidate and hint shapes,
//! which stay the ones the one-shot parses.

use serde::{Deserialize, Serialize};
use tribal_domain::{Candidate, RelationHint};

/// The agentic extraction submission, as `submit_result` accepts it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(
    description = "The knowledge items extracted from the input, with any intra-batch relation \
    hints. Empty when the input holds no extractable knowledge."
)]
pub(crate) struct ExtractionSubmission {
    /// The extracted candidates.
    #[schemars(
        description = "Knowledge items extracted from the input, each an atomic, self-contained \
        claim. Empty when the input contains no extractable knowledge."
    )]
    pub candidates: Vec<Candidate>,
    /// Intra-batch relation hints between candidates, by index.
    #[serde(default)]
    #[schemars(
        description = "Relation hints between candidates in this batch, each referencing a \
        candidate by its zero-based position. Empty unless one candidate is genuinely derived \
        from another."
    )]
    pub relation_hints: Vec<RelationHint>,
}

/// The extraction `submit_result` tool's input schema: a binding-hash
/// input, so its stability is part of the binding's identity.
pub(crate) fn extraction_submission_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(ExtractionSubmission))
        .expect("schema_for! produces serialisable output")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_submission_parses_the_wire_shape() {
        let submission: ExtractionSubmission = serde_json::from_value(serde_json::json!({
            "candidates": [{
                "kind": "fact",
                "content": "tokens expire after an hour",
                "suggested_tags": ["auth"],
            }],
            "relation_hints": [],
        }))
        .expect("parses");
        assert_eq!(submission.candidates.len(), 1);
    }

    #[test]
    fn test_relation_hints_default_to_empty() {
        let submission: ExtractionSubmission =
            serde_json::from_value(serde_json::json!({ "candidates": [] })).expect("parses");
        assert!(submission.relation_hints.is_empty());
    }

    #[test]
    fn test_unknown_fields_are_rejected() {
        let result = serde_json::from_value::<ExtractionSubmission>(serde_json::json!({
            "candidates": [],
            "items": [],
        }));
        assert!(result.is_err());
    }

    #[test]
    fn test_the_schema_names_the_candidate_and_hint_fields() {
        let schema = extraction_submission_schema();
        let rendered = schema.to_string();
        assert!(rendered.contains("candidates"));
        assert!(rendered.contains("relation_hints"));
    }
}
