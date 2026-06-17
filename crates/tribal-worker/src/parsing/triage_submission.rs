//! The agentic triage submission: the `submit_result` tool's schema.
//!
//! ID-referencing where the one-shot classification is index-referencing:
//! the loop's model reads item identifiers from its rendered context and
//! committed tool results, and the submission copies them back. The
//! validator pipeline resolves every referenced id against what the
//! thread actually saw, so the schema needs no slots mechanism.

use serde::{Deserialize, Serialize};
use tribal_domain::RelationSuggestion;

/// The upper bound on the handoff's length, in characters. The handoff
/// is curated context for the downstream relation stage, not a second
/// transcript; the validator bounces anything longer.
pub(crate) const HANDOFF_MAX_CHARS: usize = 2_000;

/// The agentic triage submission, as `submit_result` accepts it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(
    description = "The final triage decision for the candidate. Contains the novel/duplicate \
    decision, an assessment of each existing claim you examined, and optional notes for the \
    downstream relation stage."
)]
pub(crate) struct TriageSubmission {
    /// Whether the candidate is novel or a duplicate.
    #[schemars(description = "Whether the candidate is novel or duplicates an existing claim.")]
    pub decision: TriageSubmissionDecision,
    /// Per-examined-item assessments with justifications.
    #[serde(default)]
    #[schemars(
        description = "One assessment per existing claim you examined, each made independently \
        of the novel/duplicate decision. Reference claims by the exact item id you were shown."
    )]
    pub considered_items: Vec<ConsideredItemAssessment>,
    /// Bounded notes for the downstream relation stage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(max = 2_000),
        description = "Optional short notes the relation stage should know: connections you \
        noticed, context that informed the decision. At most 2000 characters; never a \
        transcript."
    )]
    pub handoff: Option<String>,
}

/// The submission's decision, referencing matches by item id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "decision")]
#[schemars(
    description = "Whether the candidate records new knowledge or duplicates an existing claim."
)]
pub(crate) enum TriageSubmissionDecision {
    /// The candidate is novel; a new claim should be created.
    #[serde(rename = "created")]
    #[schemars(
        description = "The candidate records knowledge not already captured by an existing \
        claim. Default to this when the candidate adds any new context, or when uncertain."
    )]
    Novel,
    /// The candidate duplicates an existing claim.
    #[serde(rename = "duplicate")]
    #[schemars(
        description = "The candidate restates an existing claim, adding no meaningful new \
        information. A candidate that contradicts an existing claim is never a duplicate."
    )]
    Duplicate {
        /// The duplicated claim, referenced by its item id.
        #[schemars(
            description = "The existing claim this candidate duplicates: its item id, copied \
            exactly as shown in your context or a tool result."
        )]
        matched_item_id: String,
    },
}

/// One examined claim's assessment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(
    description = "An independent assessment of one existing claim's relationship to the \
    candidate."
)]
pub(crate) struct ConsideredItemAssessment {
    /// The examined claim, referenced by its item id.
    #[schemars(
        description = "The existing claim being assessed: its item id, copied exactly as \
        shown in your context or a tool result."
    )]
    pub item_id: String,
    /// The suggested relation classification.
    #[schemars(description = "How the candidate relates to the existing claim.")]
    pub suggested_relation: RelationSuggestion,
    /// The reasoning for the classification.
    #[schemars(
        description = "Why this assessment was chosen, grounded in the content of both claims."
    )]
    pub justification: String,
}

/// The `submit_result` tool's input schema: a binding-hash input, so
/// its stability is part of the binding's identity.
pub(crate) fn triage_submission_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(TriageSubmission))
        .expect("schema_for! produces serialisable output")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_submission_parses_the_wire_shape() {
        let submission: TriageSubmission = serde_json::from_value(serde_json::json!({
            "decision": {
                "decision": "duplicate",
                "matched_item_id": "ki_00000000-0000-0000-0000-000000000001",
            },
            "considered_items": [{
                "item_id": "ki_00000000-0000-0000-0000-000000000001",
                "suggested_relation": "supports",
                "justification": "restates the claim",
            }],
            "handoff": "the candidate links auth and caching",
        }))
        .expect("parses");
        assert!(matches!(
            submission.decision,
            TriageSubmissionDecision::Duplicate { ref matched_item_id }
                if matched_item_id == "ki_00000000-0000-0000-0000-000000000001"
        ));
        assert_eq!(submission.considered_items.len(), 1);
    }

    #[test]
    fn test_unknown_fields_are_rejected() {
        // The submission is a strict contract: an unrecognised field is a
        // bounce with diagnostics, never silently dropped.
        let result = serde_json::from_value::<TriageSubmission>(serde_json::json!({
            "decision": {"decision": "created"},
            "verdict": "novel",
        }));
        assert!(result.is_err());
    }

    #[test]
    fn test_the_schema_names_the_decision_discriminator() {
        let schema = triage_submission_schema();
        let rendered = schema.to_string();
        assert!(rendered.contains("matched_item_id"));
        assert!(rendered.contains("considered_items"));
        assert!(rendered.contains("handoff"));
    }
}
