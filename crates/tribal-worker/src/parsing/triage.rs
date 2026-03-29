//! Triage response parsing and LLM response types.

use serde::Deserialize;
use tribal_domain::{KnowledgeItemId, RelationSuggestion};
use tribal_inference::CompletionResponse;

use crate::error::StageError;

// ---------------------------------------------------------------------------
// TriageClassification
// ---------------------------------------------------------------------------

/// The triage agent's classification of a candidate.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[schemars(
    description = "The triage classification for a single candidate. Contains the \
    novel/duplicate decision and independent per-item relationship assessments."
)]
pub(crate) struct TriageClassification {
    /// Whether the candidate is novel or a duplicate.
    #[schemars(
        description = "The classification decision. Default to 'created' (novel) unless \
        the candidate makes the exact same specific claim as an existing item with no \
        meaningful new information. When uncertain, choose 'created'."
    )]
    pub outcome: TriageDecision,
    /// Per-similar-item decisions with justifications.
    #[schemars(
        description = "One entry per similar item provided. Each assessment is independent \
        of the outcome decision and must include a justification referencing both items."
    )]
    pub similar_item_decisions: Vec<SimilarItemClassification>,
}

impl TriageClassification {
    /// Reconciles the classification against system invariants,
    /// correcting known classes of model error.
    pub fn reconcile(&mut self) {
        self.reconcile_contradiction_as_duplicate();
    }

    /// A candidate that contradicts any existing item cannot be a
    /// duplicate — the knowledge base needs both perspectives.
    fn reconcile_contradiction_as_duplicate(&mut self) {
        if !matches!(self.outcome, TriageDecision::Duplicate { .. }) {
            return;
        }

        let has_contradiction = self
            .similar_item_decisions
            .iter()
            .any(|d| d.suggested_relation == RelationSuggestion::Contradicts);

        if has_contradiction {
            tracing::warn!(
                "overriding duplicate to novel — \
                 model classified as duplicate but a similar item was assessed as contradicts",
            );
            self.outcome = TriageDecision::Novel;
        }
    }
}

/// The triage decision for a candidate.
///
/// Uses expressive Rust names (`Novel`/`Duplicate`) with serde renames
/// to match the wire format (`created`/`duplicate`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(tag = "decision")]
#[schemars(
    description = "The classification outcome. 'created' is the default — only use \
    'duplicate' when the candidate makes the exact same claim as an existing item. \
    A candidate assessed as contradicting any similar item MUST be 'created'."
)]
pub(crate) enum TriageDecision {
    /// The candidate is novel — a new knowledge item should be created.
    #[serde(rename = "created")]
    #[schemars(
        description = "The candidate contains information not already captured. \
        This is the default — use when the candidate adds any new context, detail, \
        correction, or perspective beyond what existing items capture."
    )]
    Novel,
    /// The candidate duplicates an existing item.
    #[serde(rename = "duplicate")]
    #[schemars(
        description = "The candidate restates an existing item with no meaningful new \
        information. Must not be used when any similar_item_decision has suggested_relation \
        'contradicts' — a contradiction is always novel."
    )]
    Duplicate {
        /// The existing item the candidate matches.
        #[schemars(
            description = "The identifier of the existing item this candidate duplicates. \
            Must reference an item from the similar items provided."
        )]
        matched_item_id: KnowledgeItemId,
    },
}

/// The triage agent's decision about a single similar item.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[schemars(
    description = "An independent assessment of one existing item's relationship to \
    the candidate."
)]
pub(crate) struct SimilarItemClassification {
    /// The existing item that was compared against.
    #[schemars(description = "The identifier of the existing item being assessed.")]
    pub item_id: KnowledgeItemId,
    /// The agent's suggested relation classification.
    #[schemars(
        description = "The relationship between the existing item and the candidate. \
        If 'contradicts', the outcome decision must be 'created' — contradictory \
        information cannot be a duplicate."
    )]
    pub suggested_relation: RelationSuggestion,
    /// The agent's reasoning for the classification.
    #[schemars(
        description = "Brief explanation referencing specific content from both items. \
        Explains why this relationship classification was chosen."
    )]
    pub justification: String,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parses a completion response into a [`TriageClassification`].
///
/// # Errors
///
/// Returns [`StageError::Parse`] if the response text cannot be
/// deserialised into [`TriageClassification`], with an operator-safe
/// `context` and the full `raw_response` for debugging.
pub(crate) fn parse_triage_response(
    response: &CompletionResponse,
) -> Result<TriageClassification, StageError> {
    serde_json::from_str::<TriageClassification>(&response.text).map_err(|e| StageError::Parse {
        context: format!("deserialising triage classification: {e}"),
        raw_response: Some(response.text.clone()),
    })
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
    fn test_parse_novel_classification() {
        let json = r#"{
            "outcome": { "decision": "created" },
            "similar_item_decisions": []
        }"#;
        let response = mock_response(json);
        let result = parse_triage_response(&response);
        assert!(result.is_ok());
        let classification = result.unwrap();
        assert!(classification.similar_item_decisions.is_empty());
    }

    #[test]
    fn test_parse_duplicate_classification() {
        let json = r#"{
            "outcome": {
                "decision": "duplicate",
                "matched_item_id": "ki_550e8400-e29b-41d4-a716-446655440000"
            },
            "similar_item_decisions": []
        }"#;
        let response = mock_response(json);
        let result = parse_triage_response(&response);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_unknown_fields_are_ignored() {
        let json = r#"{
            "outcome": { "decision": "created" },
            "similar_item_decisions": [],
            "extra_field": true
        }"#;
        let response = mock_response(json);
        let result = parse_triage_response(&response);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_invalid_json_returns_parse_error() {
        let response = mock_response("not json at all");
        let result = parse_triage_response(&response);
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
    fn test_parse_with_similar_item_decisions() {
        let json = r#"{
            "outcome": { "decision": "created" },
            "similar_item_decisions": [
                {
                    "item_id": "ki_550e8400-e29b-41d4-a716-446655440000",
                    "suggested_relation": "supports",
                    "justification": "Both describe Rust memory safety"
                }
            ]
        }"#;
        let response = mock_response(json);
        let result = parse_triage_response(&response);
        assert!(result.is_ok());
        let classification = result.unwrap();
        assert_eq!(classification.similar_item_decisions.len(), 1);
    }
}
