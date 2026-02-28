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
#[allow(dead_code)]
pub(crate) struct TriageClassification {
    /// Whether the candidate is novel or a duplicate.
    pub outcome: TriageDecision,
    /// Per-similar-item decisions with justifications.
    pub similar_item_decisions: Vec<SimilarItemClassification>,
}

/// The triage decision for a candidate.
///
/// Uses expressive Rust names (`Novel`/`Duplicate`) with serde renames
/// to match the wire format (`created`/`duplicate`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(tag = "decision")]
#[allow(dead_code)]
pub(crate) enum TriageDecision {
    /// The candidate is novel — a new knowledge item should be created.
    #[serde(rename = "created")]
    Novel,
    /// The candidate duplicates an existing item.
    #[serde(rename = "duplicate")]
    Duplicate {
        /// The existing item the candidate matches.
        matched_item_id: KnowledgeItemId,
    },
}

/// The triage agent's decision about a single similar item.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[allow(dead_code)]
pub(crate) struct SimilarItemClassification {
    /// The existing item that was compared against.
    pub item_id: KnowledgeItemId,
    /// The agent's suggested relation classification.
    pub suggested_relation: RelationSuggestion,
    /// The agent's reasoning for the classification.
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
#[allow(dead_code)]
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
