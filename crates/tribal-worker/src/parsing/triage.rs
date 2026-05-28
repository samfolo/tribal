//! Triage response parsing and LLM response types.

use serde::Deserialize;
use tribal_domain::RelationSuggestion;
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
    #[schemars(description = "Whether the candidate is novel or duplicates an existing item.")]
    pub outcome: TriageDecision,
    /// Per-similar-item decisions with justifications.
    #[schemars(
        description = "One assessment per provided similar item, each made independently \
        of the novel/duplicate outcome."
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

/// A reference to one of the similar items provided to triage.
///
/// Triage's index space contains *only* the similar items returned by
/// semantic search, numbered by their zero-based position in the prompt's
/// numbered list. (This differs from the relation stage's unified space,
/// which also numbers the batch's extraction candidates — hence a
/// triage-local type.) The worker resolves the index to a knowledge item
/// against the search results before persisting; an out-of-range index is
/// a handled outcome, never a parse failure.
///
/// Modelled as a typed structure rather than a bare integer or string, so an
/// ill-formed reference is unrepresentable at the schema boundary.
/// `#[serde(tag = "kind")]` (internally tagged) gives explicit discrimination
/// and room to add further reference kinds without a wire break.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind")]
#[schemars(description = "A reference to one of the provided similar items by its context index.")]
pub(crate) enum TriageItemReference {
    /// A similar item, identified by its zero-based position in the
    /// numbered similar-items list.
    ///
    /// Wire format: `{"kind": "context_index", "context_index": 0}`.
    #[serde(rename = "context_index")]
    #[schemars(
        description = "A similar item, referenced by its zero-based index in the \
        numbered similar-items list shown in the prompt."
    )]
    ContextIndex { context_index: u32 },
}

/// The triage decision for a candidate.
///
/// Uses expressive Rust names (`Novel`/`Duplicate`) with serde renames
/// to match the wire format (`created`/`duplicate`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(tag = "decision")]
#[schemars(
    description = "Whether the candidate records new knowledge or duplicates an existing item."
)]
pub(crate) enum TriageDecision {
    /// The candidate is novel — a new knowledge item should be created.
    #[serde(rename = "created")]
    #[schemars(
        description = "The candidate records knowledge not already captured by an \
        existing item. Default to this when the candidate adds any new context, or when \
        uncertain."
    )]
    Novel,
    /// The candidate duplicates an existing item.
    #[serde(rename = "duplicate")]
    #[schemars(
        description = "The candidate restates an existing item, adding no meaningful new \
        information. A candidate that contradicts an existing item is never a duplicate."
    )]
    Duplicate {
        /// The similar item the candidate duplicates, referenced by index.
        #[schemars(
            description = "The existing item this candidate duplicates, referenced by its \
            context index."
        )]
        matched_item: TriageItemReference,
    },
}

/// The triage agent's decision about a single similar item.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[schemars(
    description = "An independent assessment of one existing item's relationship to \
    the candidate."
)]
pub(crate) struct SimilarItemClassification {
    /// The similar item that was compared against, referenced by index.
    #[schemars(description = "The existing item being assessed, referenced by its context index.")]
    pub item: TriageItemReference,
    /// The agent's suggested relation classification.
    #[schemars(description = "How the candidate relates to the existing item.")]
    pub suggested_relation: RelationSuggestion,
    /// The agent's reasoning for the classification.
    #[schemars(
        description = "Why this assessment was chosen, grounded in the content of both items."
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
                "matched_item": { "kind": "context_index", "context_index": 0 }
            },
            "similar_item_decisions": []
        }"#;
        let response = mock_response(json);
        let result = parse_triage_response(&response);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_duplicate_with_context_index() {
        let json = r#"{
            "outcome": {
                "decision": "duplicate",
                "matched_item": { "kind": "context_index", "context_index": 2 }
            },
            "similar_item_decisions": []
        }"#;
        let response = mock_response(json);
        let classification = parse_triage_response(&response).unwrap();
        assert!(matches!(
            classification.outcome,
            TriageDecision::Duplicate {
                matched_item: TriageItemReference::ContextIndex { context_index: 2 }
            }
        ));
    }

    #[test]
    fn test_parse_duplicate_rejects_placeholder_string() {
        // A bare string (a UUID or a hallucinated placeholder like
        // "existing-item-1") is not admissible: the schema accepts only the
        // typed context-index reference.
        let json = r#"{
            "outcome": { "decision": "duplicate", "matched_item": "existing-item-1" },
            "similar_item_decisions": []
        }"#;
        let response = mock_response(json);
        assert!(parse_triage_response(&response).is_err());
    }

    #[test]
    fn test_parse_rejects_placeholder_string_in_similar_item_decision() {
        // The per-item reference is the same typed field, so a placeholder
        // string is inadmissible there too.
        let json = r#"{
            "outcome": { "decision": "created" },
            "similar_item_decisions": [
                {
                    "item": "existing-item-1",
                    "suggested_relation": "supports",
                    "justification": "placeholder reference"
                }
            ]
        }"#;
        let response = mock_response(json);
        assert!(parse_triage_response(&response).is_err());
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
                    "item": { "kind": "context_index", "context_index": 0 },
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
        assert!(matches!(
            classification.similar_item_decisions[0].item,
            TriageItemReference::ContextIndex { context_index: 0 }
        ));
    }

    // -- reconcile --------------------------------------------------------

    fn classification_with_decisions(
        decision: &str,
        matched_index: Option<u32>,
        relations: &[&str],
    ) -> TriageClassification {
        let outcome_json = match (decision, matched_index) {
            ("duplicate", Some(index)) => format!(
                r#"{{"decision": "duplicate", "matched_item": {{"kind": "context_index", "context_index": {index}}}}}"#
            ),
            _ => r#"{"decision": "created"}"#.to_owned(),
        };

        let decisions: Vec<SimilarItemClassification> = relations
            .iter()
            .enumerate()
            .map(|(i, rel)| SimilarItemClassification {
                item: TriageItemReference::ContextIndex {
                    context_index: u32::try_from(i).unwrap(),
                },
                suggested_relation: rel.parse().unwrap(),
                justification: "test".to_owned(),
            })
            .collect();

        TriageClassification {
            outcome: serde_json::from_str(&outcome_json).unwrap(),
            similar_item_decisions: decisions,
        }
    }

    #[test]
    fn test_reconcile_overrides_duplicate_with_contradiction() {
        let mut c = classification_with_decisions("duplicate", Some(0), &["contradicts"]);
        c.reconcile();
        assert!(matches!(c.outcome, TriageDecision::Novel));
    }

    #[test]
    fn test_reconcile_overrides_when_any_decision_contradicts() {
        let mut c = classification_with_decisions(
            "duplicate",
            Some(0),
            &["supports", "unrelated", "contradicts"],
        );
        c.reconcile();
        assert!(matches!(c.outcome, TriageDecision::Novel));
    }

    #[test]
    fn test_reconcile_preserves_duplicate_without_contradiction() {
        let mut c = classification_with_decisions("duplicate", Some(0), &["supports", "unrelated"]);
        c.reconcile();
        assert!(matches!(c.outcome, TriageDecision::Duplicate { .. }));
    }

    #[test]
    fn test_reconcile_preserves_novel_unchanged() {
        let mut c = classification_with_decisions("created", None, &["contradicts"]);
        c.reconcile();
        assert!(matches!(c.outcome, TriageDecision::Novel));
    }
}
