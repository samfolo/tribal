//! Synthetic context builder for prompt template validation.
//!
//! Calls the same per-(stage, role) context builders that production uses,
//! with synthetic data. Adding a new context variable or parameter to a
//! builder is automatically reflected here; adding a new parameter causes
//! a compile error at the call site below.

use serde_json::json;
use tribal_domain::{
    Candidate, KnowledgeItemId, KnowledgeKind, PromptRole, PromptStage, RelationHint,
    RelationSuggestion,
};

use super::{
    CandidateOutcome, RelationPromptContext, SimilarItemContext, SimilarItemDecisionContext,
    extraction_user_context, relation_user_context, triage_user_context, variables::system_context,
};

/// Builds a [`tera::Context`] matching the production context shape for
/// the given (stage, role) pair.
///
/// Delegates to the same context builder functions that the production
/// `assemble_*_prompt` functions use. The builder parameter lists are
/// the compile-time contract: if production adds a new parameter, this
/// call site must be updated.
///
/// # Panics
///
/// Panics if the hardcoded synthetic JSON cannot be deserialised into
/// the corresponding domain type. This is a programming error — the
/// JSON literals are compile-time constants.
#[must_use]
pub fn synthetic_validation_context(stage: PromptStage, role: PromptRole) -> tera::Context {
    match (stage, role) {
        (_, PromptRole::System) => system_context("{}"),

        (PromptStage::Extraction, PromptRole::User) => extraction_user_context("x", &["x"]),

        (PromptStage::Triage, PromptRole::User) => {
            let candidate: Candidate = serde_json::from_value(json!({
                "kind": "fact",
                "content": "x",
                "suggested_tags": ["x"],
            }))
            .expect("synthetic candidate is valid");

            let similar = SimilarItemContext {
                item_id: KnowledgeItemId::new(),
                kind: KnowledgeKind::Fact,
                content: "x".to_owned(),
                similarity_score: 0.5,
                tags: vec!["x".to_owned()],
            };

            triage_user_context(&candidate, &[similar], &["x"])
        }

        (PromptStage::Relation, PromptRole::User) => {
            let candidate: Candidate = serde_json::from_value(json!({
                "kind": "fact",
                "content": "x",
                "suggested_tags": ["x"],
            }))
            .expect("synthetic candidate is valid");

            let hint: RelationHint = serde_json::from_value(json!({
                "source_index": 0,
                "target_index": 1,
                "hint_type": "derived_from",
            }))
            .expect("synthetic relation hint is valid");

            let outcome = CandidateOutcome {
                batch_index: 0,
                candidate: &candidate,
                outcome: "novel".to_owned(),
                item_id: Some(KnowledgeItemId::new()),
            };

            let decision = SimilarItemDecisionContext {
                batch_index: 0,
                matched_item_id: KnowledgeItemId::new(),
                matched_content: "x".to_owned(),
                similarity_score: 0.5,
                suggested_relation: RelationSuggestion::Supports,
                justification: "x".to_owned(),
            };

            let prompt_context = RelationPromptContext {
                candidates: vec![outcome],
                relation_hints: &[hint],
                similar_item_decisions: &[decision],
            };

            relation_user_context(&prompt_context)
        }
    }
}
