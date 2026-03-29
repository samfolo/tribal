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
    extraction_user_context,
    legends::SimilarityBand,
    relation_user_context,
    renderer::PromptRenderer,
    triage_user_context,
    variables::{extraction_system_context, relation_system_context, triage_system_context},
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
    let mut ctx = match (stage, role) {
        (PromptStage::Extraction, PromptRole::System) => extraction_system_context(),
        (PromptStage::Triage, PromptRole::System) => triage_system_context(),
        (PromptStage::Relation, PromptRole::System) => relation_system_context(),

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
                similarity_label: SimilarityBand::from(0.5).to_string(),
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
                outcome: "created".to_owned(),
                item_id: Some(KnowledgeItemId::new()),
            };

            let decision = SimilarItemDecisionContext {
                batch_index: 0,
                matched_item_id: KnowledgeItemId::new(),
                matched_content: "x".to_owned(),
                similarity_score: 0.5,
                similarity_label: SimilarityBand::from(0.5).to_string(),
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
    };

    // In production, PromptRenderer injects reserved variables at
    // render time. For validation contexts, we inject them here so the
    // server's required-variable check covers them.
    if role == PromptRole::User {
        PromptRenderer::inject_validation_defaults(&mut ctx);
    }

    ctx
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::{PromptRole, PromptStage};

    use super::*;

    /// Renders every embedded default template against its synthetic
    /// context. Mirrors the server's hot-reload validation path.
    #[test]
    fn test_synthetic_context_renders_all_embedded_defaults() {
        let pairs: [(PromptStage, PromptRole, &str); 6] = [
            (
                PromptStage::Extraction,
                PromptRole::System,
                include_str!("../../../../prompts/extraction/system.tera"),
            ),
            (
                PromptStage::Extraction,
                PromptRole::User,
                include_str!("../../../../prompts/extraction/user.tera"),
            ),
            (
                PromptStage::Triage,
                PromptRole::System,
                include_str!("../../../../prompts/triage/system.tera"),
            ),
            (
                PromptStage::Triage,
                PromptRole::User,
                include_str!("../../../../prompts/triage/user.tera"),
            ),
            (
                PromptStage::Relation,
                PromptRole::System,
                include_str!("../../../../prompts/relation/system.tera"),
            ),
            (
                PromptStage::Relation,
                PromptRole::User,
                include_str!("../../../../prompts/relation/user.tera"),
            ),
        ];
        for (stage, role, content) in &pairs {
            let ctx = synthetic_validation_context(*stage, *role);
            let result = tera::Tera::one_off(content, &ctx, false);
            assert!(
                result.is_ok(),
                "embedded default for {stage}/{role} failed to render against synthetic context: {}",
                result.unwrap_err(),
            );
        }
    }
}
