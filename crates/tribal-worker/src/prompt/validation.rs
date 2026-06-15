//! Synthetic context builder for prompt template validation.
//!
//! Calls the same per-(stage, role) context builders that production uses,
//! with synthetic data. Adding a new context variable or parameter to a
//! builder is automatically reflected here; adding a new parameter causes
//! a compile error at the call site below.

use serde_json::json;
use tribal_domain::{
    Candidate, KnowledgeItemId, KnowledgeKind, PromptClass, PromptRole, PromptStage, RelationHint,
    RelationSuggestion,
};

use super::{
    CandidateOutcome, RelationPromptContext, SimilarItemContext, SimilarItemDecisionContext,
    VerifierConsideredItem, VerifierSubmissionContext, extraction_user_context,
    legends::SimilarityBand,
    loop_user_context, relation_user_context, triage_user_context,
    variables::{
        extraction_system_context, inject_validation_defaults, relation_system_context,
        triage_system_context,
    },
    verifier_user_context,
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
pub fn synthetic_validation_context(
    stage: PromptStage,
    class: PromptClass,
    role: PromptRole,
) -> tera::Context {
    let mut ctx = match class {
        PromptClass::Loop => loop_validation_context(stage, role),
        PromptClass::Verifier => verifier_validation_context(role),
        PromptClass::OneShot => one_shot_validation_context(stage, role),
    };

    // In production, the renderer injects reserved variables at
    // render time for all prompts. Validation matches this so that
    // the server's hot-reload validator sees the same variable set.
    // The server excludes reserved keys from the "must reference"
    // check via reserved_keys().
    inject_validation_defaults(&mut ctx);

    ctx
}

/// The agentic loop's synthetic contexts, by stage. The triage loop user
/// prompt consumes the triage shape; the relation loop user prompt consumes
/// the relation shape, and its system prompt renders the relation legends.
fn loop_validation_context(stage: PromptStage, role: PromptRole) -> tera::Context {
    match stage {
        PromptStage::Extraction => extraction_loop_validation_context(role),
        PromptStage::Triage => triage_loop_validation_context(role),
        PromptStage::Relation => relation_loop_validation_context(role),
    }
}

/// The extraction loop's synthetic context: a static system prompt, a user
/// prompt over the raw input and tag registry — the one-shot's own shape.
fn extraction_loop_validation_context(role: PromptRole) -> tera::Context {
    match role {
        PromptRole::System => extraction_system_context(),
        PromptRole::User => extraction_user_context("x", &["x"]),
    }
}

/// The triage loop's synthetic context: a static system prompt, a user
/// prompt over the candidate and tags, with no prefetched similar items
/// (the loop reaches the corpus through its search tool).
fn triage_loop_validation_context(role: PromptRole) -> tera::Context {
    match role {
        PromptRole::System => tera::Context::new(),
        PromptRole::User => {
            let candidate: Candidate = serde_json::from_value(json!({
                "kind": "fact",
                "content": "x",
                "suggested_tags": ["x"],
            }))
            .expect("synthetic candidate is valid");

            loop_user_context(&candidate, &["x"])
        }
    }
}

/// The relation loop's synthetic context: the system prompt renders the
/// relation legends, the user prompt the batch shape with committed ids.
fn relation_loop_validation_context(role: PromptRole) -> tera::Context {
    match role {
        PromptRole::System => relation_system_context(),
        PromptRole::User => {
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
                handoff: None,
            };

            let decision = SimilarItemDecisionContext {
                batch_index: 0,
                context_index: 1,
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
    }
}

/// The verifier's synthetic contexts: the system prompt is static, the
/// user prompt consumes the same production builder the submission
/// pipeline renders through, so a variable added there is reflected here.
fn verifier_validation_context(role: PromptRole) -> tera::Context {
    match role {
        PromptRole::System => tera::Context::new(),
        PromptRole::User => {
            let candidate: Candidate = serde_json::from_value(json!({
                "kind": "fact",
                "content": "x",
                "suggested_tags": ["x"],
            }))
            .expect("synthetic candidate is valid");

            let submission = VerifierSubmissionContext {
                decision: "duplicate".to_owned(),
                matched_item_id: Some(KnowledgeItemId::new().to_string()),
                handoff: Some("x".to_owned()),
            };
            let considered = VerifierConsideredItem {
                item_id: KnowledgeItemId::new().to_string(),
                kind: KnowledgeKind::Fact,
                assessment: "x".to_owned(),
                content: "x".to_owned(),
            };

            verifier_user_context(&candidate, &submission, &[considered])
        }
    }
}

/// The launched one-shot synthetic contexts.
fn one_shot_validation_context(stage: PromptStage, role: PromptRole) -> tera::Context {
    match (stage, role) {
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
                handoff: None,
            };

            let decision = SimilarItemDecisionContext {
                batch_index: 0,
                context_index: 1,
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
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::{PromptClass, PromptRole, PromptStage};

    use super::*;

    /// Renders every embedded default template against its synthetic
    /// context. Mirrors the server's hot-reload validation path.
    #[test]
    fn test_synthetic_context_renders_all_embedded_defaults() {
        let pairs: [(PromptStage, PromptClass, PromptRole, &str); 14] = [
            (
                PromptStage::Extraction,
                PromptClass::OneShot,
                PromptRole::System,
                include_str!("../../../../prompts/extraction/system.tera"),
            ),
            (
                PromptStage::Extraction,
                PromptClass::OneShot,
                PromptRole::User,
                include_str!("../../../../prompts/extraction/user.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::OneShot,
                PromptRole::System,
                include_str!("../../../../prompts/triage/system.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::OneShot,
                PromptRole::User,
                include_str!("../../../../prompts/triage/user.tera"),
            ),
            (
                PromptStage::Relation,
                PromptClass::OneShot,
                PromptRole::System,
                include_str!("../../../../prompts/relation/system.tera"),
            ),
            (
                PromptStage::Relation,
                PromptClass::OneShot,
                PromptRole::User,
                include_str!("../../../../prompts/relation/user.tera"),
            ),
            (
                PromptStage::Extraction,
                PromptClass::Loop,
                PromptRole::System,
                include_str!("../../../../prompts/extraction/loop_system.tera"),
            ),
            (
                PromptStage::Extraction,
                PromptClass::Loop,
                PromptRole::User,
                include_str!("../../../../prompts/extraction/loop_user.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::Loop,
                PromptRole::System,
                include_str!("../../../../prompts/triage/loop_system.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::Loop,
                PromptRole::User,
                include_str!("../../../../prompts/triage/loop_user.tera"),
            ),
            (
                PromptStage::Relation,
                PromptClass::Loop,
                PromptRole::System,
                include_str!("../../../../prompts/relation/loop_system.tera"),
            ),
            (
                PromptStage::Relation,
                PromptClass::Loop,
                PromptRole::User,
                include_str!("../../../../prompts/relation/loop_user.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::Verifier,
                PromptRole::System,
                include_str!("../../../../prompts/triage/verifier_system.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::Verifier,
                PromptRole::User,
                include_str!("../../../../prompts/triage/verifier_user.tera"),
            ),
        ];
        for (stage, class, role, content) in &pairs {
            let ctx = synthetic_validation_context(*stage, *class, *role);
            let result = tera::Tera::one_off(content, &ctx, false);
            assert!(
                result.is_ok(),
                "embedded default for {stage}/{class}/{role} failed to render against synthetic \
                 context: {}",
                result.unwrap_err(),
            );
        }
    }
}
