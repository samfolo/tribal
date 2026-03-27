//! Relation prompt assembly: template rendering and request construction.

use schemars::schema_for;
use serde::Serialize;
use tribal_domain::{Candidate, KnowledgeItemId, RelationHint, RelationSuggestion};
use tribal_inference::{CompletionRequest, Message, ResponseFormat, Role};

use crate::{
    error::StageError,
    parsing::RelationOutput,
    prompt::variables::{
        VAR_CANDIDATES, VAR_RELATION_HINTS, VAR_SCHEMA, VAR_SIMILAR_ITEM_DECISIONS,
    },
};

// ---------------------------------------------------------------------------
// Prompt context types
// ---------------------------------------------------------------------------

/// The full episode context provided to the relation agent.
///
/// Assembled from multiple database reads. This is the prompt-facing
/// representation — simpler than `RelationContext` which carries full
/// domain objects. Fields are structured for Tera template rendering.
///
/// Borrows from `RelationContext` to avoid cloning large collections.
#[derive(Debug, Serialize)]
pub(crate) struct RelationPromptContext<'a> {
    /// Each candidate with its triage outcome and created item ID (if any).
    pub candidates: Vec<CandidateOutcome<'a>>,
    /// Intra-batch relation hints from extraction.
    pub relation_hints: &'a [RelationHint],
    /// Similar item decisions from triage (all candidates combined).
    pub similar_item_decisions: &'a [SimilarItemDecisionContext],
}

/// A candidate paired with its triage outcome for the relation prompt.
#[derive(Debug, Serialize)]
pub(crate) struct CandidateOutcome<'a> {
    /// The candidate's position in the extraction batch.
    pub batch_index: u32,
    /// The original extracted candidate.
    pub candidate: &'a Candidate,
    /// The triage outcome: `"created"`, `"duplicate"`, or `"failed"`.
    pub outcome: String,
    /// The resolved `KnowledgeItemId` for created or duplicate
    /// outcomes. `None` for failed.
    pub item_id: Option<KnowledgeItemId>,
}

/// A triage similar item decision for prompt inclusion.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct SimilarItemDecisionContext {
    /// The candidate's position in the extraction batch.
    pub batch_index: u32,
    /// The existing item that was compared against.
    pub matched_item_id: KnowledgeItemId,
    /// The matched item's content.
    pub matched_content: String,
    /// Cosine similarity score. `f32` because this value is read from
    /// `TriageSimilarItemDecision` rows where precision was already
    /// reduced to `REAL` at the triage commit boundary.
    pub similarity_score: f32,
    /// The triage agent's suggested relation classification.
    pub suggested_relation: RelationSuggestion,
    /// The triage agent's reasoning for the classification.
    pub justification: String,
}

// ---------------------------------------------------------------------------
// Context builders
// ---------------------------------------------------------------------------

/// Builds the user prompt context for the relation stage.
///
/// Both the production assembly and the hot-reload validator call this,
/// so adding a variable here is automatically reflected in both paths.
pub(crate) fn relation_user_context(context: &RelationPromptContext<'_>) -> tera::Context {
    let mut ctx = tera::Context::new();
    ctx.insert(VAR_CANDIDATES, &context.candidates);
    ctx.insert(VAR_RELATION_HINTS, context.relation_hints);
    ctx.insert(VAR_SIMILAR_ITEM_DECISIONS, context.similar_item_decisions);
    ctx
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Assembles a [`CompletionRequest`] for the relation stage.
///
/// # Errors
///
/// Returns [`StageError::TemplateRender`] if either template cannot be
/// rendered.
pub(crate) fn assemble_relation_prompt(
    system_template: &str,
    user_template: &str,
    context: &RelationPromptContext<'_>,
) -> Result<CompletionRequest, StageError> {
    let schema = schema_for!(RelationOutput);
    let schema_value =
        serde_json::to_value(&schema).expect("schema_for! produces serialisable output");
    let schema_pretty =
        serde_json::to_string_pretty(&schema).expect("schema_for! produces serialisable output");

    let system_ctx = super::variables::system_context(&schema_pretty);

    let rendered_system =
        tera::Tera::one_off(system_template, &system_ctx, false).map_err(|e| {
            StageError::TemplateRender {
                context: "rendering relation system prompt".into(),
                source: e,
            }
        })?;

    let user_ctx = relation_user_context(context);

    let rendered_user = tera::Tera::one_off(user_template, &user_ctx, false).map_err(|e| {
        StageError::TemplateRender {
            context: "rendering relation user prompt".into(),
            source: e,
        }
    })?;

    Ok(CompletionRequest {
        system: Some(rendered_system),
        messages: vec![Message {
            role: Role::User,
            content: rendered_user,
        }],
        temperature: None,
        max_tokens: None,
        response_format: Some(ResponseFormat::JsonSchema {
            schema: schema_value,
        }),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::TaskErrorKind;

    use super::*;

    fn test_candidate(content: &str) -> Candidate {
        serde_json::from_value(serde_json::json!({
            "kind": "fact",
            "content": content,
            "suggested_tags": ["rust", "performance"],
        }))
        .expect("valid candidate JSON")
    }

    fn test_relation_hint(source: u32, target: u32) -> RelationHint {
        serde_json::from_value(serde_json::json!({
            "source_index": source,
            "target_index": target,
            "hint_type": "derived_from",
        }))
        .expect("valid relation hint JSON")
    }

    struct RichTestData {
        candidates: Vec<Candidate>,
        relation_hints: Vec<RelationHint>,
        similar_item_decisions: Vec<SimilarItemDecisionContext>,
        ki_a: KnowledgeItemId,
        ki_b: KnowledgeItemId,
    }

    fn rich_test_data() -> RichTestData {
        let ki_a: KnowledgeItemId = "ki_550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        let ki_b: KnowledgeItemId = "ki_660e8400-e29b-41d4-a716-446655440000".parse().unwrap();

        RichTestData {
            candidates: vec![
                test_candidate("Rust has zero-cost abstractions"),
                test_candidate("Ownership prevents data races"),
                test_candidate("Borrow checker validates lifetimes"),
            ],
            relation_hints: vec![test_relation_hint(0, 1)],
            similar_item_decisions: vec![SimilarItemDecisionContext {
                batch_index: 0,
                matched_item_id: ki_b,
                matched_content: "Existing item about memory safety".into(),
                similarity_score: 0.87,
                suggested_relation: RelationSuggestion::Supports,
                justification: "Both discuss Rust memory guarantees".into(),
            }],
            ki_a,
            ki_b,
        }
    }

    fn rich_context(data: &RichTestData) -> RelationPromptContext<'_> {
        RelationPromptContext {
            candidates: vec![
                CandidateOutcome {
                    batch_index: 0,
                    candidate: &data.candidates[0],
                    outcome: "created".into(),
                    item_id: Some(data.ki_a),
                },
                CandidateOutcome {
                    batch_index: 1,
                    candidate: &data.candidates[1],
                    outcome: "duplicate".into(),
                    item_id: Some(data.ki_b),
                },
                CandidateOutcome {
                    batch_index: 2,
                    candidate: &data.candidates[2],
                    outcome: "failed".into(),
                    item_id: None,
                },
            ],
            relation_hints: &data.relation_hints,
            similar_item_decisions: &data.similar_item_decisions,
        }
    }

    #[test]
    fn test_invalid_system_template_returns_template_render_error() {
        let data = rich_test_data();
        let ctx = rich_context(&data);
        let result = assemble_relation_prompt(
            "{{ invalid | nonexistent_filter }}",
            "{{ candidates }}",
            &ctx,
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.to_error_kind(), TaskErrorKind::InternalError);
    }

    #[test]
    fn test_invalid_user_template_returns_template_render_error() {
        let data = rich_test_data();
        let ctx = rich_context(&data);
        let result = assemble_relation_prompt("system", "{{ invalid | nonexistent_filter }}", &ctx);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.to_error_kind(), TaskErrorKind::InternalError);
    }

    #[test]
    fn test_renders_candidates_into_user_prompt() {
        let data = rich_test_data();
        let ctx = rich_context(&data);
        let system_template = "system";
        let user_template = concat!(
            "{% for c in candidates %}",
            "{{ c.batch_index }}: {{ c.outcome }}",
            "{% if c.item_id %} [{{ c.item_id }}]{% endif %}",
            " — {{ c.candidate.content }}\n",
            "{% endfor %}",
        );
        let request = assemble_relation_prompt(system_template, user_template, &ctx).unwrap();
        let user_content = &request.messages[0].content;

        assert!(
            user_content.contains("0: created"),
            "user prompt: {user_content}",
        );
        assert!(
            user_content.contains("ki_550e8400"),
            "created item_id should render: {user_content}",
        );
        assert!(
            user_content.contains("Rust has zero-cost abstractions"),
            "candidate content should render: {user_content}",
        );
        assert!(
            user_content.contains("1: duplicate"),
            "user prompt: {user_content}",
        );
        assert!(
            user_content.contains("2: failed"),
            "user prompt: {user_content}",
        );
    }

    #[test]
    fn test_renders_relation_hints_into_user_prompt() {
        let data = rich_test_data();
        let ctx = rich_context(&data);
        let user_template = concat!(
            "{% for h in relation_hints %}",
            "{{ h.source_index }} -> {{ h.target_index }}: {{ h.hint_type }}\n",
            "{% endfor %}",
        );
        let request = assemble_relation_prompt("system", user_template, &ctx).unwrap();
        let user_content = &request.messages[0].content;

        assert!(
            user_content.contains("0 -> 1: derived_from"),
            "relation hint should render: {user_content}",
        );
    }

    #[test]
    fn test_renders_similar_item_decisions_into_user_prompt() {
        let data = rich_test_data();
        let ctx = rich_context(&data);
        let user_template = concat!(
            "{% for d in similar_item_decisions %}",
            "batch {{ d.batch_index }}: {{ d.matched_item_id }} ",
            "({{ d.similarity_score }}) {{ d.suggested_relation }} — {{ d.justification }}\n",
            "{% endfor %}",
        );
        let request = assemble_relation_prompt("system", user_template, &ctx).unwrap();
        let user_content = &request.messages[0].content;

        assert!(
            user_content.contains("batch 0:"),
            "batch_index should render: {user_content}",
        );
        assert!(
            user_content.contains("ki_660e8400"),
            "matched_item_id should render: {user_content}",
        );
        assert!(
            user_content.contains("0.87"),
            "similarity_score should render: {user_content}",
        );
        assert!(
            user_content.contains("supports"),
            "suggested_relation should render: {user_content}",
        );
        assert!(
            user_content.contains("Both discuss Rust memory guarantees"),
            "justification should render: {user_content}",
        );
    }

    #[test]
    fn test_response_format_is_json_schema() {
        let data = rich_test_data();
        let ctx = rich_context(&data);
        let request = assemble_relation_prompt("system", "user", &ctx).unwrap();
        assert!(
            matches!(
                request.response_format,
                Some(ResponseFormat::JsonSchema { .. })
            ),
            "expected JsonSchema response format",
        );
    }

    #[test]
    fn test_schema_variable_rendered_in_system() {
        let data = rich_test_data();
        let ctx = rich_context(&data);
        let request = assemble_relation_prompt("Schema: {{ schema }}", "user", &ctx).unwrap();
        let system = request.system.unwrap();
        assert!(
            system.contains("RelationOutput"),
            "schema should contain type name: {system}",
        );
    }

    #[test]
    fn test_user_message_contains_rendered_context() {
        let data = rich_test_data();
        let ctx = rich_context(&data);
        let user_template = concat!(
            "{% for c in candidates %}{{ c.candidate.content }}\n{% endfor %}",
            "{% for h in relation_hints %}{{ h.hint_type }}\n{% endfor %}",
            "{% for d in similar_item_decisions %}{{ d.justification }}\n{% endfor %}",
        );
        let request = assemble_relation_prompt("system", user_template, &ctx).unwrap();
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.messages[0].role, Role::User);

        let content = &request.messages[0].content;
        assert!(
            content.contains("Rust has zero-cost abstractions"),
            "should contain candidate content: {content}",
        );
        assert!(
            content.contains("derived_from"),
            "should contain relation hint: {content}",
        );
        assert!(
            content.contains("Both discuss Rust memory guarantees"),
            "should contain decision justification: {content}",
        );
    }
}
