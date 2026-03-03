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
#[derive(Debug, Clone, Serialize)]
pub(crate) struct RelationPromptContext {
    /// Each candidate with its triage outcome and created item ID (if any).
    pub candidates: Vec<CandidateOutcome>,
    /// Intra-batch relation hints from extraction.
    pub relation_hints: Vec<RelationHint>,
    /// Similar item decisions from triage (all candidates combined).
    pub similar_item_decisions: Vec<SimilarItemDecisionContext>,
}

/// A candidate paired with its triage outcome for the relation prompt.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CandidateOutcome {
    /// The candidate's position in the extraction batch.
    pub batch_index: u32,
    /// The original extracted candidate.
    pub candidate: Candidate,
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
// Public API
// ---------------------------------------------------------------------------

/// Assembles a [`CompletionRequest`] for the relation stage.
///
/// Renders the Tera template with the [`VAR_CANDIDATES`],
/// [`VAR_RELATION_HINTS`], [`VAR_SIMILAR_ITEM_DECISIONS`], and
/// [`VAR_SCHEMA`] context variables. The rendered text becomes the
/// system prompt; a JSON serialisation of the context is sent as
/// the user message.
///
/// # Errors
///
/// Returns [`StageError::TemplateRender`] if the template cannot be
/// rendered.
pub(crate) fn assemble_relation_prompt(
    template_content: &str,
    context: &RelationPromptContext,
) -> Result<CompletionRequest, StageError> {
    let schema = schema_for!(RelationOutput);
    let schema_value =
        serde_json::to_value(&schema).expect("schema_for! produces serialisable output");
    let schema_pretty =
        serde_json::to_string_pretty(&schema).expect("schema_for! produces serialisable output");

    let mut tera_ctx = tera::Context::new();
    tera_ctx.insert(VAR_CANDIDATES, &context.candidates);
    tera_ctx.insert(VAR_RELATION_HINTS, &context.relation_hints);
    tera_ctx.insert(VAR_SIMILAR_ITEM_DECISIONS, &context.similar_item_decisions);
    tera_ctx.insert(VAR_SCHEMA, &schema_pretty);

    let rendered = tera::Tera::one_off(template_content, &tera_ctx, false).map_err(|e| {
        StageError::TemplateRender {
            context: "rendering relation prompt".into(),
            source: e,
        }
    })?;

    let user_content =
        serde_json::to_string_pretty(context).expect("RelationPromptContext is serialisable");

    Ok(CompletionRequest {
        system: Some(rendered),
        messages: vec![Message {
            role: Role::User,
            content: user_content,
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

    fn rich_context() -> RelationPromptContext {
        let ki_a: KnowledgeItemId = "ki_550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        let ki_b: KnowledgeItemId = "ki_660e8400-e29b-41d4-a716-446655440000".parse().unwrap();

        RelationPromptContext {
            candidates: vec![
                CandidateOutcome {
                    batch_index: 0,
                    candidate: test_candidate("Rust has zero-cost abstractions"),
                    outcome: "created".into(),
                    item_id: Some(ki_a),
                },
                CandidateOutcome {
                    batch_index: 1,
                    candidate: test_candidate("Ownership prevents data races"),
                    outcome: "duplicate".into(),
                    item_id: None,
                },
                CandidateOutcome {
                    batch_index: 2,
                    candidate: test_candidate("Borrow checker validates lifetimes"),
                    outcome: "failed".into(),
                    item_id: None,
                },
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
        }
    }

    #[test]
    fn test_invalid_template_returns_template_render_error() {
        let ctx = rich_context();
        let result = assemble_relation_prompt("{{ invalid | nonexistent_filter }}", &ctx);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.to_error_kind(), TaskErrorKind::InternalError);
    }

    #[test]
    fn test_renders_candidates_into_prompt() {
        let ctx = rich_context();
        let template = concat!(
            "{% for c in candidates %}",
            "{{ c.batch_index }}: {{ c.outcome }}",
            "{% if c.item_id %} [{{ c.item_id }}]{% endif %}",
            " — {{ c.candidate.content }}\n",
            "{% endfor %}",
        );
        let request = assemble_relation_prompt(template, &ctx).unwrap();
        let system = request.system.unwrap();

        assert!(system.contains("0: created"), "system prompt: {system}");
        assert!(
            system.contains("ki_550e8400"),
            "created item_id should render: {system}",
        );
        assert!(
            system.contains("Rust has zero-cost abstractions"),
            "candidate content should render: {system}",
        );
        assert!(system.contains("1: duplicate"), "system prompt: {system}");
        assert!(system.contains("2: failed"), "system prompt: {system}");
    }

    #[test]
    fn test_renders_relation_hints_into_prompt() {
        let ctx = rich_context();
        let template = concat!(
            "{% for h in relation_hints %}",
            "{{ h.source_index }} -> {{ h.target_index }}: {{ h.hint_type }}\n",
            "{% endfor %}",
        );
        let request = assemble_relation_prompt(template, &ctx).unwrap();
        let system = request.system.unwrap();

        assert!(
            system.contains("0 -> 1: derived_from"),
            "relation hint should render: {system}",
        );
    }

    #[test]
    fn test_renders_similar_item_decisions_into_prompt() {
        let ctx = rich_context();
        let template = concat!(
            "{% for d in similar_item_decisions %}",
            "batch {{ d.batch_index }}: {{ d.matched_item_id }} ",
            "({{ d.similarity_score }}) {{ d.suggested_relation }} — {{ d.justification }}\n",
            "{% endfor %}",
        );
        let request = assemble_relation_prompt(template, &ctx).unwrap();
        let system = request.system.unwrap();

        assert!(
            system.contains("batch 0:"),
            "batch_index should render: {system}",
        );
        assert!(
            system.contains("ki_660e8400"),
            "matched_item_id should render: {system}",
        );
        assert!(
            system.contains("0.87"),
            "similarity_score should render: {system}",
        );
        assert!(
            system.contains("supports"),
            "suggested_relation should render: {system}",
        );
        assert!(
            system.contains("Both discuss Rust memory guarantees"),
            "justification should render: {system}",
        );
    }

    #[test]
    fn test_response_format_is_json_schema() {
        let ctx = rich_context();
        let request = assemble_relation_prompt("minimal template", &ctx).unwrap();
        assert!(
            matches!(
                request.response_format,
                Some(ResponseFormat::JsonSchema { .. })
            ),
            "expected JsonSchema response format",
        );
    }

    #[test]
    fn test_schema_variable_rendered() {
        let ctx = rich_context();
        let request = assemble_relation_prompt("Schema: {{ schema }}", &ctx).unwrap();
        let system = request.system.unwrap();
        assert!(
            system.contains("RelationOutput"),
            "schema should contain type name: {system}",
        );
    }

    #[test]
    fn test_user_message_is_json_serialised_context() {
        let ctx = rich_context();
        let request = assemble_relation_prompt("template", &ctx).unwrap();
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.messages[0].role, Role::User);

        let parsed: serde_json::Value =
            serde_json::from_str(&request.messages[0].content).expect("valid JSON");
        let candidates = parsed["candidates"].as_array().unwrap();
        assert_eq!(candidates.len(), 3);
        assert_eq!(candidates[0]["outcome"], "created");
        assert_eq!(candidates[1]["outcome"], "duplicate");
        assert_eq!(candidates[2]["outcome"], "failed");
    }
}
