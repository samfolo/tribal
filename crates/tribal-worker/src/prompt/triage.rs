//! Triage prompt assembly: template rendering and request construction.

use schemars::schema_for;
use serde::Serialize;
use tribal_db::SemanticSearchResult;
use tribal_domain::{Candidate, KnowledgeItemId, KnowledgeKind, TagRegistryEntry};
use tribal_inference::{CompletionRequest, Message, ResponseFormat, Role};

use crate::{
    error::StageError,
    parsing::TriageClassification,
    prompt::variables::{VAR_CANDIDATE, VAR_SIMILAR_ITEMS, VAR_TAGS, triage_system_context},
};

use super::{legends::SimilarityBand, renderer::PromptRenderer};

// ---------------------------------------------------------------------------
// SimilarItemContext
// ---------------------------------------------------------------------------

/// A similar item enriched with fields needed by the triage prompt template.
///
/// Constructed from [`SemanticSearchResult`] by extracting the fields
/// the template needs to render.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct SimilarItemContext {
    /// The existing knowledge item identifier.
    pub item_id: KnowledgeItemId,
    /// Classification of the existing item.
    pub kind: KnowledgeKind,
    /// The existing item's content.
    pub content: String,
    /// Cosine similarity score.
    pub similarity_score: f64,
    /// Human-readable label for the similarity score.
    pub similarity_label: String,
    /// The existing item's tags.
    pub tags: Vec<String>,
}

impl From<&SemanticSearchResult> for SimilarItemContext {
    fn from(result: &SemanticSearchResult) -> Self {
        Self {
            item_id: result.item.id(),
            kind: result.item.kind(),
            content: result.item.content().to_owned(),
            similarity_score: result.similarity,
            similarity_label: SimilarityBand::from_score(result.similarity).to_string(),
            tags: result.item.tags().to_vec(),
        }
    }
}

// ---------------------------------------------------------------------------
// Context builders
// ---------------------------------------------------------------------------

/// Builds the user prompt context for the triage stage.
///
/// Both the production assembly and the validation tests call this,
/// so adding a variable here is automatically reflected in both paths.
pub(crate) fn triage_user_context(
    candidate: &Candidate,
    similar_items: &[SimilarItemContext],
    tags: &[&str],
) -> tera::Context {
    let mut ctx = tera::Context::new();
    ctx.insert(VAR_CANDIDATE, candidate);
    ctx.insert(VAR_SIMILAR_ITEMS, similar_items);
    ctx.insert(VAR_TAGS, tags);
    ctx
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Assembles a [`CompletionRequest`] for the triage stage.
///
/// # Errors
///
/// Returns [`StageError::TemplateRender`] if either template cannot be
/// rendered.
pub(crate) fn assemble_triage_prompt(
    system_template: &str,
    user_template: &str,
    candidate: &Candidate,
    similar_items: &[SimilarItemContext],
    tag_registry: &[TagRegistryEntry],
) -> Result<CompletionRequest, StageError> {
    let schema = schema_for!(TriageClassification);
    let schema_value =
        serde_json::to_value(&schema).expect("schema_for! produces serialisable output");

    let renderer = PromptRenderer::new();

    let system_ctx = triage_system_context();
    let rendered_system =
        renderer.render(system_template, system_ctx, "rendering triage system prompt")?;

    let tags: Vec<&str> = tag_registry.iter().map(TagRegistryEntry::tag).collect();
    let user_ctx = triage_user_context(candidate, similar_items, &tags);
    let rendered_user =
        renderer.render(user_template, user_ctx, "rendering triage user prompt")?;

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
    use tribal_test_utils::a_tag_registry_entry;

    use super::*;

    fn test_candidate() -> Candidate {
        serde_json::from_value(serde_json::json!({
            "kind": "fact",
            "content": "Rust has zero-cost abstractions",
            "suggested_tags": ["rust", "performance"],
        }))
        .expect("valid candidate JSON")
    }

    #[test]
    fn test_invalid_system_template_returns_template_render_error() {
        let result = assemble_triage_prompt(
            "{{ invalid | nonexistent_filter }}",
            "{{ candidate.content }}",
            &test_candidate(),
            &[],
            &[],
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.to_error_kind(), TaskErrorKind::InternalError);
    }

    #[test]
    fn test_invalid_user_template_returns_template_render_error() {
        let result = assemble_triage_prompt(
            "system",
            "{{ invalid | nonexistent_filter }}",
            &test_candidate(),
            &[],
            &[],
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.to_error_kind(), TaskErrorKind::InternalError);
    }

    #[test]
    fn test_renders_tags_into_user_prompt() {
        let tags = vec![
            a_tag_registry_entry().tag("rust".to_owned()).build(),
            a_tag_registry_entry().tag("testing".to_owned()).build(),
        ];
        let result = assemble_triage_prompt(
            "system",
            "Tags: {% for tag in tags %}{{ tag }} {% endfor %}",
            &test_candidate(),
            &[],
            &tags,
        );
        assert!(result.is_ok());
        let request = result.unwrap();
        let user_content = &request.messages[0].content;
        assert!(user_content.contains("rust"), "user prompt: {user_content}");
        assert!(
            user_content.contains("testing"),
            "user prompt: {user_content}",
        );
    }

    #[test]
    fn test_response_format_is_json_schema() {
        let result = assemble_triage_prompt(
            "system",
            "{{ candidate.content }}",
            &test_candidate(),
            &[],
            &[],
        );
        assert!(result.is_ok());
        let request = result.unwrap();
        assert!(
            matches!(
                request.response_format,
                Some(ResponseFormat::JsonSchema { .. })
            ),
            "expected JsonSchema response format",
        );
    }

    #[test]
    fn test_candidate_content_rendered_in_user_message() {
        let result = assemble_triage_prompt(
            "system",
            "{{ candidate.content }}",
            &test_candidate(),
            &[],
            &[],
        );
        assert!(result.is_ok());
        let request = result.unwrap();
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.messages[0].role, Role::User);
        assert!(
            request.messages[0]
                .content
                .contains("Rust has zero-cost abstractions"),
        );
    }

    #[test]
    fn test_similarity_label_rendered_for_similar_items() {
        let similar = SimilarItemContext {
            item_id: KnowledgeItemId::new(),
            kind: KnowledgeKind::Fact,
            content: "existing item".to_owned(),
            similarity_score: 0.72,
            similarity_label: SimilarityBand::from_score(0.72).to_string(),
            tags: vec![],
        };
        let result = assemble_triage_prompt(
            "system",
            "{% for item in similar_items %}{{ item.similarity_label }}{% endfor %}",
            &test_candidate(),
            &[similar],
            &[],
        );
        let request = result.unwrap();
        assert!(
            request.messages[0].content.contains("high"),
            "should contain label: {}",
            request.messages[0].content,
        );
    }

    #[test]
    fn test_system_prompt_contains_similarity_legend() {
        let result = assemble_triage_prompt(
            "{{ similarity_score_legend }}",
            "{{ candidate.content }}",
            &test_candidate(),
            &[],
            &[],
        );
        let request = result.unwrap();
        let system = request.system.unwrap();
        assert!(system.contains("low"), "legend should contain bands: {system}");
        assert!(
            system.contains("very high"),
            "legend should contain bands: {system}",
        );
    }
}
