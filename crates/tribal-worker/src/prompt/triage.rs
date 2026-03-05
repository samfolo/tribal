//! Triage prompt assembly: template rendering and request construction.

use schemars::schema_for;
use serde::Serialize;
use tribal_db::SemanticSearchResult;
use tribal_domain::{Candidate, KnowledgeItemId, KnowledgeKind, TagRegistryEntry};
use tribal_inference::{CompletionRequest, Message, ResponseFormat, Role};

use crate::{
    error::StageError,
    parsing::TriageClassification,
    prompt::variables::{VAR_CANDIDATE, VAR_SCHEMA, VAR_SIMILAR_ITEMS, VAR_TAGS},
};

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
            tags: result.item.tags().to_vec(),
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Assembles a [`CompletionRequest`] for the triage stage.
///
/// Renders the system template with [`VAR_SCHEMA`] only, and the user
/// template with [`VAR_CANDIDATE`], [`VAR_SIMILAR_ITEMS`], and
/// [`VAR_TAGS`].
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
    let schema_pretty =
        serde_json::to_string_pretty(&schema).expect("schema_for! produces serialisable output");

    // System context: schema only.
    let mut system_ctx = tera::Context::new();
    system_ctx.insert(VAR_SCHEMA, &schema_pretty);

    let rendered_system =
        tera::Tera::one_off(system_template, &system_ctx, false).map_err(|e| {
            StageError::TemplateRender {
                context: "rendering triage system prompt".into(),
                source: e,
            }
        })?;

    // User context: candidate, similar items, tags.
    let tags: Vec<&str> = tag_registry.iter().map(TagRegistryEntry::tag).collect();

    let mut user_ctx = tera::Context::new();
    user_ctx.insert(VAR_CANDIDATE, candidate);
    user_ctx.insert(VAR_SIMILAR_ITEMS, similar_items);
    user_ctx.insert(VAR_TAGS, &tags);

    let rendered_user = tera::Tera::one_off(user_template, &user_ctx, false).map_err(|e| {
        StageError::TemplateRender {
            context: "rendering triage user prompt".into(),
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
    fn test_schema_variable_rendered_in_system() {
        let result = assemble_triage_prompt(
            "Schema: {{ schema }}",
            "{{ candidate.content }}",
            &test_candidate(),
            &[],
            &[],
        );
        assert!(result.is_ok());
        let request = result.unwrap();
        let system = request.system.unwrap();
        assert!(
            system.contains("TriageClassification"),
            "schema should contain type name: {system}",
        );
    }
}
