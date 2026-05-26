//! Extraction prompt assembly: template rendering and request construction.

use schemars::schema_for;
use tribal_domain::{StageParameters, TagRegistryEntry};
use tribal_inference::{CompletionRequest, Message, ResponseFormat, Role};

use super::renderer::PromptRenderer;
use crate::{
    error::StageError,
    parsing::ExtractionOutput,
    prompt::{
        narrow_temperature,
        variables::{VAR_RAW_INPUT, VAR_TAGS, extraction_system_context},
    },
};

// ---------------------------------------------------------------------------
// Context builders
// ---------------------------------------------------------------------------

/// Builds the user prompt context for the extraction stage.
///
/// Both the production assembly and the validation tests call this,
/// so adding a variable here is automatically reflected in both paths.
pub(crate) fn extraction_user_context(raw_input: &str, tags: &[&str]) -> tera::Context {
    let mut ctx = tera::Context::new();
    ctx.insert(VAR_TAGS, tags);
    ctx.insert(VAR_RAW_INPUT, raw_input);
    ctx
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Assembles a [`CompletionRequest`] for the extraction stage.
///
/// # Errors
///
/// Returns [`StageError::TemplateRender`] if either template cannot be
/// rendered.
pub(crate) fn assemble_extraction_prompt(
    system_template: &str,
    user_template: &str,
    raw_input: &str,
    tag_registry: &[TagRegistryEntry],
    params: &StageParameters,
) -> Result<CompletionRequest, StageError> {
    let schema = schema_for!(ExtractionOutput);
    let schema_value =
        serde_json::to_value(&schema).expect("schema_for! produces serialisable output");

    let renderer = PromptRenderer::new();

    let system_ctx = extraction_system_context();
    let rendered_system = renderer.render(
        system_template,
        system_ctx,
        "rendering extraction system prompt",
    )?;

    let tags: Vec<&str> = tag_registry.iter().map(TagRegistryEntry::tag).collect();
    let user_ctx = extraction_user_context(raw_input, &tags);
    let rendered_user =
        renderer.render(user_template, user_ctx, "rendering extraction user prompt")?;

    Ok(CompletionRequest {
        system: Some(rendered_system),
        messages: vec![Message {
            role: Role::User,
            content: rendered_user,
        }],
        temperature: narrow_temperature(params.temperature),
        max_tokens: params.max_tokens,
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

    #[test]
    fn test_invalid_system_template_returns_template_render_error() {
        let result = assemble_extraction_prompt(
            "{{ invalid | nonexistent_filter }}",
            "{{ raw_input }}",
            "some input",
            &[],
            &StageParameters::default(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.to_error_kind(), TaskErrorKind::InternalError);
    }

    #[test]
    fn test_invalid_user_template_returns_template_render_error() {
        let result = assemble_extraction_prompt(
            "system",
            "{{ invalid | nonexistent_filter }}",
            "some input",
            &[],
            &StageParameters::default(),
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
        let result = assemble_extraction_prompt(
            "system",
            "Tags: {% for tag in tags %}{{ tag }} {% endfor %}",
            "input text",
            &tags,
            &StageParameters::default(),
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
        let result = assemble_extraction_prompt(
            "system",
            "{{ raw_input }}",
            "input",
            &[],
            &StageParameters::default(),
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
    fn test_raw_input_rendered_in_user_message() {
        let result = assemble_extraction_prompt(
            "system",
            "{{ raw_input }}",
            "the raw input",
            &[],
            &StageParameters::default(),
        );
        assert!(result.is_ok());
        let request = result.unwrap();
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.messages[0].role, Role::User);
        assert!(request.messages[0].content.contains("the raw input"));
    }

    #[test]
    fn test_nonce_available_in_user_prompt() {
        let result = assemble_extraction_prompt(
            "system",
            "fence:{{ nonce }}",
            "input",
            &[],
            &StageParameters::default(),
        );
        let request = result.unwrap();
        let content = &request.messages[0].content;
        assert!(
            content.starts_with("fence:"),
            "nonce should render: {content}",
        );
        assert!(content.len() > "fence:".len(), "nonce should be non-empty");
    }

    #[test]
    fn test_stage_parameters_reach_request() {
        let params = StageParameters {
            temperature: Some(0.5),
            max_tokens: Some(256),
        };
        let request =
            assemble_extraction_prompt("system", "{{ raw_input }}", "input", &[], &params).unwrap();
        assert_eq!(request.temperature, Some(0.5));
        assert_eq!(request.max_tokens, Some(256));
    }
}
