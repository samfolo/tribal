//! Extraction prompt assembly: template rendering and request construction.

use schemars::schema_for;
use tribal_domain::TagRegistryEntry;
use tribal_inference::{CompletionRequest, Message, ResponseFormat, Role};

use crate::{
    error::StageError,
    parsing::ExtractionOutput,
    prompt::variables::{VAR_RAW_INPUT, VAR_SCHEMA, VAR_TAGS},
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Assembles a [`CompletionRequest`] for the extraction stage.
///
/// Renders the system template with [`VAR_SCHEMA`] only, and the user
/// template with [`VAR_TAGS`] (from the tag registry) and
/// [`VAR_RAW_INPUT`] (the verbatim ingest text).
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
) -> Result<CompletionRequest, StageError> {
    let schema = schema_for!(ExtractionOutput);
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
                context: "rendering extraction system prompt".into(),
                source: e,
            }
        })?;

    // User context: tags + raw input.
    let tags: Vec<&str> = tag_registry.iter().map(TagRegistryEntry::tag).collect();

    let mut user_ctx = tera::Context::new();
    user_ctx.insert(VAR_TAGS, &tags);
    user_ctx.insert(VAR_RAW_INPUT, raw_input);

    let rendered_user =
        tera::Tera::one_off(user_template, &user_ctx, false).map_err(|e| {
            StageError::TemplateRender {
                context: "rendering extraction user prompt".into(),
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

    #[test]
    fn test_invalid_system_template_returns_template_render_error() {
        let result = assemble_extraction_prompt(
            "{{ invalid | nonexistent_filter }}",
            "{{ raw_input }}",
            "some input",
            &[],
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
    fn test_schema_rendered_in_system_prompt() {
        let result = assemble_extraction_prompt(
            "Schema: {{ schema }}",
            "{{ raw_input }}",
            "input",
            &[],
        );
        assert!(result.is_ok());
        let request = result.unwrap();
        let system = request.system.unwrap();
        assert!(
            system.contains("ExtractionOutput"),
            "schema should contain type name: {system}",
        );
    }

    #[test]
    fn test_response_format_is_json_schema() {
        let result =
            assemble_extraction_prompt("system", "{{ raw_input }}", "input", &[]);
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
        let result =
            assemble_extraction_prompt("system", "{{ raw_input }}", "the raw input", &[]);
        assert!(result.is_ok());
        let request = result.unwrap();
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.messages[0].role, Role::User);
        assert!(request.messages[0].content.contains("the raw input"));
    }
}
