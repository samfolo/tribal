//! Extraction prompt assembly: template rendering and request construction.

use schemars::schema_for;
use tribal_domain::TagRegistryEntry;
use tribal_inference::{CompletionRequest, Message, ResponseFormat, Role};

use crate::error::StageError;
use crate::parsing::ExtractionOutput;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Tera context variable: the raw input text.
const VAR_RAW_INPUT: &str = "raw_input";

/// Tera context variable: the tag registry as a list of strings.
const VAR_TAGS: &str = "tags";

/// Tera context variable: the JSON Schema for the expected output.
const VAR_SCHEMA: &str = "schema";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Assembles a [`CompletionRequest`] for the extraction stage.
///
/// Renders the Tera template with the [`VAR_RAW_INPUT`], [`VAR_TAGS`]
/// (from the tag registry), and [`VAR_SCHEMA`] (JSON Schema for
/// [`ExtractionOutput`]) context variables. The rendered text becomes
/// the system prompt; the raw input is sent as a user message.
///
/// # Errors
///
/// Returns [`StageError::TemplateRender`] if the template cannot be
/// rendered.
pub(crate) fn assemble_extraction_prompt(
    template_content: &str,
    raw_input: &str,
    tag_registry: &[TagRegistryEntry],
) -> Result<CompletionRequest, StageError> {
    let schema = schema_for!(ExtractionOutput);
    let schema_value =
        serde_json::to_value(&schema).expect("schema_for! produces serialisable output");
    let schema_pretty =
        serde_json::to_string_pretty(&schema).expect("schema_for! produces serialisable output");

    let tags: Vec<&str> = tag_registry.iter().map(|e| e.tag()).collect();

    let mut context = tera::Context::new();
    context.insert(VAR_RAW_INPUT, raw_input);
    context.insert(VAR_TAGS, &tags);
    context.insert(VAR_SCHEMA, &schema_pretty);

    let rendered = tera::Tera::one_off(template_content, &context, false).map_err(|e| {
        StageError::TemplateRender {
            context: "rendering extraction prompt".into(),
            source: e,
        }
    })?;

    Ok(CompletionRequest {
        system: Some(rendered),
        messages: vec![Message {
            role: Role::User,
            content: raw_input.to_owned(),
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
    fn test_invalid_template_returns_template_render_error() {
        let result = assemble_extraction_prompt(
            "{{ invalid | nonexistent_filter }}",
            "some input",
            &[],
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.to_error_kind(), TaskErrorKind::InternalError);
    }

    #[test]
    fn test_renders_tags_into_prompt() {
        let tags = vec![
            a_tag_registry_entry().tag("rust".to_owned()).build(),
            a_tag_registry_entry().tag("testing".to_owned()).build(),
        ];
        let result = assemble_extraction_prompt(
            "Tags: {% for tag in tags %}{{ tag }} {% endfor %}",
            "input text",
            &tags,
        );
        assert!(result.is_ok());
        let request = result.unwrap();
        let system = request.system.unwrap();
        assert!(system.contains("rust"), "system prompt: {system}");
        assert!(system.contains("testing"), "system prompt: {system}");
    }

    #[test]
    fn test_response_format_is_json_schema() {
        let result = assemble_extraction_prompt("minimal template", "input", &[]);
        assert!(result.is_ok());
        let request = result.unwrap();
        assert!(
            matches!(request.response_format, Some(ResponseFormat::JsonSchema { .. })),
            "expected JsonSchema response format",
        );
    }

    #[test]
    fn test_raw_input_sent_as_user_message() {
        let result = assemble_extraction_prompt("template", "the raw input", &[]);
        assert!(result.is_ok());
        let request = result.unwrap();
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.messages[0].role, Role::User);
        assert_eq!(request.messages[0].content, "the raw input");
    }
}
