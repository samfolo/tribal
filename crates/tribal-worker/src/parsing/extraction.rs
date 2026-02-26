//! Extraction response parsing.

use tribal_inference::CompletionResponse;

use crate::error::StageError;
use crate::stages::extraction::ExtractionOutput;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parses a completion response into an [`ExtractionOutput`].
///
/// # Errors
///
/// Returns [`StageError::Parse`] if the response text cannot be
/// deserialised into [`ExtractionOutput`], with an operator-safe
/// `context` and the full `raw_response` for debugging.
pub(crate) fn parse_extraction_response(
    response: &CompletionResponse,
) -> Result<ExtractionOutput, StageError> {
    serde_json::from_str::<ExtractionOutput>(&response.text).map_err(|e| StageError::Parse {
        context: format!("deserialising extraction output: {e}"),
        raw_response: Some(response.text.clone()),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tribal_inference::CompletionUsage;

    use super::*;

    fn mock_response(text: &str) -> CompletionResponse {
        CompletionResponse {
            text: text.to_owned(),
            usage: CompletionUsage {
                provider: "test".into(),
                model: "test-model".into(),
                input_tokens: 0,
                output_tokens: 0,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                total_tokens: 0,
                latency: Duration::ZERO,
            },
        }
    }

    #[test]
    fn test_parse_valid_json() {
        let json = r#"{"candidates": [], "relation_hints": []}"#;
        let response = mock_response(json);
        let result = parse_extraction_response(&response);
        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.candidates.is_empty());
        assert!(output.relation_hints.is_empty());
    }

    #[test]
    fn test_parse_invalid_json_returns_parse_error() {
        let response = mock_response("not json at all");
        let result = parse_extraction_response(&response);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            StageError::Parse { raw_response, .. } => {
                assert_eq!(raw_response.as_deref(), Some("not json at all"));
            }
            other => panic!("expected StageError::Parse, got {other}"),
        }
    }

    #[test]
    fn test_parse_missing_relation_hints_defaults_to_empty() {
        let json = r#"{"candidates": []}"#;
        let response = mock_response(json);
        let result = parse_extraction_response(&response);
        assert!(result.is_ok());
        assert!(result.unwrap().relation_hints.is_empty());
    }

    #[test]
    fn test_parse_unknown_fields_are_ignored() {
        let json = r#"{"candidates": [], "relation_hints": [], "extra_field": true}"#;
        let response = mock_response(json);
        let result = parse_extraction_response(&response);
        assert!(result.is_ok());
    }
}
