//! Shared deserialisation helpers for stage responses.
//!
//! A model that does not honour the structured-output schema may wrap its JSON
//! in a Markdown code fence. [`tolerant`] parses strictly first and unwraps a
//! fence only when that parse fails, so a payload that is already valid JSON —
//! including one whose string values contain backticks — is never altered.

use serde::de::DeserializeOwned;

/// Deserialises `text` as JSON, retrying once without a wrapping Markdown code
/// fence if the first parse fails.
///
/// A valid payload deserialises on the first attempt and is returned untouched.
/// Only on failure is a surrounding fence stripped and the parse retried; if the
/// retry also fails, the original error is returned so diagnostics reflect the
/// response as received.
pub(crate) fn tolerant<T: DeserializeOwned>(text: &str) -> Result<T, serde_json::Error> {
    match serde_json::from_str::<T>(text) {
        Ok(value) => Ok(value),
        Err(original) => match strip_code_fence(text) {
            Some(unwrapped) => serde_json::from_str::<T>(&unwrapped).or(Err(original)),
            None => Err(original),
        },
    }
}

/// Returns the contents of a Markdown code fence wrapping the whole of `text`,
/// or `None` when `text` is not fence-wrapped.
///
/// Handles a triple-backtick fence with an optional info string (` ```json `,
/// ` ``` `) and a single-backtick wrap; surrounding whitespace is ignored. A
/// fence with no matching close is left intact.
fn strip_code_fence(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if let Some(rest) = trimmed.strip_prefix("```") {
        let body = rest.strip_suffix("```")?;
        // Drop the optional info string on the opening line.
        let inner = body.split_once('\n').map_or(body, |(_info, after)| after);
        return Some(inner.trim().to_owned());
    }
    let inner = trimmed.strip_prefix('`')?.strip_suffix('`')?;
    Some(inner.trim().to_owned())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn test_parses_plain_json_unchanged() {
        let value: Value = tolerant(r#"{"a": 1}"#).unwrap();
        assert_eq!(value, json!({ "a": 1 }));
    }

    #[test]
    fn test_parses_json_fenced_with_tag() {
        let value: Value = tolerant("```json\n{\"a\": 1}\n```").unwrap();
        assert_eq!(value, json!({ "a": 1 }));
    }

    #[test]
    fn test_parses_json_fenced_without_tag() {
        let value: Value = tolerant("```\n{\"a\": 1}\n```").unwrap();
        assert_eq!(value, json!({ "a": 1 }));
    }

    #[test]
    fn test_parses_single_backtick_wrap() {
        let value: Value = tolerant("`{\"a\": 1}`").unwrap();
        assert_eq!(value, json!({ "a": 1 }));
    }

    #[test]
    fn test_parses_fence_with_surrounding_whitespace() {
        let value: Value = tolerant("  ```json\n{\"a\": 1}\n```  \n").unwrap();
        assert_eq!(value, json!({ "a": 1 }));
    }

    #[test]
    fn test_preserves_backticks_inside_valid_payload() {
        // A valid payload whose string value contains a code fence parses on the
        // first attempt, so the fence stripper never runs and cannot corrupt it.
        let source = r#"{"content": "```rust\nlet x = 1;\n```"}"#;
        let value: Value = tolerant(source).unwrap();
        assert_eq!(value["content"], json!("```rust\nlet x = 1;\n```"));
    }

    #[test]
    fn test_returns_error_for_malformed_non_fenced_json() {
        assert!(tolerant::<Value>(r#"{"a":"#).is_err());
    }

    #[test]
    fn test_returns_error_when_fence_unwrap_is_still_invalid() {
        assert!(tolerant::<Value>("```json\nnot json\n```").is_err());
    }

    #[test]
    fn test_strip_leaves_non_fenced_text_untouched() {
        assert_eq!(strip_code_fence(r#"{"a": 1}"#), None);
    }

    #[test]
    fn test_strip_leaves_unterminated_fence_intact() {
        assert_eq!(strip_code_fence("```json\n{\"a\": 1}"), None);
    }
}
