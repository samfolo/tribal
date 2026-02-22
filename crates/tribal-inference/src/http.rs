//! HTTP response utilities shared across inference provider implementations.

use reqwest::StatusCode;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of characters to include when previewing a response body
/// in error context strings.
const BODY_PREVIEW_LIMIT: usize = 200;

/// Anthropic's overloaded status code, semantically equivalent to 503.
const ANTHROPIC_OVERLOADED: u16 = 529;

// ---------------------------------------------------------------------------
// Status classification
// ---------------------------------------------------------------------------

/// Classifies whether an HTTP status code represents a transient,
/// retryable error.
///
/// Retryable statuses: 429 (Too Many Requests), 5xx (server errors),
/// and 529 (Anthropic overloaded).
pub(crate) fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
        || status.as_u16() == ANTHROPIC_OVERLOADED
}

// ---------------------------------------------------------------------------
// Body preview
// ---------------------------------------------------------------------------

/// Produces a truncated, whitespace-normalised preview of a response body
/// for inclusion in error context strings.
///
/// Newlines, tabs, and consecutive whitespace are collapsed to single
/// spaces.  The result is truncated to at most [`BODY_PREVIEW_LIMIT`]
/// characters with `"..."` appended when truncated.  Truncation is
/// UTF-8 safe — it never splits a multi-byte codepoint.
pub(crate) fn body_preview(body: &str) -> String {
    let normalised: String = body.split_whitespace().collect::<Vec<_>>().join(" ");

    if normalised.len() <= BODY_PREVIEW_LIMIT {
        return normalised;
    }

    let boundary = normalised.floor_char_boundary(BODY_PREVIEW_LIMIT);
    format!("{}...", &normalised[..boundary])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- is_retryable_status ------------------------------------------------

    #[test]
    fn test_is_retryable_status_retryable_codes() {
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(is_retryable_status(
            StatusCode::from_u16(ANTHROPIC_OVERLOADED).unwrap()
        ));
    }

    #[test]
    fn test_is_retryable_status_non_retryable_codes() {
        assert!(!is_retryable_status(StatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(StatusCode::NOT_FOUND));
        assert!(!is_retryable_status(StatusCode::OK));
    }

    // -- body_preview -------------------------------------------------------

    #[test]
    fn test_body_preview_short_unchanged() {
        let input = "short response";
        assert_eq!(body_preview(input), "short response");
    }

    #[test]
    fn test_body_preview_whitespace_normalised() {
        let input = "line one\n\tline two\r\n  line  three";
        assert_eq!(body_preview(input), "line one line two line three");
    }

    #[test]
    fn test_body_preview_long_truncated() {
        let input = "a".repeat(300);
        let result = body_preview(&input);
        assert!(result.ends_with("..."));
        assert_eq!(result.len(), BODY_PREVIEW_LIMIT + 3);
    }

    #[test]
    fn test_body_preview_multibyte_safe() {
        // £ is 2 bytes in UTF-8.
        let input = "£".repeat(300);
        let result = body_preview(&input);
        assert!(result.ends_with("..."));
        // Must not panic on multi-byte boundary.
        // floor_char_boundary rounds down, so we get at most 200 bytes
        // of content (100 £ chars) + "...".
        assert!(result.len() <= BODY_PREVIEW_LIMIT + 3);
    }

    #[test]
    fn test_body_preview_exact_limit_no_ellipsis() {
        let input = "a".repeat(BODY_PREVIEW_LIMIT);
        assert_eq!(body_preview(&input), input);
    }

    #[test]
    fn test_body_preview_empty_input() {
        assert_eq!(body_preview(""), "");
    }

    #[test]
    fn test_body_preview_only_whitespace() {
        assert_eq!(body_preview("   \n\t  \r\n  "), "");
    }
}
