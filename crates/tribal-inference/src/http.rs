//! HTTP and URL utilities shared across inference provider implementations.

use std::time::Duration;

use reqwest::StatusCode;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of bytes to include when previewing a response body in
/// error context strings.
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
/// bytes with `"..."` appended when truncated.  Truncation is UTF-8
/// safe — it never splits a multi-byte codepoint.
pub(crate) fn body_preview(body: &str) -> String {
    let normalised: String = body.split_whitespace().collect::<Vec<_>>().join(" ");

    if normalised.len() <= BODY_PREVIEW_LIMIT {
        return normalised;
    }

    let boundary = normalised.floor_char_boundary(BODY_PREVIEW_LIMIT);
    format!("{}...", &normalised[..boundary])
}

// ---------------------------------------------------------------------------
// URL helpers
// ---------------------------------------------------------------------------

/// Strips trailing slashes from a base URL to ensure consistent path
/// concatenation (e.g. `format!("{base_url}/api/embed")`).
pub(crate) fn normalise_base_url(url: impl Into<String>) -> String {
    let mut url = url.into();
    while url.ends_with('/') {
        url.pop();
    }
    url
}

// ---------------------------------------------------------------------------
// Latency helpers
// ---------------------------------------------------------------------------

/// Converts a [`Duration`] to whole milliseconds, saturating at
/// [`u64::MAX`] for durations that exceed the representable range.
pub(crate) fn latency_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
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
        // of content (100 £ codepoints) + "...".
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

    // -- normalise_base_url -------------------------------------------------

    #[test]
    fn test_normalise_base_url_strips_trailing_slashes() {
        assert_eq!(
            normalise_base_url("http://localhost:11434/"),
            "http://localhost:11434"
        );
    }

    #[test]
    fn test_normalise_base_url_strips_multiple_slashes() {
        assert_eq!(
            normalise_base_url("http://localhost///"),
            "http://localhost"
        );
    }

    #[test]
    fn test_normalise_base_url_no_trailing_slash_unchanged() {
        assert_eq!(
            normalise_base_url("http://localhost:11434"),
            "http://localhost:11434"
        );
    }

    // -- latency_ms ---------------------------------------------------------

    #[test]
    fn test_latency_ms_converts_duration() {
        assert_eq!(latency_ms(Duration::from_millis(42)), 42);
    }

    #[test]
    fn test_latency_ms_zero() {
        assert_eq!(latency_ms(Duration::ZERO), 0);
    }
}
