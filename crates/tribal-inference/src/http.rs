//! HTTP and URL utilities shared across inference provider implementations.

use std::time::Duration;

use reqwest::StatusCode;
use tribal_domain::span_attrs;

use tribal_domain::CompletionUsage;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of bytes to include when previewing a response body in
/// error context strings.
const BODY_PREVIEW_LIMIT: usize = 200;

/// The overloaded status code (`Anthropic`'s, semantically equivalent to 503);
/// the embedding retry classifier maps it to its own `Overloaded` class.
pub(crate) const OVERLOADED_STATUS: u16 = 529;

/// Default `max_tokens` for completion probe requests.
pub(crate) const PROBE_MAX_TOKENS: u32 = 8;

/// Content sent by inference provider `probe_model()` calls.
pub const INFERENCE_PROBE_INPUT: &str = "Respond with OK";

/// Content sent by embedding provider `probe_model()` calls.
pub const EMBEDDING_PROBE_INPUT: &str = "tribal probe";

// ---------------------------------------------------------------------------
// Status classification
// ---------------------------------------------------------------------------

/// Classifies whether an HTTP status code represents a transient,
/// retryable error.
///
/// Retryable statuses: 429 (Too Many Requests), 5xx (server errors),
/// and 529 (overloaded).
pub(crate) fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
        || status.as_u16() == OVERLOADED_STATUS
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
// Completion span recording
// ---------------------------------------------------------------------------

/// Records completion token counts, cache counts, and latency on the
/// current tracing span, then emits a `debug`-level log event.
pub(crate) fn record_completion_usage(usage: &CompletionUsage) {
    let latency_ms = latency_ms(usage.latency);

    let current = tracing::Span::current();
    current.record(span_attrs::LLM_TOKENS_INPUT, usage.input_tokens);
    current.record(span_attrs::LLM_TOKENS_OUTPUT, usage.output_tokens);
    current.record(span_attrs::LLM_TOKENS_TOTAL, usage.total_tokens);
    current.record(span_attrs::LLM_LATENCY_MS, latency_ms);
    current.record(span_attrs::LLM_TOKENS_CACHE_READ, usage.cache_read_tokens);
    current.record(span_attrs::LLM_TOKENS_CACHE_WRITE, usage.cache_write_tokens);

    tracing::debug!(
        input_tokens = usage.input_tokens,
        output_tokens = usage.output_tokens,
        total_tokens = usage.total_tokens,
        cache_read_tokens = usage.cache_read_tokens,
        cache_write_tokens = usage.cache_write_tokens,
        latency_ms,
        "completion generated",
    );
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
            StatusCode::from_u16(OVERLOADED_STATUS).unwrap()
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
