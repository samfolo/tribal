//! Crate-level error type and construction helpers for `tribal-inference`.
//!
//! [`InferenceError`] is the single error enum for the inference layer.
//! All variants use named fields; wrapped errors carry `#[source]` for
//! error chain propagation.
//!
//! The `map_*` helpers centralise error construction so that logging,
//! status classification, and body truncation are consistent across all
//! provider implementations.

use std::time::Duration;

use reqwest::StatusCode;
use thiserror::Error;
use tribal_domain::EmbeddingErrorClass;

use crate::http::{OVERLOADED_STATUS, body_preview, is_retryable_status};

/// Errors produced by the inference layer.
///
/// All variants use named fields.  `#[source]` preserves the error chain
/// for tracing and debugging.  Variants with
/// `Box<dyn Error + Send + Sync>` sources accept any provider-specific
/// error type.
#[derive(Debug, Error)]
pub enum InferenceError {
    /// The inference provider is unreachable or refused the connection.
    #[error("provider {provider} unavailable: {reason}")]
    ProviderUnavailable {
        /// The provider name (e.g. `"ollama"`, `"anthropic"`).
        provider: String,
        /// Human-readable description of why the provider is unavailable.
        reason: String,
        /// The HTTP status that produced this error, when it came from a
        /// non-success response (rather than a network or body-read failure).
        /// Drives the embedding retry classifier's `RateLimited`/`Overloaded`
        /// split.
        status: Option<u16>,
        /// The `Retry-After` delay the provider asked for, parsed from the
        /// header's delta-seconds form. The reindex retry path uses it for a
        /// rate-limited or overloaded task's `available_at`.
        retry_after: Option<Duration>,
    },

    /// An embedding generation call failed.
    #[error("embedding generation failed for model {model}: {context}")]
    EmbeddingFailed {
        /// The model that was called.
        model: String,
        /// Human-readable description of what the call was trying to do.
        context: String,
        /// The underlying provider error, if one exists.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// An LLM completion call failed.
    #[error("LLM call failed for model {model}: {context}")]
    LlmCallFailed {
        /// The model that was called.
        model: String,
        /// Human-readable description of what the call was trying to do.
        context: String,
        /// The underlying provider error, if one exists.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// The provider returned a response that could not be parsed into
    /// the expected shape.
    #[error("response parse failed: expected {expected_shape}, got: {actual}")]
    ResponseParseFailed {
        /// Description of the expected response structure.
        expected_shape: String,
        /// The actual response content (or a summary of it).
        actual: String,
    },

    /// Timed out waiting for the provider's concurrency permit; the
    /// request was never sent.
    #[error("timed out waiting for a permit for provider {provider_key} after {waited_ms}ms")]
    PermitTimeout {
        /// The provider key whose permits were exhausted.
        provider_key: String,
        /// How long the call waited before giving up, in milliseconds.
        waited_ms: u64,
    },
}

impl InferenceError {
    /// Constructs a [`InferenceError::ProviderUnavailable`] carrying no HTTP
    /// status or `Retry-After`, for the call sites that report unavailability
    /// from outside the wire boundary (a registry lookup, a timeout, a
    /// synchronous "unsupported" rejection).
    #[must_use]
    pub fn provider_unavailable(provider: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ProviderUnavailable {
            provider: provider.into(),
            reason: reason.into(),
            status: None,
            retry_after: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Retry classification
// ---------------------------------------------------------------------------

/// Classifies an embedding error for the reindex retry path.
///
/// A retryable [`InferenceError::ProviderUnavailable`] splits by the HTTP status
/// it carries: 429 is [`EmbeddingErrorClass::RateLimited`], 529 is
/// [`EmbeddingErrorClass::Overloaded`], and any other status or a statusless
/// network/timeout failure is [`EmbeddingErrorClass::Transient`]. A permit
/// wait that timed out is local backpressure, also
/// [`EmbeddingErrorClass::Transient`]. An
/// [`InferenceError::EmbeddingFailed`] (400 shape, 401, 403, 404) or an
/// unparseable response is [`EmbeddingErrorClass::Permanent`] and quarantined.
#[must_use]
pub fn classify_embedding_error(error: &InferenceError) -> EmbeddingErrorClass {
    match error {
        InferenceError::ProviderUnavailable { status, .. } => match status {
            Some(429) => EmbeddingErrorClass::RateLimited,
            Some(OVERLOADED_STATUS) => EmbeddingErrorClass::Overloaded,
            _ => EmbeddingErrorClass::Transient,
        },
        InferenceError::PermitTimeout { .. } => EmbeddingErrorClass::Transient,
        InferenceError::EmbeddingFailed { .. }
        | InferenceError::LlmCallFailed { .. }
        | InferenceError::ResponseParseFailed { .. } => EmbeddingErrorClass::Permanent,
    }
}

/// Extracts the `Retry-After` delay a [`InferenceError::ProviderUnavailable`]
/// carries, when one was parsed from the wire response.
#[must_use]
pub fn embedding_retry_after(error: &InferenceError) -> Option<Duration> {
    match error {
        InferenceError::ProviderUnavailable { retry_after, .. } => *retry_after,
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Error mapping helpers
// ---------------------------------------------------------------------------

/// Maps a non-success HTTP status to an [`InferenceError`].
///
/// Retryable statuses (429, 5xx, 529) produce [`InferenceError::ProviderUnavailable`],
/// carrying the status and any parsed `Retry-After` delay so the embedding retry
/// classifier can distinguish rate-limited and overloaded from plain transient.
/// All other statuses delegate to `non_retryable` for the caller to choose the
/// appropriate variant (`EmbeddingFailed` or `LlmCallFailed`).
pub(crate) fn map_http_error(
    status: StatusCode,
    body: &str,
    provider: &str,
    extra: &[(&str, &str)],
    non_retryable: impl FnOnce(String) -> InferenceError,
) -> InferenceError {
    let mut context = format!("HTTP {status}: {}", body_preview(body));
    for (key, value) in extra {
        use std::fmt::Write;
        let _ = write!(context, "; {key}: {value}");
    }

    if is_retryable_status(status) {
        tracing::warn!(%status, provider, "provider returned retryable error");
        InferenceError::ProviderUnavailable {
            provider: provider.to_owned(),
            reason: context,
            status: Some(status.as_u16()),
            retry_after: parse_retry_after(extra),
        }
    } else {
        tracing::warn!(%status, provider, "provider returned non-retryable error");
        non_retryable(context)
    }
}

/// Parses a `Retry-After` value from the metadata pairs into a [`Duration`].
///
/// Only the delta-seconds form is honoured (the form every provider we call
/// sends); an HTTP-date value yields `None` and falls back to the caller's
/// exponential backoff, while still appearing in the free-text reason.
fn parse_retry_after(extra: &[(&str, &str)]) -> Option<Duration> {
    extra
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("Retry-After"))
        .and_then(|(_, value)| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

/// Maps a `reqwest` send failure to [`InferenceError::ProviderUnavailable`].
pub(crate) fn map_send_error(e: &reqwest::Error, provider: &str) -> InferenceError {
    tracing::warn!(error = %e, "request failed");
    InferenceError::provider_unavailable(provider, e.to_string())
}

/// Maps a response body read failure to [`InferenceError::ProviderUnavailable`].
pub(crate) fn map_body_read_error(e: &reqwest::Error, provider: &str) -> InferenceError {
    InferenceError::provider_unavailable(provider, format!("failed to read response body: {e}"))
}

/// Maps a JSON deserialisation failure to [`InferenceError::ResponseParseFailed`].
pub(crate) fn map_json_parse_error(
    e: &serde_json::Error,
    expected: &str,
    body: &str,
) -> InferenceError {
    tracing::warn!(error = %e, "failed to parse response");
    tracing::debug!(body = %body, "raw response body");
    InferenceError::ResponseParseFailed {
        expected_shape: expected.to_owned(),
        actual: format!("invalid JSON: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_embedding_error() {
        // A statusless provider-unavailable (network/timeout) is plain transient.
        assert_eq!(
            classify_embedding_error(&InferenceError::provider_unavailable(
                "ollama",
                "connection refused",
            )),
            EmbeddingErrorClass::Transient,
        );
        assert_eq!(
            classify_embedding_error(&InferenceError::EmbeddingFailed {
                model: "m".to_owned(),
                context: "HTTP 400".to_owned(),
                source: None,
            }),
            EmbeddingErrorClass::Permanent,
        );
        assert_eq!(
            classify_embedding_error(&InferenceError::ResponseParseFailed {
                expected_shape: "x".to_owned(),
                actual: "y".to_owned(),
            }),
            EmbeddingErrorClass::Permanent,
        );
    }

    #[test]
    fn test_classify_embedding_error_429_is_rate_limited() {
        assert_eq!(
            classify_embedding_error(&InferenceError::ProviderUnavailable {
                provider: "openai".to_owned(),
                reason: "HTTP 429".to_owned(),
                status: Some(429),
                retry_after: Some(Duration::from_secs(30)),
            }),
            EmbeddingErrorClass::RateLimited,
        );
    }

    #[test]
    fn test_classify_embedding_error_529_is_overloaded() {
        assert_eq!(
            classify_embedding_error(&InferenceError::ProviderUnavailable {
                provider: "anthropic".to_owned(),
                reason: "HTTP 529".to_owned(),
                status: Some(529),
                retry_after: None,
            }),
            EmbeddingErrorClass::Overloaded,
        );
    }

    #[test]
    fn test_classify_embedding_error_other_5xx_is_transient() {
        assert_eq!(
            classify_embedding_error(&InferenceError::ProviderUnavailable {
                provider: "openai".to_owned(),
                reason: "HTTP 503".to_owned(),
                status: Some(503),
                retry_after: None,
            }),
            EmbeddingErrorClass::Transient,
        );
    }

    #[test]
    fn test_embedding_retry_after_returns_the_parsed_delay() {
        let err = InferenceError::ProviderUnavailable {
            provider: "openai".to_owned(),
            reason: "HTTP 429".to_owned(),
            status: Some(429),
            retry_after: Some(Duration::from_secs(45)),
        };
        assert_eq!(embedding_retry_after(&err), Some(Duration::from_secs(45)));
    }

    #[test]
    fn test_embedding_retry_after_is_none_without_a_header() {
        let err = InferenceError::provider_unavailable("ollama", "connection refused");
        assert_eq!(embedding_retry_after(&err), None);
    }

    #[test]
    fn test_display_provider_unavailable() {
        let err = InferenceError::provider_unavailable("ollama", "connection refused");
        assert_eq!(
            err.to_string(),
            "provider ollama unavailable: connection refused"
        );
    }

    #[test]
    fn test_display_embedding_failed() {
        let err = InferenceError::EmbeddingFailed {
            model: "nomic-embed-text".to_owned(),
            context: "generating candidate embedding".to_owned(),
            source: Some(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "request timed out",
            ))),
        };
        assert_eq!(
            err.to_string(),
            "embedding generation failed for model nomic-embed-text: \
             generating candidate embedding"
        );
    }

    #[test]
    fn test_display_llm_call_failed() {
        let err = InferenceError::LlmCallFailed {
            model: "claude-sonnet".to_owned(),
            context: "extraction prompt".to_owned(),
            source: Some(Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset",
            ))),
        };
        assert_eq!(
            err.to_string(),
            "LLM call failed for model claude-sonnet: extraction prompt"
        );
    }

    #[test]
    fn test_display_response_parse_failed() {
        let err = InferenceError::ResponseParseFailed {
            expected_shape: "JSON object with 'items' array".to_owned(),
            actual: "plain text response".to_owned(),
        };
        assert_eq!(
            err.to_string(),
            "response parse failed: expected JSON object with 'items' array, \
             got: plain text response"
        );
    }

    #[test]
    fn test_embedding_failed_source_chain() {
        let inner = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out");
        let err = InferenceError::EmbeddingFailed {
            model: "model".to_owned(),
            context: "ctx".to_owned(),
            source: Some(Box::new(inner)),
        };
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_llm_call_failed_source_chain() {
        let inner = std::io::Error::other("upstream");
        let err = InferenceError::LlmCallFailed {
            model: "model".to_owned(),
            context: "ctx".to_owned(),
            source: Some(Box::new(inner)),
        };
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_embedding_failed_source_none() {
        let err = InferenceError::EmbeddingFailed {
            model: "model".to_owned(),
            context: "empty input".to_owned(),
            source: None,
        };
        assert!(std::error::Error::source(&err).is_none());
    }

    #[test]
    fn test_llm_call_failed_source_none() {
        let err = InferenceError::LlmCallFailed {
            model: "model".to_owned(),
            context: "empty messages".to_owned(),
            source: None,
        };
        assert!(std::error::Error::source(&err).is_none());
    }

    // -- map_http_error -----------------------------------------------------

    #[test]
    fn test_map_http_error_server_error_returns_provider_unavailable() {
        let err = map_http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "oops",
            "test-provider",
            &[],
            |ctx| InferenceError::EmbeddingFailed {
                model: "m".to_owned(),
                context: ctx,
                source: None,
            },
        );
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref provider, .. }
            if provider == "test-provider"
        ));
    }

    #[test]
    fn test_map_http_error_too_many_requests_returns_provider_unavailable() {
        let err = map_http_error(
            StatusCode::TOO_MANY_REQUESTS,
            "slow down",
            "test-provider",
            &[],
            |ctx| InferenceError::EmbeddingFailed {
                model: "m".to_owned(),
                context: ctx,
                source: None,
            },
        );
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref reason, .. }
            if reason.contains("429")
        ));
    }

    #[test]
    fn test_map_http_error_529_returns_provider_unavailable() {
        let status = StatusCode::from_u16(529).unwrap();
        let err = map_http_error(status, "overloaded", "anthropic", &[], |ctx| {
            InferenceError::LlmCallFailed {
                model: "m".to_owned(),
                context: ctx,
                source: None,
            }
        });
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref reason, .. }
            if reason.contains("529")
        ));
    }

    #[test]
    fn test_map_http_error_client_error_delegates_to_closure() {
        let err = map_http_error(
            StatusCode::BAD_REQUEST,
            "bad input",
            "test-provider",
            &[],
            |ctx| InferenceError::EmbeddingFailed {
                model: "test-model".to_owned(),
                context: ctx,
                source: None,
            },
        );
        assert!(matches!(
            err,
            InferenceError::EmbeddingFailed {
                ref model,
                ref context,
                ..
            }
            if model == "test-model" && context.contains("400")
        ));
    }

    #[test]
    fn test_map_http_error_includes_body_preview() {
        let long_body = "x".repeat(500);
        let err = map_http_error(
            StatusCode::BAD_REQUEST,
            &long_body,
            "test-provider",
            &[],
            |ctx| InferenceError::EmbeddingFailed {
                model: "m".to_owned(),
                context: ctx,
                source: None,
            },
        );
        assert!(matches!(
            err,
            InferenceError::EmbeddingFailed { ref context, .. }
            if context.contains("...") && context.len() < 300
        ));
    }

    #[test]
    fn test_map_http_error_appends_extra_metadata() {
        let err = map_http_error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate limited",
            "openai",
            &[("Retry-After", "30")],
            |ctx| InferenceError::EmbeddingFailed {
                model: "m".to_owned(),
                context: ctx,
                source: None,
            },
        );
        assert!(
            matches!(
                err,
                InferenceError::ProviderUnavailable {
                    ref reason,
                    status: Some(429),
                    retry_after: Some(delay),
                    ..
                }
                if reason.contains("429")
                    && reason.contains("; Retry-After: 30")
                    && delay == Duration::from_secs(30)
            ),
            "expected ProviderUnavailable with status, parsed Retry-After, and metadata, got {err:?}"
        );
    }

    // -- map_json_parse_error -----------------------------------------------

    #[test]
    fn test_map_json_parse_error_returns_response_parse_failed() {
        let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err = map_json_parse_error(&json_err, "SomeResponse JSON object", "raw body");
        assert!(matches!(
            err,
            InferenceError::ResponseParseFailed {
                ref expected_shape,
                ref actual,
            }
            if expected_shape == "SomeResponse JSON object"
                && actual.starts_with("invalid JSON:")
        ));
    }
}
