//! Crate-level error type for `tribal-inference`.
//!
//! [`InferenceError`] is the single error enum for the inference layer.
//! All variants use named fields; wrapped errors carry `#[source]` for
//! error chain propagation.

use thiserror::Error;

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
    },

    /// An embedding generation call failed.
    #[error("embedding generation failed for model {model}: {context}")]
    EmbeddingFailed {
        /// The model that was called.
        model: String,
        /// Human-readable description of what the call was trying to do.
        context: String,
        /// The underlying provider error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// An LLM completion call failed.
    #[error("LLM call failed for model {model}: {context}")]
    LlmCallFailed {
        /// The model that was called.
        model: String,
        /// Human-readable description of what the call was trying to do.
        context: String,
        /// The underlying provider error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_provider_unavailable() {
        let err = InferenceError::ProviderUnavailable {
            provider: "ollama".to_owned(),
            reason: "connection refused".to_owned(),
        };
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
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "request timed out",
            )),
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
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset",
            )),
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
            source: Box::new(inner),
        };
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_llm_call_failed_source_chain() {
        let inner = std::io::Error::other("upstream");
        let err = InferenceError::LlmCallFailed {
            model: "model".to_owned(),
            context: "ctx".to_owned(),
            source: Box::new(inner),
        };
        assert!(std::error::Error::source(&err).is_some());
    }
}
