//! Response and error factory functions for mock inference providers.
//!
//! Response factories produce fully-formed response values with sensible
//! defaults. Error factories return closures that produce a fresh
//! [`InferenceError`] on each invocation (since `InferenceError` is not
//! `Clone`).

use std::time::Duration;

use tribal_inference::{
    CompletionResponse, CompletionUsage, EmbeddingResponse, EmbeddingUsage, InferenceError,
};

/// Produces a [`CompletionResponse`] with the given text and mock defaults.
///
/// Defaults: `provider = "mock"`, `model = "mock-model"`,
/// `input_tokens = 100`, `output_tokens = 50`, `latency = ZERO`.
pub fn a_completion_response(text: impl Into<String>) -> CompletionResponse {
    CompletionResponse {
        text: text.into(),
        usage: CompletionUsage {
            provider: "mock".to_owned(),
            model: "mock-model".to_owned(),
            input_tokens: 100,
            output_tokens: 50,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            total_tokens: 150,
            latency: Duration::ZERO,
        },
    }
}

/// Produces an [`EmbeddingResponse`] with the given vector and mock defaults.
///
/// Defaults: `provider = "mock"`, `model = "mock-embed-model"`,
/// `total_tokens = 10`, `latency = ZERO`.
pub fn an_embedding_response(vector: Vec<f32>) -> EmbeddingResponse {
    EmbeddingResponse {
        vector,
        usage: EmbeddingUsage {
            provider: "mock".to_owned(),
            model: "mock-embed-model".to_owned(),
            total_tokens: 10,
            latency: Duration::ZERO,
        },
    }
}

/// Error factory type shared by all error factory functions.
pub type ErrorFactory = Box<dyn Fn() -> InferenceError + Send + Sync>;

/// Produces a closure returning [`InferenceError::ProviderUnavailable`].
pub fn a_provider_unavailable(reason: impl Into<String>) -> ErrorFactory {
    let reason = reason.into();
    Box::new(move || InferenceError::ProviderUnavailable {
        provider: "mock".to_owned(),
        reason: reason.clone(),
    })
}

/// Produces a closure returning [`InferenceError::LlmCallFailed`] with a
/// synthetic `io::Error` source.
pub fn an_llm_call_failure(
    model: impl Into<String>,
    context: impl Into<String>,
) -> ErrorFactory {
    let model = model.into();
    let context = context.into();
    Box::new(move || InferenceError::LlmCallFailed {
        model: model.clone(),
        context: context.clone(),
        source: Box::new(std::io::Error::other("simulated LLM failure")),
    })
}

/// Produces a closure returning [`InferenceError::EmbeddingFailed`] with a
/// synthetic `io::Error` source.
pub fn an_embedding_failure(
    model: impl Into<String>,
    context: impl Into<String>,
) -> ErrorFactory {
    let model = model.into();
    let context = context.into();
    Box::new(move || InferenceError::EmbeddingFailed {
        model: model.clone(),
        context: context.clone(),
        source: Box::new(std::io::Error::other("simulated embedding failure")),
    })
}

/// Produces a closure returning [`InferenceError::ResponseParseFailed`].
pub fn a_parse_failure(
    expected_shape: impl Into<String>,
    actual: impl Into<String>,
) -> ErrorFactory {
    let expected_shape = expected_shape.into();
    let actual = actual.into();
    Box::new(move || InferenceError::ResponseParseFailed {
        expected_shape: expected_shape.clone(),
        actual: actual.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completion_response_has_mock_defaults() {
        let resp = a_completion_response("hello world");
        assert_eq!(resp.text, "hello world");
        assert_eq!(resp.usage.provider, "mock");
        assert_eq!(resp.usage.model, "mock-model");
        assert_eq!(resp.usage.input_tokens, 100);
        assert_eq!(resp.usage.output_tokens, 50);
        assert_eq!(resp.usage.total_tokens, 150);
        assert_eq!(resp.usage.latency, Duration::ZERO);
    }

    #[test]
    fn test_embedding_response_has_mock_defaults() {
        let resp = an_embedding_response(vec![0.1, 0.2, 0.3]);
        assert_eq!(resp.vector, vec![0.1, 0.2, 0.3]);
        assert_eq!(resp.usage.provider, "mock");
        assert_eq!(resp.usage.model, "mock-embed-model");
        assert_eq!(resp.usage.total_tokens, 10);
        assert_eq!(resp.usage.latency, Duration::ZERO);
    }

    #[test]
    fn test_error_factories_produce_correct_variants() {
        let err = a_provider_unavailable("down")();
        assert!(matches!(err, InferenceError::ProviderUnavailable { .. }));

        let err = an_llm_call_failure("llama3", "timeout")();
        assert!(matches!(err, InferenceError::LlmCallFailed { .. }));

        let err = an_embedding_failure("nomic", "oom")();
        assert!(matches!(err, InferenceError::EmbeddingFailed { .. }));

        let err = a_parse_failure("JSON object", "{bad}")();
        assert!(matches!(err, InferenceError::ResponseParseFailed { .. }));
    }

    #[test]
    fn test_error_factories_produce_distinct_instances() {
        let factory = an_llm_call_failure("llama3", "retry test");
        let err_a = factory();
        let err_b = factory();

        let msg_a = format!("{err_a}");
        let msg_b = format!("{err_b}");
        assert_eq!(msg_a, msg_b);

        // Both format to the same Debug string (same field values).
        let dbg_a = format!("{err_a:?}");
        let dbg_b = format!("{err_b:?}");
        assert_eq!(dbg_a, dbg_b);
    }
}
