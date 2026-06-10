//! Response and error factory functions for mock inference providers.
//!
//! Response factories produce fully-formed response values with sensible
//! defaults. Error factories return closures that produce a fresh
//! [`InferenceError`] on each invocation (since `InferenceError` is not
//! `Clone`).

use std::time::Duration;

use tribal_domain::{CompletionResponse, CompletionUsage, EmbeddingUsage};
use tribal_inference::{EmbeddingResponse, InferenceError};

use crate::mock::async_dispatch::core::ErrorFactory;

/// Inference-specific error factory type alias.
pub type InferenceErrorFactory = ErrorFactory<InferenceError>;

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

/// Produces a closure returning a statusless [`InferenceError::ProviderUnavailable`]
/// (the network/timeout shape, which classifies as plain transient).
pub fn a_provider_unavailable(reason: impl Into<String>) -> InferenceErrorFactory {
    let reason = reason.into();
    Box::new(move || InferenceError::provider_unavailable("mock", reason.clone()))
}

/// Produces a closure returning a rate-limited (HTTP 429)
/// [`InferenceError::ProviderUnavailable`] carrying the given `Retry-After` delay.
pub fn a_rate_limited(retry_after: Duration) -> InferenceErrorFactory {
    Box::new(move || InferenceError::ProviderUnavailable {
        provider: "mock".to_owned(),
        reason: "HTTP 429".to_owned(),
        status: Some(429),
        retry_after: Some(retry_after),
    })
}

/// Produces a closure returning an overloaded (HTTP 529)
/// [`InferenceError::ProviderUnavailable`].
pub fn an_overloaded() -> InferenceErrorFactory {
    Box::new(|| InferenceError::ProviderUnavailable {
        provider: "mock".to_owned(),
        reason: "HTTP 529".to_owned(),
        status: Some(529),
        retry_after: None,
    })
}

/// Produces a closure returning [`InferenceError::LlmCallFailed`] with a
/// synthetic `io::Error` source.
pub fn an_llm_call_failure(
    model: impl Into<String>,
    context: impl Into<String>,
) -> InferenceErrorFactory {
    let model = model.into();
    let context = context.into();
    Box::new(move || InferenceError::LlmCallFailed {
        model: model.clone(),
        context: context.clone(),
        source: Some(Box::new(std::io::Error::other("simulated LLM failure"))),
    })
}

/// Produces a closure returning [`InferenceError::EmbeddingFailed`] with a
/// synthetic `io::Error` source.
pub fn an_embedding_failure(
    model: impl Into<String>,
    context: impl Into<String>,
) -> InferenceErrorFactory {
    let model = model.into();
    let context = context.into();
    Box::new(move || InferenceError::EmbeddingFailed {
        model: model.clone(),
        context: context.clone(),
        source: Some(Box::new(std::io::Error::other(
            "simulated embedding failure",
        ))),
    })
}

/// Produces a closure returning [`InferenceError::ResponseParseFailed`].
pub fn a_parse_failure(
    expected_shape: impl Into<String>,
    actual: impl Into<String>,
) -> InferenceErrorFactory {
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
    fn test_error_factories_produce_correct_variants_and_fields() {
        let err = a_provider_unavailable("down")();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable {
                ref provider,
                ref reason,
                status: None,
                retry_after: None,
            }
            if provider == "mock" && reason == "down"
        ));

        let err = a_rate_limited(Duration::from_secs(30))();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable {
                status: Some(429),
                retry_after: Some(delay),
                ..
            }
            if delay == Duration::from_secs(30)
        ));

        let err = an_overloaded()();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable {
                status: Some(529),
                retry_after: None,
                ..
            }
        ));

        let err = an_llm_call_failure("llama3", "timeout")();
        assert!(matches!(
            err,
            InferenceError::LlmCallFailed { ref model, ref context, .. }
            if model == "llama3" && context == "timeout"
        ));

        let err = an_embedding_failure("nomic", "oom")();
        assert!(matches!(
            err,
            InferenceError::EmbeddingFailed { ref model, ref context, .. }
            if model == "nomic" && context == "oom"
        ));

        let err = a_parse_failure("JSON object", "{bad}")();
        assert!(matches!(
            err,
            InferenceError::ResponseParseFailed { ref expected_shape, ref actual }
            if expected_shape == "JSON object" && actual == "{bad}"
        ));
    }

    #[test]
    fn test_error_factory_produces_fresh_error_per_invocation() {
        let factory = an_llm_call_failure("llama3", "retry test");
        let err_a = factory();
        let err_b = factory();

        // Same Display output confirms same field values.
        assert_eq!(
            format!("{err_a}"),
            "LLM call failed for model llama3: retry test"
        );
        assert_eq!(format!("{err_a}"), format!("{err_b}"));
    }
}
