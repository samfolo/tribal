//! Inference provider traits.
//!
//! [`InferenceProvider`] abstracts LLM completion calls.
//! [`EmbeddingProvider`] abstracts embedding generation calls.
//! Both traits are object-safe and require `Send + Sync` so they
//! can be held as `Arc<dyn InferenceProvider>` or
//! `Arc<dyn EmbeddingProvider>`.

use async_trait::async_trait;

use crate::error::InferenceError;
use crate::request::{CompletionRequest, EmbeddingRequest};
use crate::response::{CompletionResponse, EmbeddingResponse};

/// Abstraction for LLM completion providers.
///
/// Implementations handle provider-specific serialisation, HTTP calls,
/// response parsing, and error mapping.  The trait is object-safe and
/// requires `Send + Sync` so it can be shared across tasks via
/// `Arc<dyn InferenceProvider>`.
#[async_trait]
pub trait InferenceProvider: Send + Sync {
    /// Sends a completion request and returns the generated response.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::ProviderUnavailable`] if the provider
    /// cannot be reached.  Returns [`InferenceError::LlmCallFailed`] if
    /// the call fails.  Returns [`InferenceError::ResponseParseFailed`]
    /// if the response cannot be parsed into the expected shape.
    async fn complete(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse, InferenceError>;
}

/// Abstraction for embedding generation providers.
///
/// Implementations handle provider-specific serialisation, HTTP calls,
/// response parsing, and error mapping.  The trait is object-safe and
/// requires `Send + Sync` so it can be shared across tasks via
/// `Arc<dyn EmbeddingProvider>`.
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Generates an embedding vector for the given input text.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::ProviderUnavailable`] if the provider
    /// cannot be reached.  Returns [`InferenceError::EmbeddingFailed`]
    /// if the embedding call fails.
    async fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> Result<EmbeddingResponse, InferenceError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    struct StubInferenceProvider;
    struct StubEmbeddingProvider;

    #[async_trait]
    impl InferenceProvider for StubInferenceProvider {
        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, InferenceError> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl EmbeddingProvider for StubEmbeddingProvider {
        async fn embed(
            &self,
            _request: EmbeddingRequest,
        ) -> Result<EmbeddingResponse, InferenceError> {
            unimplemented!()
        }
    }

    /// Both traits must be object-safe so stages can hold them as
    /// `Arc<dyn Trait>`.  This test verifies the bounds at compile
    /// time by constructing trait objects from stub implementations.
    #[test]
    fn test_traits_are_object_safe() {
        let _inference: Arc<dyn InferenceProvider> = Arc::new(StubInferenceProvider);
        let _embedding: Arc<dyn EmbeddingProvider> = Arc::new(StubEmbeddingProvider);
    }
}
