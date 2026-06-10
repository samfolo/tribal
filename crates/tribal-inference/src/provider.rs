//! Inference provider traits.
//!
//! [`InferenceProvider`] abstracts LLM completion calls.
//! [`EmbeddingProvider`] abstracts embedding generation calls.
//! Both traits are object-safe and require `Send + Sync` so they
//! can be held as `Arc<dyn InferenceProvider>` or
//! `Arc<dyn EmbeddingProvider>`.

use std::time::Instant;

use async_trait::async_trait;
use tribal_domain::{CompletionResponse, EmbeddingUsage, InferenceEvent};

use crate::{
    error::InferenceError,
    request::{CompletionRequest, EmbeddingRequest},
    response::EmbeddingResponse,
    stream::InferenceEventStream,
};

// ---------------------------------------------------------------------------
// ProviderIdentity
// ---------------------------------------------------------------------------

/// Identifies an inference or embedding provider by name and model.
///
/// Stored on each provider implementation and returned by reference
/// from the [`InferenceProvider::identity`] and
/// [`EmbeddingProvider::identity`] trait methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderIdentity {
    /// The provider name (e.g. `"ollama"`, `"anthropic"`, `"openai"`).
    pub name: String,
    /// The model identifier (e.g. `"llama3"`, `"claude-sonnet-4-20250514"`).
    pub model: String,
}

/// Abstraction for LLM completion providers.
///
/// Implementations handle provider-specific serialisation, HTTP calls,
/// response parsing, and error mapping.  The trait is object-safe and
/// requires `Send + Sync` so it can be shared across tasks via
/// `Arc<dyn InferenceProvider>`.
#[async_trait]
pub trait InferenceProvider: Send + Sync {
    /// Returns the provider name and model identifier.
    fn identity(&self) -> &ProviderIdentity;

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

    /// Sends a completion request over the provider's native streaming
    /// wire, returning the event stream.
    ///
    /// The folded stream is equivalent to [`complete`](Self::complete) on
    /// the same request: identical text and usage, with deltas surfaced as
    /// they arrive. The default implementation serves providers without a
    /// native streaming path by making the buffered call and emitting the
    /// terminal event alone.
    ///
    /// # Errors
    ///
    /// Returns the same error mapping as [`complete`](Self::complete) for
    /// failures before the stream opens; failures mid-stream arrive as the
    /// stream's final item.
    async fn complete_stream(
        &self,
        request: CompletionRequest,
    ) -> Result<InferenceEventStream, InferenceError> {
        let response = self.complete(request).await?;
        Ok(Box::pin(futures_util::stream::once(async move {
            Ok(InferenceEvent::Completed { response })
        })))
    }
}

/// The outcome of an [`EmbeddingProvider::embed_many`] batch call.
///
/// `items` holds one result per input, in input order: a successful vector or
/// that input's own error, so a single poison input fails only its own slot
/// rather than the whole batch. `usage` is the aggregate token and latency
/// record for the batch as a whole.
#[derive(Debug)]
pub struct BatchEmbeddingResult {
    /// One result per input, in the order the inputs were supplied.
    pub items: Vec<Result<Vec<f32>, InferenceError>>,

    /// Aggregate token usage and wall-clock latency for the batch.
    pub usage: EmbeddingUsage,
}

/// Abstraction for embedding generation providers.
///
/// Implementations handle provider-specific serialisation, HTTP calls,
/// response parsing, and error mapping.  The trait is object-safe and
/// requires `Send + Sync` so it can be shared across tasks via
/// `Arc<dyn EmbeddingProvider>`.
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Returns the provider name and model identifier.
    fn identity(&self) -> &ProviderIdentity;

    /// Generates an embedding vector for the given input text.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::ProviderUnavailable`] if the provider
    /// cannot be reached.  Returns [`InferenceError::EmbeddingFailed`]
    /// if the embedding call fails.
    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, InferenceError>;

    /// Generates embeddings for a batch of inputs, preserving input order.
    ///
    /// The default implementation issues one [`embed`](Self::embed) call per
    /// input, so a single poison input fails only its own slot. A provider
    /// with a native array endpoint may override this to issue fewer
    /// round-trips, but must preserve the same poison-isolation contract:
    /// `items[i]` is the result for `requests[i]`, and one bad input never
    /// fails its neighbours. The returned [`BatchEmbeddingResult::usage`]
    /// sums the per-input token counts and measures the batch wall-clock.
    async fn embed_many(&self, requests: Vec<EmbeddingRequest>) -> BatchEmbeddingResult {
        let started = Instant::now();
        let mut items = Vec::with_capacity(requests.len());
        let mut total_tokens: u32 = 0;

        for request in requests {
            match self.embed(request).await {
                Ok(response) => {
                    total_tokens = total_tokens.saturating_add(response.usage.total_tokens);
                    items.push(Ok(response.vector));
                }
                Err(error) => items.push(Err(error)),
            }
        }

        let identity = self.identity();
        BatchEmbeddingResult {
            items,
            usage: EmbeddingUsage {
                provider: identity.name.clone(),
                model: identity.model.clone(),
                total_tokens,
                latency: started.elapsed(),
            },
        }
    }

    /// Resolves the provider-native revision token for the served model: a
    /// content-addressed digest or dated snapshot that changes when the model's
    /// geometry changes. The cheapest reliable drift signal where a provider
    /// exposes one, with the probe digest as the cross-provider backstop.
    ///
    /// Best-effort and non-failing: the default returns empty, as does an
    /// override whose lookup does not resolve.
    async fn revision_token(&self) -> String {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tribal_domain::EmbeddingPurpose;

    use super::*;

    struct StubInferenceProvider {
        identity: ProviderIdentity,
    }

    struct StubEmbeddingProvider {
        identity: ProviderIdentity,
    }

    /// An embedding provider whose `embed` returns a canned two-element
    /// vector for any input except `"poison"`, which fails. Drives the
    /// default `embed_many` loop without a network.
    struct ScriptedEmbeddingProvider {
        identity: ProviderIdentity,
    }

    fn stub_identity() -> ProviderIdentity {
        ProviderIdentity {
            name: "stub".to_owned(),
            model: "stub-model".to_owned(),
        }
    }

    fn an_embedding_request(input: &str) -> EmbeddingRequest {
        EmbeddingRequest {
            input: input.to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        }
    }

    #[async_trait]
    impl InferenceProvider for StubInferenceProvider {
        fn identity(&self) -> &ProviderIdentity {
            &self.identity
        }

        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, InferenceError> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl EmbeddingProvider for StubEmbeddingProvider {
        fn identity(&self) -> &ProviderIdentity {
            &self.identity
        }

        async fn embed(
            &self,
            _request: EmbeddingRequest,
        ) -> Result<EmbeddingResponse, InferenceError> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl EmbeddingProvider for ScriptedEmbeddingProvider {
        fn identity(&self) -> &ProviderIdentity {
            &self.identity
        }

        async fn embed(
            &self,
            request: EmbeddingRequest,
        ) -> Result<EmbeddingResponse, InferenceError> {
            if request.input == "poison" {
                return Err(InferenceError::EmbeddingFailed {
                    model: self.identity.model.clone(),
                    context: "poison input".to_owned(),
                    source: None,
                });
            }
            Ok(EmbeddingResponse {
                vector: vec![1.0, 2.0],
                usage: EmbeddingUsage {
                    provider: self.identity.name.clone(),
                    model: self.identity.model.clone(),
                    total_tokens: 3,
                    latency: Duration::from_millis(1),
                },
            })
        }
    }

    /// Both traits must be object-safe so stages can hold them as
    /// `Arc<dyn Trait>`.  This test verifies the bounds at compile
    /// time by constructing trait objects from stub implementations.
    #[test]
    fn test_traits_are_object_safe() {
        let _inference: Arc<dyn InferenceProvider> = Arc::new(StubInferenceProvider {
            identity: stub_identity(),
        });
        let _embedding: Arc<dyn EmbeddingProvider> = Arc::new(StubEmbeddingProvider {
            identity: stub_identity(),
        });
    }

    #[tokio::test]
    async fn test_embed_many_default_localises_poison_and_aggregates_usage() {
        let provider = ScriptedEmbeddingProvider {
            identity: stub_identity(),
        };
        let requests = vec![
            an_embedding_request("alpha"),
            an_embedding_request("poison"),
            an_embedding_request("beta"),
        ];

        let result = provider.embed_many(requests).await;

        // One slot per input, in order; only the poison slot failed.
        assert_eq!(result.items.len(), 3);
        assert_eq!(result.items[0].as_ref().unwrap(), &vec![1.0, 2.0]);
        assert!(matches!(
            result.items[1],
            Err(InferenceError::EmbeddingFailed { .. })
        ));
        assert_eq!(result.items[2].as_ref().unwrap(), &vec![1.0, 2.0]);

        // Usage aggregates the two successes (3 tokens each); the poison
        // contributes nothing.
        assert_eq!(result.usage.total_tokens, 6);
        assert_eq!(result.usage.provider, "stub");
        assert_eq!(result.usage.model, "stub-model");
    }

    #[tokio::test]
    async fn test_embed_many_empty_input_yields_empty_items() {
        let provider = ScriptedEmbeddingProvider {
            identity: stub_identity(),
        };
        let result = provider.embed_many(Vec::new()).await;
        assert!(result.items.is_empty());
        assert_eq!(result.usage.total_tokens, 0);
    }
}
