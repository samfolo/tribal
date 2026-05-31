//! Construction of a concrete embedding provider from a resolved identity.
//!
//! Both server boot (from the active profile) and the reindex worker (from a
//! building profile) construct an [`EmbeddingProvider`] for a
//! `(provider_kind, base_url, model, dimensions)` target with a resolved
//! credential, so the provider-selection match lives here once rather than in
//! each caller.

use std::sync::Arc;

use tribal_domain::ProviderKind;

use crate::{EmbeddingProvider, OllamaEmbeddingProvider, OpenAiEmbeddingProvider};

/// A provider kind that has no embedding API was requested.
#[derive(Debug, thiserror::Error)]
#[error("{provider} does not provide an embedding API")]
pub struct UnsupportedEmbeddingProvider {
    /// The unsupported provider kind.
    pub provider: ProviderKind,
}

/// Constructs the concrete embedding provider for a resolved target.
///
/// `api_key` is ignored for providers that need none (Ollama); the caller has
/// already resolved it (empty when absent).
///
/// # Errors
///
/// Returns [`UnsupportedEmbeddingProvider`] for a provider kind with no
/// embedding API (Anthropic).
pub fn make_embedding_provider(
    provider_kind: ProviderKind,
    client: reqwest::Client,
    base_url: &str,
    model: &str,
    dimensions: u32,
    api_key: &str,
) -> Result<Arc<dyn EmbeddingProvider>, UnsupportedEmbeddingProvider> {
    match provider_kind {
        ProviderKind::Ollama => Ok(Arc::new(OllamaEmbeddingProvider::new(
            client, base_url, model, dimensions,
        ))),
        ProviderKind::OpenAi => Ok(Arc::new(OpenAiEmbeddingProvider::new(
            client, base_url, model, api_key, dimensions,
        ))),
        ProviderKind::Anthropic => Err(UnsupportedEmbeddingProvider {
            provider: ProviderKind::Anthropic,
        }),
    }
}
