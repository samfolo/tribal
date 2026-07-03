//! Construction of a concrete embedding provider from a resolved identity.
//!
//! Both server boot (from the active profile) and the reindex worker (from a
//! building profile) construct an [`EmbeddingProvider`] for a
//! `(provider_kind, base_url, model, dimensions)` target with a resolved
//! credential, so the provider-selection match lives here once rather than in
//! each caller.

use std::sync::Arc;

use tribal_domain::ProviderKind;

use crate::{EmbeddingProvider, ollama::OllamaEmbeddingProvider, openai::OpenAiEmbeddingProvider};

/// An embedding provider that cannot be constructed for the requested kind.
#[derive(Debug, thiserror::Error)]
pub enum EmbeddingProviderUnavailable {
    /// The kind has no embedding API at all.
    #[error("{provider} does not provide an embedding API; use Ollama or OpenAI for embeddings")]
    NoEmbeddingApi {
        /// The kind with no embedding API.
        provider: ProviderKind,
    },
    /// The platform serves embeddings through the managed gateway, not a
    /// locally-constructed embedding provider.
    #[error(
        "the platform provider serves embeddings through the managed gateway, \
         not a local embedding provider"
    )]
    ManagedGatewayTransport,
}

/// Rejects a provider kind with no embedding API.
///
/// The structural check runs before credential resolution in the gateway, so
/// a keyless target on an unsupported kind reports the missing API rather
/// than a missing credential no key could remedy.
///
/// # Errors
///
/// Returns [`EmbeddingProviderUnavailable::NoEmbeddingApi`] for a provider kind
/// with no embedding API (Anthropic).
pub(crate) fn ensure_embedding_support(
    provider_kind: ProviderKind,
) -> Result<(), EmbeddingProviderUnavailable> {
    if provider_kind.supports_embedding() {
        Ok(())
    } else {
        Err(EmbeddingProviderUnavailable::NoEmbeddingApi {
            provider: provider_kind,
        })
    }
}

/// Constructs the concrete embedding provider for a resolved target.
///
/// `api_key` is ignored for providers that need none (Ollama); the caller has
/// already resolved it (empty when absent).
///
/// # Errors
///
/// Returns [`EmbeddingProviderUnavailable::NoEmbeddingApi`] for a kind with no
/// embedding API (Anthropic), and
/// [`EmbeddingProviderUnavailable::ManagedGatewayTransport`] for the platform,
/// whose embeddings are served by the gateway rather than a local provider.
pub fn make_embedding_provider(
    provider_kind: ProviderKind,
    client: reqwest::Client,
    base_url: &str,
    model: &str,
    dimensions: u32,
    api_key: &str,
) -> Result<Arc<dyn EmbeddingProvider>, EmbeddingProviderUnavailable> {
    match provider_kind {
        ProviderKind::Ollama => Ok(Arc::new(OllamaEmbeddingProvider::new(
            client, base_url, model, dimensions,
        ))),
        ProviderKind::OpenAi => Ok(Arc::new(OpenAiEmbeddingProvider::new(
            client, base_url, model, api_key, dimensions,
        ))),
        ProviderKind::Anthropic => Err(EmbeddingProviderUnavailable::NoEmbeddingApi {
            provider: ProviderKind::Anthropic,
        }),
        ProviderKind::Platform => Err(EmbeddingProviderUnavailable::ManagedGatewayTransport),
    }
}
