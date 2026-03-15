//! Provider registry construction, provider instantiation, and embedding probe.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tribal_config::{
    EmbeddingConfig, ProviderKind, StageInferenceConfig, TribalConfig,
};
use tribal_inference::{
    AnthropicInferenceProvider, EmbeddingProvider, InferenceProvider, OllamaEmbeddingProvider,
    OllamaInferenceProvider, OpenAiEmbeddingProvider, OpenAiInferenceProvider, ProviderKey,
    ProviderLimits, ProviderRegistry, RequestClass,
};

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Expect messages
// ---------------------------------------------------------------------------

const EXPECT_LIMITS: &str = "provider limits must be configured for all providers";
const EXPECT_CLIENT: &str = "provider key must have an HTTP client in registry";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Builds the [`ProviderRegistry`] from the application configuration.
///
/// Creates one `(ProviderKey, ProviderLimits)` entry for each distinct
/// (provider kind, base URL, request class) combination across the
/// embedding and inference configurations.
///
/// # Panics
///
/// Panics if `config.limits.providers` does not contain limits for a
/// configured provider kind.
pub(crate) fn build_provider_registry(
    config: &TribalConfig,
) -> Result<ProviderRegistry, AppError> {
    let mut entries: Vec<(ProviderKey, ProviderLimits)> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    // Embedding provider entry.
    let emb = &config.embedding;
    add_entry(
        &mut entries,
        &mut seen,
        emb.provider,
        &emb.base_url,
        RequestClass::Embedding,
        config,
    )?;

    // Inference provider entries (extraction, triage, relation).
    for stage in &[
        &config.inference.extraction,
        &config.inference.triage,
        &config.inference.relation,
    ] {
        add_entry(
            &mut entries,
            &mut seen,
            stage.provider,
            &stage.base_url,
            RequestClass::Inference,
            config,
        )?;
    }

    ProviderRegistry::new(entries).map_err(|source| AppError::ProviderRegistry { source })
}

/// Constructs the embedding provider from configuration.
///
/// Returns the boxed provider and the registry key for semaphore lookups.
/// Probes the provider before returning — logs a warning on failure but
/// does not fail startup.
///
/// # Panics
///
/// Panics if the registry does not contain the provider key.
pub(crate) async fn build_embedding_provider(
    registry: &ProviderRegistry,
    config: &EmbeddingConfig,
) -> Result<(Arc<dyn EmbeddingProvider>, ProviderKey), AppError> {
    let url = resolve_base_url(config.provider, &config.base_url);
    let key = ProviderKey::new(config.provider.as_str(), &url, RequestClass::Embedding)
        .map_err(|source| AppError::ProviderRegistry { source })?;

    let client = registry.client(&key).expect(EXPECT_CLIENT).clone();

    let provider: Arc<dyn EmbeddingProvider> = match config.provider {
        ProviderKind::Ollama => {
            let p = OllamaEmbeddingProvider::new(
                client,
                &url,
                &config.model,
                config.dimensions,
            );
            probe_embedding(&p).await;
            Arc::new(p)
        }
        ProviderKind::OpenAi => {
            let p = OpenAiEmbeddingProvider::new(
                client,
                &url,
                &config.model,
                config.api_key.as_deref().unwrap_or_default(),
                config.dimensions,
            );
            probe_embedding(&p).await;
            Arc::new(p)
        }
        ProviderKind::Anthropic => {
            // Validation rejects this configuration, so this arm is
            // unreachable in normal operation.
            unreachable!("Anthropic does not provide an embedding API");
        }
    };

    Ok((provider, key))
}

/// Constructs an inference provider for a single pipeline stage.
///
/// Returns the boxed provider and the registry key for semaphore lookups.
///
/// # Panics
///
/// Panics if the registry does not contain the provider key.
pub(crate) fn build_inference_provider(
    registry: &ProviderRegistry,
    config: &StageInferenceConfig,
) -> Result<(Arc<dyn InferenceProvider>, ProviderKey), AppError> {
    let url = resolve_base_url(config.provider, &config.base_url);
    let key = ProviderKey::new(config.provider.as_str(), &url, RequestClass::Inference)
        .map_err(|source| AppError::ProviderRegistry { source })?;

    let client = registry.client(&key).expect(EXPECT_CLIENT).clone();

    let provider: Arc<dyn InferenceProvider> = match config.provider {
        ProviderKind::Ollama => {
            Arc::new(OllamaInferenceProvider::new(client, &url, &config.model))
        }
        ProviderKind::OpenAi => Arc::new(OpenAiInferenceProvider::new(
            client,
            &url,
            &config.model,
            config.api_key.as_deref().unwrap_or_default(),
        )),
        ProviderKind::Anthropic => Arc::new(AnthropicInferenceProvider::new(
            client,
            &url,
            &config.model,
            config.api_key.as_deref().unwrap_or_default(),
        )),
    };

    Ok((provider, key))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Adds a deduplicated registry entry.
///
/// # Panics
///
/// Panics if `config.limits.providers` does not contain the given provider.
fn add_entry(
    entries: &mut Vec<(ProviderKey, ProviderLimits)>,
    seen: &mut HashSet<String>,
    provider: ProviderKind,
    base_url: &Option<String>,
    request_class: RequestClass,
    config: &TribalConfig,
) -> Result<(), AppError> {
    let url = resolve_base_url(provider, base_url);
    let key = ProviderKey::new(provider.as_str(), &url, request_class)
        .map_err(|source| AppError::ProviderRegistry { source })?;

    let dedup_key = format!("{}:{}:{:?}", provider.as_str(), url, request_class);
    if seen.insert(dedup_key) {
        let limits_config = config
            .limits
            .providers
            .get(&provider)
            .expect(EXPECT_LIMITS);

        entries.push((
            key,
            ProviderLimits {
                max_in_flight: limits_config.max_in_flight,
                request_timeout: Duration::from_millis(limits_config.request_timeout_ms),
            },
        ));
    }

    Ok(())
}

/// Resolves the base URL for a provider, falling back to the provider's
/// default when no explicit URL is configured.
fn resolve_base_url(provider: ProviderKind, config_url: &Option<String>) -> String {
    config_url
        .as_deref()
        .unwrap_or(provider.default_base_url())
        .to_owned()
}

/// Sends a lightweight probe request to the embedding provider.
///
/// Logs a warning on failure but does not fail startup — the provider may
/// become available before the first real request arrives.
async fn probe_embedding(provider: &(impl tribal_inference::EmbeddingProvider + ?Sized)) {
    let request = tribal_inference::EmbeddingRequest {
        texts: vec!["probe".into()],
    };

    match provider.embed(request).await {
        Ok(_) => tracing::info!("embedding model probe succeeded"),
        Err(e) => tracing::warn!(%e, "embedding model probe failed (non-fatal)"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_config::{
        DEFAULT_ANTHROPIC_BASE_URL, DEFAULT_OLLAMA_BASE_URL, DEFAULT_OPENAI_BASE_URL,
    };

    use super::*;

    #[test]
    fn test_resolve_base_url_uses_default_when_none() {
        assert_eq!(
            resolve_base_url(ProviderKind::Ollama, &None),
            DEFAULT_OLLAMA_BASE_URL,
        );
        assert_eq!(
            resolve_base_url(ProviderKind::Anthropic, &None),
            DEFAULT_ANTHROPIC_BASE_URL,
        );
        assert_eq!(
            resolve_base_url(ProviderKind::OpenAi, &None),
            DEFAULT_OPENAI_BASE_URL,
        );
    }

    #[test]
    fn test_resolve_base_url_uses_explicit_when_some() {
        let custom = Some("https://custom.example.com".to_owned());
        assert_eq!(
            resolve_base_url(ProviderKind::Ollama, &custom),
            "https://custom.example.com",
        );
    }
}
