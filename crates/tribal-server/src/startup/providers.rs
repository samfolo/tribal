//! Provider registry construction, provider instantiation, and embedding probe.

use std::sync::Arc;
use std::time::Duration;

use tribal_config::{
    EmbeddingConfig, ProviderKind, ProviderLimitsConfig, StageInferenceConfig, TribalConfig,
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

const EXPECT_SEMAPHORE: &str = "provider key must be present in registry";
const EXPECT_CLIENT: &str = "provider key must have an HTTP client in registry";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Builds the [`ProviderRegistry`] from the application configuration.
///
/// Creates one `(ProviderKey, ProviderLimits)` entry for each distinct
/// (provider kind, base URL, request class) combination across the
/// embedding and inference configurations.
pub(crate) fn build_provider_registry(
    config: &TribalConfig,
) -> Result<ProviderRegistry, AppError> {
    let mut entries: Vec<(ProviderKey, ProviderLimits)> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    let add_entry = |entries: &mut Vec<(ProviderKey, ProviderLimits)>,
                     seen: &mut std::collections::HashSet<String>,
                     provider: ProviderKind,
                     base_url: &Option<String>,
                     request_class: RequestClass,
                     limits_config: &ProviderLimitsConfig| {
        let url = resolve_base_url(provider, base_url);
        let key = ProviderKey::new(provider.as_str(), &url, request_class)?;

        // Deduplicate: same provider + URL + request class should only
        // appear once in the registry.
        let dedup_key = format!("{}:{}:{:?}", provider.as_str(), url, request_class);
        if seen.insert(dedup_key) {
            entries.push((
                key,
                ProviderLimits {
                    max_in_flight: limits_config.max_in_flight,
                    request_timeout: Duration::from_millis(limits_config.request_timeout_ms),
                },
            ));
        }
        Ok::<_, AppError>(())
    };

    let default_limits = ProviderLimitsConfig {
        max_in_flight: 2,
        request_timeout_ms: 180_000,
    };

    // Embedding provider entry.
    let emb = &config.embedding;
    let emb_limits = config
        .limits
        .providers
        .get(&emb.provider)
        .unwrap_or(&default_limits);
    add_entry(
        &mut entries,
        &mut seen,
        emb.provider,
        &emb.base_url,
        RequestClass::Embedding,
        emb_limits,
    )?;

    // Inference provider entries (extraction, triage, relation).
    let stages = [
        &config.inference.extraction,
        &config.inference.triage,
        &config.inference.relation,
    ];
    for stage in &stages {
        let limits = config
            .limits
            .providers
            .get(&stage.provider)
            .unwrap_or(&default_limits);
        add_entry(
            &mut entries,
            &mut seen,
            stage.provider,
            &stage.base_url,
            RequestClass::Inference,
            limits,
        )?;
    }

    ProviderRegistry::new(entries).map_err(|source| AppError::ProviderRegistry { source })
}

/// Constructs the embedding provider from configuration.
///
/// Returns the boxed provider and the registry key for semaphore lookups.
///
/// # Panics
///
/// Panics if the registry does not contain the provider key — this
/// indicates a bug in `build_provider_registry`.
pub(crate) fn build_embedding_provider(
    registry: &ProviderRegistry,
    config: &EmbeddingConfig,
) -> Result<(Arc<dyn EmbeddingProvider>, ProviderKey), AppError> {
    let url = resolve_base_url(config.provider, &config.base_url);
    let key = ProviderKey::new(config.provider.as_str(), &url, RequestClass::Embedding)
        .map_err(|source| AppError::ProviderRegistry { source })?;

    let client = registry.client(&key).expect(EXPECT_CLIENT).clone();

    let provider: Arc<dyn EmbeddingProvider> = match config.provider {
        ProviderKind::Ollama => Arc::new(OllamaEmbeddingProvider::new(
            client,
            &url,
            &config.model,
            config.dimensions,
        )),
        ProviderKind::OpenAi => Arc::new(OpenAiEmbeddingProvider::new(
            client,
            &url,
            &config.model,
            config.api_key.as_deref().unwrap_or_default(),
            config.dimensions,
        )),
        ProviderKind::Anthropic => {
            // Anthropic does not offer an embedding API. Fall back to a
            // descriptive error rather than panicking.
            return Err(AppError::ProjectResolution {
                context: "Anthropic does not provide an embedding API; \
                          use Ollama or OpenAI for embeddings"
                    .into(),
            });
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
/// Panics if the registry does not contain the provider key — this
/// indicates a bug in `build_provider_registry`.
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

/// Sends a lightweight probe request to the embedding provider.
///
/// Logs a warning on failure but does not fail startup — the provider may
/// become available before the first real request arrives.
pub(crate) async fn probe_embedding(
    provider: &dyn EmbeddingProvider,
    registry: &ProviderRegistry,
    key: &ProviderKey,
) {
    let semaphore = registry.semaphore(key).expect(EXPECT_SEMAPHORE);
    let _permit = match semaphore.acquire().await {
        Ok(permit) => permit,
        Err(_) => {
            tracing::warn!("embedding probe skipped: semaphore closed");
            return;
        }
    };

    let request = tribal_inference::EmbeddingRequest {
        texts: vec!["probe".into()],
    };

    match provider.embed(request).await {
        Ok(_) => tracing::info!("embedding model probe succeeded"),
        Err(e) => tracing::warn!(%e, "embedding model probe failed (non-fatal)"),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolves the base URL for a provider, falling back to the provider's
/// default when no explicit URL is configured.
fn resolve_base_url(provider: ProviderKind, config_url: &Option<String>) -> String {
    config_url
        .as_deref()
        .unwrap_or(provider.default_base_url())
        .to_owned()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_config::{DEFAULT_ANTHROPIC_BASE_URL, DEFAULT_OLLAMA_BASE_URL, DEFAULT_OPENAI_BASE_URL};

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
