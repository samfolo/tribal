//! Provider registry construction, provider instantiation, and startup probes.

use std::{sync::Arc, time::Duration};

use tribal_config::{
    ConfigError, EmbeddingConfig, ProviderKind, StageInferenceConfig, TribalConfig,
};
use tribal_inference::{
    AnthropicInferenceProvider, EmbeddingProvider, InferenceProvider, OllamaEmbeddingProvider,
    OllamaInferenceProvider, OpenAiEmbeddingProvider, OpenAiInferenceProvider, ProviderKey,
    ProviderLimits, ProviderRegistry, RequestClass,
};

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Error messages
// ---------------------------------------------------------------------------

const MISSING_LIMITS: &str = "no limits configured for provider";
const MISSING_CLIENT: &str = "no HTTP client in registry for provider key";
const ANTHROPIC_EMBEDDING_UNSUPPORTED: &str =
    "Anthropic does not provide an embedding API; use Ollama or OpenAI for embeddings";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Builds the [`ProviderRegistry`] from the application configuration.
///
/// Creates one `(ProviderKey, ProviderLimits)` entry for each distinct
/// (provider kind, base URL, request class) combination across the
/// embedding and inference configurations.
pub(crate) fn build_provider_registry(config: &TribalConfig) -> Result<ProviderRegistry, AppError> {
    let mut entries: Vec<(ProviderKey, ProviderLimits)> = Vec::new();

    // Embedding provider entry.
    add_entry(
        &mut entries,
        config.embedding.provider,
        config.embedding.base_url.as_ref(),
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
            stage.provider,
            stage.base_url.as_ref(),
            RequestClass::Inference,
            config,
        )?;
    }

    ProviderRegistry::new(entries).map_err(|source| AppError::ProviderRegistry { source })
}

/// Constructs the embedding provider from configuration.
///
/// Returns the boxed provider and the registry key for semaphore lookups.
/// Calls `probe_model` on the concrete provider before boxing — logs a
/// warning on failure but does not fail startup.
pub(crate) async fn build_embedding_provider(
    registry: &ProviderRegistry,
    config: &EmbeddingConfig,
) -> Result<(Arc<dyn EmbeddingProvider>, ProviderKey), AppError> {
    let url = resolve_base_url(config.provider, config.base_url.as_ref());
    let key = ProviderKey::new(config.provider.to_string(), &url, RequestClass::Embedding)
        .map_err(|source| AppError::ProviderRegistry { source })?;

    let client = get_client(registry, &key)?.clone();

    let provider: Arc<dyn EmbeddingProvider> = match config.provider {
        ProviderKind::Ollama => {
            let p = OllamaEmbeddingProvider::new(client, &url, &config.model, config.dimensions);
            if let Err(e) = p.probe_model().await {
                tracing::warn!(%e, "embedding model probe failed (non-fatal)");
            }
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
            if let Err(e) = p.probe_model().await {
                tracing::warn!(%e, "embedding model probe failed (non-fatal)");
            }
            Arc::new(p)
        }
        ProviderKind::Anthropic => {
            return Err(AppError::Config {
                source: ConfigError::ValidationFailed {
                    errors: vec![ANTHROPIC_EMBEDDING_UNSUPPORTED.into()],
                },
            });
        }
    };

    Ok((provider, key))
}

/// Constructs an inference provider for a single pipeline stage.
///
/// Returns the boxed provider and the registry key for semaphore lookups.
/// Calls `probe_model` on the concrete provider before boxing — logs a
/// warning on failure but does not fail startup.
pub(crate) async fn build_inference_provider(
    registry: &ProviderRegistry,
    config: &StageInferenceConfig,
) -> Result<(Arc<dyn InferenceProvider>, ProviderKey), AppError> {
    let url = resolve_base_url(config.provider, config.base_url.as_ref());
    let key = ProviderKey::new(config.provider.to_string(), &url, RequestClass::Inference)
        .map_err(|source| AppError::ProviderRegistry { source })?;

    let client = get_client(registry, &key)?.clone();

    let provider: Arc<dyn InferenceProvider> = match config.provider {
        ProviderKind::Ollama => {
            let p = OllamaInferenceProvider::new(client, &url, &config.model);
            if let Err(e) = p.probe_model().await {
                tracing::warn!(%e, "inference model probe failed (non-fatal)");
            }
            Arc::new(p)
        }
        ProviderKind::OpenAi => {
            let p = OpenAiInferenceProvider::new(
                client,
                &url,
                &config.model,
                config.api_key.as_deref().unwrap_or_default(),
            );
            if let Err(e) = p.probe_model().await {
                tracing::warn!(%e, "inference model probe failed (non-fatal)");
            }
            Arc::new(p)
        }
        ProviderKind::Anthropic => {
            let p = AnthropicInferenceProvider::new(
                client,
                &url,
                &config.model,
                config.api_key.as_deref().unwrap_or_default(),
            );
            if let Err(e) = p.probe_model().await {
                tracing::warn!(%e, "inference model probe failed (non-fatal)");
            }
            Arc::new(p)
        }
    };

    Ok((provider, key))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Adds a registry entry, skipping duplicates based on the normalised
/// [`ProviderKey`].
fn add_entry(
    entries: &mut Vec<(ProviderKey, ProviderLimits)>,
    provider: ProviderKind,
    base_url: Option<&String>,
    request_class: RequestClass,
    config: &TribalConfig,
) -> Result<(), AppError> {
    let url = resolve_base_url(provider, base_url);
    let key = ProviderKey::new(provider.to_string(), &url, request_class)
        .map_err(|source| AppError::ProviderRegistry { source })?;

    let already_present = entries.iter().any(|(k, _)| k == &key);
    if !already_present {
        let limits_config =
            config
                .limits
                .providers
                .get(&provider)
                .ok_or_else(|| AppError::ProviderSetup {
                    context: format!("{MISSING_LIMITS}: {provider}"),
                })?;

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

/// Retrieves the HTTP client for a provider key from the registry.
fn get_client<'a>(
    registry: &'a ProviderRegistry,
    key: &ProviderKey,
) -> Result<&'a reqwest::Client, AppError> {
    registry.client(key).ok_or_else(|| AppError::ProviderSetup {
        context: format!(
            "{MISSING_CLIENT}: {} ({}, {})",
            key.provider_kind(),
            key.normalised_base_url(),
            key.request_class(),
        ),
    })
}

/// Resolves the base URL for a provider, falling back to the provider's
/// default when no explicit URL is configured.
fn resolve_base_url(provider: ProviderKind, config_url: Option<&String>) -> String {
    config_url
        .map_or(provider.default_base_url(), String::as_str)
        .to_owned()
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
            resolve_base_url(ProviderKind::Ollama, None),
            DEFAULT_OLLAMA_BASE_URL,
        );
        assert_eq!(
            resolve_base_url(ProviderKind::Anthropic, None),
            DEFAULT_ANTHROPIC_BASE_URL,
        );
        assert_eq!(
            resolve_base_url(ProviderKind::OpenAi, None),
            DEFAULT_OPENAI_BASE_URL,
        );
    }

    #[test]
    fn test_resolve_base_url_uses_explicit_when_some() {
        let custom = "https://custom.example.com".to_owned();
        assert_eq!(
            resolve_base_url(ProviderKind::Ollama, Some(&custom)),
            "https://custom.example.com",
        );
    }
}
