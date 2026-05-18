//! Provider registry construction, provider instantiation, and startup probes.

use std::{sync::Arc, time::Duration};

use tribal_config::{
    ConfigError, EmbeddingConfig, ProviderKind, StageInferenceConfig, TribalConfig,
};
use tribal_inference::{
    AnthropicInferenceProvider, EmbeddingProvider, InferenceError, InferenceProvider,
    OllamaEmbeddingProvider, OllamaInferenceProvider, OpenAiEmbeddingProvider,
    OpenAiInferenceProvider, ProviderKey, ProviderLimits, ProviderRegistry, RequestClass,
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
/// Calls [`probe_embedding_provider`] before construction — logs a
/// warning on failure but does not fail startup.
pub(crate) async fn build_embedding_provider(
    registry: &ProviderRegistry,
    config: &EmbeddingConfig,
) -> Result<(Arc<dyn EmbeddingProvider>, ProviderKey), AppError> {
    let url = resolve_base_url(config.provider, config.base_url.as_ref());
    let key = ProviderKey::new(config.provider.to_string(), &url, RequestClass::Embedding)
        .map_err(|source| AppError::ProviderRegistry { source })?;

    let client = get_client(registry, &key)?.clone();

    if let Err(e) = probe_embedding_provider(client.clone(), config).await {
        tracing::warn!(%e, "embedding model probe failed (non-fatal)");
    }

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
            api_key_str(config.api_key.as_ref()),
            config.dimensions,
        )),
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
/// Calls [`probe_inference_provider`] before construction — logs a
/// warning on failure but does not fail startup.
pub(crate) async fn build_inference_provider(
    registry: &ProviderRegistry,
    config: &StageInferenceConfig,
) -> Result<(Arc<dyn InferenceProvider>, ProviderKey), AppError> {
    let url = resolve_base_url(config.provider, config.base_url.as_ref());
    let key = ProviderKey::new(config.provider.to_string(), &url, RequestClass::Inference)
        .map_err(|source| AppError::ProviderRegistry { source })?;

    let client = get_client(registry, &key)?.clone();

    if let Err(e) = probe_inference_provider(client.clone(), config).await {
        tracing::warn!(%e, "inference model probe failed (non-fatal)");
    }

    let provider: Arc<dyn InferenceProvider> = match config.provider {
        ProviderKind::Ollama => Arc::new(OllamaInferenceProvider::new(client, &url, &config.model)),
        ProviderKind::OpenAi => Arc::new(OpenAiInferenceProvider::new(
            client,
            &url,
            &config.model,
            api_key_str(config.api_key.as_ref()),
        )),
        ProviderKind::Anthropic => Arc::new(AnthropicInferenceProvider::new(
            client,
            &url,
            &config.model,
            api_key_str(config.api_key.as_ref()),
        )),
    };

    Ok((provider, key))
}

/// Probes the configured embedding model by constructing the matching
/// concrete provider and calling its `probe_model` method.
pub(crate) async fn probe_embedding_provider(
    client: reqwest::Client,
    config: &EmbeddingConfig,
) -> Result<(), InferenceError> {
    let url = resolve_base_url(config.provider, config.base_url.as_ref());

    match config.provider {
        ProviderKind::Ollama => {
            OllamaEmbeddingProvider::new(client, &url, &config.model, config.dimensions)
                .probe_model()
                .await
        }
        ProviderKind::OpenAi => {
            OpenAiEmbeddingProvider::new(
                client,
                &url,
                &config.model,
                api_key_str(config.api_key.as_ref()),
                config.dimensions,
            )
            .probe_model()
            .await
        }
        ProviderKind::Anthropic => Err(InferenceError::ProviderUnavailable {
            provider: ProviderKind::Anthropic.to_string(),
            reason: ANTHROPIC_EMBEDDING_UNSUPPORTED.to_owned(),
        }),
    }
}

/// Probes the configured inference model for a pipeline stage by
/// constructing the matching concrete provider and calling its
/// `probe_model` method.
pub(crate) async fn probe_inference_provider(
    client: reqwest::Client,
    config: &StageInferenceConfig,
) -> Result<(), InferenceError> {
    let url = resolve_base_url(config.provider, config.base_url.as_ref());

    match config.provider {
        ProviderKind::Ollama => {
            OllamaInferenceProvider::new(client, &url, &config.model)
                .probe_model()
                .await
        }
        ProviderKind::OpenAi => {
            OpenAiInferenceProvider::new(
                client,
                &url,
                &config.model,
                api_key_str(config.api_key.as_ref()),
            )
            .probe_model()
            .await
        }
        ProviderKind::Anthropic => {
            AnthropicInferenceProvider::new(
                client,
                &url,
                &config.model,
                api_key_str(config.api_key.as_ref()),
            )
            .probe_model()
            .await
        }
    }
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

/// Renders an optional API key as the `&str` the concrete provider
/// constructors expect — empty when absent, matching the prior
/// `.unwrap_or_default()` chain at each call site.
fn api_key_str(key: Option<&tribal_domain::ApiKey>) -> &str {
    key.map(tribal_domain::ApiKey::as_str).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_config::{
        DEFAULT_ANTHROPIC_BASE_URL, DEFAULT_OLLAMA_BASE_URL, DEFAULT_OPENAI_BASE_URL,
        InferenceConfig, StageInferenceConfig,
    };
    use tribal_inference::{OLLAMA_EMBED_PATH, OLLAMA_TAGS_PATH, OPENAI_CHAT_PATH};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    // -- resolve_base_url -----------------------------------------------------

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

    // -- probe_embedding_provider --------------------------------------------

    fn ollama_embedding_config(base_url: String) -> EmbeddingConfig {
        EmbeddingConfig {
            provider: ProviderKind::Ollama,
            model: "nomic-embed-text:v1.5".into(),
            dimensions: 3,
            base_url: Some(base_url),
            api_key: None,
        }
    }

    #[tokio::test]
    async fn test_probe_embedding_provider_short_circuits_for_anthropic() {
        // No mock server: if the Anthropic arm attempted a network
        // call this would surface as a connection error, not the
        // synchronous ProviderUnavailable the arm returns.
        let mut config = ollama_embedding_config("http://127.0.0.1:0".into());
        config.provider = ProviderKind::Anthropic;

        let err = probe_embedding_provider(reqwest::Client::new(), &config)
            .await
            .expect_err("Anthropic embedding must reject without a network call");
        assert!(
            matches!(
                err,
                InferenceError::ProviderUnavailable { ref provider, .. } if provider == "anthropic"
            ),
            "expected ProviderUnavailable with provider=anthropic, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_probe_embedding_provider_dispatches_to_ollama_and_surfaces_5xx() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(OLLAMA_TAGS_PATH))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(OLLAMA_EMBED_PATH))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let config = ollama_embedding_config(server.uri());
        let err = probe_embedding_provider(reqwest::Client::new(), &config)
            .await
            .expect_err("probe should fail when endpoint returns 5xx");
        assert!(
            matches!(err, InferenceError::ProviderUnavailable { .. }),
            "expected ProviderUnavailable, got {err:?}"
        );
    }

    // -- probe_inference_provider --------------------------------------------

    fn openai_inference_stage(base_url: String) -> StageInferenceConfig {
        // Start from the production default so the remaining fields
        // track the real configuration instead of being pinned to
        // arbitrary test values.
        let mut stage = InferenceConfig::default().triage;
        stage.provider = ProviderKind::OpenAi;
        stage.model = "gpt-4o-mini".into();
        stage.base_url = Some(base_url);
        stage.api_key = Some("sk-test".parse().expect("test fixture is a valid api key"));
        stage
    }

    #[tokio::test]
    async fn test_probe_inference_provider_dispatches_to_openai_and_surfaces_5xx() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(OPENAI_CHAT_PATH))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let stage = openai_inference_stage(server.uri());
        let err = probe_inference_provider(reqwest::Client::new(), &stage)
            .await
            .expect_err("probe should fail when endpoint returns 5xx");
        assert!(
            matches!(err, InferenceError::ProviderUnavailable { .. }),
            "expected ProviderUnavailable, got {err:?}"
        );
    }
}
