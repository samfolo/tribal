//! Provider registry construction, provider instantiation, and startup probes.

use std::time::Duration;

use tribal_config::{CredentialCatalogue, StageInferenceConfig, TribalConfig};
use tribal_domain::{EmbeddingProfile, ProviderKind, TaskType};
use tribal_inference::{
    CompletionStageSpec, CompletionStageSpecs, CredentialError, EmbeddingCredentialResolver,
    EmbeddingTarget, InferenceGateway, ProviderKey, ProviderLimits, ProviderRegistry, RequestClass,
    UsageAttribution, effective_stage_parameters,
};

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Error messages
// ---------------------------------------------------------------------------

const MISSING_LIMITS: &str = "no limits configured for provider";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Builds the [`ProviderRegistry`] from the active profile and the inference
/// configuration.
///
/// Creates one `(ProviderKey, ProviderLimits)` entry for each distinct
/// (provider kind, base URL, request class) combination: the embedding entry
/// comes from the **active profile** (the live embedding identity), the
/// inference entries from per-stage config.
pub(crate) fn build_provider_registry(
    config: &TribalConfig,
    active_profile: &EmbeddingProfile,
) -> Result<ProviderRegistry, AppError> {
    let mut entries: Vec<(ProviderKey, ProviderLimits)> = Vec::new();

    // Embedding provider entry, from the active profile's endpoint.
    let embedding_base_url = active_profile.normalised_base_url().to_owned();
    add_entry(
        &mut entries,
        active_profile.provider_kind(),
        Some(&embedding_base_url),
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

/// Builds a [`ProviderRegistry`] from the inference configuration alone,
/// for commands that run without reading an active profile. Embedding
/// endpoints register dynamically when the gateway first resolves them.
pub(crate) fn build_command_registry(config: &TribalConfig) -> Result<ProviderRegistry, AppError> {
    let mut entries: Vec<(ProviderKey, ProviderLimits)> = Vec::new();
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

/// Translates the per-stage inference configuration into the gateway's
/// completion specifications, resolving each stage's base URL, key, and
/// effective (post-reconcile) sampling parameters.
pub(crate) fn completion_stage_specs(config: &TribalConfig) -> CompletionStageSpecs {
    CompletionStageSpecs {
        extraction: completion_stage_spec("extraction", &config.inference.extraction),
        triage: completion_stage_spec("triage", &config.inference.triage),
        relation: completion_stage_spec("relation", &config.inference.relation),
    }
}

fn completion_stage_spec(stage: &str, config: &StageInferenceConfig) -> CompletionStageSpec {
    CompletionStageSpec {
        provider: config.provider,
        model: config.model.clone(),
        // The spec keeps the configured spelling so the binding hash is
        // unchanged by normalisation; the resume-route guard canonicalises
        // both sides when it compares, and the gateway normalises when it
        // routes.
        base_url: resolve_base_url(config.provider, config.base_url.as_ref()),
        api_key: api_key_str(config.api_key.as_ref()).to_owned(),
        parameters: effective_stage_parameters(
            stage,
            config.provider,
            &config.model,
            config.temperature,
            config.max_tokens,
        ),
    }
}

/// The gateway's embedding credential resolver, backed by the config
/// catalogue: fail-closed for key-requiring providers, empty for
/// providers that need none.
pub(crate) struct CatalogueCredentialResolver {
    catalogue: CredentialCatalogue,
}

impl CatalogueCredentialResolver {
    pub(crate) fn new(catalogue: CredentialCatalogue) -> Self {
        Self { catalogue }
    }
}

impl EmbeddingCredentialResolver for CatalogueCredentialResolver {
    fn resolve(
        &self,
        provider: ProviderKind,
        normalised_base_url: &str,
    ) -> Result<String, CredentialError> {
        self.catalogue
            .resolve_api_key(provider, normalised_base_url)
            .map(ToOwned::to_owned)
            .map_err(|e| CredentialError {
                provider: e.provider,
                base_url: e.base_url.clone(),
                message: e.to_string(),
            })
    }
}

/// Validates the active embedding identity fail-closed at boot: a
/// key-requiring provider with no catalogue credential, or a provider
/// kind with no embedding API, aborts startup with the endpoint named
/// rather than booting into a server whose every ingest and discover
/// fails.
///
/// The credential pre-check is skipped for a kind with no embedding API,
/// mirroring the gateway's structural-before-credential ordering: such an
/// identity fails on the missing API, not on a credential no key could
/// remedy.
///
/// # Errors
///
/// Returns [`AppError::EmbeddingCredentialUnresolved`] for a missing
/// credential and [`AppError::ProviderSetup`] for an identity the gateway
/// cannot serve.
pub(crate) fn validate_embedding_identity(
    gateway: &InferenceGateway,
    config: &TribalConfig,
    active_profile: &EmbeddingProfile,
) -> Result<(), AppError> {
    let provider = active_profile.provider_kind();
    if provider.supports_embedding() {
        config
            .credentials
            .resolve_api_key(provider, active_profile.normalised_base_url())
            .map_err(|e| AppError::EmbeddingCredentialUnresolved {
                provider: e.provider,
                base_url: e.base_url.clone(),
                kind: e.kind,
                provider_upper: provider.as_str().to_uppercase(),
            })?;
    }
    gateway
        .prepare_embedding_target(&EmbeddingTarget::from(active_profile))
        .map_err(|e| AppError::ProviderSetup {
            context: e.to_string(),
        })
}

/// Probes every configured provider through the gateway at startup: the
/// three stage completions and the active embedding profile. Probe
/// failures are logged but never fail boot — a provider may be down at
/// start and healthy by the first task.
pub(crate) async fn probe_startup_providers(
    gateway: &InferenceGateway,
    active_profile: &EmbeddingProfile,
) {
    for stage in [TaskType::Extraction, TaskType::Triage, TaskType::Relation] {
        if let Err(e) = gateway
            .probe_completion(stage, &UsageAttribution::default())
            .await
        {
            tracing::warn!(%stage, %e, "inference model probe failed (non-fatal)");
        }
    }

    if let Err(e) = gateway
        .probe_embedding(
            &EmbeddingTarget::from(active_profile),
            &UsageAttribution::default(),
        )
        .await
    {
        tracing::warn!(%e, "embedding model probe failed (non-fatal)");
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
    // Platform is served by the managed gateway, not a local endpoint; reject it
    // here with an honest reason rather than let its absent base URL fail opaquely below.
    if provider == ProviderKind::Platform {
        return Err(AppError::ProviderSetup {
            context: "the platform provider is served by the managed gateway, \
                      not a local provider"
                .to_owned(),
        });
    }

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

/// Resolves a provider's base URL: the configured URL when set, else the
/// provider's compile-time default, else an empty string for a provider with no
/// default (the platform, whose gateway endpoint is deployment-bound, not
/// defaulted). An empty result is not a usable endpoint — it is rejected where
/// the provider URL is parsed, never silently accepted.
pub(super) fn resolve_base_url(provider: ProviderKind, config_url: Option<&String>) -> String {
    config_url
        .map(String::as_str)
        .or_else(|| provider.default_base_url())
        .unwrap_or_default()
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
    use std::sync::Arc;

    use tribal_config::MissingApiKeyKind;
    use tribal_inference::NoopLedgerSink;
    use tribal_test_utils::an_embedding_profile;

    use super::*;

    // -- resolve_base_url -----------------------------------------------------

    #[test]
    fn test_resolve_base_url_uses_default_when_none() {
        assert_eq!(
            resolve_base_url(ProviderKind::Ollama, None),
            ProviderKind::DEFAULT_OLLAMA_BASE_URL,
        );
        assert_eq!(
            resolve_base_url(ProviderKind::Anthropic, None),
            ProviderKind::DEFAULT_ANTHROPIC_BASE_URL,
        );
        assert_eq!(
            resolve_base_url(ProviderKind::OpenAi, None),
            ProviderKind::DEFAULT_OPENAI_BASE_URL,
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

    #[test]
    fn test_catalogue_resolver_fails_closed_for_keyless_cloud_provider() {
        // OpenAI requires a key; an empty catalogue resolves to the
        // fail-closed error naming the connection.
        let resolver = CatalogueCredentialResolver::new(CredentialCatalogue::default());
        let err = resolver
            .resolve(ProviderKind::OpenAi, "https://api.openai.com:443/v1")
            .expect_err("a keyless cloud provider must fail closed");
        assert_eq!(err.provider, ProviderKind::OpenAi);
        assert_eq!(err.base_url, "https://api.openai.com:443/v1");
    }

    #[test]
    fn test_catalogue_resolver_allows_keyless_ollama() {
        let resolver = CatalogueCredentialResolver::new(CredentialCatalogue::default());
        let key = resolver
            .resolve(ProviderKind::Ollama, "http://localhost:11434")
            .expect("ollama needs no key");
        assert_eq!(key, "");
    }

    #[test]
    fn test_registry_rejects_platform_inference_provider() {
        let mut config = TribalConfig::default();
        config.inference.extraction.provider = ProviderKind::Platform;
        let err = build_command_registry(&config).expect_err("platform is not a local provider");
        assert!(
            matches!(err, AppError::ProviderSetup { .. }),
            "expected the honest ProviderSetup rejection, got {err:?}",
        );
    }

    // -- validate_embedding_identity -------------------------------------------

    fn a_gateway(config: &TribalConfig) -> InferenceGateway {
        let registry = build_command_registry(config).expect("a registry from default config");
        InferenceGateway::new(
            registry,
            &completion_stage_specs(config),
            Arc::new(CatalogueCredentialResolver::new(config.credentials.clone())),
            Arc::new(NoopLedgerSink),
        )
        .expect("a gateway from registered endpoints")
    }

    #[test]
    fn test_validate_embedding_identity_allows_keyless_ollama() {
        let config = TribalConfig::default();
        let gateway = a_gateway(&config);
        let active = an_embedding_profile()
            .provider_kind(ProviderKind::Ollama)
            .normalised_base_url("http://localhost:11434".to_owned())
            .build();

        validate_embedding_identity(&gateway, &config, &active)
            .expect("a keyless ollama identity boots");
    }

    #[test]
    fn test_validate_embedding_identity_fails_closed_without_a_credential() {
        let config = TribalConfig::default();
        let gateway = a_gateway(&config);
        let active = an_embedding_profile()
            .provider_kind(ProviderKind::OpenAi)
            .normalised_base_url("https://api.openai.com:443/v1".to_owned())
            .build();

        let err = validate_embedding_identity(&gateway, &config, &active)
            .expect_err("a keyless cloud identity must abort boot");
        assert!(matches!(
            err,
            AppError::EmbeddingCredentialUnresolved {
                provider: ProviderKind::OpenAi,
                kind: MissingApiKeyKind::NoMatchingEntry,
                ..
            }
        ));
    }
}
