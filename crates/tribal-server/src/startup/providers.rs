//! Provider registry construction, provider instantiation, and startup probes.

use std::time::Duration;

use tribal_config::{
    ProviderConnectionResolutionError, ProviderConnections, StageInferenceConfig, TribalConfig,
};
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
    for (_, stage) in config.inference.stages() {
        let connection = config
            .provider_connections
            .require(&stage.connection)
            .map_err(|source| connection_error(&source))?;
        add_entry(
            &mut entries,
            connection.provider(),
            connection.base_url(),
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
    for (_, stage) in config.inference.stages() {
        let connection = config
            .provider_connections
            .require(&stage.connection)
            .map_err(|source| connection_error(&source))?;
        add_entry(
            &mut entries,
            connection.provider(),
            connection.base_url(),
            RequestClass::Inference,
            config,
        )?;
    }
    ProviderRegistry::new(entries).map_err(|source| AppError::ProviderRegistry { source })
}

/// Translates the per-stage inference configuration into the gateway's
/// completion specifications, resolving each stage's base URL, key, and
/// effective (post-reconcile) sampling parameters.
pub(crate) fn completion_stage_specs(
    config: &TribalConfig,
) -> Result<CompletionStageSpecs, AppError> {
    Ok(CompletionStageSpecs {
        extraction: completion_stage_spec("extraction", &config.inference.extraction, config)?,
        triage: completion_stage_spec("triage", &config.inference.triage, config)?,
        relation: completion_stage_spec("relation", &config.inference.relation, config)?,
    })
}

fn completion_stage_spec(
    stage: &str,
    stage_config: &StageInferenceConfig,
    config: &TribalConfig,
) -> Result<CompletionStageSpec, AppError> {
    let connection = config
        .provider_connections
        .require(&stage_config.connection)
        .map_err(|source| connection_error(&source))?;
    let provider = connection.provider();
    let api_key = config
        .provider_connections
        .resolve_api_key(&stage_config.connection)
        .map_err(|source| connection_error(&source))?;
    Ok(CompletionStageSpec {
        provider,
        model: stage_config.model.clone(),
        // The spec keeps the configured spelling so the binding hash is
        // unchanged by normalisation; the resume-route guard canonicalises
        // both sides when it compares, and the gateway normalises when it
        // routes.
        base_url: resolve_base_url(provider, connection.base_url()),
        api_key: api_key.to_owned(),
        parameters: effective_stage_parameters(
            stage,
            provider,
            &stage_config.model,
            stage_config.temperature,
            stage_config.max_tokens,
        ),
    })
}

/// The gateway's embedding credential resolver, backed by the config
/// catalogue: fail-closed for key-requiring providers, empty for
/// providers that need none.
pub(crate) struct ProviderConnectionCredentialResolver {
    connections: ProviderConnections,
}

impl ProviderConnectionCredentialResolver {
    pub(crate) fn new(connections: ProviderConnections) -> Self {
        Self { connections }
    }
}

impl EmbeddingCredentialResolver for ProviderConnectionCredentialResolver {
    fn resolve(
        &self,
        provider: ProviderKind,
        normalised_base_url: &str,
    ) -> Result<String, CredentialError> {
        self.connections
            .resolve_endpoint_api_key(provider, normalised_base_url)
            .map(ToOwned::to_owned)
            .map_err(|e| CredentialError {
                provider,
                base_url: normalised_base_url.to_owned(),
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
            .provider_connections
            .resolve_endpoint_api_key(provider, active_profile.normalised_base_url())
            .map_err(|source| AppError::EmbeddingCredentialUnresolved {
                provider,
                base_url: active_profile.normalised_base_url().to_owned(),
                source,
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
    base_url: Option<&str>,
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

/// Resolves a provider's base URL: the configured URL when set, else the
/// provider's compile-time default, else an empty string for a provider with no
/// default (the platform, whose gateway endpoint is deployment-bound, not
/// defaulted). An empty result is not a usable endpoint — it is rejected where
/// the provider URL is parsed, never silently accepted.
pub(super) fn resolve_base_url(provider: ProviderKind, config_url: Option<&str>) -> String {
    config_url
        .or_else(|| provider.default_base_url())
        .unwrap_or_default()
        .to_owned()
}

fn connection_error(source: &ProviderConnectionResolutionError) -> AppError {
    AppError::ConfigInvariant {
        reason: source.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_config::{ProviderConnectionConfig, ProviderConnectionResolutionError};
    use tribal_domain::ProviderConnectionName;
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
    fn test_connection_resolver_fails_closed_for_unowned_cloud_endpoint() {
        let resolver = ProviderConnectionCredentialResolver::new(ProviderConnections::default());
        let err = resolver
            .resolve(ProviderKind::OpenAi, "https://api.openai.com:443/v1")
            .expect_err("a keyless cloud provider must fail closed");
        assert_eq!(err.provider, ProviderKind::OpenAi);
        assert_eq!(err.base_url, "https://api.openai.com:443/v1");
        assert!(err.message.contains("no provider connection owns"));
    }

    #[test]
    fn test_connection_resolver_allows_keyless_ollama() {
        let resolver = ProviderConnectionCredentialResolver::new(ProviderConnections::default());
        let key = resolver
            .resolve(ProviderKind::Ollama, "http://localhost:11434")
            .expect("ollama needs no key");
        assert_eq!(key, "");
    }

    #[test]
    fn test_registry_refuses_platform_until_managed_transport_exists() {
        let mut config = TribalConfig::default();
        let name = ProviderConnectionName::parse("managed").expect("fixture name is valid");
        config
            .provider_connections
            .insert(name.clone(), ProviderConnectionConfig::Platform {});
        config.inference.extraction.connection = name;

        let error = build_command_registry(&config)
            .expect_err("platform has no local endpoint before managed transport exists");
        assert!(
            error.to_string().contains("provider registry"),
            "unexpected error: {error}",
        );
    }

    // -- validate_embedding_identity -------------------------------------------

    fn a_gateway(config: &TribalConfig) -> InferenceGateway {
        let registry = build_command_registry(config).expect("a registry from default config");
        let specs = completion_stage_specs(config).expect("completion specs from default config");
        InferenceGateway::new(
            registry,
            &specs,
            Arc::new(ProviderConnectionCredentialResolver::new(
                config.provider_connections.clone(),
            )),
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
                source: ProviderConnectionResolutionError::MissingEndpoint { .. },
                ..
            }
        ));
    }
}
