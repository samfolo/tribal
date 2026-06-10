//! The inference façade: the one internal port every completion and
//! embedding request routes through.
//!
//! The façade owns the policy that call sites must not carry themselves:
//! credential resolution for both the per-stage completion endpoints and
//! the embedding endpoints, provider construction and caching, concurrency
//! permits, span emission, and accounting through the [`LedgerSink`].
//! Call sites supply a request, a permit-wait policy, and their
//! [`UsageAttribution`]; everything else happens behind this boundary.
//!
//! Completion calls offer two entry points with one contract: `complete`
//! awaits the terminal result over the buffered wire, and
//! `complete_stream` returns the live event stream over the native
//! streaming wire. The two paths build identical request bodies apart from
//! the stream-specific fields and fold to identical results.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use tokio::sync::OwnedSemaphorePermit;
use tracing::Instrument;
use tribal_domain::{
    CompletionResponse, EmbeddingProfile, EmbeddingProfileId, EmbeddingPurpose, InferenceEvent,
    ProviderKind, TaskType, TokenUsageStage, Usage, gen_ai,
};

use crate::{
    BatchEmbeddingResult, CompletionRequest, EmbeddingProvider, EmbeddingRequest, InferenceError,
    InferenceProvider, Message, ProviderKey, ProviderLimits, ProviderRegistry,
    ProviderRegistryError, RequestClass, Role,
    anthropic::AnthropicInferenceProvider,
    embedding_factory::make_embedding_provider,
    http::{EMBEDDING_PROBE_INPUT, INFERENCE_PROBE_INPUT, PROBE_MAX_TOKENS, latency_ms},
    ledger::{LedgerSink, UsageAttribution},
    ollama::OllamaInferenceProvider,
    openai::OpenAiInferenceProvider,
    response::EmbeddingResponse,
    stream::InferenceEventStream,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Limits applied to an embedding endpoint registered for the first time
/// after boot (a reindex target on a brand-new endpoint). An endpoint the
/// boot-time providers already cover reuses their client and semaphore.
const DEFAULT_DYNAMIC_EMBEDDING_LIMITS: ProviderLimits = ProviderLimits {
    max_in_flight: 4,
    request_timeout: Duration::from_mins(2),
};

/// Provider semaphores are never closed: the registry holds them for the
/// process lifetime.
const SEMAPHORE_NEVER_CLOSED: &str = "provider semaphore is never closed";

// ---------------------------------------------------------------------------
// Permit policy
// ---------------------------------------------------------------------------

/// How long a call may wait for its provider's concurrency permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermitWait {
    /// Wait at most this long; exhausting it fails the call with
    /// [`InferenceError::PermitTimeout`] before anything is sent.
    Bounded {
        /// The maximum wait.
        limit: Duration,
    },
    /// Wait until a permit frees.
    Unbounded,
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

/// A required embedding credential could not be resolved.
///
/// `message` is the resolver's own description; it reaches operators
/// verbatim, so implementations phrase it to name the missing connection
/// and how to supply it.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct CredentialError {
    /// The provider whose credential was required.
    pub provider: ProviderKind,
    /// The normalised endpoint the credential was required for.
    pub base_url: String,
    /// The resolver's description of the failure.
    pub message: String,
}

/// Resolves the API key for an embedding endpoint.
///
/// Implementations are fail-closed for key-requiring providers and resolve
/// an empty key for providers that need none (Ollama).
pub trait EmbeddingCredentialResolver: Send + Sync {
    /// Resolves the key for `(provider, normalised_base_url)`.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialError`] when a key-requiring provider has no
    /// usable credential for the endpoint.
    fn resolve(
        &self,
        provider: ProviderKind,
        normalised_base_url: &str,
    ) -> Result<String, CredentialError>;
}

// ---------------------------------------------------------------------------
// Targets and specifications
// ---------------------------------------------------------------------------

/// The resolved identity of an embedding endpoint and model.
///
/// Usually derived from an [`EmbeddingProfile`]; constructed from fields
/// for identities that have no profile row yet, such as a genesis seed
/// being probed before first provisioning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingTarget {
    /// The provider kind.
    pub provider: ProviderKind,
    /// The model identifier.
    pub model: String,
    /// The expected output dimensionality.
    pub dimensions: u32,
    /// The endpoint base URL.
    pub base_url: String,
    /// The profile this target derives from; targets with one cache their
    /// built provider by it.
    pub profile_id: Option<EmbeddingProfileId>,
}

impl From<&EmbeddingProfile> for EmbeddingTarget {
    fn from(profile: &EmbeddingProfile) -> Self {
        Self {
            provider: profile.provider_kind(),
            model: profile.model().to_owned(),
            dimensions: profile.dimensions(),
            base_url: profile.normalised_base_url().to_owned(),
            profile_id: Some(profile.id()),
        }
    }
}

/// The resolved completion endpoint for one pipeline stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionStageSpec {
    /// The provider kind.
    pub provider: ProviderKind,
    /// The model identifier.
    pub model: String,
    /// The endpoint base URL, already defaulted by the caller.
    pub base_url: String,
    /// The resolved API key, empty for providers that need none.
    pub api_key: String,
}

/// The completion endpoints for the three pipeline stages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionStageSpecs {
    /// The extraction stage endpoint.
    pub extraction: CompletionStageSpec,
    /// The triage stage endpoint.
    pub triage: CompletionStageSpec,
    /// The relation stage endpoint.
    pub relation: CompletionStageSpec,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// One input of an [`InferenceFacade::embed_group`] call failed.
///
/// Carries the failing input's position so the caller can attribute the
/// failure precisely (the group's inputs usually have distinct roles).
#[derive(Debug, thiserror::Error)]
#[error("group embed failed at input {index}: {source}")]
pub struct EmbedGroupError {
    /// The position of the failing input within the group.
    pub index: usize,
    /// The underlying failure.
    #[source]
    pub source: InferenceError,
}

/// The façade could not be constructed.
#[derive(Debug, thiserror::Error)]
pub enum FacadeBuildError {
    /// A stage's completion endpoint could not be keyed.
    #[error("keying the {stage} completion endpoint: {source}")]
    CompletionEndpoint {
        /// The stage whose endpoint failed to key.
        stage: TaskType,
        /// The keying failure.
        #[source]
        source: ProviderRegistryError,
    },

    /// The registry holds no HTTP client for a stage's endpoint.
    #[error("no HTTP client in the registry for provider key {key}")]
    MissingClient {
        /// The unresolvable key.
        key: ProviderKey,
    },
}

// ---------------------------------------------------------------------------
// InferenceFacade
// ---------------------------------------------------------------------------

/// One completion endpoint, bound: the built provider and its registry key.
struct CompletionBinding {
    provider: Arc<dyn InferenceProvider>,
    key: ProviderKey,
}

/// A built embedding provider and the registry key its permits live under.
struct ResolvedEmbedding {
    provider: Arc<dyn EmbeddingProvider>,
    key: ProviderKey,
}

/// The internal port for every completion and embedding request.
pub struct InferenceFacade {
    registry: ProviderRegistry,
    extraction: CompletionBinding,
    triage: CompletionBinding,
    relation: CompletionBinding,
    embedding_cache: DashMap<EmbeddingProfileId, Arc<dyn EmbeddingProvider>>,
    credentials: Arc<dyn EmbeddingCredentialResolver>,
    sink: Arc<dyn LedgerSink>,
}

impl InferenceFacade {
    /// Builds the façade: keys each stage endpoint, constructs its
    /// provider from the spec, and takes ownership of the registry.
    ///
    /// # Errors
    ///
    /// Returns [`FacadeBuildError`] when a stage endpoint cannot be keyed
    /// or the registry holds no client for it.
    pub fn new(
        registry: ProviderRegistry,
        stages: &CompletionStageSpecs,
        credentials: Arc<dyn EmbeddingCredentialResolver>,
        sink: Arc<dyn LedgerSink>,
    ) -> Result<Self, FacadeBuildError> {
        let extraction = build_binding(&registry, TaskType::Extraction, &stages.extraction)?;
        let triage = build_binding(&registry, TaskType::Triage, &stages.triage)?;
        let relation = build_binding(&registry, TaskType::Relation, &stages.relation)?;
        Ok(Self {
            registry,
            extraction,
            triage,
            relation,
            embedding_cache: DashMap::new(),
            credentials,
            sink,
        })
    }

    /// Returns the provider identity bound to a pipeline stage, for
    /// callers that record provenance (which provider and model produced
    /// an artefact) without making a call.
    #[must_use]
    pub fn completion_identity(&self, stage: TaskType) -> &crate::ProviderIdentity {
        self.binding(stage).provider.identity()
    }

    fn binding(&self, stage: TaskType) -> &CompletionBinding {
        match stage {
            TaskType::Extraction => &self.extraction,
            TaskType::Triage => &self.triage,
            TaskType::Relation => &self.relation,
        }
    }

    // -- Completions ---------------------------------------------------------

    /// Sends a completion for a pipeline stage and awaits the result over
    /// the buffered wire.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::PermitTimeout`] when the permit wait is
    /// exhausted, or the provider's own error mapping for the call itself.
    pub async fn complete(
        &self,
        stage: TaskType,
        request: CompletionRequest,
        wait: PermitWait,
        attribution: &UsageAttribution,
    ) -> Result<CompletionResponse, InferenceError> {
        let binding = self.binding(stage);
        let _permit = self.acquire(&binding.key, wait).await?;
        let response = binding.provider.complete(request).await?;
        self.record_completion(&response, TokenUsageStage::from(stage), attribution)
            .await;
        Ok(response)
    }

    /// Sends a completion for a pipeline stage over the native streaming
    /// wire, returning the event stream.
    ///
    /// The returned stream owns its concurrency permit, released when the
    /// stream is dropped or finishes, so the per-provider bound covers live
    /// connections rather than call initiations. Usage is recorded when the
    /// terminal event passes through.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::PermitTimeout`] when the permit wait is
    /// exhausted, or the provider's error mapping for failures before the
    /// stream opens; failures mid-stream arrive as the stream's final item.
    pub async fn complete_stream(
        &self,
        stage: TaskType,
        request: CompletionRequest,
        wait: PermitWait,
        attribution: &UsageAttribution,
    ) -> Result<InferenceEventStream, InferenceError> {
        let binding = self.binding(stage);
        let permit = self.acquire(&binding.key, wait).await?;
        let stream = binding.provider.complete_stream(request).await?;
        Ok(self.recorded_stream(stream, permit, TokenUsageStage::from(stage), attribution))
    }

    /// Probes a stage's completion endpoint with the canonical probe
    /// request: a real, minimal, billable call verifying reachability and
    /// model access, ledgered like any other call.
    ///
    /// # Errors
    ///
    /// Returns the same error mapping as [`complete`](Self::complete).
    pub async fn probe_completion(
        &self,
        stage: TaskType,
        attribution: &UsageAttribution,
    ) -> Result<(), InferenceError> {
        let binding = self.binding(stage);
        let span = tracing::info_span!(
            "tribal.provider.probe",
            { gen_ai::PROVIDER_NAME } = binding.key.provider_kind(),
            { gen_ai::REQUEST_MODEL } = %binding.provider.identity().model,
        );

        async {
            self.check_ollama_tags(&binding.key, &binding.provider.identity().model)
                .await;

            let request = CompletionRequest {
                system: None,
                messages: vec![Message {
                    role: Role::User,
                    content: INFERENCE_PROBE_INPUT.to_owned(),
                }],
                temperature: Some(0.0),
                max_tokens: Some(PROBE_MAX_TOKENS),
                response_format: None,
            };
            let _permit = self.acquire(&binding.key, PermitWait::Unbounded).await?;
            let response = binding.provider.complete(request).await?;
            self.record_completion(&response, TokenUsageStage::Probe, attribution)
                .await;

            tracing::info!(
                "model {} probe succeeded",
                binding.provider.identity().model
            );
            Ok(())
        }
        .instrument(span)
        .await
    }

    // -- Embeddings ----------------------------------------------------------

    /// Embeds one input against an embedding target.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::PermitTimeout`] when the permit wait is
    /// exhausted, [`InferenceError::ProviderUnavailable`] when the target's
    /// provider cannot be resolved (endpoint, credential, or an embedding
    /// API the provider kind lacks), or the provider's own error mapping.
    pub async fn embed(
        &self,
        target: &EmbeddingTarget,
        request: EmbeddingRequest,
        wait: PermitWait,
        attribution: &UsageAttribution,
    ) -> Result<EmbeddingResponse, InferenceError> {
        let resolved = self.resolve_embedding(target)?;
        let _permit = self.acquire(&resolved.key, wait).await?;
        let purpose = request.purpose;
        let response = resolved.provider.embed(request).await?;
        self.record_embedding(&response.usage, purpose, attribution)
            .await;
        Ok(response)
    }

    /// Embeds a group of inputs under one concurrency permit, sequentially
    /// and fail-fast, with one ledger record per input.
    ///
    /// The permit covers the whole group so a multi-part embed (an item and
    /// its tags) occupies exactly one concurrency slot, matching a single
    /// call's footprint.
    ///
    /// # Errors
    ///
    /// Returns [`EmbedGroupError`] naming the first failing input; its
    /// source follows [`embed`](Self::embed)'s error mapping.
    pub async fn embed_group(
        &self,
        target: &EmbeddingTarget,
        requests: Vec<EmbeddingRequest>,
        wait: PermitWait,
        attribution: &UsageAttribution,
    ) -> Result<Vec<EmbeddingResponse>, EmbedGroupError> {
        let on_setup = |source: InferenceError| EmbedGroupError { index: 0, source };
        let resolved = self.resolve_embedding(target).map_err(on_setup)?;
        let _permit = self.acquire(&resolved.key, wait).await.map_err(on_setup)?;

        let mut responses = Vec::with_capacity(requests.len());
        for (index, request) in requests.into_iter().enumerate() {
            let purpose = request.purpose;
            let response = resolved
                .provider
                .embed(request)
                .await
                .map_err(|source| EmbedGroupError { index, source })?;
            self.record_embedding(&response.usage, purpose, attribution)
                .await;
            responses.push(response);
        }
        Ok(responses)
    }

    /// Embeds a batch of inputs with one shared purpose through the
    /// provider's batch path, holding the permit across the whole batch.
    ///
    /// Poison isolation is the provider's batch contract: one bad input
    /// fails only its own slot. The batch's aggregate usage is ledgered as
    /// one record carrying `purpose`.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::PermitTimeout`] when the permit wait is
    /// exhausted, or [`InferenceError::ProviderUnavailable`] when the
    /// target's provider cannot be resolved. Per-input failures are inside
    /// the returned [`BatchEmbeddingResult`].
    pub async fn embed_many(
        &self,
        target: &EmbeddingTarget,
        inputs: Vec<String>,
        purpose: EmbeddingPurpose,
        wait: PermitWait,
        attribution: &UsageAttribution,
    ) -> Result<BatchEmbeddingResult, InferenceError> {
        let resolved = self.resolve_embedding(target)?;
        let _permit = self.acquire(&resolved.key, wait).await?;

        let requests = inputs
            .into_iter()
            .map(|input| EmbeddingRequest { input, purpose })
            .collect();
        let result = resolved.provider.embed_many(requests).await;
        self.record_embedding(&result.usage, purpose, attribution)
            .await;
        Ok(result)
    }

    /// Resolves and caches an embedding target's provider without making a
    /// wire call: keys the endpoint, registers it if new, and validates the
    /// credential fail-closed.
    ///
    /// For callers that must surface a misconfigured target before starting
    /// work that would otherwise fail mid-flight.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::ProviderUnavailable`] when the target
    /// cannot be keyed, registered, credentialed, or constructed.
    pub fn prepare_embedding_target(&self, target: &EmbeddingTarget) -> Result<(), InferenceError> {
        self.resolve_embedding(target).map(|_| ())
    }

    /// Resolves the provider-native revision token for an embedding target:
    /// best-effort and non-failing, empty when the provider exposes none.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::ProviderUnavailable`] when the target's
    /// provider cannot be resolved.
    pub async fn revision_token(&self, target: &EmbeddingTarget) -> Result<String, InferenceError> {
        let resolved = self.resolve_embedding(target)?;
        Ok(resolved.provider.revision_token().await)
    }

    /// Probes an embedding target with the canonical probe input: a real,
    /// minimal, billable call verifying reachability and output geometry,
    /// ledgered like any other call. The returned response carries the
    /// probe vector for drift fingerprinting.
    ///
    /// # Errors
    ///
    /// As [`embed`](Self::embed).
    pub async fn probe_embedding(
        &self,
        target: &EmbeddingTarget,
        attribution: &UsageAttribution,
    ) -> Result<EmbeddingResponse, InferenceError> {
        let span = tracing::info_span!(
            "tribal.provider.probe",
            { gen_ai::PROVIDER_NAME } = target.provider.as_str(),
            { gen_ai::REQUEST_MODEL } = %target.model,
        );

        async {
            let resolved = self.resolve_embedding(target)?;
            self.check_ollama_tags(&resolved.key, &target.model).await;

            let request = EmbeddingRequest {
                input: EMBEDDING_PROBE_INPUT.to_owned(),
                purpose: EmbeddingPurpose::Probe,
            };
            let _permit = self.acquire(&resolved.key, PermitWait::Unbounded).await?;
            let response = resolved.provider.embed(request).await?;
            self.record_embedding(&response.usage, EmbeddingPurpose::Probe, attribution)
                .await;

            tracing::info!("model {} probe succeeded", target.model);
            Ok(response)
        }
        .instrument(span)
        .await
    }

    // -- Internals -----------------------------------------------------------

    /// Resolves the built provider and registry key for an embedding
    /// target: the per-profile cache first, then registration (idempotent,
    /// covering endpoints discovered after boot), credential resolution,
    /// and construction.
    fn resolve_embedding(
        &self,
        target: &EmbeddingTarget,
    ) -> Result<ResolvedEmbedding, InferenceError> {
        let key = ProviderKey::new(
            target.provider.to_string(),
            &target.base_url,
            RequestClass::Embedding,
        )
        .map_err(|e| {
            InferenceError::provider_unavailable(
                target.provider.to_string(),
                format!("keying the embedding endpoint: {e}"),
            )
        })?;

        // Registration precedes the cache check so a cached provider's
        // permits always resolve, however the cache entry arrived; an
        // endpoint already registered (boot or earlier resolution) is a
        // no-op.
        self.registry
            .register_building(key.clone(), &DEFAULT_DYNAMIC_EMBEDDING_LIMITS)
            .map_err(|e| {
                InferenceError::provider_unavailable(
                    target.provider.to_string(),
                    format!("registering the embedding endpoint: {e}"),
                )
            })?;

        if let Some(cached) = target
            .profile_id
            .and_then(|id| self.embedding_cache.get(&id).map(|p| p.value().clone()))
        {
            return Ok(ResolvedEmbedding {
                provider: cached,
                key,
            });
        }

        let client = self.registry.resolve_client(&key).ok_or_else(|| {
            InferenceError::provider_unavailable(
                target.provider.to_string(),
                format!("the embedding endpoint resolved no HTTP client: {key}"),
            )
        })?;
        let api_key = self
            .credentials
            .resolve(target.provider, key.normalised_base_url())
            .map_err(|e| {
                InferenceError::provider_unavailable(target.provider.to_string(), e.to_string())
            })?;

        let provider = make_embedding_provider(
            target.provider,
            client,
            key.normalised_base_url(),
            &target.model,
            target.dimensions,
            &api_key,
        )
        .map_err(|e| {
            InferenceError::provider_unavailable(target.provider.to_string(), e.to_string())
        })?;

        if let Some(id) = target.profile_id {
            self.embedding_cache.insert(id, Arc::clone(&provider));
        }
        Ok(ResolvedEmbedding { provider, key })
    }

    /// Acquires a permit for the key, reporting the wait to the sink.
    ///
    /// # Panics
    ///
    /// Panics if the provider semaphore is closed, which never happens:
    /// the registry holds semaphores for the process lifetime.
    async fn acquire(
        &self,
        key: &ProviderKey,
        wait: PermitWait,
    ) -> Result<OwnedSemaphorePermit, InferenceError> {
        let semaphore = self.registry.resolve_semaphore(key).ok_or_else(|| {
            InferenceError::provider_unavailable(
                key.provider_kind(),
                format!("no concurrency semaphore registered for provider key {key}"),
            )
        })?;

        let started = Instant::now();
        let acquired = match wait {
            PermitWait::Bounded { limit } => {
                tokio::time::timeout(limit, Arc::clone(&semaphore).acquire_owned())
                    .await
                    .map_err(|_| InferenceError::PermitTimeout {
                        provider_key: key.to_string(),
                        waited_ms: latency_ms(started.elapsed()),
                    })?
            }
            PermitWait::Unbounded => Arc::clone(&semaphore).acquire_owned().await,
        };
        let permit = acquired.expect(SEMAPHORE_NEVER_CLOSED);
        self.sink
            .record_semaphore_wait(&key.to_string(), started.elapsed());
        Ok(permit)
    }

    /// Runs the Ollama tags pre-check for an Ollama-bound key: a
    /// best-effort, log-only check that the model is locally available.
    async fn check_ollama_tags(&self, key: &ProviderKey, model: &str) {
        if key.provider_kind() != ProviderKind::Ollama.as_str() {
            return;
        }
        let Some(client) = self.registry.resolve_client(key) else {
            return;
        };
        crate::ollama::tags::check_tags(&client, key.normalised_base_url(), model).await;
    }

    async fn record_completion(
        &self,
        response: &CompletionResponse,
        stage: TokenUsageStage,
        attribution: &UsageAttribution,
    ) {
        let usage = Usage::Completion {
            usage: response.usage.clone(),
        };
        self.sink.record_usage(&usage, stage, attribution).await;
    }

    async fn record_embedding(
        &self,
        usage: &tribal_domain::EmbeddingUsage,
        purpose: EmbeddingPurpose,
        attribution: &UsageAttribution,
    ) {
        let usage = Usage::Embedding {
            usage: usage.clone(),
            purpose,
        };
        self.sink
            .record_usage(&usage, TokenUsageStage::Embedding { purpose }, attribution)
            .await;
    }

    /// Wraps a provider stream so it owns its permit and records the
    /// terminal event's usage as it passes through.
    fn recorded_stream(
        &self,
        stream: InferenceEventStream,
        permit: OwnedSemaphorePermit,
        stage: TokenUsageStage,
        attribution: &UsageAttribution,
    ) -> InferenceEventStream {
        use futures_util::StreamExt;

        let sink = Arc::clone(&self.sink);
        let attribution = Arc::new(attribution.clone());
        let recorded = stream.then(move |item| {
            let sink = Arc::clone(&sink);
            let attribution = Arc::clone(&attribution);
            async move {
                if let Ok(InferenceEvent::Completed { response }) = &item {
                    let usage = Usage::Completion {
                        usage: response.usage.clone(),
                    };
                    sink.record_usage(&usage, stage, &attribution).await;
                }
                item
            }
        });

        Box::pin(PermitStream {
            inner: Box::pin(recorded),
            _permit: permit,
        })
    }
}

/// Builds one stage's completion binding from its spec.
fn build_binding(
    registry: &ProviderRegistry,
    stage: TaskType,
    spec: &CompletionStageSpec,
) -> Result<CompletionBinding, FacadeBuildError> {
    let key = ProviderKey::new(
        spec.provider.to_string(),
        &spec.base_url,
        RequestClass::Inference,
    )
    .map_err(|source| FacadeBuildError::CompletionEndpoint { stage, source })?;
    let client = registry
        .resolve_client(&key)
        .ok_or_else(|| FacadeBuildError::MissingClient { key: key.clone() })?;

    let provider: Arc<dyn InferenceProvider> = match spec.provider {
        ProviderKind::Ollama => Arc::new(OllamaInferenceProvider::new(
            client,
            &spec.base_url,
            &spec.model,
        )),
        ProviderKind::OpenAi => Arc::new(OpenAiInferenceProvider::new(
            client,
            &spec.base_url,
            &spec.model,
            &spec.api_key,
        )),
        ProviderKind::Anthropic => Arc::new(AnthropicInferenceProvider::new(
            client,
            &spec.base_url,
            &spec.model,
            &spec.api_key,
        )),
    };

    Ok(CompletionBinding { provider, key })
}

/// A stream that owns the concurrency permit for its wire connection,
/// releasing it when the stream is dropped or finishes.
struct PermitStream {
    inner: InferenceEventStream,
    _permit: OwnedSemaphorePermit,
}

impl futures_util::Stream for PermitStream {
    type Item = Result<InferenceEvent, InferenceError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

// ---------------------------------------------------------------------------
// Test injection
// ---------------------------------------------------------------------------

/// One injected stage provider: the prebuilt provider and the registry key
/// its permits live under.
#[cfg(any(test, feature = "test-helpers"))]
pub struct InjectedCompletion {
    /// The prebuilt provider.
    pub provider: Arc<dyn InferenceProvider>,
    /// The registry key for permit resolution.
    pub key: ProviderKey,
}

/// One injected embedding provider, cached under its profile id.
#[cfg(any(test, feature = "test-helpers"))]
pub struct InjectedEmbedding {
    /// The profile the provider serves.
    pub profile_id: EmbeddingProfileId,
    /// The prebuilt provider.
    pub provider: Arc<dyn EmbeddingProvider>,
}

/// The full set of injected parts for [`InferenceFacade::with_providers`].
#[cfg(any(test, feature = "test-helpers"))]
pub struct InjectedProviders {
    /// The registry holding semaphores for every injected key.
    pub registry: ProviderRegistry,
    /// The extraction stage provider.
    pub extraction: InjectedCompletion,
    /// The triage stage provider.
    pub triage: InjectedCompletion,
    /// The relation stage provider.
    pub relation: InjectedCompletion,
    /// Embedding providers pre-seeded into the per-profile cache.
    pub embeddings: Vec<InjectedEmbedding>,
    /// The credential resolver for embedding targets outside the cache.
    pub credentials: Arc<dyn EmbeddingCredentialResolver>,
    /// The accounting sink.
    pub sink: Arc<dyn LedgerSink>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl InferenceFacade {
    /// Binds a prebuilt embedding provider to a profile id in the
    /// per-profile cache, so a test can route a profile at a scripted
    /// provider after construction.
    pub fn inject_embedding_provider(
        &self,
        profile_id: EmbeddingProfileId,
        provider: Arc<dyn EmbeddingProvider>,
    ) {
        self.embedding_cache.insert(profile_id, provider);
    }

    /// Test-only constructor injecting prebuilt providers beneath the
    /// façade, so tests drive the real policy surface (permits, accounting,
    /// caching) over scripted providers.
    #[must_use]
    pub fn with_providers(parts: InjectedProviders) -> Self {
        let facade = Self {
            registry: parts.registry,
            extraction: CompletionBinding {
                provider: parts.extraction.provider,
                key: parts.extraction.key,
            },
            triage: CompletionBinding {
                provider: parts.triage.provider,
                key: parts.triage.key,
            },
            relation: CompletionBinding {
                provider: parts.relation.provider,
                key: parts.relation.key,
            },
            embedding_cache: DashMap::new(),
            credentials: parts.credentials,
            sink: parts.sink,
        };
        for injected in parts.embeddings {
            facade
                .embedding_cache
                .insert(injected.profile_id, injected.provider);
        }
        facade
    }
}

/// A resolver that returns an empty key for every endpoint, for tests
/// whose providers require no credential.
#[cfg(any(test, feature = "test-helpers"))]
#[derive(Debug, Default, Clone, Copy)]
pub struct KeylessCredentialResolver;

#[cfg(any(test, feature = "test-helpers"))]
impl EmbeddingCredentialResolver for KeylessCredentialResolver {
    fn resolve(
        &self,
        _provider: ProviderKind,
        _normalised_base_url: &str,
    ) -> Result<String, CredentialError> {
        Ok(String::new())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use futures_util::StreamExt;
    use tribal_domain::{CompletionUsage, EmbeddingUsage, JobId};

    use super::*;
    use crate::ProviderIdentity;

    const STAGE_URL: &str = "http://localhost:11434";
    const EMBED_URL: &str = "http://localhost:11500";

    // -- Scripted providers and a recording sink ------------------------------

    /// Counts buffered and streaming calls, returning canned responses.
    struct ScriptedInference {
        identity: ProviderIdentity,
        buffered_calls: AtomicUsize,
        streamed_calls: AtomicUsize,
    }

    impl ScriptedInference {
        fn new() -> Self {
            Self {
                identity: ProviderIdentity {
                    name: "scripted".to_owned(),
                    model: "scripted-model".to_owned(),
                },
                buffered_calls: AtomicUsize::new(0),
                streamed_calls: AtomicUsize::new(0),
            }
        }

        fn canned_response(&self) -> CompletionResponse {
            CompletionResponse {
                text: "canned".to_owned(),
                usage: CompletionUsage {
                    provider: self.identity.name.clone(),
                    model: self.identity.model.clone(),
                    input_tokens: 10,
                    output_tokens: 5,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                    total_tokens: 15,
                    latency: Duration::from_millis(7),
                },
            }
        }
    }

    #[async_trait]
    impl InferenceProvider for ScriptedInference {
        fn identity(&self) -> &ProviderIdentity {
            &self.identity
        }

        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, InferenceError> {
            self.buffered_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.canned_response())
        }

        async fn complete_stream(
            &self,
            _request: CompletionRequest,
        ) -> Result<InferenceEventStream, InferenceError> {
            self.streamed_calls.fetch_add(1, Ordering::SeqCst);
            let response = self.canned_response();
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(InferenceEvent::TextDelta {
                    text: "canned".to_owned(),
                }),
                Ok(InferenceEvent::Completed { response }),
            ])))
        }
    }

    struct ScriptedEmbedding {
        identity: ProviderIdentity,
    }

    impl ScriptedEmbedding {
        fn new() -> Self {
            Self {
                identity: ProviderIdentity {
                    name: "scripted".to_owned(),
                    model: "scripted-embed".to_owned(),
                },
            }
        }
    }

    #[async_trait]
    impl EmbeddingProvider for ScriptedEmbedding {
        fn identity(&self) -> &ProviderIdentity {
            &self.identity
        }

        async fn embed(
            &self,
            _request: EmbeddingRequest,
        ) -> Result<EmbeddingResponse, InferenceError> {
            Ok(EmbeddingResponse {
                vector: vec![0.1, 0.2],
                usage: EmbeddingUsage {
                    provider: self.identity.name.clone(),
                    model: self.identity.model.clone(),
                    total_tokens: 3,
                    latency: Duration::from_millis(2),
                },
            })
        }
    }

    /// One usage record as the sink received it.
    struct RecordedUsage {
        usage: Usage,
        stage: TokenUsageStage,
        attribution: UsageAttribution,
    }

    #[derive(Default)]
    struct RecordingSink {
        usages: Mutex<Vec<RecordedUsage>>,
        waits: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl LedgerSink for RecordingSink {
        async fn record_usage(
            &self,
            usage: &Usage,
            stage: TokenUsageStage,
            attribution: &UsageAttribution,
        ) {
            self.usages.lock().unwrap().push(RecordedUsage {
                usage: usage.clone(),
                stage,
                attribution: attribution.clone(),
            });
        }

        fn record_semaphore_wait(&self, provider_key: &str, _wait: Duration) {
            self.waits.lock().unwrap().push(provider_key.to_owned());
        }
    }

    // -- Fixture assembly ------------------------------------------------------

    struct Fixture {
        facade: InferenceFacade,
        inference: Arc<ScriptedInference>,
        sink: Arc<RecordingSink>,
        profile_id: EmbeddingProfileId,
    }

    fn stage_key() -> ProviderKey {
        ProviderKey::new("scripted", STAGE_URL, RequestClass::Inference).unwrap()
    }

    fn embed_key() -> ProviderKey {
        ProviderKey::new("ollama", EMBED_URL, RequestClass::Embedding).unwrap()
    }

    fn a_fixture(stage_permits: u32) -> Fixture {
        let registry = ProviderRegistry::new(vec![
            (
                stage_key(),
                ProviderLimits {
                    max_in_flight: stage_permits,
                    request_timeout: Duration::from_secs(5),
                },
            ),
            (
                embed_key(),
                ProviderLimits {
                    max_in_flight: 1,
                    request_timeout: Duration::from_secs(5),
                },
            ),
        ])
        .unwrap();

        let inference = Arc::new(ScriptedInference::new());
        let sink = Arc::new(RecordingSink::default());
        let profile_id = EmbeddingProfileId::new();

        let facade = InferenceFacade::with_providers(InjectedProviders {
            registry,
            extraction: InjectedCompletion {
                provider: Arc::clone(&inference) as Arc<dyn InferenceProvider>,
                key: stage_key(),
            },
            triage: InjectedCompletion {
                provider: Arc::clone(&inference) as Arc<dyn InferenceProvider>,
                key: stage_key(),
            },
            relation: InjectedCompletion {
                provider: Arc::clone(&inference) as Arc<dyn InferenceProvider>,
                key: stage_key(),
            },
            embeddings: vec![InjectedEmbedding {
                profile_id,
                provider: Arc::new(ScriptedEmbedding::new()),
            }],
            credentials: Arc::new(KeylessCredentialResolver),
            sink: Arc::clone(&sink) as Arc<dyn LedgerSink>,
        });

        Fixture {
            facade,
            inference,
            sink,
            profile_id,
        }
    }

    fn a_request() -> CompletionRequest {
        CompletionRequest {
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: "input".to_owned(),
            }],
            temperature: None,
            max_tokens: None,
            response_format: None,
        }
    }

    fn an_attribution() -> UsageAttribution {
        UsageAttribution {
            job_id: Some(JobId::new()),
            attempt: 2,
            trace_id: Some("trace-1".to_owned()),
            ..UsageAttribution::default()
        }
    }

    fn an_embedding_target(fixture: &Fixture) -> EmbeddingTarget {
        EmbeddingTarget {
            provider: ProviderKind::Ollama,
            model: "scripted-embed".to_owned(),
            dimensions: 2,
            base_url: EMBED_URL.to_owned(),
            profile_id: Some(fixture.profile_id),
        }
    }

    // -- Accounting -------------------------------------------------------------

    #[tokio::test]
    async fn test_complete_records_usage_with_stage_and_attribution() {
        let fixture = a_fixture(1);
        let attribution = an_attribution();

        fixture
            .facade
            .complete(
                TaskType::Triage,
                a_request(),
                PermitWait::Unbounded,
                &attribution,
            )
            .await
            .unwrap();

        let usages = fixture.sink.usages.lock().unwrap();
        assert_eq!(usages.len(), 1);
        assert!(matches!(usages[0].stage, TokenUsageStage::Triage));
        assert_eq!(usages[0].attribution, attribution);
        assert!(matches!(
            &usages[0].usage,
            Usage::Completion { usage } if usage.total_tokens == 15
        ));
    }

    #[tokio::test]
    async fn test_complete_reports_the_semaphore_wait() {
        let fixture = a_fixture(1);

        fixture
            .facade
            .complete(
                TaskType::Extraction,
                a_request(),
                PermitWait::Unbounded,
                &an_attribution(),
            )
            .await
            .unwrap();

        let waits = fixture.sink.waits.lock().unwrap();
        assert_eq!(waits.as_slice(), [stage_key().to_string()]);
    }

    #[tokio::test]
    async fn test_probe_completion_records_probe_stage() {
        let fixture = a_fixture(1);

        fixture
            .facade
            .probe_completion(TaskType::Relation, &UsageAttribution::default())
            .await
            .unwrap();

        let usages = fixture.sink.usages.lock().unwrap();
        assert_eq!(usages.len(), 1);
        assert!(matches!(usages[0].stage, TokenUsageStage::Probe));
    }

    #[tokio::test]
    async fn test_embed_records_purpose_in_both_usage_and_stage() {
        let fixture = a_fixture(1);
        let target = an_embedding_target(&fixture);

        fixture
            .facade
            .embed(
                &target,
                EmbeddingRequest {
                    input: "query text".to_owned(),
                    purpose: EmbeddingPurpose::Query,
                },
                PermitWait::Unbounded,
                &UsageAttribution::default(),
            )
            .await
            .unwrap();

        let usages = fixture.sink.usages.lock().unwrap();
        assert_eq!(usages.len(), 1);
        // The purpose flows from the request into both halves of the
        // record, so they cannot disagree.
        assert!(matches!(
            usages[0].stage,
            TokenUsageStage::Embedding {
                purpose: EmbeddingPurpose::Query
            }
        ));
        assert!(matches!(
            &usages[0].usage,
            Usage::Embedding {
                purpose: EmbeddingPurpose::Query,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_probe_embedding_records_probe_purpose_and_returns_vector() {
        let fixture = a_fixture(1);
        let target = an_embedding_target(&fixture);

        let response = fixture
            .facade
            .probe_embedding(&target, &UsageAttribution::default())
            .await
            .unwrap();

        assert_eq!(response.vector, vec![0.1, 0.2]);
        let usages = fixture.sink.usages.lock().unwrap();
        assert!(matches!(
            usages[0].stage,
            TokenUsageStage::Embedding {
                purpose: EmbeddingPurpose::Probe
            }
        ));
    }

    #[tokio::test]
    async fn test_embed_group_records_one_usage_per_input() {
        let fixture = a_fixture(1);
        let target = an_embedding_target(&fixture);

        let responses = fixture
            .facade
            .embed_group(
                &target,
                vec![
                    EmbeddingRequest {
                        input: "item".to_owned(),
                        purpose: EmbeddingPurpose::Candidate,
                    },
                    EmbeddingRequest {
                        input: "tag".to_owned(),
                        purpose: EmbeddingPurpose::Tag,
                    },
                ],
                PermitWait::Unbounded,
                &UsageAttribution::default(),
            )
            .await
            .unwrap();

        assert_eq!(responses.len(), 2);
        let usages = fixture.sink.usages.lock().unwrap();
        assert_eq!(usages.len(), 2);
        assert!(matches!(
            usages[0].stage,
            TokenUsageStage::Embedding {
                purpose: EmbeddingPurpose::Candidate
            }
        ));
        assert!(matches!(
            usages[1].stage,
            TokenUsageStage::Embedding {
                purpose: EmbeddingPurpose::Tag
            }
        ));
        // One permit acquisition covers the whole group.
        let waits = fixture.sink.waits.lock().unwrap();
        assert_eq!(waits.len(), 1);
    }

    #[tokio::test]
    async fn test_embed_many_records_one_aggregate_usage() {
        let fixture = a_fixture(1);
        let target = an_embedding_target(&fixture);

        let result = fixture
            .facade
            .embed_many(
                &target,
                vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
                EmbeddingPurpose::Candidate,
                PermitWait::Unbounded,
                &UsageAttribution::default(),
            )
            .await
            .unwrap();

        assert_eq!(result.items.len(), 3);
        let usages = fixture.sink.usages.lock().unwrap();
        assert_eq!(usages.len(), 1, "a batch ledgers one aggregate record");
        assert!(matches!(
            &usages[0].usage,
            Usage::Embedding {
                usage,
                purpose: EmbeddingPurpose::Candidate,
            } if usage.total_tokens == 9
        ));
    }

    // -- Transport choice --------------------------------------------------------

    #[tokio::test]
    async fn test_complete_uses_the_buffered_wire() {
        let fixture = a_fixture(1);

        fixture
            .facade
            .complete(
                TaskType::Extraction,
                a_request(),
                PermitWait::Unbounded,
                &an_attribution(),
            )
            .await
            .unwrap();

        assert_eq!(fixture.inference.buffered_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.inference.streamed_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_complete_stream_uses_the_streaming_wire_and_records_terminal() {
        let fixture = a_fixture(1);
        let attribution = an_attribution();

        let stream = fixture
            .facade
            .complete_stream(
                TaskType::Extraction,
                a_request(),
                PermitWait::Unbounded,
                &attribution,
            )
            .await
            .unwrap();
        let events: Vec<_> = stream.collect().await;

        assert_eq!(fixture.inference.buffered_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.inference.streamed_calls.load(Ordering::SeqCst), 1);
        assert_eq!(events.len(), 2);

        let usages = fixture.sink.usages.lock().unwrap();
        assert_eq!(usages.len(), 1, "the terminal event records exactly once");
        assert!(matches!(usages[0].stage, TokenUsageStage::Extraction));
        assert_eq!(usages[0].attribution, attribution);
    }

    // -- Permits -------------------------------------------------------------------

    #[tokio::test]
    async fn test_stream_owns_its_permit_until_dropped() {
        let fixture = a_fixture(1);
        let semaphore = fixture
            .facade
            .registry
            .resolve_semaphore(&stage_key())
            .unwrap();

        let stream = fixture
            .facade
            .complete_stream(
                TaskType::Extraction,
                a_request(),
                PermitWait::Unbounded,
                &an_attribution(),
            )
            .await
            .unwrap();

        assert_eq!(
            semaphore.available_permits(),
            0,
            "the live stream holds the permit"
        );
        drop(stream);
        assert_eq!(
            semaphore.available_permits(),
            1,
            "dropping the stream releases the permit"
        );
    }

    #[tokio::test]
    async fn test_bounded_wait_times_out_as_permit_timeout() {
        let fixture = a_fixture(1);
        let semaphore = fixture
            .facade
            .registry
            .resolve_semaphore(&stage_key())
            .unwrap();
        let held = Arc::clone(&semaphore).acquire_owned().await.unwrap();

        let err = fixture
            .facade
            .complete(
                TaskType::Extraction,
                a_request(),
                PermitWait::Bounded {
                    limit: Duration::from_millis(10),
                },
                &an_attribution(),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            InferenceError::PermitTimeout { ref provider_key, .. }
                if provider_key == &stage_key().to_string()
        ));
        assert!(
            fixture.sink.usages.lock().unwrap().is_empty(),
            "a request never sent records no usage"
        );
        drop(held);
    }

    // -- Embedding resolution --------------------------------------------------------

    #[tokio::test]
    async fn test_embed_unknown_profile_builds_and_caches_the_provider() {
        let fixture = a_fixture(1);
        // A fresh profile id: not in the cache, so the façade builds the
        // real provider from the target identity (no wire call is made;
        // construction alone proves resolution).
        let target = EmbeddingTarget {
            provider: ProviderKind::Ollama,
            model: "nomic-embed-text:v1.5".to_owned(),
            dimensions: 768,
            base_url: EMBED_URL.to_owned(),
            profile_id: Some(EmbeddingProfileId::new()),
        };

        let resolved = fixture.facade.resolve_embedding(&target).unwrap();
        assert_eq!(resolved.provider.identity().model, "nomic-embed-text:v1.5");
        assert!(
            fixture
                .facade
                .embedding_cache
                .contains_key(&target.profile_id.unwrap()),
            "the built provider is cached by profile id"
        );
    }

    #[tokio::test]
    async fn test_embed_anthropic_target_is_rejected() {
        let fixture = a_fixture(1);
        let target = EmbeddingTarget {
            provider: ProviderKind::Anthropic,
            model: "claude".to_owned(),
            dimensions: 768,
            base_url: "https://api.anthropic.com".to_owned(),
            profile_id: None,
        };

        let err = fixture
            .facade
            .embed(
                &target,
                EmbeddingRequest {
                    input: "x".to_owned(),
                    purpose: EmbeddingPurpose::Query,
                },
                PermitWait::Unbounded,
                &UsageAttribution::default(),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref provider, ref reason, .. }
                if provider == "anthropic" && reason.contains("does not provide an embedding API")
        ));
    }

    #[tokio::test]
    async fn test_new_builds_providers_and_probe_sends_the_canonical_request() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/chat"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "llama3",
                "message": {"role": "assistant", "content": "OK"},
                "done": true,
                "prompt_eval_count": 4,
                "eval_count": 2,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let spec = CompletionStageSpec {
            provider: ProviderKind::Ollama,
            model: "llama3".to_owned(),
            base_url: server.uri(),
            api_key: String::new(),
        };
        let key = ProviderKey::new(
            spec.provider.to_string(),
            &spec.base_url,
            RequestClass::Inference,
        )
        .unwrap();
        let registry = ProviderRegistry::new(vec![(
            key,
            ProviderLimits {
                max_in_flight: 1,
                request_timeout: Duration::from_secs(5),
            },
        )])
        .unwrap();
        let sink = Arc::new(RecordingSink::default());

        let facade = InferenceFacade::new(
            registry,
            &CompletionStageSpecs {
                extraction: spec.clone(),
                triage: spec.clone(),
                relation: spec,
            },
            Arc::new(KeylessCredentialResolver),
            Arc::clone(&sink) as Arc<dyn LedgerSink>,
        )
        .expect("the façade builds from registered endpoints");

        facade
            .probe_completion(TaskType::Triage, &UsageAttribution::default())
            .await
            .expect("the probe succeeds against the mock");

        // The canonical probe request reached the wire (and the tags
        // pre-check stayed best-effort: its 404 did not fail the probe).
        let request = &server.received_requests().await.unwrap()
            [usize::from(server.received_requests().await.unwrap().len() > 1)];
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(
            body["messages"][0]["content"],
            serde_json::json!(crate::http::INFERENCE_PROBE_INPUT),
        );
        assert_eq!(
            body["options"]["num_predict"],
            serde_json::json!(crate::http::PROBE_MAX_TOKENS),
        );
        assert!(body.get("stream").is_some_and(|v| v == false));

        // The probe ledgers under the probe stage.
        let usages = sink.usages.lock().unwrap();
        assert_eq!(usages.len(), 1);
        assert!(matches!(usages[0].stage, TokenUsageStage::Probe));
    }

    #[tokio::test]
    async fn test_credential_failure_maps_to_provider_unavailable() {
        struct FailingResolver;
        impl EmbeddingCredentialResolver for FailingResolver {
            fn resolve(
                &self,
                provider: ProviderKind,
                normalised_base_url: &str,
            ) -> Result<String, CredentialError> {
                Err(CredentialError {
                    provider,
                    base_url: normalised_base_url.to_owned(),
                    message: "no API key catalogued for the endpoint".to_owned(),
                })
            }
        }

        let registry = ProviderRegistry::new(Vec::new()).unwrap();
        let inference = Arc::new(ScriptedInference::new());
        let facade = InferenceFacade::with_providers(InjectedProviders {
            registry,
            extraction: InjectedCompletion {
                provider: Arc::clone(&inference) as Arc<dyn InferenceProvider>,
                key: stage_key(),
            },
            triage: InjectedCompletion {
                provider: Arc::clone(&inference) as Arc<dyn InferenceProvider>,
                key: stage_key(),
            },
            relation: InjectedCompletion {
                provider: inference as Arc<dyn InferenceProvider>,
                key: stage_key(),
            },
            embeddings: vec![],
            credentials: Arc::new(FailingResolver),
            sink: Arc::new(RecordingSink::default()),
        });

        let target = EmbeddingTarget {
            provider: ProviderKind::OpenAi,
            model: "text-embedding-3-small".to_owned(),
            dimensions: 1536,
            base_url: "https://api.openai.com/v1".to_owned(),
            profile_id: None,
        };
        let err = facade
            .resolve_embedding(&target)
            .err()
            .expect("a failing credential resolver must fail resolution");

        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref reason, .. }
                if reason.contains("no API key catalogued")
        ));
    }
}
