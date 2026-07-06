//! Shared imports, constants, and helpers for worker integration tests.

pub(super) use std::{sync::Arc, time::Duration};

pub(super) use dashmap::DashMap;
pub(super) use tokio_util::sync::CancellationToken;
pub(super) use tribal_agent_runtime::PgLedgerSink;
pub(super) use tribal_common::JobStateTxs;
pub(super) use tribal_config::WorkerConfig;
pub(super) use tribal_db::{
    EmbeddingProfileRepository, ExtractionResultRepository, ItemObservationRepository,
    JobRepository, JobStatusTransition, KnowledgeItemRepository, NewReindexRun, NewTagEmbedding,
    PgEmbeddingProfileRepository, PgExtractionResultRepository, PgItemObservationRepository,
    PgJobRepository, PgKnowledgeItemRepository, PgReferenceRepository, PgReindexRunRepository,
    PgRelationRepository, PgTagEmbeddingRepository, PgTagRegistryRepository, PgTaskRepository,
    PgTokenUsageRepository, PgTriageResultRepository, ReferenceRepository, ReindexRunRepository,
    RelationRepository, TagEmbeddingRepository, TagRegistryRepository, TaskRepository,
    TokenUsageRepository, TriageResultRepository,
};
pub(super) use tribal_domain::{
    EmbeddingPurpose, JobOutcome, JobStatus, KnowledgeKind, PipelineStage, PrincipalId, ProjectId,
    PromptVersionId, ProviderKind, RelationBatchId, SourceType, TaskErrorKind, TaskStatus,
    TaskType, TriageOutcome,
};
pub(super) use tribal_inference::{
    CompletionStageSpec, CompletionStageSpecs, EmbeddingProvider, InferenceGateway,
    InferenceProvider, InjectedEmbedding, InjectedProviders, ProviderKey, ProviderLimits,
    ProviderRegistry, RequestClass,
};
pub(super) use tribal_telemetry::noop_recorder;
pub(super) use tribal_test_utils::{
    ExhaustBehaviour, MockEmbeddingProvider, MockInferenceProvider, MockProviderOptions, Seed,
    TestDb, a_candidate, a_completion_response, a_new_embedding_profile, a_new_extraction_result,
    a_new_job, a_new_knowledge_item, a_new_prompt_version, a_new_system_fingerprint, a_new_task,
    a_new_triage_result_created, a_new_triage_result_duplicate, a_relation_hint,
    active_embedding_profile, an_embedding_response, backdate_task_heartbeat, candidates_json,
    duration::{
        CLAIM_SETTLE, EARLY_ABORT_BOUND, HEARTBEAT_DETECT, LONG_PROVIDER_DELAY, MULTI_CYCLE_SETTLE,
        POLL_INTERVAL, POLL_SETTLE, STALE_HEARTBEAT_BACKDATE,
    },
    find_active_embedding, item,
    polling::{poll_job_status, poll_task_status, poll_until},
    seed_extraction_job, seed_multiple_triage_tasks, seed_relation_job, seed_triage_job,
    set_retry_count, set_task_status_by_job, upsert_system_fingerprint,
};
use tribal_worker::Worker;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(super) const WORKER_INSTANCE: &str = "test-worker";

/// Batch index used by [`seed_triage_job`] for the single triage task it creates.
pub(super) const SEED_TRIAGE_BATCH_INDEX: u32 = 0;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Opens a raw (non-pooled) connection to the test database.
///
/// # Panics
///
/// Panics if the connection cannot be established.
pub(super) async fn raw_conn(ctx: &TestDb) -> sqlx::PgConnection {
    ctx.raw_connection().await.expect("raw connection")
}

/// Seeds a principal, project, and prompt versions (system + user)
/// via the [`Seed`] builder, returning the IDs needed to create a job.
pub(super) async fn setup_prerequisites(
    ctx: &TestDb,
    suffix: &str,
) -> (PrincipalId, ProjectId, PromptVersionId, PromptVersionId) {
    let mut conn = raw_conn(ctx).await;

    let seed_result = Seed::new()
        .define_project("proj", format!("git@github.com:test/worker-{suffix}.git"))
        .define_principal("user", format!("user:worker-test-{suffix}"))
        // The triage stage resolves the active embedding profile, so seed one.
        .set_embedding_model("mock-model", 768)
        .define_prompt_version("system-pv", a_new_prompt_version().build())
        .define_prompt_version(
            "user-pv",
            a_new_prompt_version()
                .role(tribal_domain::PromptRole::User)
                .content_hash("c".repeat(64))
                .content("test user prompt content".to_owned())
                .build(),
        )
        .execute(&mut conn)
        .await;

    (
        seed_result.principal_id("user"),
        seed_result.project_id("proj"),
        seed_result.prompt_version_id("system-pv"),
        seed_result.prompt_version_id("user-pv"),
    )
}

/// Builds a [`Worker`] with mock providers and short timeouts suitable
/// for integration testing.
///
/// When `inference` or `embedding` is `None`, a default mock is used.
/// The default inference mock returns errors on exhaustion (rather than
/// panicking) so the extraction stub's provider call is handled cleanly.
pub(super) async fn build_test_worker(
    pool: sqlx::PgPool,
    cancellation_token: CancellationToken,
    config: WorkerConfig,
    inference: Option<Arc<dyn InferenceProvider>>,
    embedding: Option<Arc<dyn EmbeddingProvider>>,
) -> Arc<Worker> {
    let (worker, _) =
        build_test_worker_with_watch(pool, cancellation_token, config, inference, embedding).await;
    worker
}

/// Like [`build_test_worker`], also returning the job-state watch map so
/// tests can subscribe to the notifications the worker sends.
pub(super) async fn build_test_worker_with_watch(
    pool: sqlx::PgPool,
    cancellation_token: CancellationToken,
    config: WorkerConfig,
    inference: Option<Arc<dyn InferenceProvider>>,
    embedding: Option<Arc<dyn EmbeddingProvider>>,
) -> (Arc<Worker>, JobStateTxs) {
    build_agentic_test_worker(
        pool,
        cancellation_token,
        config,
        inference,
        embedding,
        tribal_config::AgentsConfig::default(),
        Arc::new(tribal_worker::NoAgenticPrompts),
    )
    .await
}

/// Like [`build_test_worker_with_watch`] with an agentic configuration
/// and prompt source, for tests that select the loop executor.
pub(super) async fn build_agentic_test_worker(
    pool: sqlx::PgPool,
    cancellation_token: CancellationToken,
    config: WorkerConfig,
    inference: Option<Arc<dyn InferenceProvider>>,
    embedding: Option<Arc<dyn EmbeddingProvider>>,
    agents: tribal_config::AgentsConfig,
    active_prompts: Arc<dyn tribal_worker::ActiveAgenticPrompts>,
) -> (Arc<Worker>, JobStateTxs) {
    let inference: Arc<dyn InferenceProvider> = inference.unwrap_or_else(|| {
        Arc::new(
            MockInferenceProvider::builder()
                .on_exhaust(tribal_test_utils::ExhaustBehaviour::Error(Box::new(|| {
                    tribal_inference::InferenceError::provider_unavailable("mock", "test stub")
                })))
                .build(),
        )
    });
    let embedding: Arc<dyn EmbeddingProvider> =
        embedding.unwrap_or_else(|| Arc::new(MockEmbeddingProvider::builder().build()));

    let key = |class| {
        ProviderKey::new("mock", "http://localhost:9999", class).expect("valid provider key")
    };

    // The triage stage resolves the embedding provider from the active profile
    // via `build_target_provider`, keyed on the profile's `(provider_kind,
    // normalised_base_url)` (the seeded genesis is the Ollama default). Register
    // that endpoint so the cache-hit semaphore resolves, then wire the mock into
    // the per-profile cache below; otherwise the worker would build a dead real
    // provider against the Ollama default endpoint.
    let embedding_endpoint = ProviderKey::new(
        ProviderKind::Ollama.to_string(),
        ProviderKind::DEFAULT_OLLAMA_BASE_URL,
        RequestClass::Embedding,
    )
    .expect("valid embedding endpoint key");

    let limits = || ProviderLimits {
        max_in_flight: 10,
        request_timeout: Duration::from_secs(30),
    };

    let registry = ProviderRegistry::new(vec![
        (key(RequestClass::Inference), limits()),
        (key(RequestClass::Embedding), limits()),
        (embedding_endpoint, limits()),
    ])
    .expect("valid registry");

    // Resolve the active profile (the seeded genesis) and bind the mock to it
    // in the gateway's per-profile cache, so the triage stage's per-call
    // provider resolution returns the mock.
    let mut embeddings = Vec::new();
    {
        let mut conn = pool
            .acquire()
            .await
            .expect("acquire connection for embedding wiring");
        if let Some(active) = PgEmbeddingProfileRepository
            .find_active(&mut conn)
            .await
            .expect("find active profile")
        {
            embeddings.push(InjectedEmbedding {
                profile_id: active.id(),
                provider: embedding,
            });
        }
    }

    // The real ledger sink, so the integration suite covers usage recording
    // end to end; metrics are dropped.
    let sink = Arc::new(PgLedgerSink::new(pool.clone(), noop_recorder()));
    let gateway = Arc::new(InferenceGateway::with_providers(
        InjectedProviders::uniform(
            registry,
            inference,
            key(RequestClass::Inference),
            embeddings,
            sink,
        ),
    ));

    let job_state_txs: JobStateTxs = Arc::new(DashMap::new());

    let worker = Arc::new(Worker::new(
        pool,
        gateway,
        test_stage_specs(),
        agents,
        active_prompts,
        cancellation_token,
        config,
        false,
        WORKER_INSTANCE.to_owned(),
        Arc::clone(&job_state_txs),
        noop_recorder(),
        None,
    ));
    (worker, job_state_txs)
}

/// The boot-time stage specs a test worker derives bindings from: one
/// mock endpoint for all three stages, matching the injected providers.
pub(super) fn test_stage_specs() -> CompletionStageSpecs {
    let spec = CompletionStageSpec {
        provider: ProviderKind::Ollama,
        model: "mock-model".to_owned(),
        base_url: "http://localhost:9999".to_owned(),
        api_key: String::new(),
        parameters: tribal_domain::StageParameters::default(),
    };
    CompletionStageSpecs {
        extraction: spec.clone(),
        triage: spec.clone(),
        relation: spec,
    }
}

/// An agent definition whose route matches the test worker's stage specs,
/// so a thread fabricated under it resumes through the worker without
/// tripping the resume-route-divergence guard (the default factory's
/// route diverges from [`test_stage_specs`] by design).
pub(super) fn a_routed_definition(
    stage: tribal_domain::TaskType,
) -> tribal_domain::AgentDefinition {
    let specs = test_stage_specs();
    let spec = match stage {
        tribal_domain::TaskType::Extraction => specs.extraction,
        tribal_domain::TaskType::Triage => specs.triage,
        tribal_domain::TaskType::Relation => specs.relation,
    };
    tribal_test_utils::an_agent_definition()
        .pipeline_stage(stage)
        .provider(spec.provider)
        .model(spec.model)
        .base_url(spec.base_url)
        .build()
}

/// Polls until a task is requeued with at least one retry.
///
/// Used by reclaim and heartbeat tests where the exact retry count
/// depends on timing.
pub(super) async fn poll_task_requeued_with_retry(
    pool: &sqlx::PgPool,
    task_id: tribal_domain::TaskId,
    timeout: Duration,
) -> tribal_domain::Task {
    poll_until("task requeued with retry", POLL_INTERVAL, timeout, || {
        let pool = pool.clone();
        async move {
            let mut conn = pool.acquire().await.ok()?;
            let task = PgTaskRepository.find_by_id(&mut conn, task_id).await.ok()?;
            if task.status() == TaskStatus::Queued && task.retry_count() >= 1 {
                Some(task)
            } else {
                None
            }
        }
    })
    .await
}

/// Returns a [`WorkerConfig`] with sub-second intervals for fast tests.
pub(super) fn test_config() -> WorkerConfig {
    WorkerConfig {
        max_concurrent_tasks: 4,
        poll_interval_ms: 100,
        task_timeout_ms: 5_000,
        task_max_retries: 3,
        heartbeat_interval_ms: 200,
        reclaim_interval_ms: 100,
        max_candidates_per_job: 20,
        triage_search_limit: 10,
        tag_similarity_threshold: 0.85,
    }
}
