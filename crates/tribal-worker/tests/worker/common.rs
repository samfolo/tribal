//! Shared imports, constants, and helpers for worker integration tests.

pub(super) use std::{sync::Arc, time::Duration};

pub(super) use dashmap::DashMap;
pub(super) use tokio_util::sync::CancellationToken;
pub(super) use tribal_common::JobStateTxs;
pub(super) use tribal_config::WorkerConfig;
pub(super) use tribal_db::{
    ExtractionResultRepository, ItemObservationRepository, JobRepository, JobStatusTransition,
    KnowledgeItemRepository, NewTagEmbedding, PgExtractionResultRepository,
    PgItemObservationRepository, PgJobRepository, PgKnowledgeItemRepository, PgReferenceRepository,
    PgRelationRepository, PgTagEmbeddingRepository, PgTagRegistryRepository, PgTaskRepository,
    PgTokenUsageRepository, PgTriageResultRepository, ReferenceRepository, RelationRepository,
    TagEmbeddingRepository, TagRegistryRepository, TaskRepository, TokenUsageRepository,
    TriageResultRepository,
};
pub(super) use tribal_domain::{
    EmbeddingPurpose, JobOutcome, JobStatus, KnowledgeKind, PipelineStage, PrincipalId, ProjectId,
    PromptVersionId, RelationBatchId, SourceType, TaskErrorKind, TaskStatus, TaskType,
    TriageOutcome,
};
pub(super) use tribal_inference::{
    EmbeddingProvider, InferenceProvider, ProviderKey, ProviderLimits, ProviderRegistry,
    RequestClass,
};
pub(super) use tribal_telemetry::noop_recorder;
pub(super) use tribal_test_utils::{
    ExhaustBehaviour, MockEmbeddingProvider, MockInferenceProvider, MockProviderOptions, Seed,
    TestContext, a_candidate, a_completion_response, a_new_extraction_result, a_new_job,
    a_new_knowledge_item, a_new_prompt_version, a_new_system_fingerprint, a_new_task,
    a_new_triage_result_created, a_new_triage_result_duplicate, a_relation_hint,
    active_embedding_profile, an_embedding_response, backdate_task_heartbeat, candidates_json,
    duration::{
        CLAIM_SETTLE, EARLY_ABORT_BOUND, HEARTBEAT_DETECT, LONG_PROVIDER_DELAY, MULTI_CYCLE_SETTLE,
        POLL_INTERVAL, POLL_SETTLE, STALE_HEARTBEAT_BACKDATE,
    },
    find_active_embedding, item,
    polling::{poll_job_status, poll_task_status, poll_until},
    seed_extraction_job, seed_multiple_triage_tasks, seed_relation_job, seed_triage_job,
    serial_lock, set_retry_count, set_task_status_by_job, test_context, truncate_all_tables,
    upsert_system_fingerprint,
};
pub(super) use tribal_worker::Worker;

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
pub(super) async fn raw_conn(ctx: &TestContext) -> sqlx::PgConnection {
    ctx.raw_connection().await.expect("raw connection")
}

/// Removes committed work data so the next serialised test starts
/// with a clean claim surface.  Called at the end of each test.
pub(super) async fn teardown(ctx: &TestContext) {
    let mut conn = raw_conn(ctx).await;
    truncate_all_tables(&mut conn).await;
}

/// Seeds a principal, project, and prompt versions (system + user)
/// via the [`Seed`] builder, returning the IDs needed to create a job.
pub(super) async fn setup_prerequisites(
    ctx: &TestContext,
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
pub(super) fn build_test_worker(
    pool: sqlx::PgPool,
    cancellation_token: CancellationToken,
    config: WorkerConfig,
    inference: Option<Arc<dyn InferenceProvider>>,
    embedding: Option<Arc<dyn EmbeddingProvider>>,
) -> Arc<Worker> {
    let inference: Arc<dyn InferenceProvider> = inference.unwrap_or_else(|| {
        Arc::new(
            MockInferenceProvider::builder()
                .on_exhaust(tribal_test_utils::ExhaustBehaviour::Error(Box::new(|| {
                    tribal_inference::InferenceError::ProviderUnavailable {
                        provider: "mock".into(),
                        reason: "test stub".into(),
                    }
                })))
                .build(),
        )
    });
    let embedding: Arc<dyn EmbeddingProvider> =
        embedding.unwrap_or_else(|| Arc::new(MockEmbeddingProvider::builder().build()));

    let key = |class| {
        ProviderKey::new("mock", "http://localhost:9999", class).expect("valid provider key")
    };

    let registry = Arc::new(
        ProviderRegistry::new(vec![
            (
                key(RequestClass::Inference),
                ProviderLimits {
                    max_in_flight: 10,
                    request_timeout: Duration::from_secs(30),
                },
            ),
            (
                key(RequestClass::Embedding),
                ProviderLimits {
                    max_in_flight: 10,
                    request_timeout: Duration::from_secs(30),
                },
            ),
        ])
        .expect("valid registry"),
    );

    let job_state_txs: JobStateTxs = Arc::new(DashMap::new());

    Arc::new(Worker::new(
        pool,
        registry,
        inference.clone(),
        inference.clone(),
        inference,
        embedding,
        key(RequestClass::Inference),
        key(RequestClass::Inference),
        key(RequestClass::Embedding),
        key(RequestClass::Inference),
        cancellation_token,
        config,
        false,
        WORKER_INSTANCE.to_owned(),
        job_state_txs,
        noop_recorder(),
    ))
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
