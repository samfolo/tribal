//! Integration tests for the worker poll-claim-dispatch loop.
//!
//! Each test seeds data via committed raw connections (not pooled),
//! constructs a [`Worker`] with mock providers, runs the worker
//! briefly, then asserts on task and job state.
//!
//! Tests are serialised via [`serial_lock`] because all workers claim
//! from the same `tasks` table — parallel execution causes cross-test
//! interference.
//!
//! Seeding and assertion queries use [`TestContext::raw_connection`]
//! rather than pool connections to avoid the `PoolConnection::drop`
//! spawn issue that leaks connections across serialised tests.

use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tribal_db::{
    EmbeddingRepository, ExtractionResultRepository, ItemObservationRepository, JobRepository,
    KnowledgeItemRepository, PgEmbeddingRepository, PgExtractionResultRepository,
    PgItemObservationRepository, PgJobRepository, PgKnowledgeItemRepository, PgPrincipalRepository,
    PgProjectRepository, PgReferenceRepository, PgTagRegistryRepository, PgTaskRepository,
    PgTriageResultRepository, PrincipalRepository, ProjectRepository, ReferenceRepository,
    TagRegistryRepository, TaskRepository, TriageResultRepository,
};
use tribal_domain::{
    JobId, JobOutcome, JobStatus, KnowledgeItemId, KnowledgeKind, PrincipalId, ProjectId,
    PromptVersionId, TaskErrorKind, TaskStatus, TaskType, TriageOutcome,
};
use tribal_inference::{
    EmbeddingProvider, InferenceProvider, ProviderKey, ProviderLimits, ProviderRegistry,
    RequestClass,
};
use tribal_test_utils::{
    ExhaustBehaviour, MockEmbeddingProvider, MockInferenceProvider, MockProviderOptions, Seed,
    TestContext, a_candidate, a_completion_response, a_new_job, a_new_knowledge_item,
    a_new_principal, a_new_project, a_new_prompt_version, a_new_task, a_new_triage_result_created,
    a_relation_hint, an_embedding_response, backdate_task_heartbeat,
    duration::{
        CLAIM_SETTLE, EARLY_ABORT_BOUND, HEARTBEAT_DETECT, LONG_PROVIDER_DELAY, MULTI_CYCLE_SETTLE,
        POLL_INTERVAL, POLL_SETTLE, STALE_HEARTBEAT_BACKDATE,
    },
    insert_prompt_version, item,
    polling::{poll_job_status, poll_task_status, poll_until},
    seed_extraction_job, seed_triage_job, serial_lock, set_retry_count, test_context,
    truncate_all_tables,
};
use tribal_worker::{Worker, WorkerConfig};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const WORKER_INSTANCE: &str = "test-worker";

/// Batch index used by [`seed_triage_job`] for the single triage task it creates.
const SEED_TRIAGE_BATCH_INDEX: u32 = 0;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Opens a raw (non-pooled) connection to the test database.
///
/// # Panics
///
/// Panics if the connection cannot be established.
async fn raw_conn(ctx: &TestContext) -> sqlx::PgConnection {
    ctx.raw_connection().await.expect("raw connection")
}

/// Removes committed work data so the next serialised test starts
/// with a clean claim surface.  Called at the end of each test.
async fn teardown(ctx: &TestContext) {
    let mut conn = raw_conn(ctx).await;
    truncate_all_tables(&mut conn).await;
}

/// Inserts a principal, project, and prompt_version, returning the IDs
/// needed to create a job.
async fn setup_prerequisites(
    ctx: &TestContext,
    suffix: &str,
) -> (PrincipalId, ProjectId, PromptVersionId) {
    let mut conn = raw_conn(ctx).await;

    let principal = PgPrincipalRepository
        .insert(
            &mut conn,
            &a_new_principal()
                .principal_key(format!("user:worker-test-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal");

    let project = PgProjectRepository
        .insert(
            &mut conn,
            &a_new_project()
                .git_remote(format!("git@github.com:test/worker-{suffix}.git"))
                .build(),
        )
        .await
        .expect("insert project");

    let pv_id = insert_prompt_version(&mut conn, &a_new_prompt_version().build()).await;

    (principal.id(), project.id(), pv_id)
}

/// Builds a [`Worker`] with mock providers and short timeouts suitable
/// for integration testing.
///
/// When `inference` or `embedding` is `None`, a default mock is used.
/// The default inference mock returns errors on exhaustion (rather than
/// panicking) so the extraction stub's provider call is handled cleanly.
fn build_test_worker(
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

    let job_state_txs: Arc<DashMap<JobId, watch::Sender<()>>> = Arc::new(DashMap::new());

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
        WORKER_INSTANCE.to_owned(),
        job_state_txs,
    ))
}

/// Polls until a task is requeued with at least one retry.
///
/// Used by reclaim and heartbeat tests where the exact retry count
/// depends on timing.
async fn poll_task_requeued_with_retry(
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
fn test_config() -> WorkerConfig {
    WorkerConfig {
        max_concurrent_tasks: 4,
        poll_interval_millis: 100,
        task_timeout_millis: 5_000,
        task_max_retries: 3,
        heartbeat_interval_millis: 200,
        reclaim_interval_millis: 100,
        max_candidates_per_job: 20,
        triage_search_limit: 10,
        include_llm_content: false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verifies that when a stage stub fails, the task is re-queued with
/// an incremented retry count, the correct error kind, and a future
/// `available_at`.
#[tokio::test]
async fn test_retry_path_increments_retry_count() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "retry").await;

    let (job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        seed_extraction_job(&mut conn, principal_id, project_id, pv_id).await
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(pool.clone(), token.clone(), test_config(), None, None);
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_requeued_with_retry(&pool, task_id, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(task.retry_count(), 1, "retry count should be incremented");
    assert_eq!(
        task.error_kind(),
        Some(TaskErrorKind::ProviderError),
        "error kind should be provider_error",
    );
    assert!(
        task.error_message().is_some(),
        "error message should be set",
    );
    assert!(
        task.available_at() > task.created_at(),
        "available_at should be in the future (backoff)",
    );

    let mut conn = raw_conn(ctx).await;
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("find job");
    assert_ne!(
        job.status(),
        JobStatus::Failed,
        "job should not be failed after first retry",
    );

    teardown(ctx).await;
}

/// Verifies that a task at its retry limit is dead-lettered and the
/// parent job transitions to Failed with a Failure outcome.
#[tokio::test]
async fn test_dead_letter_path_transitions_task_and_job() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let config = test_config();

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "dead-letter").await;

    let (job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        let (job_id, task_id) =
            seed_extraction_job(&mut conn, principal_id, project_id, pv_id).await;

        // Pre-set retry_count to max_retries so the next failure
        // triggers dead-lettering.
        set_retry_count(&mut conn, task_id, config.task_max_retries).await;

        (job_id, task_id)
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(pool.clone(), token.clone(), config, None, None);
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::DeadLetter, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(
        task.error_kind(),
        Some(TaskErrorKind::ProviderError),
        "error kind should be provider_error",
    );

    let job = poll_job_status(&pool, job_id, JobStatus::Failed, POLL_SETTLE).await;

    assert_eq!(
        job.outcome(),
        Some(JobOutcome::Failure),
        "job outcome should be Failure",
    );
    assert!(
        job.error_message().is_some(),
        "job error message should be set",
    );
    assert!(
        job.completed_at().is_some(),
        "job completed_at should be set",
    );

    teardown(ctx).await;
}

/// Verifies that the worker never exceeds `max_concurrent_tasks`
/// in-flight tasks, using the peak concurrency spy built into the
/// `Worker`.
#[tokio::test]
async fn test_concurrency_limit_respected() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let max_concurrent = 2_usize;

    let config = WorkerConfig {
        max_concurrent_tasks: max_concurrent,
        ..test_config()
    };

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "concurrency").await;

    // Seed more tasks than max_concurrent.
    let task_count = max_concurrent + 2;
    {
        let mut conn = raw_conn(ctx).await;
        for i in 0..task_count {
            let job = PgJobRepository
                .insert(
                    &mut conn,
                    &a_new_job()
                        .project_id(project_id)
                        .principal_id(principal_id)
                        .extraction_prompt_version_id(pv_id)
                        .triage_prompt_version_id(pv_id)
                        .relation_prompt_version_id(pv_id)
                        .raw_input(format!("concurrency test input {i}"))
                        .build(),
                )
                .await
                .expect("insert job");
            PgTaskRepository
                .insert(
                    &mut conn,
                    &a_new_task()
                        .job_id(job.id())
                        .task_type(TaskType::Extraction)
                        .build(),
                )
                .await
                .expect("insert task");
        }
    }

    let token = CancellationToken::new();
    let worker = build_test_worker(pool, token.clone(), config, None, None);

    let worker_handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    // Let the worker run through a couple of cycles.
    tokio::time::sleep(MULTI_CYCLE_SETTLE).await;
    token.cancel();
    let _ = worker_handle.await;

    // The Worker tracks the high-water mark of simultaneously in-flight
    // tasks via an AtomicUsize counter incremented/decremented around
    // each task dispatch.  This is deterministic and not racy.
    let peak = worker.peak_concurrent();
    assert!(
        peak <= max_concurrent,
        "peak concurrency {peak} exceeded limit {max_concurrent}",
    );

    // Verify at least one task was dispatched (otherwise the assertion
    // above is vacuously true).
    assert!(peak > 0, "worker should have processed at least one task");

    teardown(ctx).await;
}

/// Verifies that the reclaim sweep requeues a task whose heartbeat
/// has expired and increments its retry count.
#[tokio::test]
async fn test_reclaim_sweep_requeues_stale_heartbeat_task() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "reclaim-requeue").await;

    let task_id = {
        let mut conn = raw_conn(ctx).await;
        let (_job_id, task_id) =
            seed_extraction_job(&mut conn, principal_id, project_id, pv_id).await;

        // Claim the task via the repository (simulating a previous
        // worker instance) and immediately backdate the heartbeat
        // beyond the task_timeout to trigger reclaim.
        let claimed = PgTaskRepository
            .claim(&mut conn, 1, "crashed-worker")
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1);

        backdate_task_heartbeat(&mut conn, task_id, STALE_HEARTBEAT_BACKDATE).await;

        task_id
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(pool.clone(), token.clone(), test_config(), None, None);
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    // Reclaim requeues the task (retry_count=1). The worker's poll
    // loop may re-dispatch it before we observe the intermediate
    // state — the extraction stub fails, handle_stage_failure requeues
    // again (retry_count=2). Both outcomes prove reclaim ran.
    let task = poll_task_requeued_with_retry(&pool, task_id, POLL_SETTLE).await;

    token.cancel();
    let _ = handle.await;

    assert_eq!(
        task.status(),
        TaskStatus::Queued,
        "task should be requeued after reclaim",
    );

    teardown(ctx).await;
}

/// Verifies that the reclaim sweep dead-letters a task whose retry
/// budget is exhausted and transitions the parent job to Failed.
#[tokio::test]
async fn test_reclaim_sweep_dead_letters_exhausted_task() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let config = test_config();

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "reclaim-dead-letter").await;

    let (job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        let (job_id, task_id) =
            seed_extraction_job(&mut conn, principal_id, project_id, pv_id).await;

        // Pre-set retry_count to max_retries so reclaim triggers
        // dead-lettering.
        set_retry_count(&mut conn, task_id, config.task_max_retries).await;

        // Claim and backdate heartbeat.
        let claimed = PgTaskRepository
            .claim(&mut conn, 1, "crashed-worker")
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1);

        backdate_task_heartbeat(&mut conn, task_id, STALE_HEARTBEAT_BACKDATE).await;

        (job_id, task_id)
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(pool.clone(), token.clone(), config, None, None);
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::DeadLetter, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(
        task.error_kind(),
        Some(TaskErrorKind::HeartbeatExpired),
        "error kind should be heartbeat_expired",
    );
    assert_eq!(
        task.error_message(),
        Some("heartbeat_expired"),
        "error message should be heartbeat_expired",
    );

    let job = poll_job_status(&pool, job_id, JobStatus::Failed, POLL_SETTLE).await;

    assert_eq!(
        job.outcome(),
        Some(JobOutcome::Failure),
        "job outcome should be Failure",
    );

    teardown(ctx).await;
}

/// Verifies that `startup_reclaim` recovers an orphaned task left
/// by a crashed worker instance.
#[tokio::test]
async fn test_startup_reclaim_recovers_orphaned_task() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "startup-reclaim").await;

    let task_id = {
        let mut conn = raw_conn(ctx).await;
        let (_job_id, task_id) =
            seed_extraction_job(&mut conn, principal_id, project_id, pv_id).await;

        // Claim and backdate heartbeat (simulating crash).
        let claimed = PgTaskRepository
            .claim(&mut conn, 1, "crashed-worker")
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1);

        backdate_task_heartbeat(&mut conn, task_id, STALE_HEARTBEAT_BACKDATE).await;

        task_id
    };

    let config = test_config();
    let token = CancellationToken::new();
    let worker = build_test_worker(pool, token.clone(), config, None, None);

    // Call startup_reclaim directly — no worker loop needed.
    let reclaimed = worker.startup_reclaim().await.expect("startup reclaim");

    assert_eq!(reclaimed, 1, "should reclaim exactly one orphaned task");

    let mut conn = raw_conn(ctx).await;
    let task = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("find task");

    assert_eq!(
        task.status(),
        TaskStatus::Queued,
        "task should be requeued after startup reclaim",
    );
    assert_eq!(task.retry_count(), 1, "retry count should be incremented");
    assert_eq!(
        task.error_kind(),
        Some(TaskErrorKind::StartupReclaim),
        "error kind should be startup_reclaim",
    );
    assert_eq!(
        task.error_message(),
        Some("startup_reclaim"),
        "error message should be startup_reclaim",
    );

    teardown(ctx).await;
}

/// Verifies that the heartbeat detects ownership loss when another
/// worker reclaims a task mid-stage, and that the worker handles the
/// interruption gracefully without corrupting task state.
#[tokio::test]
async fn test_heartbeat_detects_ownership_loss_mid_stage() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "ownership-loss").await;

    let task_id = {
        let mut conn = raw_conn(ctx).await;
        let (_job_id, task_id) =
            seed_extraction_job(&mut conn, principal_id, project_id, pv_id).await;
        task_id
    };

    // Mock with a long delay keeps the extraction stage in-flight long
    // enough for the heartbeat to detect an external reclaim.
    let inference = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response("delayed"),
                Some(MockProviderOptions {
                    delay: Some(LONG_PROVIDER_DELAY),
                }),
            )
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let inference_ref = Arc::clone(&inference);

    let config = WorkerConfig {
        max_concurrent_tasks: 1,
        heartbeat_interval_millis: 100,
        // Disable reclaim sweep so it does not interfere with the
        // manual reclaim injection below.
        reclaim_interval_millis: 120_000,
        ..test_config()
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        config,
        Some(inference as Arc<dyn InferenceProvider>),
        None,
    );

    let start = std::time::Instant::now();

    let worker_handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    // Poll until the mock provider has been called, confirming the
    // extraction stage is in-flight.
    poll_until(
        "provider called at least once",
        POLL_INTERVAL,
        CLAIM_SETTLE,
        || {
            let count = inference_ref.call_count();
            async move { if count >= 1 { Some(()) } else { None } }
        },
    )
    .await;

    // Simulate external reclaim: backdate the heartbeat far beyond the
    // timeout window, then call reclaim_stale to requeue the task.
    // This clears the claim_token, causing the next heartbeat tick to
    // return 0 rows and fire the ownership_lost signal.
    {
        let mut conn = raw_conn(ctx).await;
        backdate_task_heartbeat(&mut conn, task_id, STALE_HEARTBEAT_BACKDATE).await;

        // Use a large flat backoff so the requeued task's available_at
        // is far in the future, preventing the worker's poll loop from
        // re-claiming it before the test asserts.
        PgTaskRepository
            .reclaim_stale(
                &mut conn,
                10,
                3,
                10,
                TaskErrorKind::HeartbeatExpired,
                "heartbeat_expired",
                Some(3600),
            )
            .await
            .expect("reclaim stale");
    }

    // Poll until the heartbeat detects ownership loss and the task
    // is requeued.
    poll_task_requeued_with_retry(&pool, task_id, HEARTBEAT_DETECT).await;

    token.cancel();
    let _ = worker_handle.await;

    let elapsed = start.elapsed();

    // If ownership loss was NOT detected, the long mock delay would
    // have to complete before the worker moved on.  The total test
    // time being well under the delay proves the heartbeat interrupted
    // the stage early.
    assert!(
        elapsed < EARLY_ABORT_BOUND,
        "expected ownership loss to abort the stage early, but test took {elapsed:?}",
    );

    let mut conn = raw_conn(ctx).await;
    let task = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("find task");

    assert!(
        task.retry_count() >= 1,
        "task should have been reclaimed at least once",
    );
    assert_eq!(
        task.error_kind(),
        Some(TaskErrorKind::HeartbeatExpired),
        "error kind should reflect the reclaim",
    );

    teardown(ctx).await;
}

// ---------------------------------------------------------------------------
// Extraction stage tests
// ---------------------------------------------------------------------------

/// Helper: builds an extraction-output JSON string from candidates and hints.
fn extraction_response_json(
    candidates: &[tribal_domain::Candidate],
    hints: &[tribal_domain::RelationHint],
) -> String {
    serde_json::json!({
        "candidates": serde_json::to_value(candidates).unwrap(),
        "relation_hints": serde_json::to_value(hints).unwrap(),
    })
    .to_string()
}

/// Verifies the happy path: the extraction stage parses a multi-candidate
/// response, creates triage tasks, persists an extraction result, and
/// transitions the job to Triaging.
#[tokio::test]
async fn test_extraction_happy_path() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "extraction-happy").await;

    let candidates = vec![
        a_candidate().content("first".to_owned()).build(),
        a_candidate().content("second".to_owned()).build(),
    ];
    let hints = vec![a_relation_hint().build()];
    let response_json = extraction_response_json(&candidates, &hints);

    let (job_id, _task_id) = {
        let mut conn = raw_conn(ctx).await;
        seed_extraction_job(&mut conn, principal_id, project_id, pv_id).await
    };

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response(&response_json), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
    );
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let job = poll_job_status(&pool, job_id, JobStatus::Triaging, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(job.batch_size(), Some(2), "batch_size should be 2");
    assert_eq!(
        job.extraction_original_count(),
        Some(2),
        "original count should be 2",
    );

    // Extraction result should be persisted.
    let mut conn = raw_conn(ctx).await;
    let extraction = PgExtractionResultRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find extraction result");
    assert!(
        extraction.is_some(),
        "extraction result should be persisted",
    );
    let extraction = extraction.unwrap();
    let persisted_candidates: Vec<serde_json::Value> =
        serde_json::from_value(extraction.candidates().clone()).expect("parse candidates");
    assert_eq!(persisted_candidates.len(), 2);

    // Two triage tasks should exist.
    let tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find tasks");
    let triage_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.task_type() == TaskType::Triage)
        .collect();
    assert_eq!(triage_tasks.len(), 2, "should create 2 triage tasks");

    teardown(ctx).await;
}

/// Verifies that zero candidates causes the job to complete immediately
/// with an Empty outcome, and no triage tasks are created.
#[tokio::test]
async fn test_extraction_zero_candidates() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, pv_id) =
        setup_prerequisites(ctx, "extraction-zero-candidates").await;

    let response_json = extraction_response_json(&[], &[]);

    let (job_id, _task_id) = {
        let mut conn = raw_conn(ctx).await;
        seed_extraction_job(&mut conn, principal_id, project_id, pv_id).await
    };

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response(&response_json), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
    );
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let job = poll_job_status(&pool, job_id, JobStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(
        job.outcome(),
        Some(JobOutcome::Empty),
        "outcome should be Empty",
    );
    assert!(job.completed_at().is_some(), "completed_at should be set");
    assert_eq!(job.batch_size(), Some(0), "batch_size should be 0");

    // No triage tasks should have been created.
    let mut conn = raw_conn(ctx).await;
    let tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find tasks");
    let triage_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.task_type() == TaskType::Triage)
        .collect();
    assert!(triage_tasks.is_empty(), "should create no triage tasks");

    teardown(ctx).await;
}

/// Verifies that candidates exceeding `max_candidates_per_job` are
/// capped, relation hints referencing out-of-range indices are
/// filtered, and the original count reflects the pre-cap total.
#[tokio::test]
async fn test_extraction_capping() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "extraction-capping").await;

    // Build 5 candidates and hints that span all 5 indices.
    let candidates: Vec<_> = (0..5)
        .map(|i| a_candidate().content(format!("candidate {i}")).build())
        .collect();
    let hints = vec![
        a_relation_hint().source_index(0).target_index(1).build(),
        a_relation_hint().source_index(2).target_index(4).build(),
    ];
    let response_json = extraction_response_json(&candidates, &hints);

    let (job_id, _task_id) = {
        let mut conn = raw_conn(ctx).await;
        seed_extraction_job(&mut conn, principal_id, project_id, pv_id).await
    };

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response(&response_json), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    // Cap at 2 candidates.
    let config = WorkerConfig {
        max_candidates_per_job: 2,
        ..test_config()
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(pool.clone(), token.clone(), config, Some(inference), None);
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let job = poll_job_status(&pool, job_id, JobStatus::Triaging, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(
        job.batch_size(),
        Some(2),
        "batch_size should be capped to 2"
    );
    assert_eq!(
        job.extraction_original_count(),
        Some(5),
        "original count should reflect pre-cap total of 5",
    );

    // Only 2 triage tasks.
    let mut conn = raw_conn(ctx).await;
    let tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find tasks");
    let triage_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.task_type() == TaskType::Triage)
        .collect();
    assert_eq!(triage_tasks.len(), 2, "should create 2 triage tasks");

    // Relation hints should be filtered: hint (0,1) is within range,
    // hint (2,4) is out of range for batch_size=2.
    let extraction = PgExtractionResultRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find extraction result")
        .expect("extraction result should exist");
    let persisted_hints: Vec<serde_json::Value> =
        serde_json::from_value(extraction.relation_hints().clone()).expect("parse hints");
    assert_eq!(
        persisted_hints.len(),
        1,
        "only hint within capped range should be persisted",
    );

    teardown(ctx).await;
}

/// Verifies that an unparseable LLM response causes the extraction
/// task to be requeued with a ParseError error kind.
#[tokio::test]
async fn test_extraction_parse_failure() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, pv_id) =
        setup_prerequisites(ctx, "extraction-parse-failure").await;

    let (_job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        seed_extraction_job(&mut conn, principal_id, project_id, pv_id).await
    };

    // Return text that is not valid ExtractionOutput JSON.
    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response("this is not valid json for extraction"),
                None,
            )
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
    );
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    // Poll for the task to be requeued with a ParseError.
    let task = poll_until(
        "task requeued with parse error",
        POLL_INTERVAL,
        POLL_SETTLE,
        || {
            let pool = pool.clone();
            async move {
                let mut conn = pool.acquire().await.ok()?;
                let task = PgTaskRepository.find_by_id(&mut conn, task_id).await.ok()?;
                if task.status() == TaskStatus::Queued
                    && task.error_kind() == Some(TaskErrorKind::ParseError)
                {
                    Some(task)
                } else {
                    None
                }
            }
        },
    )
    .await;

    token.cancel();
    let _ = handle.await;

    assert_eq!(
        task.error_kind(),
        Some(TaskErrorKind::ParseError),
        "error kind should be parse_error",
    );
    assert!(
        task.error_message().is_some(),
        "error message should be set",
    );

    teardown(ctx).await;
}

// ---------------------------------------------------------------------------
// Triage stage tests
// ---------------------------------------------------------------------------

/// Helper: builds a triage Novel response JSON string.
fn triage_novel_response_json() -> String {
    serde_json::json!({
        "outcome": { "decision": "created" },
        "similar_item_decisions": [],
    })
    .to_string()
}

/// Helper: builds a triage Duplicate response JSON string for the given
/// matched item.
fn triage_duplicate_response_json(matched_item_id: KnowledgeItemId) -> String {
    serde_json::json!({
        "outcome": {
            "decision": "duplicate",
            "matched_item_id": matched_item_id.to_string(),
        },
        "similar_item_decisions": [],
    })
    .to_string()
}

/// Verifies the novel path: the triage stage classifies a candidate as
/// novel, creates a knowledge item with embedding and references,
/// registers new tags, and records a Created triage result.
#[tokio::test]
async fn test_triage_novel_path() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "triage-novel").await;

    let candidates = vec![
        a_candidate()
            .content("Rust has zero-cost abstractions".to_owned())
            .suggested_tags(vec!["rust".to_owned(), "performance".to_owned()])
            .suggested_references(vec![serde_json::json!({
                "reference_type": "url",
                "value": "https://example.com/rust",
                "description": "Rust documentation",
            })])
            .build(),
    ];

    let (job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        seed_triage_job(&mut conn, principal_id, project_id, pv_id, &candidates).await
    };

    let embedding_vector = vec![0.1_f32; 768];

    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(embedding_vector), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response(triage_novel_response_json()), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        Some(embedding),
    );
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(task.status(), TaskStatus::Completed);

    // Verify triage result with Created outcome.
    let mut conn = raw_conn(ctx).await;
    let triage_result = PgTriageResultRepository
        .find_by_job_id_and_batch_index(&mut conn, job_id, SEED_TRIAGE_BATCH_INDEX)
        .await
        .expect("find triage result")
        .expect("triage result should exist");

    let TriageOutcome::Created { item_id } = triage_result.outcome() else {
        panic!(
            "expected Created outcome, got {:?}",
            triage_result.outcome()
        );
    };
    let ki_id = *item_id;

    // Verify knowledge item was created with correct content and tags.
    let item = PgKnowledgeItemRepository
        .find_by_id(&mut conn, ki_id)
        .await
        .expect("find knowledge item");
    assert_eq!(item.content(), "Rust has zero-cost abstractions");
    assert_eq!(item.project_id(), project_id);
    assert!(
        item.tags().contains(&"rust".to_owned()),
        "tags should contain 'rust': {:?}",
        item.tags(),
    );
    assert!(
        item.tags().contains(&"performance".to_owned()),
        "tags should contain 'performance': {:?}",
        item.tags(),
    );

    // Verify embedding was created.
    let emb = PgEmbeddingRepository
        .find_by_knowledge_item_id(&mut conn, ki_id, "mock-model")
        .await
        .expect("find embedding");
    assert!(emb.is_some(), "embedding should exist for mock-model");

    // Verify reference was created.
    let references = PgReferenceRepository
        .find_by_knowledge_item_id(&mut conn, ki_id)
        .await
        .expect("find references");
    assert_eq!(references.len(), 1, "should have one reference");
    assert_eq!(references[0].value(), "https://example.com/rust");

    // Verify new tags were registered.
    let tags = PgTagRegistryRepository
        .find_all(&mut conn)
        .await
        .expect("find tags");
    let tag_names: Vec<&str> = tags.iter().map(|t| t.tag()).collect();
    assert!(
        tag_names.contains(&"rust"),
        "tag registry should contain 'rust': {tag_names:?}",
    );
    assert!(
        tag_names.contains(&"performance"),
        "tag registry should contain 'performance': {tag_names:?}",
    );

    teardown(ctx).await;
}

/// Verifies the duplicate path: the triage stage classifies a candidate
/// as a duplicate of an existing knowledge item, creates an observation,
/// and records a Duplicate triage result.
#[tokio::test]
async fn test_triage_duplicate_path() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    // Seed an existing knowledge item with an embedding via the Seed
    // builder so semantic search returns it as a match.
    let mut conn = raw_conn(ctx).await;
    let seed_result = Seed::new()
        .define_project("proj", "git@github.com:test/triage-dup.git")
        .define_principal("user", "user:triage-duplicate")
        .set_embedding_model("mock-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("existing", item(KnowledgeKind::Fact, "existing knowledge"));
        })
        .execute(&mut conn)
        .await;

    let principal_id = seed_result.principal_id("user");
    let project_id = seed_result.project_id("proj");
    let ki_id = seed_result.item_id("existing");

    // Read the deterministic embedding vector back from the database so
    // the mock provider can return the same vector for cosine similarity.
    let seeded_embedding = PgEmbeddingRepository
        .find_by_knowledge_item_id(&mut conn, ki_id, "mock-model")
        .await
        .expect("find seeded embedding")
        .expect("seeded embedding should exist");
    let embedding_vector = seeded_embedding.embedding().to_vec();

    let pv_id = insert_prompt_version(&mut conn, &a_new_prompt_version().build()).await;

    let candidates = vec![
        a_candidate()
            .content("duplicate content".to_owned())
            .build(),
    ];

    let (job_id, task_id) =
        seed_triage_job(&mut conn, principal_id, project_id, pv_id, &candidates).await;
    drop(conn);

    // Mock embedding returns the same vector so cosine similarity is 1.0.
    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(embedding_vector), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response(triage_duplicate_response_json(ki_id)),
                None,
            )
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        Some(embedding),
    );
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(task.status(), TaskStatus::Completed);

    // Verify triage result with Duplicate outcome.
    let mut conn = raw_conn(ctx).await;
    let triage_result = PgTriageResultRepository
        .find_by_job_id_and_batch_index(&mut conn, job_id, SEED_TRIAGE_BATCH_INDEX)
        .await
        .expect("find triage result")
        .expect("triage result should exist");

    assert!(
        matches!(
            triage_result.outcome(),
            TriageOutcome::Duplicate { matched_item_id, .. } if *matched_item_id == ki_id
        ),
        "expected Duplicate outcome matching {ki_id}, got {:?}",
        triage_result.outcome(),
    );

    // Verify observation was created against the matched item.
    let observations = PgItemObservationRepository
        .find_by_knowledge_item_id(&mut conn, ki_id)
        .await
        .expect("find observations");
    assert_eq!(observations.len(), 1, "should have one observation");

    teardown(ctx).await;
}

/// Verifies that an unparseable LLM response causes the triage task
/// to be requeued with a ParseError error kind.
#[tokio::test]
async fn test_triage_parse_failure() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "triage-parse-failure").await;

    let candidates = vec![a_candidate().build()];

    let (_job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        seed_triage_job(&mut conn, principal_id, project_id, pv_id, &candidates).await
    };

    // Embedding succeeds (called before the LLM).
    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.1_f32; 768]), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    // Inference returns text that is not valid triage JSON.
    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response("this is not valid triage json"), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        Some(embedding),
    );
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_until(
        "triage task requeued with parse error",
        POLL_INTERVAL,
        POLL_SETTLE,
        || {
            let pool = pool.clone();
            async move {
                let mut conn = pool.acquire().await.ok()?;
                let task = PgTaskRepository.find_by_id(&mut conn, task_id).await.ok()?;
                if task.status() == TaskStatus::Queued
                    && task.error_kind() == Some(TaskErrorKind::ParseError)
                {
                    Some(task)
                } else {
                    None
                }
            }
        },
    )
    .await;

    token.cancel();
    let _ = handle.await;

    assert_eq!(
        task.error_kind(),
        Some(TaskErrorKind::ParseError),
        "error kind should be parse_error",
    );
    assert!(
        task.error_message().is_some(),
        "error message should be set",
    );

    teardown(ctx).await;
}

/// Verifies the idempotency guard: when a triage result already exists
/// for the `(job_id, batch_index)` pair, the stage returns NoOp without
/// calling any providers, and the task is completed.
#[tokio::test]
async fn test_triage_idempotency_skip() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, pv_id) = setup_prerequisites(ctx, "triage-idempotency").await;

    let candidates = vec![a_candidate().build()];

    let (job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        let (job_id, task_id) =
            seed_triage_job(&mut conn, principal_id, project_id, pv_id, &candidates).await;

        // Pre-seed a knowledge item so the triage result FK is satisfied.
        let ki = PgKnowledgeItemRepository
            .insert(
                &mut conn,
                &a_new_knowledge_item()
                    .project_id(project_id)
                    .principal_id(principal_id)
                    .build(),
            )
            .await
            .expect("setup: insert knowledge item");

        // Pre-seed a triage result for the seeded batch index.
        PgTriageResultRepository
            .insert(
                &mut conn,
                &a_new_triage_result_created()
                    .job_id(job_id)
                    .batch_index(SEED_TRIAGE_BATCH_INDEX)
                    .outcome(TriageOutcome::Created { item_id: ki.id() })
                    .build(),
            )
            .await
            .expect("setup: insert triage result");

        (job_id, task_id)
    };

    // Neither provider should be called — the idempotency check returns
    // early before any embedding or inference calls.
    let inference: Arc<MockInferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_exhaust(ExhaustBehaviour::Error(Box::new(|| {
                tribal_inference::InferenceError::ProviderUnavailable {
                    provider: "mock".into(),
                    reason: "inference should not be called".into(),
                }
            })))
            .build(),
    );
    let inference_ref = Arc::clone(&inference);

    let embedding: Arc<MockEmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_exhaust(ExhaustBehaviour::Error(Box::new(|| {
                tribal_inference::InferenceError::ProviderUnavailable {
                    provider: "mock".into(),
                    reason: "embedding should not be called".into(),
                }
            })))
            .build(),
    );
    let embedding_ref = Arc::clone(&embedding);

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference as Arc<dyn InferenceProvider>),
        Some(embedding as Arc<dyn EmbeddingProvider>),
    );
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(task.status(), TaskStatus::Completed);

    // Verify no provider calls were made.
    assert_eq!(
        inference_ref.call_count(),
        0,
        "inference should not be called",
    );
    assert_eq!(
        embedding_ref.call_count(),
        0,
        "embedding should not be called",
    );

    // Verify no additional triage results were created.
    let mut conn = raw_conn(ctx).await;
    let results = PgTriageResultRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find triage results");
    assert_eq!(
        results.len(),
        1,
        "should have exactly one triage result (pre-seeded)",
    );

    teardown(ctx).await;
}
