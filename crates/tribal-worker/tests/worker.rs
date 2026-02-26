//! Integration tests for the worker poll-claim-dispatch loop.
//!
//! Each test seeds data via committed raw connections (not pooled),
//! constructs a [`Worker`] with mock providers (whose stubs always
//! fail), runs the worker briefly, then asserts on task and job state.
//!
//! Tests are serialised via [`serial_lock`] because all workers claim
//! from the same `tasks` table — parallel execution causes cross-test
//! interference.
//!
//! Seeding and assertion queries use [`TestContext::raw_connection`]
//! rather than pool connections to avoid the `PoolConnection::drop`
//! spawn issue that leaks connections across serialised tests.

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tribal_db::{
    JobRepository, PgJobRepository, PgPrincipalRepository, PgProjectRepository, PgTaskRepository,
    PrincipalRepository, ProjectRepository, TaskRepository,
};
use tribal_domain::{
    JobId, JobOutcome, JobStatus, PrincipalId, ProjectId, PromptVersionId, TaskErrorKind,
    TaskStatus, TaskType,
};
use tribal_inference::{
    EmbeddingProvider, InferenceProvider, ProviderKey, ProviderLimits, ProviderRegistry,
    RequestClass,
};
use tribal_test_utils::{
    ExhaustBehaviour, MockEmbeddingProvider, MockInferenceProvider, MockOptions, TestContext,
    a_completion_response, a_new_job, a_new_principal, a_new_project, a_new_task,
    backdate_task_heartbeat, serial_lock, test_context,
};
use tribal_worker::{Worker, WorkerConfig};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const WORKER_INSTANCE: &str = "test-worker";

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
    sqlx::query("DELETE FROM token_usage")
        .execute(&mut conn)
        .await
        .ok();
    sqlx::query("DELETE FROM tasks")
        .execute(&mut conn)
        .await
        .ok();
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

    let content_hash = format!("{:064x}", uuid::Uuid::new_v4().as_u128());
    let pv_id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO prompt_versions (stage, content_hash, content) \
         VALUES ('extraction', $1, 'test prompt') RETURNING id",
    )
    .bind(&content_hash)
    .fetch_one(&mut conn)
    .await
    .expect("insert prompt_version");

    (principal.id(), project.id(), PromptVersionId::from(pv_id))
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
                    request_timeout: std::time::Duration::from_secs(30),
                },
            ),
            (
                key(RequestClass::Embedding),
                ProviderLimits {
                    max_in_flight: 10,
                    request_timeout: std::time::Duration::from_secs(30),
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

/// Returns a [`WorkerConfig`] with aggressive timeouts for fast tests.
fn test_config() -> WorkerConfig {
    WorkerConfig {
        max_concurrent_tasks: 4,
        poll_interval_seconds: 1,
        task_timeout_seconds: 10,
        task_max_retries: 3,
        heartbeat_interval_seconds: 5,
        reclaim_interval_seconds: 1,
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
        let job = PgJobRepository
            .insert(
                &mut conn,
                &a_new_job()
                    .project_id(project_id)
                    .principal_id(principal_id)
                    .extraction_prompt_version_id(pv_id)
                    .triage_prompt_version_id(pv_id)
                    .relation_prompt_version_id(pv_id)
                    .build(),
            )
            .await
            .expect("insert job");
        let task = PgTaskRepository
            .insert(
                &mut conn,
                &a_new_task()
                    .job_id(job.id())
                    .task_type(TaskType::Extraction)
                    .build(),
            )
            .await
            .expect("insert task");
        (job.id(), task.id())
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(pool, token.clone(), test_config(), None, None);

    let worker_handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    token.cancel();
    let _ = worker_handle.await;

    let mut conn = raw_conn(ctx).await;
    let task = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("find task");

    assert_eq!(
        task.status(),
        TaskStatus::Queued,
        "task should be re-queued"
    );
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
        let job = PgJobRepository
            .insert(
                &mut conn,
                &a_new_job()
                    .project_id(project_id)
                    .principal_id(principal_id)
                    .extraction_prompt_version_id(pv_id)
                    .triage_prompt_version_id(pv_id)
                    .relation_prompt_version_id(pv_id)
                    .build(),
            )
            .await
            .expect("insert job");
        let task = PgTaskRepository
            .insert(
                &mut conn,
                &a_new_task()
                    .job_id(job.id())
                    .task_type(TaskType::Extraction)
                    .build(),
            )
            .await
            .expect("insert task");

        // Pre-set retry_count to max_retries so the next failure
        // triggers dead-lettering.
        sqlx::query("UPDATE tasks SET retry_count = $1 WHERE id = $2")
            .bind(i32::try_from(config.task_max_retries).unwrap())
            .bind(task.id().inner())
            .execute(&mut conn)
            .await
            .expect("set retry_count");

        (job.id(), task.id())
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(pool, token.clone(), config, None, None);

    let worker_handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    token.cancel();
    let _ = worker_handle.await;

    let mut conn = raw_conn(ctx).await;

    let task = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("find task");

    assert_eq!(
        task.status(),
        TaskStatus::DeadLetter,
        "task should be dead-lettered",
    );
    assert_eq!(
        task.error_kind(),
        Some(TaskErrorKind::ProviderError),
        "error kind should be provider_error",
    );

    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("find job");

    assert_eq!(job.status(), JobStatus::Failed, "job should be failed");
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
        poll_interval_seconds: 1,
        task_timeout_seconds: 10,
        task_max_retries: 3,
        heartbeat_interval_seconds: 5,
        reclaim_interval_seconds: 1,
        max_candidates_per_job: 20,
        triage_search_limit: 10,
        include_llm_content: false,
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
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
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
        let job = PgJobRepository
            .insert(
                &mut conn,
                &a_new_job()
                    .project_id(project_id)
                    .principal_id(principal_id)
                    .extraction_prompt_version_id(pv_id)
                    .triage_prompt_version_id(pv_id)
                    .relation_prompt_version_id(pv_id)
                    .build(),
            )
            .await
            .expect("insert job");
        let task = PgTaskRepository
            .insert(
                &mut conn,
                &a_new_task()
                    .job_id(job.id())
                    .task_type(TaskType::Extraction)
                    .build(),
            )
            .await
            .expect("insert task");

        // Claim the task via the repository (simulating a previous
        // worker instance) and immediately backdate the heartbeat
        // beyond the task_timeout to trigger reclaim.
        let claimed = PgTaskRepository
            .claim(&mut conn, 1, "crashed-worker")
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1);

        backdate_task_heartbeat(&mut conn, task.id(), std::time::Duration::from_secs(120)).await;

        task.id()
    };

    let config = test_config();
    let token = CancellationToken::new();
    let worker = build_test_worker(pool, token.clone(), config, None, None);

    let worker_handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    // Let the reclaim loop run (reclaim_interval = 1s in test config).
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    token.cancel();
    let _ = worker_handle.await;

    let mut conn = raw_conn(ctx).await;
    let task = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("find task");

    // Reclaim requeues the task (retry_count=1).  The worker's poll
    // loop may subsequently re-dispatch it before we assert — the
    // extraction stub fails, handle_stage_failure requeues again
    // (retry_count=2).  Both outcomes prove reclaim ran.
    assert_eq!(
        task.status(),
        TaskStatus::Queued,
        "task should be requeued after reclaim",
    );
    assert!(
        task.retry_count() >= 1,
        "retry count should be at least 1 (reclaim incremented it)",
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
        let job = PgJobRepository
            .insert(
                &mut conn,
                &a_new_job()
                    .project_id(project_id)
                    .principal_id(principal_id)
                    .extraction_prompt_version_id(pv_id)
                    .triage_prompt_version_id(pv_id)
                    .relation_prompt_version_id(pv_id)
                    .build(),
            )
            .await
            .expect("insert job");
        let task = PgTaskRepository
            .insert(
                &mut conn,
                &a_new_task()
                    .job_id(job.id())
                    .task_type(TaskType::Extraction)
                    .build(),
            )
            .await
            .expect("insert task");

        // Pre-set retry_count to max_retries so reclaim triggers
        // dead-lettering.
        sqlx::query("UPDATE tasks SET retry_count = $1 WHERE id = $2")
            .bind(i32::try_from(config.task_max_retries).unwrap())
            .bind(task.id().inner())
            .execute(&mut conn)
            .await
            .expect("set retry_count");

        // Claim and backdate heartbeat.
        let claimed = PgTaskRepository
            .claim(&mut conn, 1, "crashed-worker")
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1);

        backdate_task_heartbeat(&mut conn, task.id(), std::time::Duration::from_secs(120)).await;

        (job.id(), task.id())
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(pool, token.clone(), config, None, None);

    let worker_handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    token.cancel();
    let _ = worker_handle.await;

    let mut conn = raw_conn(ctx).await;

    let task = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("find task");

    assert_eq!(
        task.status(),
        TaskStatus::DeadLetter,
        "task should be dead-lettered",
    );
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

    // The reclaim loop should have transitioned the parent job to
    // Failed via fail_stale_dead_lettered_jobs.
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("find job");

    assert_eq!(job.status(), JobStatus::Failed, "job should be failed");
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
        let job = PgJobRepository
            .insert(
                &mut conn,
                &a_new_job()
                    .project_id(project_id)
                    .principal_id(principal_id)
                    .extraction_prompt_version_id(pv_id)
                    .triage_prompt_version_id(pv_id)
                    .relation_prompt_version_id(pv_id)
                    .build(),
            )
            .await
            .expect("insert job");
        let task = PgTaskRepository
            .insert(
                &mut conn,
                &a_new_task()
                    .job_id(job.id())
                    .task_type(TaskType::Extraction)
                    .build(),
            )
            .await
            .expect("insert task");

        // Claim and backdate heartbeat (simulating crash).
        let claimed = PgTaskRepository
            .claim(&mut conn, 1, "crashed-worker")
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1);

        backdate_task_heartbeat(&mut conn, task.id(), std::time::Duration::from_secs(120)).await;

        task.id()
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
        let job = PgJobRepository
            .insert(
                &mut conn,
                &a_new_job()
                    .project_id(project_id)
                    .principal_id(principal_id)
                    .extraction_prompt_version_id(pv_id)
                    .triage_prompt_version_id(pv_id)
                    .relation_prompt_version_id(pv_id)
                    .build(),
            )
            .await
            .expect("insert job");
        let task = PgTaskRepository
            .insert(
                &mut conn,
                &a_new_task()
                    .job_id(job.id())
                    .task_type(TaskType::Extraction)
                    .build(),
            )
            .await
            .expect("insert task");
        task.id()
    };

    // Mock with a long delay keeps the extraction stage in-flight long
    // enough for the heartbeat to detect an external reclaim.
    let inference = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response("delayed"),
                Some(MockOptions {
                    delay: Some(std::time::Duration::from_secs(30)),
                }),
            )
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let inference_ref = Arc::clone(&inference);

    let config = WorkerConfig {
        max_concurrent_tasks: 1,
        poll_interval_seconds: 1,
        task_timeout_seconds: 60,
        task_max_retries: 3,
        heartbeat_interval_seconds: 1,
        reclaim_interval_seconds: 120,
        max_candidates_per_job: 20,
        triage_search_limit: 10,
        include_llm_content: false,
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool,
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

    // Wait for the worker to claim and begin dispatching.  After 2s
    // the extraction stage should be blocked inside the 30s mock delay.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(
        inference_ref.call_count() >= 1,
        "extraction stage should have called the provider",
    );

    // Simulate external reclaim: backdate the heartbeat far beyond the
    // timeout window, then call reclaim_stale to requeue the task.
    // This clears the claim_token, causing the next heartbeat tick to
    // return 0 rows and fire the ownership_lost signal.
    {
        let mut conn = raw_conn(ctx).await;
        backdate_task_heartbeat(&mut conn, task_id, std::time::Duration::from_secs(120)).await;

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

    // Heartbeat interval is 1s — give it time to detect the loss.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    token.cancel();
    let _ = worker_handle.await;

    let elapsed = start.elapsed();

    // If ownership loss was NOT detected, the 30s mock delay would
    // have to complete before the worker moved on.  The total test
    // time being well under 30s proves the heartbeat interrupted the
    // stage early.
    assert!(
        elapsed < std::time::Duration::from_secs(15),
        "expected ownership loss to abort the stage early, but test took {elapsed:?}",
    );

    let mut conn = raw_conn(ctx).await;
    let task = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("find task");

    // The task retains the state set by reclaim_stale — Queued with
    // retry_count incremented and error_kind=HeartbeatExpired.
    // handle_stage_failure detected the claim_token mismatch (0 rows
    // affected) and correctly declined to overwrite.
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
