//! Integration tests for the worker poll-claim-dispatch loop.
//!
//! Each test seeds data via committed transactions on the shared pool,
//! constructs a [`Worker`] with mock providers (whose stubs always
//! fail), runs the worker briefly, then asserts on task and job state.

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
use tribal_inference::{ProviderKey, ProviderLimits, ProviderRegistry, RequestClass};
use tribal_test_utils::{
    MockEmbeddingProvider, MockInferenceProvider, a_new_job, a_new_principal, a_new_project,
    a_new_task, test_context,
};
use tribal_worker::{Worker, WorkerConfig};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const WORKER_INSTANCE: &str = "test-worker";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal, project, and prompt_version, returning the IDs
/// needed to create a job.
async fn setup_prerequisites(
    conn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (PrincipalId, ProjectId, PromptVersionId) {
    let principal = PgPrincipalRepository
        .insert(
            conn,
            &a_new_principal()
                .principal_key(format!("user:worker-test-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal");

    let project = PgProjectRepository
        .insert(
            conn,
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
    .fetch_one(&mut *conn)
    .await
    .expect("insert prompt_version");

    (principal.id(), project.id(), PromptVersionId::from(pv_id))
}

/// Builds a [`Worker`] with mock providers and short timeouts suitable
/// for integration testing.
fn build_test_worker(
    pool: sqlx::PgPool,
    cancellation_token: CancellationToken,
    config: WorkerConfig,
) -> Arc<Worker> {
    let inference = Arc::new(MockInferenceProvider::builder().build());
    let embedding = Arc::new(MockEmbeddingProvider::builder().build());

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
    let ctx = test_context().await;
    let pool = ctx.pool().clone();

    let (principal_id, project_id, pv_id) = {
        let mut conn = pool.acquire().await.expect("acquire");
        setup_prerequisites(&mut conn, "retry").await
    };

    let (job_id, task_id) = {
        let mut conn = pool.acquire().await.expect("acquire");
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
    let worker = build_test_worker(pool.clone(), token.clone(), test_config());

    // Run the worker for long enough to claim and fail the task.
    let worker_handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    // Wait for the worker to process the task, then cancel.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    token.cancel();
    let _ = worker_handle.await;

    // Assert: the task should be re-queued with retry_count = 1.
    let mut conn = pool.acquire().await.expect("acquire");
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

    // The job should still exist (not failed, since retries remain).
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("find job");
    assert_ne!(
        job.status(),
        JobStatus::Failed,
        "job should not be failed after first retry",
    );
}

/// Verifies that a task at its retry limit is dead-lettered and the
/// parent job transitions to Failed with a Failure outcome.
#[tokio::test]
async fn test_dead_letter_path_transitions_task_and_job() {
    let ctx = test_context().await;
    let pool = ctx.pool().clone();
    let config = test_config();

    let (principal_id, project_id, pv_id) = {
        let mut conn = pool.acquire().await.expect("acquire");
        setup_prerequisites(&mut conn, "dead-letter").await
    };

    let (job_id, task_id) = {
        let mut conn = pool.acquire().await.expect("acquire");
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
            .execute(&mut *conn)
            .await
            .expect("set retry_count");

        (job.id(), task.id())
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(pool.clone(), token.clone(), config);

    let worker_handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    token.cancel();
    let _ = worker_handle.await;

    let mut conn = pool.acquire().await.expect("acquire");

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
}

/// Verifies that the worker never exceeds `max_concurrent_tasks`
/// in-flight tasks, using the peak concurrency spy built into the
/// `Worker`.
#[tokio::test]
async fn test_concurrency_limit_respected() {
    let ctx = test_context().await;
    let pool = ctx.pool().clone();
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

    let (principal_id, project_id, pv_id) = {
        let mut conn = pool.acquire().await.expect("acquire");
        setup_prerequisites(&mut conn, "concurrency").await
    };

    // Seed more tasks than max_concurrent.
    let task_count = max_concurrent + 2;
    {
        let mut conn = pool.acquire().await.expect("acquire");
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
    let worker = build_test_worker(pool.clone(), token.clone(), config);

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
}
