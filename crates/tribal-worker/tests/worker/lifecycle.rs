use super::common::*;

/// Verifies that when a stage stub fails, the task is re-queued with
/// an incremented retry count, the correct error kind, and a future
/// `available_at`.
#[tokio::test]
async fn test_retry_path_increments_retry_count() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "retry").await;

    let (job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
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

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "dead-letter").await;

    let (job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        let (job_id, task_id) = seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await;

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

    let (task, job) = tokio::join!(
        poll_task_status(&pool, task_id, TaskStatus::DeadLetter, POLL_SETTLE),
        poll_job_status(&pool, job_id, JobStatus::Failed, POLL_SETTLE),
    );

    token.cancel();
    let _ = handle.await;

    assert_eq!(
        task.error_kind(),
        Some(TaskErrorKind::ProviderError),
        "error kind should be provider_error",
    );
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

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "concurrency").await;

    // Seed more tasks than max_concurrent.
    let task_count = max_concurrent + 2;
    {
        let mut conn = raw_conn(ctx).await;

        let fingerprint_hash = upsert_system_fingerprint(
            &mut conn,
            &a_new_system_fingerprint()
                .extraction_system_prompt_version_id(system_pv_id)
                .extraction_user_prompt_version_id(user_pv_id)
                .triage_system_prompt_version_id(system_pv_id)
                .triage_user_prompt_version_id(user_pv_id)
                .relation_system_prompt_version_id(system_pv_id)
                .relation_user_prompt_version_id(user_pv_id)
                .build(),
        )
        .await;

        for i in 0..task_count {
            let job = PgJobRepository
                .insert(
                    &mut conn,
                    &a_new_job()
                        .project_id(project_id)
                        .principal_id(principal_id)
                        .extraction_system_prompt_version_id(system_pv_id)
                        .extraction_user_prompt_version_id(user_pv_id)
                        .triage_system_prompt_version_id(system_pv_id)
                        .triage_user_prompt_version_id(user_pv_id)
                        .relation_system_prompt_version_id(system_pv_id)
                        .relation_user_prompt_version_id(user_pv_id)
                        .system_fingerprint_hash(fingerprint_hash.clone())
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

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "reclaim-requeue").await;

    let task_id = {
        let mut conn = raw_conn(ctx).await;
        let (_job_id, task_id) = seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await;

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

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "reclaim-dead-letter").await;

    let (job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        let (job_id, task_id) = seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await;

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

    // Poll for both the task dead-letter and job failure concurrently.
    // Both must complete before cancelling — cancellation aborts the
    // reclaim loop, which would prevent heal_dead_lettered_jobs from
    // running if it hasn't completed.
    let (task, job) = tokio::join!(
        poll_task_status(&pool, task_id, TaskStatus::DeadLetter, MULTI_CYCLE_SETTLE),
        poll_job_status(&pool, job_id, JobStatus::Failed, MULTI_CYCLE_SETTLE),
    );

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

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "startup-reclaim").await;

    let task_id = {
        let mut conn = raw_conn(ctx).await;
        let (_job_id, task_id) = seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await;

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

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "ownership-loss").await;

    let task_id = {
        let mut conn = raw_conn(ctx).await;
        let (_job_id, task_id) = seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await;
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
        // Use a 1 s heartbeat interval to avoid racing with the
        // manual backdate + reclaim injection below.  A 100 ms
        // interval can refresh the heartbeat between the backdate
        // and reclaim_stale calls, silently undoing the backdate.
        heartbeat_interval_ms: 1_000,
        // Disable reclaim sweep so it does not interfere with the
        // manual reclaim injection below.
        reclaim_interval_ms: 120_000,
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
