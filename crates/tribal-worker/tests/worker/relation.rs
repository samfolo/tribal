use super::{common::*, fixtures::context_index_relation_response_json};

/// Verifies the happy path: the relation stage calls the LLM, parses
/// the response, commits relations, sets `committed_batch_id`, and
/// transitions the job to `Completed` with a `Success` outcome.
#[tokio::test]
async fn test_relation_stage_commits_relations_and_completes_job() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "relation-happy-path").await;

    let candidates = vec![
        a_candidate()
            .content("Rust has zero-cost abstractions".to_owned())
            .build(),
        a_candidate()
            .content("Ownership prevents data races".to_owned())
            .build(),
    ];
    let relation_hints = vec![a_relation_hint().build()];

    let (job_id, task_id, ki_ids) = {
        let mut conn = raw_conn(ctx).await;
        seed_relation_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
            &candidates,
            &relation_hints,
        )
        .await
    };

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response(context_index_relation_response_json(ki_ids.len())),
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

    let (task, job) = tokio::join!(
        poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE),
        poll_job_status(&pool, job_id, JobStatus::Completed, POLL_SETTLE),
    );

    token.cancel();
    let _ = handle.await;

    assert_eq!(task.status(), TaskStatus::Completed);
    assert_eq!(job.status(), JobStatus::Completed);
    assert_eq!(
        job.outcome(),
        Some(JobOutcome::Success),
        "all candidates were Created so outcome should be Success",
    );
    assert!(
        job.committed_batch_id().is_some(),
        "committed_batch_id should be set",
    );

    // Verify relations were committed.
    let mut conn = raw_conn(ctx).await;
    let outbound = PgRelationRepository
        .find_outbound(&mut conn, ki_ids[0], None)
        .await
        .expect("find outbound relations");
    assert_eq!(
        outbound.len(),
        1,
        "should have one outbound relation from first item",
    );
    assert_eq!(outbound[0].target_id(), ki_ids[1]);
    assert_eq!(
        outbound[0].justification(),
        Some("Test relation"),
        "justification should be persisted",
    );

    teardown(ctx).await;
}

/// Verifies that when all triage outcomes are duplicates, relations
/// between matched existing items are committed and the job completes
/// with an `Empty` outcome.
#[tokio::test]
async fn test_relation_stage_all_duplicates_empty_outcome() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let candidates = vec![a_candidate().build(), a_candidate().build()];

    let (job_id, task_id, matched_ki_a, matched_ki_b) = {
        let mut conn = raw_conn(ctx).await;

        // Seed pre-existing data via Seed builder.
        let seed_result = Seed::new()
            .define_project("proj", "git@github.com:test/relation-all-dup.git")
            .define_principal("user", "user:relation-all-dup")
            .define_prompt_version("system-pv", a_new_prompt_version().build())
            .define_prompt_version(
                "user-pv",
                a_new_prompt_version()
                    .role(tribal_domain::PromptRole::User)
                    .content_hash("c".repeat(64))
                    .content("test user prompt content".to_owned())
                    .build(),
            )
            .as_principal("user")
            .for_project("proj", |store| {
                store
                    .add_item(
                        "existing_a",
                        item(KnowledgeKind::Fact, "existing item A content").skip_embed(),
                    )
                    .observe("existing_a", SourceType::AgentMediated)
                    .add_item(
                        "existing_b",
                        item(KnowledgeKind::Fact, "existing item B content").skip_embed(),
                    )
                    .observe("existing_b", SourceType::AgentMediated);
            })
            .execute(&mut conn)
            .await;

        let principal_id = seed_result.principal_id("user");
        let project_id = seed_result.project_id("proj");
        let system_pv_id = seed_result.prompt_version_id("system-pv");
        let user_pv_id = seed_result.prompt_version_id("user-pv");
        let matched_ki_a = seed_result.item_id("existing_a");
        let matched_ki_b = seed_result.item_id("existing_b");
        let obs_id_a = seed_result.observation_ids("existing_a")[0];
        let obs_id_b = seed_result.observation_ids("existing_b")[0];

        // Build a job in Relating status with two Duplicate triage outcomes.
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
                    .system_fingerprint_hash(fingerprint_hash)
                    .build(),
            )
            .await
            .expect("setup: insert job");
        let job_id = job.id();

        PgTaskRepository
            .insert_for_test(
                &mut conn,
                &a_new_task()
                    .job_id(job_id)
                    .task_type(TaskType::Extraction)
                    .build(),
                TaskStatus::Completed,
            )
            .await
            .expect("setup: insert extraction task");

        PgExtractionResultRepository
            .insert(
                &mut conn,
                &a_new_extraction_result()
                    .job_id(job_id)
                    .candidates(candidates_json(&candidates))
                    .build(),
            )
            .await
            .expect("setup: insert extraction result");

        PgJobRepository
            .update_batch_size(&mut conn, job_id, 2, 2)
            .await
            .expect("setup: update batch size");

        PgJobRepository
            .update_status(
                &mut conn,
                job_id,
                &JobStatusTransition::builder()
                    .status(JobStatus::Triaging)
                    .build(),
            )
            .await
            .expect("setup: transition to triaging");

        for (idx, (matched_id, obs_id)) in [(matched_ki_a, obs_id_a), (matched_ki_b, obs_id_b)]
            .iter()
            .enumerate()
        {
            let batch_index = u32::try_from(idx).unwrap();

            PgTaskRepository
                .insert_for_test(
                    &mut conn,
                    &a_new_task()
                        .job_id(job_id)
                        .task_type(TaskType::Triage)
                        .batch_index(Some(batch_index))
                        .build(),
                    TaskStatus::Completed,
                )
                .await
                .expect("setup: insert triage task");

            PgTriageResultRepository
                .insert(
                    &mut conn,
                    &a_new_triage_result_duplicate()
                        .job_id(job_id)
                        .batch_index(batch_index)
                        .outcome(TriageOutcome::Duplicate {
                            observation_id: *obs_id,
                            matched_item_id: *matched_id,
                        })
                        .build(),
                )
                .await
                .expect("setup: insert triage result");
        }

        PgJobRepository
            .update_status(
                &mut conn,
                job_id,
                &JobStatusTransition::builder()
                    .status(JobStatus::Relating)
                    .build(),
            )
            .await
            .expect("setup: transition to relating");

        let relation_task = PgTaskRepository
            .insert(
                &mut conn,
                &a_new_task()
                    .job_id(job_id)
                    .task_type(TaskType::Relation)
                    .build(),
            )
            .await
            .expect("setup: insert relation task");

        (job_id, relation_task.id(), matched_ki_a, matched_ki_b)
    };

    // Mock returns a relation between the two existing items.
    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response(context_index_relation_response_json(2)),
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

    let job = poll_job_status(&pool, job_id, JobStatus::Completed, POLL_SETTLE).await;

    token.cancel();
    let _ = handle.await;

    assert_eq!(job.status(), JobStatus::Completed);
    assert_eq!(
        job.outcome(),
        Some(JobOutcome::Empty),
        "all duplicates should produce Empty outcome",
    );
    assert!(
        job.committed_batch_id().is_some(),
        "committed_batch_id should be set",
    );

    // Task completion is atomic with the job transition — no polling needed.
    let mut conn = raw_conn(ctx).await;
    let task = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("find relation task");
    assert_eq!(
        task.status(),
        TaskStatus::Completed,
        "relation task should be completed atomically with job",
    );

    // Verify relations between existing items were committed.
    let outbound = PgRelationRepository
        .find_outbound(&mut conn, matched_ki_a, None)
        .await
        .expect("find outbound relations");
    assert_eq!(
        outbound.len(),
        1,
        "should have one relation between existing items",
    );
    assert_eq!(outbound[0].target_id(), matched_ki_b);

    teardown(ctx).await;
}

/// Verifies the idempotency guard: when `committed_batch_id` is already
/// set, the relation stage skips the LLM call and completes the task
/// without modifying job state.
#[tokio::test]
async fn test_relation_stage_idempotency_skip() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "relation-idempotency").await;

    let candidates = vec![a_candidate().build()];

    let task_id = {
        let mut conn = raw_conn(ctx).await;
        let (job_id, task_id, _) = seed_relation_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
            &candidates,
            &[],
        )
        .await;

        // Pre-set committed_batch_id to simulate a previous successful
        // relation commit.
        PgJobRepository
            .set_committed_batch_id(&mut conn, job_id, RelationBatchId::new())
            .await
            .expect("setup: set committed_batch_id")
            .expect("setup: batch_id should not be set yet");

        task_id
    };

    // The inference provider should never be called.
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

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference as Arc<dyn InferenceProvider>),
        None,
    );
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;

    token.cancel();
    let _ = handle.await;

    assert_eq!(task.status(), TaskStatus::Completed);

    assert_eq!(
        inference_ref.call_count(),
        0,
        "inference should not be called when committed_batch_id is set",
    );

    teardown(ctx).await;
}

/// Verifies that an unparseable LLM response causes a `ParseError`
/// requeue, matching the extraction and triage parse-failure tests.
#[tokio::test]
async fn test_relation_parse_failure() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "relation-parse-failure").await;

    let candidates = vec![a_candidate().build()];

    let task_id = {
        let mut conn = raw_conn(ctx).await;
        let (_job_id, task_id, _) = seed_relation_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
            &candidates,
            &[],
        )
        .await;
        task_id
    };

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response("this is not valid relation json"),
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

    let task = poll_until(
        "relation task requeued with parse error",
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

/// Verifies that when the LLM returns only `Supersedes` edges (all
/// dropped by normalisation), the job still completes with zero
/// committed relations.
#[tokio::test]
async fn test_relation_stage_all_edges_dropped() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "relation-all-dropped").await;

    let candidates = vec![
        a_candidate().content("First candidate".to_owned()).build(),
        a_candidate().content("Second candidate".to_owned()).build(),
    ];

    let (job_id, _task_id, ki_ids) = {
        let mut conn = raw_conn(ctx).await;
        seed_relation_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
            &candidates,
            &[],
        )
        .await
    };

    // Return only Supersedes edges — all will be dropped.
    let supersedes_json = serde_json::json!({
        "relations": [{
            "source": { "kind": "context_index", "context_index": 0 },
            "target": { "kind": "context_index", "context_index": 1 },
            "relation_type": "supersedes",
            "justification": "Should be dropped",
        }]
    })
    .to_string();

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response(supersedes_json), None)
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

    assert_eq!(job.status(), JobStatus::Completed);
    assert_eq!(
        job.outcome(),
        Some(JobOutcome::Success),
        "all Created triage outcomes with zero committed relations is still Success",
    );
    assert!(
        job.committed_batch_id().is_some(),
        "committed_batch_id should be set even with zero relations",
    );

    // Verify no relations were committed.
    let mut conn = raw_conn(ctx).await;
    let outbound = PgRelationRepository
        .find_outbound(&mut conn, ki_ids[0], None)
        .await
        .expect("find outbound relations");
    assert!(
        outbound.is_empty(),
        "no relations should be committed when all edges are dropped",
    );

    teardown(ctx).await;
}
