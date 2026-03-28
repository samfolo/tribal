use super::{
    common::*,
    fixtures::{
        batch_index_relation_response_json, extraction_response_json,
        triage_duplicate_response_json, triage_novel_response_json,
    },
};

/// Verifies that the extraction stage records a single completion token
/// usage entry with correct provider, model, token counts, prompt version
/// IDs, and trace context.
#[tokio::test]
async fn test_extraction_records_token_usage() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "extraction-token-usage").await;

    let candidates = vec![a_candidate().build()];
    let response_json = extraction_response_json(&candidates, &[]);

    // Inline job creation to set trace_context (seed_extraction_job
    // does not expose this parameter).
    let (job_id, _task_id) = {
        let mut conn = raw_conn(ctx).await;
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
                    .trace_context(Some(
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
                    ))
                    .build(),
            )
            .await
            .expect("setup: insert job");
        let task = PgTaskRepository
            .insert(
                &mut conn,
                &a_new_task()
                    .job_id(job.id())
                    .task_type(TaskType::Extraction)
                    .build(),
            )
            .await
            .expect("setup: insert task");
        (job.id(), task.id())
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

    poll_job_status(&pool, job_id, JobStatus::Triaging, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(ctx).await;
    let records = PgTokenUsageRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find token usage");

    assert_eq!(
        records.len(),
        1,
        "extraction should produce 1 token usage record"
    );

    let r = &records[0];
    assert_eq!(r.stage(), PipelineStage::Extraction);
    assert_eq!(r.purpose(), None);
    assert_eq!(r.provider(), "mock");
    assert_eq!(r.model(), "mock-model");
    assert_eq!(r.tokens_input(), 100);
    assert_eq!(r.tokens_output(), 50);
    assert_eq!(r.tokens_cache_read(), 0);
    assert_eq!(r.tokens_cache_write(), 0);
    assert_eq!(r.tokens_total(), 150);
    assert_eq!(r.latency_ms(), 0);
    assert_eq!(r.job_id(), Some(job_id));
    assert_eq!(r.attempt(), 0);
    assert_eq!(r.system_prompt_version_id(), Some(system_pv_id));
    assert_eq!(r.user_prompt_version_id(), Some(user_pv_id));
    assert_eq!(r.trace_id(), Some("4bf92f3577b34da6a3ce929d0e0e4736"));

    teardown(ctx).await;
}

/// Runs an extraction job with the given `trace_context` and asserts that
/// the worker completes the stage and records exactly one token usage entry.
async fn assert_extraction_with_trace_context(trace_context: Option<String>, label: &str) {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, label).await;

    let candidates = vec![a_candidate().build()];
    let response_json = extraction_response_json(&candidates, &[]);

    let (job_id, _task_id) = {
        let mut conn = raw_conn(ctx).await;
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
                    .trace_context(trace_context)
                    .build(),
            )
            .await
            .expect("setup: insert job");
        let task = PgTaskRepository
            .insert(
                &mut conn,
                &a_new_task()
                    .job_id(job.id())
                    .task_type(TaskType::Extraction)
                    .build(),
            )
            .await
            .expect("setup: insert task");
        (job.id(), task.id())
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

    poll_job_status(&pool, job_id, JobStatus::Triaging, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(ctx).await;
    let records = PgTokenUsageRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find token usage");

    assert_eq!(records.len(), 1, "{label}: expected 1 token usage record");

    let r = &records[0];
    assert_eq!(r.stage(), PipelineStage::Extraction);
    assert_eq!(r.job_id(), Some(job_id));
    // Without an OTel layer the fallback to current_trace_id() also
    // returns None, so token_usage.trace_id is None in test environments.
    assert!(
        r.trace_id().is_none(),
        "{label}: trace_id should be None without an OTel layer",
    );

    teardown(ctx).await;
}

/// The worker completes extraction and records token usage when
/// `trace_context` is null on the job row.
#[tokio::test]
async fn test_extraction_records_token_usage_with_null_trace_context() {
    assert_extraction_with_trace_context(None, "null trace_context").await;
}

/// The worker completes extraction and records token usage when
/// `trace_context` contains a malformed (non-W3C) string.
#[tokio::test]
async fn test_extraction_records_token_usage_with_malformed_trace_context() {
    assert_extraction_with_trace_context(
        Some("not-a-valid-traceparent".to_owned()),
        "malformed trace_context",
    )
    .await;
}

/// Verifies that the triage novel path records exactly 4 token usage
/// entries: 1 candidate embedding, 1 triage completion, and 2 tag
/// embeddings (one per unregistered tag).
#[tokio::test]
async fn test_triage_novel_records_token_usage() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "triage-novel-token-usage").await;

    let candidates = vec![
        a_candidate()
            .content("Token usage test candidate".to_owned())
            .suggested_tags(vec!["alpha".to_owned(), "beta".to_owned()])
            .build(),
    ];

    let (job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        seed_triage_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
            &candidates,
        )
        .await
    };

    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.1_f32; 768]), None)
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

    poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(ctx).await;
    let records = PgTokenUsageRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find token usage");

    assert_eq!(
        records.len(),
        4,
        "triage novel with 2 tags should produce 4 token usage records",
    );

    let candidate_embeds: Vec<_> = records
        .iter()
        .filter(|r| {
            r.stage() == PipelineStage::Embedding
                && r.purpose() == Some(EmbeddingPurpose::Candidate)
        })
        .collect();
    let tag_embeds: Vec<_> = records
        .iter()
        .filter(|r| {
            r.stage() == PipelineStage::Embedding && r.purpose() == Some(EmbeddingPurpose::Tag)
        })
        .collect();
    let completions: Vec<_> = records
        .iter()
        .filter(|r| r.stage() == PipelineStage::Triage && r.purpose().is_none())
        .collect();

    assert_eq!(
        candidate_embeds.len(),
        1,
        "should have 1 candidate embedding record"
    );
    assert_eq!(tag_embeds.len(), 2, "should have 2 tag embedding records");
    assert_eq!(
        completions.len(),
        1,
        "should have 1 triage completion record"
    );

    // Assert completion record fields.
    let c = completions[0];
    assert_eq!(c.provider(), "mock");
    assert_eq!(c.model(), "mock-model");
    assert_eq!(c.tokens_input(), 100);
    assert_eq!(c.tokens_output(), 50);
    assert_eq!(c.tokens_total(), 150);
    assert_eq!(c.system_prompt_version_id(), Some(system_pv_id));
    assert_eq!(c.user_prompt_version_id(), Some(user_pv_id));
    assert_eq!(c.job_id(), Some(job_id));
    assert_eq!(c.task_id(), Some(task_id));
    assert_eq!(c.attempt(), 0);

    // Assert embedding records have no prompt version IDs.
    for emb in candidate_embeds.iter().chain(tag_embeds.iter()) {
        assert_eq!(emb.provider(), "mock");
        assert_eq!(emb.model(), "mock-embed-model");
        assert_eq!(emb.tokens_input(), 10);
        assert_eq!(emb.tokens_output(), 0);
        assert_eq!(emb.tokens_total(), 10);
        assert_eq!(emb.system_prompt_version_id(), None);
        assert_eq!(emb.user_prompt_version_id(), None);
    }

    teardown(ctx).await;
}

/// Verifies that the triage duplicate path records exactly 2 token usage
/// entries: 1 candidate embedding and 1 triage completion (no tag
/// embeddings since duplicates skip tag resolution).
#[tokio::test]
async fn test_triage_duplicate_records_token_usage() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let mut conn = raw_conn(ctx).await;
    let seed_result = Seed::new()
        .define_project("proj", "git@github.com:test/triage-dup-token-usage.git")
        .define_principal("user", "user:triage-dup-token-usage")
        .define_prompt_version("system-pv", a_new_prompt_version().build())
        .define_prompt_version(
            "user-pv",
            a_new_prompt_version()
                .role(tribal_domain::PromptRole::User)
                .content_hash("c".repeat(64))
                .content("test user prompt content".to_owned())
                .build(),
        )
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
    let system_pv_id = seed_result.prompt_version_id("system-pv");
    let user_pv_id = seed_result.prompt_version_id("user-pv");

    let seeded_embedding = PgEmbeddingRepository
        .find_by_knowledge_item_id(&mut conn, ki_id, "mock-model")
        .await
        .expect("find seeded embedding")
        .expect("seeded embedding should exist");
    let embedding_vector = seeded_embedding.embedding().to_vec();

    let candidates = vec![
        a_candidate()
            .content("duplicate content".to_owned())
            .build(),
    ];

    let (job_id, task_id) = seed_triage_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
        &candidates,
    )
    .await;
    drop(conn);

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

    poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(ctx).await;
    let records = PgTokenUsageRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find token usage");

    assert_eq!(
        records.len(),
        2,
        "triage duplicate should produce 2 token usage records",
    );

    let candidate_embeds: Vec<_> = records
        .iter()
        .filter(|r| {
            r.stage() == PipelineStage::Embedding
                && r.purpose() == Some(EmbeddingPurpose::Candidate)
        })
        .collect();
    let completions: Vec<_> = records
        .iter()
        .filter(|r| r.stage() == PipelineStage::Triage && r.purpose().is_none())
        .collect();

    assert_eq!(
        candidate_embeds.len(),
        1,
        "should have 1 candidate embedding record"
    );
    assert_eq!(
        completions.len(),
        1,
        "should have 1 triage completion record"
    );

    // Verify no tag embeddings were recorded.
    let tag_embeds: Vec<_> = records
        .iter()
        .filter(|r| {
            r.stage() == PipelineStage::Embedding && r.purpose() == Some(EmbeddingPurpose::Tag)
        })
        .collect();
    assert!(
        tag_embeds.is_empty(),
        "duplicate path should not produce tag embeddings"
    );

    // Assert completion has prompt version IDs.
    let c = completions[0];
    assert_eq!(c.system_prompt_version_id(), Some(system_pv_id));
    assert_eq!(c.user_prompt_version_id(), Some(user_pv_id));

    // Assert embedding has no prompt version IDs.
    let e = candidate_embeds[0];
    assert_eq!(e.system_prompt_version_id(), None);
    assert_eq!(e.user_prompt_version_id(), None);

    teardown(ctx).await;
}

/// Verifies that the relation stage records a single completion token
/// usage entry with correct fields and `trace_id = None` (no trace
/// context on the job).
#[tokio::test]
async fn test_relation_records_token_usage() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "relation-token-usage").await;

    let candidates = vec![
        a_candidate().content("first item".to_owned()).build(),
        a_candidate().content("second item".to_owned()).build(),
    ];
    let relation_hints = vec![a_relation_hint().build()];

    let (job_id, _task_id, ki_ids) = {
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
                a_completion_response(batch_index_relation_response_json(ki_ids.len())),
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

    poll_job_status(&pool, job_id, JobStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(ctx).await;
    let records = PgTokenUsageRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find token usage");

    assert_eq!(
        records.len(),
        1,
        "relation should produce 1 token usage record"
    );

    let r = &records[0];
    assert_eq!(r.stage(), PipelineStage::Relation);
    assert_eq!(r.purpose(), None);
    assert_eq!(r.provider(), "mock");
    assert_eq!(r.model(), "mock-model");
    assert_eq!(r.tokens_input(), 100);
    assert_eq!(r.tokens_output(), 50);
    assert_eq!(r.tokens_total(), 150);
    assert_eq!(r.latency_ms(), 0);
    assert_eq!(r.job_id(), Some(job_id));
    assert_eq!(r.attempt(), 0);
    assert_eq!(r.system_prompt_version_id(), Some(system_pv_id));
    assert_eq!(r.user_prompt_version_id(), Some(user_pv_id));
    assert_eq!(r.trace_id(), None, "job has no trace context");

    teardown(ctx).await;
}

/// Verifies that the token usage `attempt` field reflects the task's
/// retry count at execution time.
#[tokio::test]
async fn test_token_usage_records_retry_attempt() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "retry-attempt-token-usage").await;

    let candidates = vec![a_candidate().build()];
    let response_json = extraction_response_json(&candidates, &[]);

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
        set_retry_count(&mut conn, task_id, 1).await;
        (job_id, task_id)
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

    poll_job_status(&pool, job_id, JobStatus::Triaging, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(ctx).await;
    let records = PgTokenUsageRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find token usage");

    assert_eq!(records.len(), 1, "should have 1 token usage record");
    assert_eq!(
        records[0].attempt(),
        1,
        "attempt should reflect the task's retry count",
    );
    assert_eq!(records[0].task_id(), Some(task_id));

    teardown(ctx).await;
}

/// Verifies that tag embedding backfill during `worker.startup()` records
/// token usage entries with `job_id = None`, `task_id = None`,
/// `stage = Embedding`, and `purpose = Tag`.
#[tokio::test]
async fn test_backfill_records_token_usage() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    {
        let mut conn = raw_conn(ctx).await;
        Seed::new()
            .define_project("proj", "git@github.com:test/backfill-token-usage.git")
            .define_principal("user", "user:backfill-token-usage")
            .define_tag("alpha")
            .define_tag("beta")
            .execute(&mut conn)
            .await;
    }

    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.1_f32; 768]), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let token = CancellationToken::new();
    let worker = build_test_worker(pool, token.clone(), test_config(), None, Some(embedding));

    worker.startup().await.expect("startup");

    let mut conn = raw_conn(ctx).await;
    let records = PgTokenUsageRepository
        .find_all_for_test(&mut conn)
        .await
        .expect("find all token usage");

    assert_eq!(
        records.len(),
        2,
        "backfill should produce 2 token usage records (one per tag)",
    );

    for r in &records {
        assert_eq!(r.job_id(), None, "backfill records have no job");
        assert_eq!(r.task_id(), None, "backfill records have no task");
        assert_eq!(r.stage(), PipelineStage::Embedding);
        assert_eq!(r.purpose(), Some(EmbeddingPurpose::Tag));
        assert_eq!(r.attempt(), 0);
        assert_eq!(r.provider(), "mock");
        assert_eq!(r.model(), "mock-embed-model");
        assert_eq!(r.tokens_input(), 10);
        assert_eq!(r.tokens_output(), 0);
        assert_eq!(r.tokens_total(), 10);
        assert_eq!(r.system_prompt_version_id(), None);
        assert_eq!(r.user_prompt_version_id(), None);
    }

    teardown(ctx).await;
}
