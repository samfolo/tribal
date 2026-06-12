use super::{
    common::*,
    fixtures::{triage_duplicate_response_json, triage_novel_response_json},
};

/// Verifies the novel path: the triage stage classifies a candidate as
/// novel, creates a knowledge item with embedding and references,
/// registers new tags, and records a Created triage result.
#[tokio::test]
async fn test_triage_novel_path() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "triage-novel").await;

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
    )
    .await;
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
    let emb = find_active_embedding(&mut conn, ki_id)
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

/// Verifies the novel commit path while a reindex is live: with a queued reindex
/// run present, the commit takes the shared cutover lock yet still completes,
/// writing its embedding against the active profile rather than the building
/// target. The drain the lock enables is exercised by the cutover race tests;
/// here the contract is that an uncontended live reindex leaves the ingest
/// path's outcome unchanged.
#[tokio::test]
async fn test_triage_novel_commits_with_a_live_reindex() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "triage-novel-reindex").await;

    // Stand up a building profile and a queued reindex run so the commit path
    // sees a live reindex and takes the shared cutover lock. The building
    // profile is a higher epoch but not yet complete, so the active profile is
    // still the genesis the embedding is written against.
    {
        let mut conn = raw_conn(ctx).await;
        let building = PgEmbeddingProfileRepository
            .insert(&mut conn, &a_new_embedding_profile().build())
            .await
            .expect("insert building profile");
        PgReindexRunRepository
            .insert(
                &mut conn,
                &NewReindexRun::builder()
                    .target_profile_id(building.id())
                    .epoch(building.epoch())
                    .initiated_by_principal_id(principal_id)
                    .build(),
            )
            .await
            .expect("insert reindex run");
    }

    let candidates = vec![
        a_candidate()
            .content("Rust has zero-cost abstractions".to_owned())
            .suggested_tags(vec!["rust".to_owned()])
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
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(
        task.status(),
        TaskStatus::Completed,
        "novel commit completes while a reindex is live",
    );

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

    // The embedding is written against the active profile, never the building
    // target (which carries no rows until the catch-up sweep fills it).
    let emb = find_active_embedding(&mut conn, *item_id)
        .await
        .expect("find embedding");
    assert!(
        emb.is_some(),
        "embedding is written against the active profile",
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

    // Read the deterministic embedding vector back from the database so
    // the mock provider can return the same vector for cosine similarity.
    let seeded_embedding = find_active_embedding(&mut conn, ki_id)
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
                // The seeded item is the sole search hit, so it is at index 0.
                a_completion_response(triage_duplicate_response_json(0)),
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
    )
    .await;
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

/// Verifies the downgrade path end-to-end: a duplicate whose matched index
/// is out of range commits as a novel item with its tags resolved — no panic,
/// parse failure, dead-letter, or observation against the seeded item. Locks
/// the resolve-before-tag-resolution ordering in `run_triage`.
#[tokio::test]
async fn test_triage_duplicate_out_of_range_downgrades_to_novel() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    // Seed an existing item so semantic search returns one hit at index 0;
    // the model references an out-of-range index instead.
    let mut conn = raw_conn(ctx).await;
    let seed_result = Seed::new()
        .define_project("proj", "git@github.com:test/triage-downgrade.git")
        .define_principal("user", "user:triage-downgrade")
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
    let existing_id = seed_result.item_id("existing");
    let system_pv_id = seed_result.prompt_version_id("system-pv");
    let user_pv_id = seed_result.prompt_version_id("user-pv");

    let seeded_embedding = find_active_embedding(&mut conn, existing_id)
        .await
        .expect("find seeded embedding")
        .expect("seeded embedding should exist");
    let embedding_vector = seeded_embedding.embedding().to_vec();

    let candidates = vec![
        a_candidate()
            .content("novel content after downgrade".to_owned())
            .suggested_tags(vec!["rust".to_owned()])
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
                // Index 99 is out of range — only the seeded item (index 0)
                // was retrieved — so the duplicate downgrades to novel.
                a_completion_response(triage_duplicate_response_json(99)),
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
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    // The task completes — no parse failure, dead-letter, or panic. Because
    // the candidate carries suggested tags, completing proves the downgraded
    // candidate ran the novel commit path (resolving tags) without hitting
    // the `resolved_tags` panic.
    assert_eq!(task.status(), TaskStatus::Completed);

    let mut conn = raw_conn(ctx).await;
    let triage_result = PgTriageResultRepository
        .find_by_job_id_and_batch_index(&mut conn, job_id, SEED_TRIAGE_BATCH_INDEX)
        .await
        .expect("find triage result")
        .expect("triage result should exist");

    // The outcome is Created against a new item, not the seeded one — the
    // duplicate downgraded rather than recording an observation.
    assert!(
        matches!(
            triage_result.outcome(),
            TriageOutcome::Created { item_id } if *item_id != existing_id
        ),
        "expected Created with a new item, got {:?}",
        triage_result.outcome(),
    );

    // No observation was recorded against the seeded item.
    let observations = PgItemObservationRepository
        .find_by_knowledge_item_id(&mut conn, existing_id)
        .await
        .expect("find observations");
    assert!(
        observations.is_empty(),
        "downgrade must not observe the seeded item",
    );

    teardown(ctx).await;
}

/// Verifies that an unparseable LLM response causes the triage task
/// to be requeued with a ParseError error kind.
#[tokio::test]
async fn test_triage_parse_failure() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "triage-parse-failure").await;

    let candidates = vec![a_candidate().build()];

    let (_job_id, task_id) = {
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
    )
    .await;
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

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "triage-idempotency").await;

    let candidates = vec![a_candidate().build()];

    let (job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
        let (job_id, task_id) = seed_triage_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
            &candidates,
        )
        .await;

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

    // The triage idempotency check should return early before the
    // embedding call.  We only assert on embedding — not inference —
    // because the triage fan-in may create a relation task whose LLM
    // call races with cancellation.
    let embedding: Arc<MockEmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_exhaust(ExhaustBehaviour::Error(Box::new(|| {
                tribal_inference::InferenceError::provider_unavailable(
                    "mock",
                    "embedding should not be called",
                )
            })))
            .build(),
    );
    let embedding_ref = Arc::clone(&embedding);

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        None,
        Some(embedding as Arc<dyn EmbeddingProvider>),
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(task.status(), TaskStatus::Completed);

    // Verify no embedding calls were made — the idempotency guard
    // short-circuits before the embedding step.
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

// ---------------------------------------------------------------------------
// Tag resolution tests
// ---------------------------------------------------------------------------

/// Creates a 768-dimensional unit vector with 1.0 at the given index.
fn make_test_embedding(dominant_index: usize) -> Vec<f32> {
    let mut v = vec![0.0_f32; 768];
    v[dominant_index] = 1.0;
    v
}

/// Verifies semantic tag resolution during the Novel triage path:
/// a candidate tag that semantically matches an existing registry entry
/// resolves to the existing tag, increments its usage count, and stores
/// embeddings for genuinely new tags.
#[tokio::test]
async fn test_triage_novel_semantic_tag_resolution() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "triage-semantic-tags").await;

    // Pre-seed "rust" in the tag registry with an embedding so semantic
    // matching can find it.
    let rust_embedding = {
        let mut conn = raw_conn(ctx).await;
        Seed::new()
            .define_project("tag-proj", "git@github.com:test/semantic-tags.git")
            .define_principal("tag-user", "user:semantic-tags")
            .set_embedding_model("mock-model", 768)
            .define_tag_with_embedding("rust")
            .execute(&mut conn)
            .await
            .tag_embedding("rust")
    };

    // "rust programming" should semantically match "rust";
    // "performance" should be a new tag.
    let candidates = vec![
        a_candidate()
            .content("Rust has zero-cost abstractions".to_owned())
            .suggested_tags(vec![
                "rust programming".to_owned(),
                "performance".to_owned(),
            ])
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

    // Embedding calls:
    // 1. Candidate content (for similarity search)
    // 2. "rust programming" tag → same vector as "rust" (cosine sim = 1.0)
    // 3. "performance" tag → orthogonal (no semantic match)
    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.1_f32; 768]), None)
            .on_embed(an_embedding_response(rust_embedding), None)
            .on_embed(an_embedding_response(make_test_embedding(1)), None)
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
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(task.status(), TaskStatus::Completed);

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

    // "rust programming" should resolve to "rust" via semantic match.
    let item = PgKnowledgeItemRepository
        .find_by_id(&mut conn, ki_id)
        .await
        .expect("find knowledge item");
    assert!(
        item.tags().contains(&"rust".to_owned()),
        "tags should contain 'rust' (semantic match): {:?}",
        item.tags(),
    );
    assert!(
        !item.tags().contains(&"rust programming".to_owned()),
        "tags should NOT contain 'rust programming': {:?}",
        item.tags(),
    );
    assert!(
        item.tags().contains(&"performance".to_owned()),
        "tags should contain 'performance' (new tag): {:?}",
        item.tags(),
    );

    // Usage count should be incremented for the semantically matched tag.
    let tags = PgTagRegistryRepository
        .find_all(&mut conn)
        .await
        .expect("find tags");
    let rust_tag = tags.iter().find(|t| t.tag() == "rust").expect("find rust");
    assert_eq!(
        rust_tag.usage_count(),
        1,
        "usage_count should be incremented for semantic match",
    );

    // New tag "performance" should have an embedding in tag_embeddings.
    let profile_id = active_embedding_profile(&mut conn).await.id();
    let missing = PgTagEmbeddingRepository
        .find_tags_missing_embeddings(&mut conn, profile_id)
        .await
        .expect("find missing");
    assert!(
        !missing.contains(&"performance".to_owned()),
        "performance should have an embedding: missing = {missing:?}",
    );

    teardown(ctx).await;
}

// ---------------------------------------------------------------------------
// Startup backfill tests
// ---------------------------------------------------------------------------

/// Verifies that startup backfill creates embeddings for tags in the
/// registry that lack them.
#[tokio::test]
async fn test_startup_backfill_embeds_missing_tags() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    // Pre-seed tags without embeddings.
    {
        let mut conn = raw_conn(ctx).await;
        Seed::new()
            .define_project("proj", "git@github.com:test/backfill.git")
            .define_principal("user", "user:backfill")
            .set_embedding_model("mock-model", 768)
            .define_tag("alpha")
            .define_tag("beta")
            .execute(&mut conn)
            .await;
    }

    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(make_test_embedding(0)), None)
            .on_embed(an_embedding_response(make_test_embedding(1)), None)
            .build(),
    );

    let token = CancellationToken::new();
    let worker = build_test_worker(pool, token.clone(), test_config(), None, Some(embedding)).await;

    worker.startup().await.expect("startup");

    let mut conn = raw_conn(ctx).await;
    let profile_id = active_embedding_profile(&mut conn).await.id();
    let missing = PgTagEmbeddingRepository
        .find_tags_missing_embeddings(&mut conn, profile_id)
        .await
        .expect("find missing");
    assert!(
        missing.is_empty(),
        "all tags should have embeddings after backfill: missing = {missing:?}",
    );

    teardown(ctx).await;
}

/// Verifies that startup backfill is idempotent: tags that already have
/// embeddings are not re-embedded.
#[tokio::test]
async fn test_startup_backfill_skips_already_embedded_tags() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    // Pre-seed a tag WITH its embedding.
    {
        let mut conn = raw_conn(ctx).await;
        Seed::new()
            .define_project("proj", "git@github.com:test/backfill-skip.git")
            .define_principal("user", "user:backfill-skip")
            .set_embedding_model("mock-model", 768)
            .define_tag_with_embedding("alpha")
            .execute(&mut conn)
            .await;
    }

    // Embedding provider should never be called.
    let embedding: Arc<MockEmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_exhaust(ExhaustBehaviour::Error(Box::new(|| {
                tribal_inference::InferenceError::provider_unavailable(
                    "mock",
                    "backfill should not embed already-embedded tags",
                )
            })))
            .build(),
    );
    let embedding_ref = Arc::clone(&embedding);

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool,
        token.clone(),
        test_config(),
        None,
        Some(embedding as Arc<dyn EmbeddingProvider>),
    )
    .await;

    worker.startup().await.expect("startup");

    assert_eq!(
        embedding_ref.call_count(),
        0,
        "embedding provider should not be called for already-embedded tags",
    );

    teardown(ctx).await;
}

/// Verifies exact match precedence: when a suggested tag matches an
/// existing registry entry exactly, the embedding provider is NOT
/// called for tag resolution — only for the candidate content embedding.
#[tokio::test]
async fn test_triage_exact_match_skips_tag_embedding() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "triage-exact-match").await;

    // Pre-seed "rust" in the registry with an embedding.
    {
        let mut conn = raw_conn(ctx).await;
        Seed::new()
            .define_project("tag-proj", "git@github.com:test/exact-match.git")
            .define_principal("tag-user", "user:exact-match")
            .set_embedding_model("mock-model", 768)
            .define_tag_with_embedding("rust")
            .execute(&mut conn)
            .await;
    }

    // Candidate suggests "rust" — should exact-match the registry entry
    // without any tag-resolution embedding calls.
    let candidates = vec![
        a_candidate()
            .content("Rust has zero-cost abstractions".to_owned())
            .suggested_tags(vec!["rust".to_owned()])
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

    // Only one embedding call should happen: the candidate content.
    // If tag resolution tried to embed "rust", the second call would
    // hit the error exhaust and fail the task.
    let embedding: Arc<MockEmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.1_f32; 768]), None)
            .on_exhaust(ExhaustBehaviour::Error(Box::new(|| {
                tribal_inference::InferenceError::provider_unavailable(
                    "mock",
                    "tag resolution should not call embedding provider",
                )
            })))
            .build(),
    );
    let embedding_ref = Arc::clone(&embedding);

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
        Some(embedding as Arc<dyn EmbeddingProvider>),
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(task.status(), TaskStatus::Completed);

    // Embedding provider called exactly once (candidate content only).
    assert_eq!(
        embedding_ref.call_count(),
        1,
        "embedding provider should be called once (candidate content), not for exact-matched tags",
    );

    // Knowledge item should have the exact-matched tag.
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

    let item = PgKnowledgeItemRepository
        .find_by_id(&mut conn, *item_id)
        .await
        .expect("find knowledge item");
    assert!(
        item.tags().contains(&"rust".to_owned()),
        "tags should contain 'rust' (exact match): {:?}",
        item.tags(),
    );

    // Usage count should be incremented for the exact-matched tag.
    let tags = PgTagRegistryRepository
        .find_all(&mut conn)
        .await
        .expect("find tags");
    let rust_tag = tags.iter().find(|t| t.tag() == "rust").expect("find rust");
    assert_eq!(
        rust_tag.usage_count(),
        1,
        "usage_count should be incremented for exact match",
    );

    teardown(ctx).await;
}

/// Verifies that an embedding provider failure during tag resolution
/// surfaces as a retryable `ProviderError`, causing the task to be
/// requeued for retry.
#[tokio::test]
async fn test_triage_tag_resolution_provider_failure_retries() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "triage-tag-provider-fail").await;

    // Candidate with a tag that won't exact-match anything, forcing
    // semantic resolution and an embedding call that will fail.
    let candidates = vec![
        a_candidate()
            .content("Something about Rust".to_owned())
            .suggested_tags(vec!["rust-lang".to_owned()])
            .build(),
    ];

    let (_job_id, task_id) = {
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

    // First embedding call (candidate content) succeeds;
    // second call (tag "rust-lang") fails with a provider error.
    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.1_f32; 768]), None)
            .on_exhaust(ExhaustBehaviour::Error(Box::new(|| {
                tribal_inference::InferenceError::provider_unavailable(
                    "mock",
                    "simulated tag embedding failure",
                )
            })))
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
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_requeued_with_retry(&pool, task_id, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(
        task.error_kind(),
        Some(TaskErrorKind::ProviderError),
        "tag resolution provider failure should surface as ProviderError",
    );
    assert!(
        task.error_message().is_some(),
        "error message should be set",
    );

    teardown(ctx).await;
}

/// Verifies multi-match determinism: when two tags in the registry have
/// identical similarity to a query, the tag with the higher usage count
/// is chosen deterministically by `resolve_tags`.
#[tokio::test]
async fn test_triage_semantic_match_determinism() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "triage-determinism").await;

    // Seed two tags with identical embeddings. Give "beta" a higher
    // usage_count so the tie-breaker is exercised.
    {
        let mut conn = raw_conn(ctx).await;
        Seed::new()
            .define_project("tag-proj", "git@github.com:test/determinism.git")
            .define_principal("tag-user", "user:determinism")
            .set_embedding_model("mock-model", 768)
            .define_tag("alpha")
            .define_tag("beta")
            .execute(&mut conn)
            .await;

        let profile_id = active_embedding_profile(&mut conn).await.id();

        // Insert identical embeddings for both tags.
        let embedding_vec = make_test_embedding(0);
        PgTagEmbeddingRepository
            .batch_upsert(
                &mut conn,
                &[
                    NewTagEmbedding::builder()
                        .tag("alpha".to_owned())
                        .embedding_profile_id(profile_id)
                        .model("mock-model".to_owned())
                        .embedding(embedding_vec.clone())
                        .build(),
                    NewTagEmbedding::builder()
                        .tag("beta".to_owned())
                        .embedding_profile_id(profile_id)
                        .model("mock-model".to_owned())
                        .embedding(embedding_vec)
                        .build(),
                ],
            )
            .await
            .expect("seed tag embeddings");

        // Bump beta's usage_count so it wins the tie-breaker.
        PgTagRegistryRepository
            .increment_usage_count(&mut conn, &["beta".to_owned()])
            .await
            .expect("increment usage count");
    }

    // Candidate tag "unknown-tag" won't exact-match anything. Its
    // embedding will be identical to alpha/beta, so both match at
    // similarity = 1.0. Beta should win (higher usage_count).
    let candidates = vec![
        a_candidate()
            .content("Content about tags".to_owned())
            .suggested_tags(vec!["unknown-tag".to_owned()])
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
            // 1. Candidate content embedding.
            .on_embed(an_embedding_response(vec![0.1_f32; 768]), None)
            // 2. "unknown-tag" tag embedding → same vector as alpha/beta.
            .on_embed(an_embedding_response(make_test_embedding(0)), None)
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
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    assert_eq!(task.status(), TaskStatus::Completed);

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

    let item = PgKnowledgeItemRepository
        .find_by_id(&mut conn, *item_id)
        .await
        .expect("find knowledge item");

    // "beta" should win the tie-breaker (higher usage_count).
    assert!(
        item.tags().contains(&"beta".to_owned()),
        "tags should contain 'beta' (higher usage_count wins tie): {:?}",
        item.tags(),
    );
    assert!(
        !item.tags().contains(&"alpha".to_owned()),
        "tags should NOT contain 'alpha' (lost tie-break): {:?}",
        item.tags(),
    );
    assert!(
        !item.tags().contains(&"unknown-tag".to_owned()),
        "tags should NOT contain 'unknown-tag' (resolved to 'beta'): {:?}",
        item.tags(),
    );

    teardown(ctx).await;
}

// ---------------------------------------------------------------------------
// Triage fan-in tests
// ---------------------------------------------------------------------------

/// Verifies that when all triage tasks complete successfully, the fan-in
/// creates a relation task and transitions the job to `Relating`.
#[tokio::test]
async fn test_triage_fan_in_all_complete() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "fan-in-all-complete").await;

    let candidates = vec![
        a_candidate()
            .content("Fan-in candidate one".to_owned())
            .suggested_tags(vec!["tag-a".to_owned()])
            .build(),
        a_candidate()
            .content("Fan-in candidate two".to_owned())
            .suggested_tags(vec!["tag-b".to_owned()])
            .build(),
    ];

    let (job_id, task_ids) = {
        let mut conn = raw_conn(ctx).await;
        seed_multiple_triage_tasks(
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
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    // Poll until both triage tasks complete and the job reaches Relating.
    let (_, _, job) = tokio::join!(
        poll_task_status(
            &pool,
            task_ids[0],
            TaskStatus::Completed,
            MULTI_CYCLE_SETTLE
        ),
        poll_task_status(
            &pool,
            task_ids[1],
            TaskStatus::Completed,
            MULTI_CYCLE_SETTLE
        ),
        poll_job_status(&pool, job_id, JobStatus::Relating, MULTI_CYCLE_SETTLE),
    );

    token.cancel();
    let _ = handle.await;

    assert_eq!(job.status(), JobStatus::Relating);

    // Verify a relation task was created.
    let mut conn = raw_conn(ctx).await;
    let tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find tasks");
    let relation_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.task_type() == TaskType::Relation)
        .collect();
    assert_eq!(
        relation_tasks.len(),
        1,
        "exactly one relation task should exist",
    );

    teardown(ctx).await;
}

/// Verifies that fan-in fires when some triage tasks complete and others
/// are dead-lettered — all that matters is that every sibling is terminal.
///
/// Both tasks are positioned at `task_max_retries` so that claim order
/// does not matter: whichever task consumes the single valid inference
/// response completes; the other fails and dead-letters immediately.
#[tokio::test]
async fn test_triage_fan_in_mixed_complete_and_dead_letter() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let config = test_config();

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "fan-in-mixed").await;

    let candidates = vec![
        a_candidate()
            .content("Mixed candidate one".to_owned())
            .suggested_tags(vec!["tag-a".to_owned()])
            .build(),
        a_candidate()
            .content("Mixed candidate two".to_owned())
            .suggested_tags(vec!["tag-b".to_owned()])
            .build(),
    ];

    let job_id = {
        let mut conn = raw_conn(ctx).await;
        let (job_id, task_ids) = seed_multiple_triage_tasks(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
            &candidates,
        )
        .await;

        // Both tasks at max retries — the one that gets the error
        // will dead-letter regardless of claim order.
        set_retry_count(&mut conn, task_ids[0], config.task_max_retries).await;
        set_retry_count(&mut conn, task_ids[1], config.task_max_retries).await;

        job_id
    };

    // One valid response, then errors. Claim order is non-deterministic,
    // so whichever task runs first completes; the other dead-letters.
    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.1_f32; 768]), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response(triage_novel_response_json()), None)
            .on_exhaust(ExhaustBehaviour::Error(Box::new(|| {
                tribal_inference::InferenceError::provider_unavailable("mock", "force dead-letter")
            })))
            .build(),
    );

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        config,
        Some(inference),
        Some(embedding),
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    // Fan-in fires once both tasks reach terminal state.
    let job = poll_job_status(&pool, job_id, JobStatus::Relating, MULTI_CYCLE_SETTLE).await;

    token.cancel();
    let _ = handle.await;

    assert_eq!(job.status(), JobStatus::Relating);

    let mut conn = raw_conn(ctx).await;
    let tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find tasks");

    // Verify mixed terminal states (order-independent).
    let triage_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.task_type() == TaskType::Triage)
        .collect();
    let completed = triage_tasks
        .iter()
        .filter(|t| t.status() == TaskStatus::Completed)
        .count();
    let dead_lettered = triage_tasks
        .iter()
        .filter(|t| t.status() == TaskStatus::DeadLetter)
        .count();
    assert_eq!(completed, 1, "exactly one triage task should be completed");
    assert_eq!(
        dead_lettered, 1,
        "exactly one triage task should be dead-lettered",
    );

    let relation_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.task_type() == TaskType::Relation)
        .collect();
    assert_eq!(
        relation_tasks.len(),
        1,
        "exactly one relation task should exist",
    );

    teardown(ctx).await;
}

/// Verifies the `ON CONFLICT DO NOTHING` guard: even when multiple triage
/// tasks race to fan-in, exactly one relation task is created.
#[tokio::test]
async fn test_triage_fan_in_multi_task_exactly_one_relation() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "fan-in-one-relation").await;

    let candidates = vec![
        a_candidate()
            .content("Race candidate one".to_owned())
            .suggested_tags(vec!["tag-a".to_owned()])
            .build(),
        a_candidate()
            .content("Race candidate two".to_owned())
            .suggested_tags(vec!["tag-b".to_owned()])
            .build(),
        a_candidate()
            .content("Race candidate three".to_owned())
            .suggested_tags(vec!["tag-c".to_owned()])
            .build(),
    ];

    let (job_id, task_ids) = {
        let mut conn = raw_conn(ctx).await;
        seed_multiple_triage_tasks(
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
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    tokio::join!(
        poll_task_status(
            &pool,
            task_ids[0],
            TaskStatus::Completed,
            MULTI_CYCLE_SETTLE
        ),
        poll_task_status(
            &pool,
            task_ids[1],
            TaskStatus::Completed,
            MULTI_CYCLE_SETTLE
        ),
        poll_task_status(
            &pool,
            task_ids[2],
            TaskStatus::Completed,
            MULTI_CYCLE_SETTLE
        ),
        poll_job_status(&pool, job_id, JobStatus::Relating, MULTI_CYCLE_SETTLE),
    );

    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(ctx).await;
    let tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find tasks");
    let relation_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.task_type() == TaskType::Relation)
        .collect();
    assert_eq!(
        relation_tasks.len(),
        1,
        "exactly one relation task should exist despite multiple fan-in attempts",
    );

    teardown(ctx).await;
}

/// Verifies the healing sweep: a job stuck in `Triaging` with all triage
/// tasks terminal but no relation task is healed by the reclaim loop.
#[tokio::test]
async fn test_heal_stuck_triaging_job() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "heal-stuck-triaging").await;

    let candidates = vec![
        a_candidate()
            .content("Stuck candidate one".to_owned())
            .suggested_tags(vec!["tag-a".to_owned()])
            .build(),
        a_candidate()
            .content("Stuck candidate two".to_owned())
            .suggested_tags(vec!["tag-b".to_owned()])
            .build(),
    ];

    let job_id = {
        let mut conn = raw_conn(ctx).await;
        let (job_id, _task_ids) = seed_multiple_triage_tasks(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
            &candidates,
        )
        .await;

        // Mark all triage tasks as completed to simulate the reclaim-sweep
        // gap where tasks reach terminal state without invoking per-task
        // fan-in code.
        set_task_status_by_job(&mut conn, job_id, TaskType::Triage, TaskStatus::Completed).await;

        job_id
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(pool.clone(), token.clone(), test_config(), None, None).await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    // The healing sweep runs on every reclaim iteration, so MULTI_CYCLE_SETTLE
    // gives enough time for the sweep to detect and heal the stuck job.
    let job = poll_job_status(&pool, job_id, JobStatus::Relating, MULTI_CYCLE_SETTLE).await;

    token.cancel();
    let _ = handle.await;

    assert_eq!(job.status(), JobStatus::Relating);

    let mut conn = raw_conn(ctx).await;
    let tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find tasks");
    let relation_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.task_type() == TaskType::Relation)
        .collect();
    assert_eq!(
        relation_tasks.len(),
        1,
        "healing sweep should create exactly one relation task",
    );

    teardown(ctx).await;
}

/// Verifies that fan-in fires correctly for a single-candidate batch
/// (one triage task).
#[tokio::test]
async fn test_triage_fan_in_single_candidate_batch() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "fan-in-single").await;

    let candidates = vec![
        a_candidate()
            .content("Solo candidate".to_owned())
            .suggested_tags(vec!["solo-tag".to_owned()])
            .build(),
    ];

    let (job_id, task_ids) = {
        let mut conn = raw_conn(ctx).await;
        seed_multiple_triage_tasks(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
            &candidates,
        )
        .await
    };
    let task_id = task_ids[0];

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
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let (_, job) = tokio::join!(
        poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE),
        poll_job_status(&pool, job_id, JobStatus::Relating, POLL_SETTLE),
    );

    token.cancel();
    let _ = handle.await;

    assert_eq!(job.status(), JobStatus::Relating);

    let mut conn = raw_conn(ctx).await;
    let tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find tasks");
    let relation_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.task_type() == TaskType::Relation)
        .collect();
    assert_eq!(
        relation_tasks.len(),
        1,
        "exactly one relation task should exist (fan-in single candidate)",
    );

    teardown(ctx).await;
}

/// A resumed triage attempt resolves the model's positional answer
/// against the similar-item slots its first attempt rendered, never a
/// re-derived search: an item inserted between attempts that outranks
/// the original cannot become the duplicate target.
#[tokio::test]
async fn test_resumed_triage_resolves_against_the_recorded_slots() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let mut conn = raw_conn(ctx).await;
    let seed_result = Seed::new()
        .define_project("proj", "git@github.com:test/triage-resume.git")
        .define_principal("user", "user:triage-resume")
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
            store.add_item("original", item(KnowledgeKind::Fact, "the original match"));
        })
        .execute(&mut conn)
        .await;

    let principal_id = seed_result.principal_id("user");
    let project_id = seed_result.project_id("proj");
    let original_id = seed_result.item_id("original");
    let system_pv_id = seed_result.prompt_version_id("system-pv");
    let user_pv_id = seed_result.prompt_version_id("user-pv");

    // The candidate's vector diverges from the original's, so the decoy
    // inserted between attempts — carrying the candidate's exact vector —
    // would strictly outrank the original in a re-derived search.
    let original_embedding = find_active_embedding(&mut conn, original_id)
        .await
        .expect("find seeded embedding")
        .expect("seeded embedding should exist");
    let mut candidate_vector = original_embedding.embedding().to_vec();
    candidate_vector[0] += 1.0;

    let candidates = vec![
        a_candidate()
            .content("resumed duplicate".to_owned())
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
            .on_embed(an_embedding_response(candidate_vector.clone()), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    // Attempt one commits its input record (slots: the original alone),
    // then fails retryably; attempt two answers duplicate-of-index-0.
    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete_error(
                || tribal_inference::InferenceError::provider_unavailable("mock", "transient"),
                None,
            )
            .on_complete(
                a_completion_response(triage_duplicate_response_json(0)),
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
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    // Attempt one has failed and re-queued; the graph drifts before the
    // retry: a decoy item whose vector equals the candidate's lands at
    // the top of any fresh search.
    poll_task_requeued_with_retry(&pool, task_id, POLL_SETTLE).await;
    let mut conn = raw_conn(ctx).await;
    let decoy = PgKnowledgeItemRepository
        .insert(
            &mut conn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .content("the decoy that outranks the original".to_owned())
                .build(),
        )
        .await
        .expect("insert decoy");
    let profile = active_embedding_profile(&mut conn).await;
    tribal_db::EmbeddingRepository::insert(
        &tribal_db::PgEmbeddingRepository,
        &mut conn,
        &tribal_db::NewEmbedding::builder()
            .knowledge_item_id(decoy.id())
            .embedding_profile_id(profile.id())
            .model("mock-model".to_owned())
            .embedding(candidate_vector)
            .build(),
    )
    .await
    .expect("insert decoy embedding");
    drop(conn);

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, MULTI_CYCLE_SETTLE).await;
    token.cancel();
    let _ = handle.await;
    assert_eq!(task.status(), TaskStatus::Completed);

    let mut conn = raw_conn(ctx).await;
    let triage_result = PgTriageResultRepository
        .find_by_job_id_and_batch_index(&mut conn, job_id, SEED_TRIAGE_BATCH_INDEX)
        .await
        .expect("find triage result")
        .expect("triage result should exist");
    assert!(
        matches!(
            triage_result.outcome(),
            TriageOutcome::Duplicate { matched_item_id, .. }
                if *matched_item_id == original_id
        ),
        "the duplicate resolves to the item the model saw, not the decoy: {:?}",
        triage_result.outcome(),
    );

    teardown(ctx).await;
}
