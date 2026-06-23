use super::{common::*, fixtures::extraction_response_json};

/// Verifies the happy path: the extraction stage parses a multi-candidate
/// response, creates triage tasks, persists an extraction result, and
/// transitions the job to Triaging.
#[tokio::test]
async fn test_extraction_happy_path() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "extraction-happy").await;

    let candidates = vec![
        a_candidate().content("first".to_owned()).build(),
        a_candidate().content("second".to_owned()).build(),
    ];
    let hints = vec![a_relation_hint().build()];
    let response_json = extraction_response_json(&candidates, &hints);

    let (job_id, _task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
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
    )
    .await;
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
    let mut conn = raw_conn(&ctx).await;
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
}

/// Verifies that zero candidates causes the job to complete immediately
/// with an Empty outcome, and no triage tasks are created.
#[tokio::test]
async fn test_extraction_zero_candidates() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "extraction-zero-candidates").await;

    let response_json = extraction_response_json(&[], &[]);

    let (job_id, _task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
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
    )
    .await;
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
    let mut conn = raw_conn(&ctx).await;
    let tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find tasks");
    let triage_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| t.task_type() == TaskType::Triage)
        .collect();
    assert!(triage_tasks.is_empty(), "should create no triage tasks");
}

/// Verifies that candidates exceeding `max_candidates_per_job` are
/// capped, relation hints referencing out-of-range indices are
/// filtered, and the original count reflects the pre-cap total.
#[tokio::test]
async fn test_extraction_capping() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "extraction-capping").await;

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
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
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
    let worker =
        build_test_worker(pool.clone(), token.clone(), config, Some(inference), None).await;
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
    let mut conn = raw_conn(&ctx).await;
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
}

/// Verifies that an unparseable LLM response causes the extraction
/// task to be requeued with a ParseError error kind.
#[tokio::test]
async fn test_extraction_parse_failure() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "extraction-parse-failure").await;

    let (_job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
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
    )
    .await;
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
}
