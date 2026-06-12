use tribal_db::{
    JobRepository, NewReindexRun, PgJobRepository, PgPrincipalRepository, PgProjectRepository,
    PgReindexRunRepository, PgTokenUsageRepository, PrincipalRepository, ProjectRepository,
    ReindexRunRepository, TokenUsageRepository,
};
use tribal_domain::{
    EmbeddingPurpose, GitRemote, JobId, PipelineStage, PrincipalId, ProjectId, TokenUsageStage,
};
use tribal_test_utils::{
    a_new_job, a_new_principal, a_new_project, a_new_prompt_version, a_new_system_fingerprint,
    a_new_token_usage, ensure_genesis_profile, insert_prompt_version, shift_timestamp_by_id,
    test_context, upsert_system_fingerprint,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal and project, returning their IDs.
async fn setup_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (PrincipalId, ProjectId) {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:token-usage-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal");

    let project = PgProjectRepository
        .insert(
            txn,
            &a_new_project()
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    &format!("test/token-usage-{suffix}"),
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project");

    (principal.id(), project.id())
}

/// Inserts a prompt version and returns its ID.
async fn setup_prompt_version(txn: &mut sqlx::PgConnection) -> tribal_domain::PromptVersionId {
    insert_prompt_version(txn, &a_new_prompt_version().build()).await
}

/// Inserts a queued job and returns its ID.
async fn setup_job(
    txn: &mut sqlx::PgConnection,
    project_id: ProjectId,
    principal_id: PrincipalId,
    pv_id: tribal_domain::PromptVersionId,
) -> JobId {
    let fingerprint_hash =
        upsert_system_fingerprint(txn, &a_new_system_fingerprint().build()).await;

    PgJobRepository
        .insert(
            txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_system_prompt_version_id(pv_id)
                .extraction_user_prompt_version_id(pv_id)
                .triage_system_prompt_version_id(pv_id)
                .triage_user_prompt_version_id(pv_id)
                .relation_system_prompt_version_id(pv_id)
                .relation_user_prompt_version_id(pv_id)
                .system_fingerprint_hash(fingerprint_hash)
                .build(),
        )
        .await
        .expect("insert job")
        .id()
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_returns_populated_token_usage() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTokenUsageRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "insert").await;
    let system_pv_id = setup_prompt_version(&mut txn).await;
    let user_pv_id = setup_prompt_version(&mut txn).await;
    let job_id = setup_job(&mut txn, project_id, principal_id, system_pv_id).await;

    let new = a_new_token_usage()
        .job_id(Some(job_id))
        .stage(TokenUsageStage::Extraction)
        .system_prompt_version_id(Some(system_pv_id))
        .user_prompt_version_id(Some(user_pv_id))
        .build();

    let tu = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(tu.id().to_string().starts_with("tu_"));
    assert_eq!(tu.job_id(), Some(job_id));
    assert!(tu.task_id().is_none());
    assert_eq!(tu.attempt(), 0);
    assert_eq!(tu.stage(), PipelineStage::Extraction);
    assert!(tu.purpose().is_none());
    assert_eq!(tu.provider(), "test-provider");
    assert_eq!(tu.model(), "test-model");
    assert_eq!(tu.tokens_input(), 100);
    assert_eq!(tu.tokens_output(), 50);
    assert_eq!(tu.tokens_cache_read(), 0);
    assert_eq!(tu.tokens_cache_write(), 0);
    assert_eq!(tu.tokens_total(), 150);
    assert_eq!(tu.latency_ms(), 200);
    assert_eq!(tu.system_prompt_version_id(), Some(system_pv_id));
    assert_eq!(tu.user_prompt_version_id(), Some(user_pv_id));
    assert!(tu.trace_id().is_none());
}

#[tokio::test]
async fn test_insert_computes_tokens_total() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTokenUsageRepository;

    let new = a_new_token_usage()
        .tokens_input(300)
        .tokens_output(75)
        .build();

    let tu = repo.insert(&mut txn, &new).await.expect("insert");
    assert_eq!(tu.tokens_total(), 375);
}

#[tokio::test]
async fn test_insert_embedding_stage_with_purpose() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTokenUsageRepository;

    let new = a_new_token_usage()
        .stage(TokenUsageStage::Embedding {
            purpose: EmbeddingPurpose::Candidate,
        })
        .build();

    let tu = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(tu.stage(), PipelineStage::Embedding);
    assert_eq!(tu.purpose(), Some(EmbeddingPurpose::Candidate));
}

#[tokio::test]
async fn test_insert_with_null_fk_fields() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTokenUsageRepository;

    let new = a_new_token_usage()
        .stage(TokenUsageStage::Embedding {
            purpose: EmbeddingPurpose::Query,
        })
        .build();

    let tu = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(tu.job_id().is_none());
    assert!(tu.task_id().is_none());
    assert!(tu.system_prompt_version_id().is_none());
    assert!(tu.user_prompt_version_id().is_none());
}

#[tokio::test]
async fn test_insert_with_trace_id_round_trips() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTokenUsageRepository;

    let new = a_new_token_usage()
        .trace_id(Some("trace-abc-123".to_owned()))
        .build();

    let tu = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(tu.trace_id(), Some("trace-abc-123"));
}

// ---------------------------------------------------------------------------
// find_by_job_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_job_id_returns_records_ordered() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTokenUsageRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "find-job").await;
    let pv_id = setup_prompt_version(&mut txn).await;
    let job_id = setup_job(&mut txn, project_id, principal_id, pv_id).await;

    let first = a_new_token_usage()
        .job_id(Some(job_id))
        .stage(TokenUsageStage::Extraction)
        .tokens_input(10)
        .tokens_output(5)
        .build();
    let second = a_new_token_usage()
        .job_id(Some(job_id))
        .stage(TokenUsageStage::Triage)
        .tokens_input(20)
        .tokens_output(10)
        .build();

    let first_tu = repo.insert(&mut txn, &first).await.expect("insert first");

    // Backdate the first record so ordering is deterministic.
    shift_timestamp_by_id(
        &mut txn,
        "token_usage",
        "created_at",
        *first_tu.id().inner(),
        chrono::Duration::hours(-1),
    )
    .await;

    repo.insert(&mut txn, &second).await.expect("insert second");

    let results = repo.find_by_job_id(&mut txn, job_id).await.expect("find");

    assert_eq!(results.len(), 2);
    // Ordered by created_at ASC — oldest first.
    assert!(results[0].created_at() <= results[1].created_at());
}

#[tokio::test]
async fn test_find_by_job_id_returns_empty_for_unknown() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTokenUsageRepository;

    let results = repo
        .find_by_job_id(&mut txn, JobId::new())
        .await
        .expect("find");

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_insert_attributes_embedding_usage_to_a_reindex_run() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTokenUsageRepository;

    // A reindex run for the foreign key, over its genesis profile and principal.
    let principal = PgPrincipalRepository
        .insert(
            &mut txn,
            &a_new_principal()
                .principal_key("user:token-usage-reindex".to_owned())
                .build(),
        )
        .await
        .expect("insert principal");
    let profile = ensure_genesis_profile(&mut txn, "reindex-model", 768).await;
    let run = PgReindexRunRepository
        .insert(
            &mut txn,
            &NewReindexRun::builder()
                .target_profile_id(profile.id())
                .epoch(profile.epoch())
                .initiated_by_principal_id(principal.id())
                .build(),
        )
        .await
        .expect("insert run");

    let new = a_new_token_usage()
        .reindex_run_id(Some(run.id()))
        .stage(TokenUsageStage::Embedding {
            purpose: EmbeddingPurpose::Candidate,
        })
        .build();

    let tu = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(tu.reindex_run_id(), Some(run.id()));
    assert!(tu.job_id().is_none());
    assert!(tu.task_id().is_none());
    assert_eq!(tu.stage(), PipelineStage::Embedding);
    assert_eq!(tu.purpose(), Some(EmbeddingPurpose::Candidate));
}
