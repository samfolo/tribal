use tribal_db::{
    PgPrincipalRepository, PgProjectRepository, PgPromptVersionRepository, PgTokenUsageRepository,
    PrincipalRepository, ProjectRepository, PromptVersionRepository, TokenUsageRepository,
    TokenUsageStage,
};
use tribal_domain::{EmbeddingPurpose, JobId, PipelineStage, PrincipalId, ProjectId};
use tribal_test_utils::{a_new_principal, a_new_project, a_new_prompt_version, a_new_token_usage, test_context};

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
                .git_remote(format!("git@github.com:test/token-usage-{suffix}.git"))
                .build(),
        )
        .await
        .expect("insert project");

    (principal.id(), project.id())
}

/// Inserts a prompt version and returns its ID.
async fn setup_prompt_version(
    txn: &mut sqlx::PgConnection,
    content_hash: &str,
) -> tribal_domain::PromptVersionId {
    PgPromptVersionRepository
        .upsert(
            txn,
            &a_new_prompt_version()
                .content_hash(content_hash.to_owned())
                .build(),
        )
        .await
        .expect("upsert prompt version")
        .id()
}

/// Inserts a job via raw SQL and returns its ID.
async fn setup_job(
    txn: &mut sqlx::PgConnection,
    project_id: ProjectId,
    principal_id: PrincipalId,
    pv_id: tribal_domain::PromptVersionId,
) -> JobId {
    let job_id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO jobs \
             (project_id, principal_id, source_context, \
              extraction_prompt_version_id, triage_prompt_version_id, \
              relation_prompt_version_id) \
         VALUES ($1, $2, $3, $4, $4, $4) \
         RETURNING id",
    )
    .bind(project_id.inner())
    .bind(principal_id.inner())
    .bind(serde_json::json!({}))
    .bind(pv_id.inner())
    .fetch_one(&mut *txn)
    .await
    .expect("insert job");

    JobId::from(job_id)
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
    let pv_id = setup_prompt_version(&mut txn, &"a".repeat(64)).await;
    let job_id = setup_job(&mut txn, project_id, principal_id, pv_id).await;

    let new = a_new_token_usage()
        .job_id(Some(job_id))
        .stage(TokenUsageStage::Extraction)
        .prompt_version_id(Some(pv_id))
        .build();

    let tu = repo.insert(&mut *txn, &new).await.expect("insert");

    assert!(tu.id().to_string().starts_with("tu_"));
    assert_eq!(tu.job_id(), Some(job_id));
    assert_eq!(tu.stage(), PipelineStage::Extraction);
    assert!(tu.purpose().is_none());
    assert_eq!(tu.provider(), "test-provider");
    assert_eq!(tu.model(), "test-model");
    assert_eq!(tu.tokens_input(), 100);
    assert_eq!(tu.tokens_output(), 50);
    assert_eq!(tu.prompt_version_id(), Some(pv_id));
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

    let tu = repo.insert(&mut *txn, &new).await.expect("insert");
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

    let tu = repo.insert(&mut *txn, &new).await.expect("insert");

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

    let tu = repo.insert(&mut *txn, &new).await.expect("insert");

    assert!(tu.job_id().is_none());
    assert!(tu.task_id().is_none());
    assert!(tu.prompt_version_id().is_none());
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
    let pv_id = setup_prompt_version(&mut txn, &"b".repeat(64)).await;
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

    repo.insert(&mut *txn, &first).await.expect("insert first");
    repo.insert(&mut *txn, &second).await.expect("insert second");

    let results = repo
        .find_by_job_id(&mut *txn, job_id)
        .await
        .expect("find");

    assert_eq!(results.len(), 2);
    assert!(results[0].created_at() <= results[1].created_at());
}

#[tokio::test]
async fn test_find_by_job_id_returns_empty_for_unknown() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTokenUsageRepository;

    let results = repo
        .find_by_job_id(&mut *txn, JobId::new())
        .await
        .expect("find");

    assert!(results.is_empty());
}
