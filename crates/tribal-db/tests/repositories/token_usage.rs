use tribal_db::{
    JobRepository, NewReindexRun, PgJobRepository, PgPrincipalRepository, PgProjectRepository,
    PgReindexRunRepository, PgTokenUsageRepository, PrincipalRepository, ProjectRepository,
    ReindexRunRepository, TokenUsageRepository,
};
use tribal_domain::{
    EmbeddingPurpose, ExecutionLocus, GitRemote, JobId, PipelineStage, PlatformBinding,
    PrincipalId, ProjectId, TokenUsageStage,
};
use tribal_test_utils::{
    TestDb, a_new_job, a_new_principal, a_new_project, a_new_prompt_version,
    a_new_system_fingerprint, a_new_token_usage, ensure_genesis_profile, insert_prompt_version,
    shift_timestamp_by_id, upsert_system_fingerprint,
};

// The execution_locus CHECK is exercised by a raw insert the repository's typed
// writer cannot express, so its SQL lives in a fixture.
const INSERT_TOKEN_USAGE_BAD_LOCUS: &str = include_str!("sql/insert_token_usage_bad_locus.sql");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolves a platform-bound principal, giving aggregation a `(user, account)`
/// to attribute spend against.
async fn bound_principal(
    txn: &mut sqlx::PgConnection,
    account_reference: &str,
    platform_user_id: &str,
) -> PrincipalId {
    PgPrincipalRepository
        .find_or_create_platform_bound(
            txn,
            &PlatformBinding::new(account_reference.to_owned(), platform_user_id.to_owned()),
        )
        .await
        .expect("resolve bound principal")
        .id()
}

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
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
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
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
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
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
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
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
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
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
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
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
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
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTokenUsageRepository;

    let results = repo
        .find_by_job_id(&mut txn, JobId::new())
        .await
        .expect("find");

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_insert_attributes_embedding_usage_to_a_reindex_run() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
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

#[tokio::test]
async fn test_find_by_job_id_surfaces_verifier_child_spend() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTokenUsageRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "child-spend").await;
    let pv_id = setup_prompt_version(&mut txn).await;
    let job_id = setup_job(&mut txn, project_id, principal_id, pv_id).await;

    // A verifier child meters to its job through the thread owner, not a
    // stage task: the row carries the job id but no task id. The job's cost
    // accounting must include it.
    let child_spend = a_new_token_usage()
        .job_id(Some(job_id))
        .stage(TokenUsageStage::Triage)
        .attempt(1)
        .build();
    let inserted = repo.insert(&mut txn, &child_spend).await.expect("insert");
    assert!(
        inserted.task_id().is_none(),
        "a verifier child has no stage task",
    );

    let for_job = repo.find_by_job_id(&mut txn, job_id).await.expect("find");
    assert!(
        for_job.iter().any(|tu| tu.id() == inserted.id()),
        "the child's task-less spend surfaces in the job's accounting",
    );
}

// ---------------------------------------------------------------------------
// Principal-attributed aggregation
// ---------------------------------------------------------------------------

/// Attributes a metered call to a principal at a locus.
fn a_usage_for(
    principal: PrincipalId,
    locus: ExecutionLocus,
    tokens_input: i32,
    tokens_output: i32,
) -> tribal_db::NewTokenUsage {
    a_new_token_usage()
        .principal_id(Some(principal))
        .execution_locus(locus)
        .stage(TokenUsageStage::Embedding {
            purpose: EmbeddingPurpose::Candidate,
        })
        .tokens_input(tokens_input)
        .tokens_output(tokens_output)
        .build()
}

#[tokio::test]
async fn test_per_user_totals_separate_edge_from_managed_spend() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTokenUsageRepository;

    let user = bound_principal(&mut txn, "account_1", "user_1").await;

    // Two edge calls locally, one managed enrichment run elsewhere.
    for usage in [
        a_usage_for(user, ExecutionLocus::Edge, 100, 50),
        a_usage_for(user, ExecutionLocus::Edge, 10, 5),
        a_usage_for(user, ExecutionLocus::Managed, 200, 0),
    ] {
        repo.insert(&mut txn, &usage).await.expect("insert");
    }

    let totals = repo
        .totals_by_platform_user(&mut txn, "user_1")
        .await
        .expect("totals");

    let edge = totals
        .iter()
        .find(|t| t.execution_locus == ExecutionLocus::Edge)
        .expect("edge totals present");
    assert_eq!(edge.records, 2);
    assert_eq!(edge.tokens_input, 110);
    assert_eq!(edge.tokens_output, 55);

    let managed = totals
        .iter()
        .find(|t| t.execution_locus == ExecutionLocus::Managed)
        .expect("managed enrichment is distinguishable");
    assert_eq!(managed.records, 1);
    assert_eq!(managed.tokens_input, 200);
    assert_eq!(managed.tokens_output, 0);
}

#[tokio::test]
async fn test_per_account_totals_sum_across_the_accounts_users() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTokenUsageRepository;

    // Two users bound to one account.
    let user_a = bound_principal(&mut txn, "account_9", "user_a").await;
    let user_b = bound_principal(&mut txn, "account_9", "user_b").await;

    repo.insert(
        &mut txn,
        &a_usage_for(user_a, ExecutionLocus::Edge, 100, 10),
    )
    .await
    .expect("insert a");
    repo.insert(&mut txn, &a_usage_for(user_b, ExecutionLocus::Edge, 1, 1))
        .await
        .expect("insert b");

    // The account view sums both users' edge spend.
    let account = repo
        .totals_by_account_reference(&mut txn, "account_9")
        .await
        .expect("account totals");
    let account_edge = account
        .iter()
        .find(|t| t.execution_locus == ExecutionLocus::Edge)
        .expect("edge totals present");
    assert_eq!(account_edge.records, 2);
    assert_eq!(account_edge.tokens_input, 101);

    // The per-user view isolates one user's spend from the other's.
    let user_a_totals = repo
        .totals_by_platform_user(&mut txn, "user_a")
        .await
        .expect("user a totals");
    let user_a_edge = user_a_totals
        .iter()
        .find(|t| t.execution_locus == ExecutionLocus::Edge)
        .expect("edge totals present");
    assert_eq!(user_a_edge.records, 1);
    assert_eq!(user_a_edge.tokens_input, 100);
}

#[tokio::test]
async fn test_unattributed_usage_survives_and_is_absent_from_per_user_totals() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTokenUsageRepository;

    // A pre-attribution write: no principal, default locus. The new columns
    // leave existing usage shape intact — it inserts and reads back as edge,
    // unattributed.
    let tu = repo
        .insert(&mut txn, &a_new_token_usage().build())
        .await
        .expect("insert");
    assert_eq!(tu.principal_id(), None);
    assert_eq!(tu.execution_locus(), ExecutionLocus::Edge);

    // With no linkage it never enters a per-user total.
    let totals = repo
        .totals_by_platform_user(&mut txn, "user_1")
        .await
        .expect("totals");
    assert!(totals.is_empty());
}

#[tokio::test]
async fn test_execution_locus_check_refuses_an_unknown_value() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    let result = sqlx::query(INSERT_TOKEN_USAGE_BAD_LOCUS)
        .execute(&mut *txn)
        .await;

    assert!(
        result.is_err(),
        "an out-of-vocabulary execution_locus must be refused",
    );
}
