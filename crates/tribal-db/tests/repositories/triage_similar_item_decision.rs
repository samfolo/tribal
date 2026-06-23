use tribal_db::{
    DbError, JobRepository, KnowledgeItemRepository, PgKnowledgeItemRepository,
    PgPrincipalRepository, PgProjectRepository, PgTriageSimilarItemDecisionRepository,
    PrincipalRepository, ProjectRepository, TriageSimilarItemDecisionRepository,
};
use tribal_domain::{
    GitRemote, JobId, KnowledgeItemId, PrincipalId, ProjectId, RelationSuggestion,
};
use tribal_test_utils::{
    TestDb, a_new_job, a_new_knowledge_item, a_new_principal, a_new_project, a_new_prompt_version,
    a_new_system_fingerprint, a_new_triage_similar_item_decision, insert_prompt_version,
    shift_timestamp_by_id, upsert_system_fingerprint,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal, project, prompt_version, and job, returning the IDs
/// needed for decision tests.
async fn setup_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (PrincipalId, ProjectId, JobId) {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:tsd-test-{suffix}"))
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
                    &format!("test/tsd-{suffix}"),
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project");

    let pv_id = insert_prompt_version(txn, &a_new_prompt_version().build()).await;

    let fingerprint_hash =
        upsert_system_fingerprint(txn, &a_new_system_fingerprint().build()).await;

    let job = tribal_db::PgJobRepository
        .insert(
            txn,
            &a_new_job()
                .project_id(project.id())
                .principal_id(principal.id())
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
        .expect("insert job");

    (principal.id(), project.id(), job.id())
}

/// Inserts a knowledge item and returns its ID.
async fn setup_item(
    txn: &mut sqlx::PgConnection,
    project_id: ProjectId,
    principal_id: PrincipalId,
) -> KnowledgeItemId {
    PgKnowledgeItemRepository
        .insert(
            txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("insert knowledge item")
        .id()
}

// ---------------------------------------------------------------------------
// batch_insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_batch_insert_returns_populated_decisions() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgTriageSimilarItemDecisionRepository;

    let (principal_id, project_id, job_id) = setup_prerequisites(&mut txn, "batch").await;
    let item_a = setup_item(&mut txn, project_id, principal_id).await;
    let item_b = setup_item(&mut txn, project_id, principal_id).await;

    let batch = vec![
        a_new_triage_similar_item_decision()
            .job_id(job_id)
            .batch_index(0)
            .matched_item_id(item_a)
            .similarity_score(0.92)
            .suggested_relation(RelationSuggestion::Supports)
            .justification_text("corroborates finding".to_owned())
            .build(),
        a_new_triage_similar_item_decision()
            .job_id(job_id)
            .batch_index(0)
            .matched_item_id(item_b)
            .similarity_score(0.45)
            .suggested_relation(RelationSuggestion::Unrelated)
            .justification_text("incidental similarity".to_owned())
            .build(),
    ];

    let results = repo
        .batch_insert(&mut txn, &batch)
        .await
        .expect("batch_insert");

    assert_eq!(results.len(), 2);
    assert!(
        results[0].id().to_string().starts_with("tsd_"),
        "expected tsd_ prefix, got: {}",
        results[0].id()
    );
    assert_eq!(results[0].job_id(), job_id);
    assert_eq!(results[0].batch_index(), 0);
    assert_eq!(results[1].job_id(), job_id);
}

#[tokio::test]
async fn test_batch_insert_duplicate_returns_unique_violation() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgTriageSimilarItemDecisionRepository;

    let (principal_id, project_id, job_id) = setup_prerequisites(&mut txn, "batch-uv").await;
    let item_id = setup_item(&mut txn, project_id, principal_id).await;

    let first = a_new_triage_similar_item_decision()
        .job_id(job_id)
        .batch_index(0)
        .matched_item_id(item_id)
        .build();

    repo.batch_insert(&mut txn, &[first])
        .await
        .expect("first insert");

    let duplicate = a_new_triage_similar_item_decision()
        .job_id(job_id)
        .batch_index(0)
        .matched_item_id(item_id)
        .build();

    let err = repo.batch_insert(&mut txn, &[duplicate]).await.unwrap_err();

    assert!(
        matches!(err, DbError::UniqueViolation { .. }),
        "expected UniqueViolation, got {err:?}"
    );
}

#[tokio::test]
async fn test_batch_insert_empty_returns_empty() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgTriageSimilarItemDecisionRepository;

    let results = repo
        .batch_insert(&mut txn, &[])
        .await
        .expect("batch_insert");

    assert!(results.is_empty());
}

// ---------------------------------------------------------------------------
// find_by_job_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_job_id_returns_all_ordered() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgTriageSimilarItemDecisionRepository;

    let (principal_id, project_id, job_id) = setup_prerequisites(&mut txn, "find-all").await;
    let item_a = setup_item(&mut txn, project_id, principal_id).await;
    let item_b = setup_item(&mut txn, project_id, principal_id).await;

    let batch = vec![
        a_new_triage_similar_item_decision()
            .job_id(job_id)
            .batch_index(1)
            .matched_item_id(item_a)
            .build(),
        a_new_triage_similar_item_decision()
            .job_id(job_id)
            .batch_index(0)
            .matched_item_id(item_b)
            .build(),
    ];

    repo.batch_insert(&mut txn, &batch).await.expect("insert");

    let results = repo.find_by_job_id(&mut txn, job_id).await.expect("find");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].batch_index(), 0);
    assert_eq!(results[1].batch_index(), 1);
}

#[tokio::test]
async fn test_find_by_job_id_returns_empty_for_unknown_job() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgTriageSimilarItemDecisionRepository;

    let results = repo
        .find_by_job_id(&mut txn, JobId::new())
        .await
        .expect("find");

    assert!(results.is_empty());
}

// ---------------------------------------------------------------------------
// find_by_job_id_and_batch_index
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_job_id_and_batch_index_returns_matching_ordered_by_created_at() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgTriageSimilarItemDecisionRepository;

    let (principal_id, project_id, job_id) = setup_prerequisites(&mut txn, "find-bi").await;
    let item_a = setup_item(&mut txn, project_id, principal_id).await;
    let item_b = setup_item(&mut txn, project_id, principal_id).await;
    let item_other = setup_item(&mut txn, project_id, principal_id).await;

    // Insert two decisions for batch_index 0 and one for batch_index 1.
    let batch = vec![
        a_new_triage_similar_item_decision()
            .job_id(job_id)
            .batch_index(0)
            .matched_item_id(item_a)
            .build(),
        a_new_triage_similar_item_decision()
            .job_id(job_id)
            .batch_index(0)
            .matched_item_id(item_b)
            .build(),
        a_new_triage_similar_item_decision()
            .job_id(job_id)
            .batch_index(1)
            .matched_item_id(item_other)
            .build(),
    ];

    let inserted = repo.batch_insert(&mut txn, &batch).await.expect("insert");

    // Backdate item_b's decision so it sorts before item_a's by created_at.
    shift_timestamp_by_id(
        &mut txn,
        "triage_similar_item_decisions",
        "created_at",
        *inserted[1].id().inner(),
        chrono::Duration::hours(-1),
    )
    .await;

    let results = repo
        .find_by_job_id_and_batch_index(&mut txn, job_id, 0)
        .await
        .expect("find");

    assert_eq!(
        results.len(),
        2,
        "should only return batch_index=0 decisions"
    );
    assert_eq!(
        results[0].matched_item_id(),
        item_b,
        "earlier created_at should come first"
    );
    assert_eq!(results[1].matched_item_id(), item_a);
}

#[tokio::test]
async fn test_find_by_job_id_and_batch_index_returns_empty_for_unknown() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgTriageSimilarItemDecisionRepository;

    let results = repo
        .find_by_job_id_and_batch_index(&mut txn, JobId::new(), 99)
        .await
        .expect("find");

    assert!(results.is_empty());
}
