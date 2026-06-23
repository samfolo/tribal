use tribal_db::{
    DbError, ExtractionResultRepository, JobRepository, PgExtractionResultRepository,
    PgJobRepository, PgPrincipalRepository, PgProjectRepository, PrincipalRepository,
    ProjectRepository,
};
use tribal_domain::GitRemote;
use tribal_test_utils::{
    TestDb, a_new_extraction_result, a_new_job, a_new_principal, a_new_project,
    a_new_prompt_version, a_new_system_fingerprint, insert_prompt_version,
    upsert_system_fingerprint,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal, project, prompt versions, and job, returning the
/// job ID needed for extraction result tests.
async fn setup_job(txn: &mut sqlx::PgConnection, suffix: &str) -> tribal_domain::JobId {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:exr-test-{suffix}"))
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
                    &format!("test/exr-{suffix}"),
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project");

    let pv_id = insert_prompt_version(txn, &a_new_prompt_version().build()).await;

    let fingerprint_hash =
        upsert_system_fingerprint(txn, &a_new_system_fingerprint().build()).await;

    let job = PgJobRepository
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

    job.id()
}

// ---------------------------------------------------------------------------
// insert — returns populated result
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_returns_populated_result() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgExtractionResultRepository;

    let job_id = setup_job(&mut txn, "insert").await;

    let candidates = serde_json::json!([
        {
            "kind": "fact",
            "content": "tokio block_on deadlocks in async context",
            "suggested_tags": ["async", "tokio"],
            "suggested_references": []
        }
    ]);
    let relation_hints = serde_json::json!([
        {"source_index": 0, "target_index": 1, "hint_type": "derived_from"}
    ]);

    let new = a_new_extraction_result()
        .job_id(job_id)
        .candidates(candidates.clone())
        .relation_hints(relation_hints.clone())
        .build();

    let result = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(
        result.id().to_string().starts_with("exr_"),
        "expected exr_ prefix, got: {}",
        result.id()
    );
    assert_eq!(result.job_id(), job_id);
    assert_eq!(result.candidates(), &candidates);
    assert_eq!(result.relation_hints(), &relation_hints);
}

// ---------------------------------------------------------------------------
// find_by_job_id — returns result
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_job_id_returns_result() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgExtractionResultRepository;

    let job_id = setup_job(&mut txn, "find").await;

    let candidates = serde_json::json!([
        {"kind": "heuristic", "content": "always size pools", "suggested_tags": [], "suggested_references": []}
    ]);

    let inserted = repo
        .insert(
            &mut txn,
            &a_new_extraction_result()
                .job_id(job_id)
                .candidates(candidates.clone())
                .build(),
        )
        .await
        .expect("insert");

    let found = repo
        .find_by_job_id(&mut txn, job_id)
        .await
        .expect("find_by_job_id")
        .expect("should exist");

    assert_eq!(found.id(), inserted.id());
    assert_eq!(found.job_id(), job_id);
    assert_eq!(found.candidates(), &candidates);
}

// ---------------------------------------------------------------------------
// find_by_job_id — returns None when absent
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_job_id_returns_none_when_absent() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgExtractionResultRepository;

    let job_id = setup_job(&mut txn, "absent").await;

    let found = repo
        .find_by_job_id(&mut txn, job_id)
        .await
        .expect("find_by_job_id");

    assert!(found.is_none());
}

// ---------------------------------------------------------------------------
// insert — unique violation on duplicate job_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_duplicate_job_id_returns_unique_violation() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgExtractionResultRepository;

    let job_id = setup_job(&mut txn, "dup").await;

    let first = a_new_extraction_result()
        .job_id(job_id)
        .candidates(serde_json::json!([{"kind": "fact", "content": "first"}]))
        .build();

    repo.insert(&mut txn, &first).await.expect("first insert");

    let second = a_new_extraction_result()
        .job_id(job_id)
        .candidates(serde_json::json!([{"kind": "fact", "content": "second"}]))
        .build();

    let err = repo
        .insert(&mut txn, &second)
        .await
        .expect_err("duplicate should fail");

    assert!(
        matches!(err, DbError::UniqueViolation { .. }),
        "expected UniqueViolation, got: {err:?}"
    );
}
