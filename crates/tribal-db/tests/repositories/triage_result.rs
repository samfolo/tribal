use tribal_db::{
    DbError, ItemObservationRepository, JobRepository, KnowledgeItemRepository,
    PgItemObservationRepository, PgKnowledgeItemRepository, PgPrincipalRepository,
    PgProjectRepository, PgTriageResultRepository, PrincipalRepository, ProjectRepository,
    TriageResultRepository,
};
use tribal_domain::{GitRemote, JobId, KnowledgeItemId, PrincipalId, ProjectId, TriageOutcome};
use tribal_test_utils::{
    a_new_item_observation, a_new_job, a_new_knowledge_item, a_new_principal, a_new_project,
    a_new_prompt_version, a_new_system_fingerprint, a_new_triage_result_created,
    a_new_triage_result_duplicate, a_new_triage_result_failed, insert_prompt_version, test_context,
    upsert_system_fingerprint,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal, project, prompt_version, and job, returning the IDs
/// needed for triage result tests.
async fn setup_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (PrincipalId, ProjectId, JobId) {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:tr-test-{suffix}"))
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
                    &format!("test/tr-{suffix}"),
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
// insert — Created outcome
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_created_outcome() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTriageResultRepository;

    let (principal_id, project_id, job_id) = setup_prerequisites(&mut txn, "created").await;
    let item_id = setup_item(&mut txn, project_id, principal_id).await;

    let new = a_new_triage_result_created()
        .job_id(job_id)
        .batch_index(0)
        .outcome(TriageOutcome::Created { item_id })
        .build();

    let result = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(
        result.id().to_string().starts_with("tr_"),
        "expected tr_ prefix, got: {}",
        result.id()
    );
    assert_eq!(result.job_id(), job_id);
    assert_eq!(result.batch_index(), 0);
    assert!(result.similar_items().is_empty());
    assert_eq!(*result.outcome(), TriageOutcome::Created { item_id });
}

// ---------------------------------------------------------------------------
// insert — Duplicate outcome
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_duplicate_outcome() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTriageResultRepository;

    let (principal_id, project_id, job_id) = setup_prerequisites(&mut txn, "duplicate").await;
    let matched_item_id = setup_item(&mut txn, project_id, principal_id).await;

    let obs = PgItemObservationRepository
        .insert(
            &mut txn,
            &a_new_item_observation()
                .knowledge_item_id(matched_item_id)
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("insert observation");

    let new = a_new_triage_result_duplicate()
        .job_id(job_id)
        .batch_index(1)
        .outcome(TriageOutcome::Duplicate {
            observation_id: obs.id(),
            matched_item_id,
        })
        .build();

    let result = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(result.batch_index(), 1);
    assert_eq!(
        *result.outcome(),
        TriageOutcome::Duplicate {
            observation_id: obs.id(),
            matched_item_id,
        }
    );
}

// ---------------------------------------------------------------------------
// insert — Failed outcome
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_failed_outcome() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTriageResultRepository;

    let (_principal_id, _project_id, job_id) = setup_prerequisites(&mut txn, "failed").await;

    let new = a_new_triage_result_failed()
        .job_id(job_id)
        .batch_index(2)
        .outcome(TriageOutcome::Failed {
            error_message: "provider timeout".to_owned(),
            retryable: true,
        })
        .build();

    let result = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(result.batch_index(), 2);
    assert_eq!(
        *result.outcome(),
        TriageOutcome::Failed {
            error_message: "provider timeout".to_owned(),
            retryable: true,
        }
    );
}

// ---------------------------------------------------------------------------
// insert — UniqueViolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_duplicate_job_batch_index_returns_unique_violation() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTriageResultRepository;

    let (principal_id, project_id, job_id) = setup_prerequisites(&mut txn, "uv").await;
    let item_id = setup_item(&mut txn, project_id, principal_id).await;

    let new = a_new_triage_result_created()
        .job_id(job_id)
        .batch_index(0)
        .outcome(TriageOutcome::Created { item_id })
        .build();

    repo.insert(&mut txn, &new).await.expect("first insert");

    let item_id_2 = setup_item(&mut txn, project_id, principal_id).await;
    let duplicate = a_new_triage_result_created()
        .job_id(job_id)
        .batch_index(0)
        .outcome(TriageOutcome::Created { item_id: item_id_2 })
        .build();

    let err = repo.insert(&mut txn, &duplicate).await.unwrap_err();
    assert!(
        matches!(err, DbError::UniqueViolation { .. }),
        "expected UniqueViolation, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// find_by_job_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_job_id_returns_all_ordered() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTriageResultRepository;

    let (principal_id, project_id, job_id) = setup_prerequisites(&mut txn, "find-all").await;
    let item_id = setup_item(&mut txn, project_id, principal_id).await;
    let matched_id = setup_item(&mut txn, project_id, principal_id).await;

    let obs = PgItemObservationRepository
        .insert(
            &mut txn,
            &a_new_item_observation()
                .knowledge_item_id(matched_id)
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("insert observation");

    // batch_index 0: Created
    repo.insert(
        &mut txn,
        &a_new_triage_result_created()
            .job_id(job_id)
            .batch_index(0)
            .outcome(TriageOutcome::Created { item_id })
            .build(),
    )
    .await
    .expect("insert created");

    // batch_index 1: Duplicate
    repo.insert(
        &mut txn,
        &a_new_triage_result_duplicate()
            .job_id(job_id)
            .batch_index(1)
            .outcome(TriageOutcome::Duplicate {
                observation_id: obs.id(),
                matched_item_id: matched_id,
            })
            .build(),
    )
    .await
    .expect("insert duplicate");

    // batch_index 2: Failed
    repo.insert(
        &mut txn,
        &a_new_triage_result_failed()
            .job_id(job_id)
            .batch_index(2)
            .build(),
    )
    .await
    .expect("insert failed");

    let results = repo.find_by_job_id(&mut txn, job_id).await.expect("find");

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].batch_index(), 0);
    assert!(matches!(
        results[0].outcome(),
        TriageOutcome::Created { .. }
    ));
    assert_eq!(results[1].batch_index(), 1);
    assert!(matches!(
        results[1].outcome(),
        TriageOutcome::Duplicate { .. }
    ));
    assert_eq!(results[2].batch_index(), 2);
    assert!(matches!(results[2].outcome(), TriageOutcome::Failed { .. }));
}

#[tokio::test]
async fn test_find_by_job_id_returns_empty_for_unknown_job() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTriageResultRepository;

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
async fn test_find_by_job_id_and_batch_index_returns_match() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTriageResultRepository;

    let (principal_id, project_id, job_id) = setup_prerequisites(&mut txn, "find-bi").await;
    let item_id = setup_item(&mut txn, project_id, principal_id).await;

    repo.insert(
        &mut txn,
        &a_new_triage_result_created()
            .job_id(job_id)
            .batch_index(5)
            .outcome(TriageOutcome::Created { item_id })
            .build(),
    )
    .await
    .expect("insert");

    let found = repo
        .find_by_job_id_and_batch_index(&mut txn, job_id, 5)
        .await
        .expect("find");

    assert!(found.is_some());
    assert_eq!(found.unwrap().batch_index(), 5);
}

#[tokio::test]
async fn test_find_by_job_id_and_batch_index_returns_none_for_unknown() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTriageResultRepository;

    let found = repo
        .find_by_job_id_and_batch_index(&mut txn, JobId::new(), 99)
        .await
        .expect("find");

    assert!(found.is_none());
}
