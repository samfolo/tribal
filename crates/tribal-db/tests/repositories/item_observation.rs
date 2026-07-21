use tribal_db::{
    ItemObservationRepository, KnowledgeItemRepository, PgItemObservationRepository,
    PgKnowledgeItemRepository, PgPrincipalRepository, PgProjectRepository, PrincipalRepository,
    ProjectRepository,
};
use tribal_domain::{GitRemote, KnowledgeItemId, PrincipalId, SourceType};
use tribal_test_utils::{
    TestDb, a_new_item_observation, a_new_knowledge_item, a_new_principal, a_new_project,
    shift_timestamp_by_id,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal, project, and knowledge item, returning the IDs
/// needed for observation tests.
async fn setup_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (PrincipalId, KnowledgeItemId) {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:obs-test-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal");

    let project = PgProjectRepository
        .insert_git(
            txn,
            &a_new_project()
                .remote(GitRemote::from_parts(
                    "github.com",
                    &format!("test/obs-{suffix}"),
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project");

    let item = PgKnowledgeItemRepository
        .insert(
            txn,
            &a_new_knowledge_item()
                .project_id(project.id())
                .principal_id(principal.id())
                .build(),
        )
        .await
        .expect("insert knowledge item");

    (principal.id(), item.id())
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_returns_populated_observation() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgItemObservationRepository;

    let (principal_id, item_id) = setup_prerequisites(&mut txn, "insert").await;

    let new = a_new_item_observation()
        .knowledge_item_id(item_id)
        .principal_id(principal_id)
        .source_type(SourceType::ManualCapture)
        .build();

    let obs = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(
        obs.id().to_string().starts_with("obs_"),
        "expected obs_ prefix, got: {}",
        obs.id()
    );
    assert_eq!(obs.knowledge_item_id(), item_id);
    assert_eq!(obs.principal_id(), principal_id);
    assert_eq!(obs.source_type(), SourceType::ManualCapture);
}

// ---------------------------------------------------------------------------
// find_by_knowledge_item_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_knowledge_item_id_returns_observations_ordered_by_observed_at() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgItemObservationRepository;

    let (principal_id, item_id) = setup_prerequisites(&mut txn, "find-ordered").await;

    // Insert two observations (both get the same now() within this txn).
    let first = repo
        .insert(
            &mut txn,
            &a_new_item_observation()
                .knowledge_item_id(item_id)
                .principal_id(principal_id)
                .source_type(SourceType::AgentMediated)
                .build(),
        )
        .await
        .expect("insert first");

    let second = repo
        .insert(
            &mut txn,
            &a_new_item_observation()
                .knowledge_item_id(item_id)
                .principal_id(principal_id)
                .source_type(SourceType::FileWatch)
                .build(),
        )
        .await
        .expect("insert second");

    // Force the second observation to have an *earlier* timestamp so we
    // can verify the query orders by observed_at, not insertion order.
    shift_timestamp_by_id(
        &mut txn,
        "item_observations",
        "observed_at",
        *second.id().inner(),
        chrono::Duration::hours(-1),
    )
    .await;

    let results = repo
        .find_by_knowledge_item_id(&mut txn, item_id)
        .await
        .expect("find");

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].id(),
        second.id(),
        "earlier observed_at should come first"
    );
    assert_eq!(results[1].id(), first.id());
}

#[tokio::test]
async fn test_find_by_knowledge_item_id_returns_empty_for_unknown_item() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgItemObservationRepository;

    let results = repo
        .find_by_knowledge_item_id(&mut txn, KnowledgeItemId::new())
        .await
        .expect("find");

    assert!(results.is_empty());
}
