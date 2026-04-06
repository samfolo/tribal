use tribal_db::{
    DbError, PgPrincipalRepository, PgRetrievalFeedbackRepository, PrincipalRepository,
    RetrievalFeedbackRepository,
};
use tribal_domain::{FeedbackRating, KnowledgeItemId, PrincipalId, RetrievalFeedbackId};
use tribal_test_utils::{
    a_new_principal, a_new_retrieval_feedback, a_new_system_fingerprint, test_context,
    upsert_system_fingerprint,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal and returns its ID.
async fn setup_principal(txn: &mut sqlx::PgConnection, suffix: &str) -> PrincipalId {
    PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:feedback-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal")
        .id()
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_returns_populated_retrieval_feedback() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgRetrievalFeedbackRepository;

    let principal_id = setup_principal(&mut txn, "insert").await;
    upsert_system_fingerprint(&mut txn, &a_new_system_fingerprint().build()).await;

    let new = a_new_retrieval_feedback()
        .principal_id(principal_id)
        .rating(FeedbackRating::Negative)
        .notes(Some("not relevant".to_owned()))
        .build();

    let fb = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(fb.id().to_string().starts_with("fb_"));
    assert_eq!(fb.trace_id(), "00000000000000000000000000000001");
    assert_eq!(fb.query_text(), "test query");
    assert_eq!(fb.embedding_model(), "nomic-embed-text:v1.5");
    assert!(fb.returned_item_ids().is_empty());
    assert!(fb.explored_anchor_ids().is_empty());
    let hash = fb.system_fingerprint_hash();
    assert_eq!(hash.len(), 64);
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(fb.principal_id(), principal_id);
    assert_eq!(fb.rating(), FeedbackRating::Negative);
    assert_eq!(fb.notes(), Some("not relevant"));
}

#[tokio::test]
async fn test_insert_with_populated_uuid_arrays() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgRetrievalFeedbackRepository;

    let principal_id = setup_principal(&mut txn, "arrays").await;
    upsert_system_fingerprint(&mut txn, &a_new_system_fingerprint().build()).await;

    let returned = vec![KnowledgeItemId::new(), KnowledgeItemId::new()];
    let explored = vec![KnowledgeItemId::new()];

    let new = a_new_retrieval_feedback()
        .principal_id(principal_id)
        .returned_item_ids(returned.clone())
        .explored_anchor_ids(explored.clone())
        .build();

    let fb = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(fb.returned_item_ids(), &returned);
    assert_eq!(fb.explored_anchor_ids(), &explored);
}

#[tokio::test]
async fn test_insert_with_empty_arrays() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgRetrievalFeedbackRepository;

    let principal_id = setup_principal(&mut txn, "empty-arrays").await;
    upsert_system_fingerprint(&mut txn, &a_new_system_fingerprint().build()).await;

    let new = a_new_retrieval_feedback()
        .principal_id(principal_id)
        .build();

    let fb = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(fb.returned_item_ids().is_empty());
    assert!(fb.explored_anchor_ids().is_empty());
}

// ---------------------------------------------------------------------------
// find_by_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_id_returns_retrieval_feedback() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgRetrievalFeedbackRepository;

    let principal_id = setup_principal(&mut txn, "find").await;
    upsert_system_fingerprint(&mut txn, &a_new_system_fingerprint().build()).await;

    let fb = repo
        .insert(
            &mut txn,
            &a_new_retrieval_feedback()
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("insert");

    let found = repo.find_by_id(&mut txn, fb.id()).await.expect("find");
    assert_eq!(found.id(), fb.id());
    assert_eq!(found.query_text(), fb.query_text());
}

#[tokio::test]
async fn test_find_by_id_not_found() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgRetrievalFeedbackRepository;

    let result = repo.find_by_id(&mut txn, RetrievalFeedbackId::new()).await;
    assert!(
        matches!(result, Err(DbError::NotFound { .. })),
        "expected NotFound, got: {result:?}"
    );
}
