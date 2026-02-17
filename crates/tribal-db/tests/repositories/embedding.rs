use tribal_db::{
    DbError, EmbeddingRepository, KnowledgeItemRepository, PgEmbeddingRepository,
    PgKnowledgeItemRepository, PgPrincipalRepository, PgProjectRepository, PrincipalRepository,
    ProjectRepository,
};
use tribal_domain::{KnowledgeItemId, PrincipalId, ProjectId};
use tribal_test_utils::{
    a_new_embedding, a_new_knowledge_item, a_new_principal, a_new_project, test_context,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal, project, and knowledge item, returning their IDs.
///
/// The `suffix` disambiguates git remotes and principal keys within a
/// single test transaction.
async fn setup_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (PrincipalId, ProjectId, KnowledgeItemId) {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:test-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal");

    let project = PgProjectRepository
        .insert(
            txn,
            &a_new_project()
                .git_remote(format!("git@github.com:test/{suffix}.git"))
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

    (principal.id(), project.id(), item.id())
}

/// Creates a 768-dimensional unit vector with 1.0 at the given index.
fn make_test_embedding(dominant_index: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; 768];
    v[dominant_index] = 1.0;
    v
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_returns_populated_embedding() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingRepository;

    let (_, _, item_id) = setup_prerequisites(&mut txn, "emb-insert-pop").await;

    let embedding_vec = make_test_embedding(0);
    let new = a_new_embedding()
        .knowledge_item_id(item_id)
        .model("text-embedding-test".to_owned())
        .embedding(embedding_vec.clone())
        .build();

    let emb = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(emb.knowledge_item_id(), item_id);
    assert_eq!(emb.model(), "text-embedding-test");
    assert_eq!(emb.dimensions(), 768);
    assert_eq!(emb.embedding(), &embedding_vec);
}

#[tokio::test]
async fn test_insert_generates_prefixed_id() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingRepository;

    let (_, _, item_id) = setup_prerequisites(&mut txn, "emb-insert-prefix").await;

    let new = a_new_embedding()
        .knowledge_item_id(item_id)
        .build();

    let emb = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(
        emb.id().to_string().starts_with("emb_"),
        "expected emb_ prefix, got: {}",
        emb.id()
    );
}

#[tokio::test]
async fn test_insert_duplicate_item_model_returns_unique_violation() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingRepository;

    let (_, _, item_id) = setup_prerequisites(&mut txn, "emb-insert-dup").await;

    let new = a_new_embedding()
        .knowledge_item_id(item_id)
        .model("same-model".to_owned())
        .build();

    repo.insert(&mut txn, &new).await.expect("first insert");

    let duplicate = a_new_embedding()
        .knowledge_item_id(item_id)
        .model("same-model".to_owned())
        .embedding(make_test_embedding(1))
        .build();

    let result = repo.insert(&mut txn, &duplicate).await;
    assert!(
        matches!(result, Err(DbError::UniqueViolation { .. })),
        "expected UniqueViolation, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// find_by_knowledge_item_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_knowledge_item_id_returns_embedding() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingRepository;

    let (_, _, item_id) = setup_prerequisites(&mut txn, "emb-find").await;

    let embedding_vec = make_test_embedding(3);
    let new = a_new_embedding()
        .knowledge_item_id(item_id)
        .model("find-test-model".to_owned())
        .embedding(embedding_vec.clone())
        .build();

    let inserted = repo.insert(&mut txn, &new).await.expect("insert");

    let found = repo
        .find_by_knowledge_item_id(&mut txn, item_id, "find-test-model")
        .await
        .expect("find");

    let found = found.expect("expected Some(Embedding)");
    assert_eq!(found.id(), inserted.id());
    assert_eq!(found.knowledge_item_id(), item_id);
    assert_eq!(found.model(), "find-test-model");
    assert_eq!(found.dimensions(), 768);
    assert_eq!(found.embedding(), &embedding_vec);
}

#[tokio::test]
async fn test_find_by_knowledge_item_id_not_found_returns_none() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingRepository;

    let found = repo
        .find_by_knowledge_item_id(&mut txn, KnowledgeItemId::new(), "nonexistent-model")
        .await
        .expect("find");

    assert!(found.is_none());
}
