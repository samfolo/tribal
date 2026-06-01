use tribal_db::{
    DbError, EmbeddingRepository, KnowledgeItemRepository, NewReindexQuarantine, NewReindexRun,
    PgEmbeddingRepository, PgKnowledgeItemRepository, PgPrincipalRepository, PgProjectRepository,
    PgReindexQuarantineRepository, PgReindexRunRepository, PrincipalRepository, ProjectRepository,
    ReindexQuarantineRepository, ReindexRunRepository,
};
use tribal_domain::{
    EmbeddingErrorClass, EmbeddingProfileId, GitRemote, KnowledgeItemId, ReindexEntityKind,
};
use tribal_test_utils::{
    a_new_embedding, a_new_knowledge_item, a_new_principal, a_new_project, ensure_genesis_profile,
    test_context,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const EMBEDDING_MODEL: &str = "text-embedding-test";

/// Inserts a principal, project, knowledge item, and a genesis embedding
/// profile, returning the item id and the profile id.
///
/// The `suffix` disambiguates git remotes and principal keys within a single
/// test transaction.
async fn setup_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (KnowledgeItemId, EmbeddingProfileId) {
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
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    &format!("test/{suffix}"),
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

    let profile = ensure_genesis_profile(txn, EMBEDDING_MODEL, 768).await;

    (item.id(), profile.id())
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

    let (item_id, profile_id) = setup_prerequisites(&mut txn, "emb-insert-pop").await;

    let embedding_vec = make_test_embedding(0);
    let new = a_new_embedding()
        .knowledge_item_id(item_id)
        .embedding_profile_id(profile_id)
        .model(EMBEDDING_MODEL.to_owned())
        .embedding(embedding_vec.clone())
        .build();

    let emb = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(emb.knowledge_item_id(), item_id);
    assert_eq!(emb.embedding_profile_id(), profile_id);
    assert_eq!(emb.model(), EMBEDDING_MODEL);
    assert_eq!(emb.dimensions(), 768);
    assert_eq!(emb.embedding(), &embedding_vec);
}

#[tokio::test]
async fn test_insert_generates_prefixed_id() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingRepository;

    let (item_id, profile_id) = setup_prerequisites(&mut txn, "emb-insert-prefix").await;

    let new = a_new_embedding()
        .knowledge_item_id(item_id)
        .embedding_profile_id(profile_id)
        .build();

    let emb = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(
        emb.id().to_string().starts_with("emb_"),
        "expected emb_ prefix, got: {}",
        emb.id()
    );
}

#[tokio::test]
async fn test_insert_duplicate_item_profile_returns_unique_violation() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingRepository;

    let (item_id, profile_id) = setup_prerequisites(&mut txn, "emb-insert-dup").await;

    let new = a_new_embedding()
        .knowledge_item_id(item_id)
        .embedding_profile_id(profile_id)
        .build();

    repo.insert(&mut txn, &new).await.expect("first insert");

    let duplicate = a_new_embedding()
        .knowledge_item_id(item_id)
        .embedding_profile_id(profile_id)
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

    let (item_id, profile_id) = setup_prerequisites(&mut txn, "emb-find").await;

    let embedding_vec = make_test_embedding(3);
    let new = a_new_embedding()
        .knowledge_item_id(item_id)
        .embedding_profile_id(profile_id)
        .model(EMBEDDING_MODEL.to_owned())
        .embedding(embedding_vec.clone())
        .build();

    let inserted = repo.insert(&mut txn, &new).await.expect("insert");

    let found = repo
        .find_by_knowledge_item_id(&mut txn, item_id, profile_id)
        .await
        .expect("find");

    let found = found.expect("expected Some(Embedding)");
    assert_eq!(found.id(), inserted.id());
    assert_eq!(found.knowledge_item_id(), item_id);
    assert_eq!(found.embedding_profile_id(), profile_id);
    assert_eq!(found.model(), EMBEDDING_MODEL);
    assert_eq!(found.dimensions(), 768);
    assert_eq!(found.embedding(), &embedding_vec);
}

#[tokio::test]
async fn test_find_by_knowledge_item_id_not_found_returns_none() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingRepository;

    let found = repo
        .find_by_knowledge_item_id(&mut txn, KnowledgeItemId::new(), EmbeddingProfileId::new())
        .await
        .expect("find");

    assert!(found.is_none());
}

// ---------------------------------------------------------------------------
// set-difference (backfill enumeration)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_items_without_embedding_is_the_set_difference() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingRepository;

    let principal = PgPrincipalRepository
        .insert(
            &mut txn,
            &a_new_principal()
                .principal_key("user:set-diff".to_owned())
                .build(),
        )
        .await
        .expect("insert principal");
    let project = PgProjectRepository
        .insert(
            &mut txn,
            &a_new_project()
                .git_remote(GitRemote::from_parts("github.com", "test/set-diff", None))
                .build(),
        )
        .await
        .expect("insert project");
    let profile = ensure_genesis_profile(&mut txn, EMBEDDING_MODEL, 768).await;

    let new_item = || {
        a_new_knowledge_item()
            .project_id(project.id())
            .principal_id(principal.id())
            .build()
    };
    let embedded = PgKnowledgeItemRepository
        .insert(&mut txn, &new_item())
        .await
        .expect("insert embedded item")
        .id();
    let missing = PgKnowledgeItemRepository
        .insert(&mut txn, &new_item())
        .await
        .expect("insert missing item")
        .id();

    // Embed only the first item under the profile.
    repo.insert(
        &mut txn,
        &a_new_embedding()
            .knowledge_item_id(embedded)
            .embedding_profile_id(profile.id())
            .model(EMBEDDING_MODEL.to_owned())
            .embedding(make_test_embedding(0))
            .build(),
    )
    .await
    .expect("insert embedding");

    // Exactly the un-embedded item remains in the set-difference.
    assert_eq!(
        repo.count_items_without_embedding(&mut txn, profile.id())
            .await
            .expect("count"),
        1,
    );
    let pending = repo
        .find_items_without_embedding(&mut txn, profile.id(), None, 10)
        .await
        .expect("find");
    assert_eq!(pending, vec![missing]);

    // Quarantining the remaining item removes it from the set-difference too,
    // so a durably-failed entity is skipped rather than re-swept forever. The
    // item quarantine entity_ref is the raw item id text.
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
    PgReindexQuarantineRepository
        .record(
            &mut txn,
            &NewReindexQuarantine {
                reindex_run_id: run.id(),
                target_profile_id: profile.id(),
                kind: ReindexEntityKind::Item,
                entity_ref: missing.inner().to_string(),
                error_class: EmbeddingErrorClass::Permanent,
                error_message: Some("bad input".to_owned()),
            },
        )
        .await
        .expect("record quarantine");

    assert_eq!(
        repo.count_items_without_embedding(&mut txn, profile.id())
            .await
            .expect("count after quarantine"),
        0,
    );
    assert!(
        repo.find_items_without_embedding(&mut txn, profile.id(), None, 10)
            .await
            .expect("find after quarantine")
            .is_empty(),
    );
}

#[tokio::test]
async fn test_batch_insert_skipping_existing_skips_embedded_pairs() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingRepository;

    let principal = PgPrincipalRepository
        .insert(
            &mut txn,
            &a_new_principal()
                .principal_key("user:batch-insert".to_owned())
                .build(),
        )
        .await
        .expect("insert principal");
    let project = PgProjectRepository
        .insert(
            &mut txn,
            &a_new_project()
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    "test/batch-insert",
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project");
    let profile = ensure_genesis_profile(&mut txn, EMBEDDING_MODEL, 768).await;

    let new_item = || {
        a_new_knowledge_item()
            .project_id(project.id())
            .principal_id(principal.id())
            .build()
    };
    let first = PgKnowledgeItemRepository
        .insert(&mut txn, &new_item())
        .await
        .expect("insert item")
        .id();
    let second = PgKnowledgeItemRepository
        .insert(&mut txn, &new_item())
        .await
        .expect("insert item")
        .id();

    // Pre-embed the first item so the batch encounters one conflict.
    repo.insert(
        &mut txn,
        &a_new_embedding()
            .knowledge_item_id(first)
            .embedding_profile_id(profile.id())
            .model(EMBEDDING_MODEL.to_owned())
            .embedding(make_test_embedding(0))
            .build(),
    )
    .await
    .expect("insert embedding");

    let row = |id| {
        a_new_embedding()
            .knowledge_item_id(id)
            .embedding_profile_id(profile.id())
            .model(EMBEDDING_MODEL.to_owned())
            .embedding(make_test_embedding(1))
            .build()
    };
    let inserted = repo
        .batch_insert_skipping_existing(&mut txn, &[row(first), row(second)])
        .await
        .expect("batch insert");
    assert_eq!(inserted, 1, "the already-embedded first item is skipped");

    assert!(
        repo.find_by_knowledge_item_id(&mut txn, second, profile.id())
            .await
            .expect("find")
            .is_some(),
        "the un-embedded second item is now embedded",
    );
}

#[tokio::test]
async fn test_find_items_without_embedding_pages_by_cursor() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingRepository;

    let principal = PgPrincipalRepository
        .insert(
            &mut txn,
            &a_new_principal()
                .principal_key("user:set-diff-page".to_owned())
                .build(),
        )
        .await
        .expect("insert principal");
    let project = PgProjectRepository
        .insert(
            &mut txn,
            &a_new_project()
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    "test/set-diff-page",
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project");
    let profile = ensure_genesis_profile(&mut txn, EMBEDDING_MODEL, 768).await;

    // Two un-embedded items; page with size 1 to walk the cursor across them.
    for _ in 0..2 {
        PgKnowledgeItemRepository
            .insert(
                &mut txn,
                &a_new_knowledge_item()
                    .project_id(project.id())
                    .principal_id(principal.id())
                    .build(),
            )
            .await
            .expect("insert item");
    }

    let page_one = repo
        .find_items_without_embedding(&mut txn, profile.id(), None, 1)
        .await
        .expect("page one");
    assert_eq!(page_one.len(), 1);

    let page_two = repo
        .find_items_without_embedding(&mut txn, profile.id(), Some(page_one[0]), 1)
        .await
        .expect("page two");
    assert_eq!(page_two.len(), 1);
    assert!(page_two[0] > page_one[0], "the cursor must advance by id");

    let page_three = repo
        .find_items_without_embedding(&mut txn, profile.id(), Some(page_two[0]), 1)
        .await
        .expect("page three");
    assert!(page_three.is_empty(), "no items remain past the last");
}
