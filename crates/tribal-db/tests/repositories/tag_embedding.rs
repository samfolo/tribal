use tribal_db::{
    PgTagEmbeddingRepository, PgTagRegistryRepository, TagEmbeddingRepository,
    TagRegistryRepository,
};
use tribal_test_utils::{
    LOCALE_SENSITIVE_TAGS, TestDb, a_new_tag_embedding, create_complete_profile,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DIM: u32 = 768;

/// Inserts tags into the registry so they can be referenced by
/// `tag_embeddings` (FK constraint).
async fn seed_tags(txn: &mut sqlx::PgConnection, tags: &[&str]) {
    let repo = PgTagRegistryRepository;
    for &tag in tags {
        repo.upsert(txn, tag).await.expect("seed tag");
    }
}

/// Creates a 768-dimensional unit vector with 1.0 at the given index.
fn make_test_embedding(dominant_index: usize) -> Vec<f32> {
    let mut v = vec![0.0_f32; 768];
    v[dominant_index] = 1.0;
    v
}

// ---------------------------------------------------------------------------
// batch_upsert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_batch_upsert_inserts_new_embeddings() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &["rust", "python"]).await;
    let profile = create_complete_profile(&mut txn, "test-model", DIM).await;

    let embeddings = vec![
        a_new_tag_embedding()
            .tag("rust".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(0))
            .build(),
        a_new_tag_embedding()
            .tag("python".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(1))
            .build(),
    ];

    repo.batch_upsert(&mut txn, &embeddings)
        .await
        .expect("batch_upsert");

    let results = repo
        .similarity_search(&mut txn, &make_test_embedding(0), profile, DIM, 0.99, 1)
        .await
        .expect("similarity_search");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].tag(), "rust");
}

#[tokio::test]
async fn test_batch_upsert_empty_is_no_op() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    repo.batch_upsert(&mut txn, &[])
        .await
        .expect("batch_upsert");
}

#[tokio::test]
async fn test_batch_upsert_on_conflict_is_idempotent() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &["rust"]).await;
    let profile = create_complete_profile(&mut txn, "test-model", DIM).await;

    repo.batch_upsert(
        &mut txn,
        &[a_new_tag_embedding()
            .tag("rust".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(0))
            .build()],
    )
    .await
    .expect("first upsert");

    // Second upsert with a different vector — ON CONFLICT DO NOTHING
    // should leave the original intact.
    repo.batch_upsert(
        &mut txn,
        &[a_new_tag_embedding()
            .tag("rust".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(5))
            .build()],
    )
    .await
    .expect("second upsert");

    // The original embedding (index 0) should still be stored.
    let results = repo
        .similarity_search(&mut txn, &make_test_embedding(0), profile, DIM, 0.99, 1)
        .await
        .expect("similarity_search");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].tag(), "rust");
}

// ---------------------------------------------------------------------------
// similarity_search
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_similarity_search_returns_match_above_threshold() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &["rust"]).await;
    let profile = create_complete_profile(&mut txn, "test-model", DIM).await;

    repo.batch_upsert(
        &mut txn,
        &[a_new_tag_embedding()
            .tag("rust".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(0))
            .build()],
    )
    .await
    .expect("upsert");

    let results = repo
        .similarity_search(&mut txn, &make_test_embedding(0), profile, DIM, 0.9, 5)
        .await
        .expect("similarity_search");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].tag(), "rust");
    assert!(
        results[0].similarity() > 0.99,
        "identical vectors should have similarity ~1.0, got {}",
        results[0].similarity(),
    );
}

#[tokio::test]
async fn test_similarity_search_excludes_below_threshold() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &["rust"]).await;
    let profile = create_complete_profile(&mut txn, "test-model", DIM).await;

    repo.batch_upsert(
        &mut txn,
        &[a_new_tag_embedding()
            .tag("rust".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(0))
            .build()],
    )
    .await
    .expect("upsert");

    // Orthogonal vector — cosine similarity = 0.0.
    let results = repo
        .similarity_search(&mut txn, &make_test_embedding(1), profile, DIM, 0.5, 5)
        .await
        .expect("similarity_search");

    assert!(
        results.is_empty(),
        "orthogonal vectors should not match at threshold 0.5: {results:?}",
    );
}

#[tokio::test]
async fn test_similarity_search_orders_by_similarity_then_usage_then_tag() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let tag_repo = PgTagRegistryRepository;
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &["alpha", "beta"]).await;
    let profile = create_complete_profile(&mut txn, "test-model", DIM).await;

    // Give "beta" a higher usage_count so tie-breaking is testable.
    tag_repo
        .increment_usage_count(&mut txn, &["beta".to_owned()])
        .await
        .expect("increment");

    // Both get the same embedding — same similarity to the query.
    let embeddings = vec![
        a_new_tag_embedding()
            .tag("alpha".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(0))
            .build(),
        a_new_tag_embedding()
            .tag("beta".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(0))
            .build(),
    ];
    repo.batch_upsert(&mut txn, &embeddings)
        .await
        .expect("upsert");

    let results = repo
        .similarity_search(&mut txn, &make_test_embedding(0), profile, DIM, 0.9, 5)
        .await
        .expect("similarity_search");

    assert_eq!(results.len(), 2);
    // Same similarity -> higher usage_count wins -> "beta" first.
    assert_eq!(results[0].tag(), "beta");
    assert_eq!(results[1].tag(), "alpha");
}

#[tokio::test]
async fn test_similarity_search_filters_by_profile() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &["rust"]).await;
    let profile_a = create_complete_profile(&mut txn, "model-a", DIM).await;
    let profile_b = create_complete_profile(&mut txn, "model-b", DIM).await;

    repo.batch_upsert(
        &mut txn,
        &[a_new_tag_embedding()
            .tag("rust".to_owned())
            .embedding_profile_id(profile_a)
            .embedding(make_test_embedding(0))
            .build()],
    )
    .await
    .expect("upsert");

    // Query a different profile; should find nothing.
    let results = repo
        .similarity_search(&mut txn, &make_test_embedding(0), profile_b, DIM, 0.5, 5)
        .await
        .expect("similarity_search");

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_similarity_search_respects_limit() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &["alpha", "beta", "gamma"]).await;
    let profile = create_complete_profile(&mut txn, "test-model", DIM).await;

    let embeddings = vec![
        a_new_tag_embedding()
            .tag("alpha".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(0))
            .build(),
        a_new_tag_embedding()
            .tag("beta".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(0))
            .build(),
        a_new_tag_embedding()
            .tag("gamma".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(0))
            .build(),
    ];
    repo.batch_upsert(&mut txn, &embeddings)
        .await
        .expect("upsert");

    let results = repo
        .similarity_search(&mut txn, &make_test_embedding(0), profile, DIM, 0.5, 1)
        .await
        .expect("similarity_search");

    assert_eq!(results.len(), 1);
}

// ---------------------------------------------------------------------------
// find_tags_missing_embeddings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_tags_missing_embeddings_returns_unembedded() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &["alpha", "beta", "gamma"]).await;
    let profile = create_complete_profile(&mut txn, "test-model", DIM).await;

    // Embed only "beta".
    repo.batch_upsert(
        &mut txn,
        &[a_new_tag_embedding()
            .tag("beta".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(0))
            .build()],
    )
    .await
    .expect("upsert");

    let missing = repo
        .find_tags_missing_embeddings(&mut txn, profile)
        .await
        .expect("find_tags_missing_embeddings");

    assert_eq!(missing, vec!["alpha", "gamma"]);
}

#[tokio::test]
async fn test_find_tags_missing_embeddings_filters_by_profile() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &["rust"]).await;
    let profile_a = create_complete_profile(&mut txn, "model-a", DIM).await;
    let profile_b = create_complete_profile(&mut txn, "model-b", DIM).await;

    // Embed for profile_a only.
    repo.batch_upsert(
        &mut txn,
        &[a_new_tag_embedding()
            .tag("rust".to_owned())
            .embedding_profile_id(profile_a)
            .embedding(make_test_embedding(0))
            .build()],
    )
    .await
    .expect("upsert");

    // Query for profile_b; "rust" should show as missing.
    let missing = repo
        .find_tags_missing_embeddings(&mut txn, profile_b)
        .await
        .expect("find_tags_missing_embeddings");

    assert_eq!(missing, vec!["rust"]);
}

#[tokio::test]
async fn test_find_tags_missing_embeddings_returns_empty_when_all_embedded() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &["rust", "python"]).await;
    let profile = create_complete_profile(&mut txn, "test-model", DIM).await;

    let embeddings = vec![
        a_new_tag_embedding()
            .tag("rust".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(0))
            .build(),
        a_new_tag_embedding()
            .tag("python".to_owned())
            .embedding_profile_id(profile)
            .embedding(make_test_embedding(1))
            .build(),
    ];
    repo.batch_upsert(&mut txn, &embeddings)
        .await
        .expect("upsert");

    let missing = repo
        .find_tags_missing_embeddings(&mut txn, profile)
        .await
        .expect("find_tags_missing_embeddings");

    assert!(missing.is_empty());
}

// ---------------------------------------------------------------------------
// Cross-locale ordering parity
// ---------------------------------------------------------------------------

/// Two tags from the locale-sensitive set whose `C` and ICU orders flip.
const TIE_BREAK_TAGS: [&str; 2] = ["zebra", "Ápple"];

/// Seeds two tags with byte-identical embeddings and equal usage counts, so
/// distance and usage tie and `te.tag` is the sole discriminator, then
/// returns the similarity order.
async fn tie_break_order(ctx: &TestDb) -> Vec<String> {
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &TIE_BREAK_TAGS).await;
    let profile = create_complete_profile(&mut txn, "test-model", DIM).await;
    let shared = make_test_embedding(0);
    let embeddings: Vec<_> = TIE_BREAK_TAGS
        .iter()
        .map(|&tag| {
            a_new_tag_embedding()
                .tag(tag.to_owned())
                .embedding_profile_id(profile)
                .embedding(shared.clone())
                .build()
        })
        .collect();
    repo.batch_upsert(&mut txn, &embeddings)
        .await
        .expect("seed");

    repo.similarity_search(&mut txn, &shared, profile, DIM, 0.5, 10)
        .await
        .expect("similarity_search")
        .iter()
        .map(|result| result.tag().to_owned())
        .collect()
}

/// Seeds the locale-sensitive tags with no embeddings and returns the order
/// `find_tags_missing_embeddings` answers with.
async fn missing_embeddings_order(ctx: &TestDb) -> Vec<String> {
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTagEmbeddingRepository;

    seed_tags(&mut txn, &LOCALE_SENSITIVE_TAGS).await;
    let profile = create_complete_profile(&mut txn, "test-model", DIM).await;

    repo.find_tags_missing_embeddings(&mut txn, profile)
        .await
        .expect("find_tags_missing_embeddings")
}

#[tokio::test]
async fn test_similarity_tie_break_agrees_across_cluster_locales() {
    let c = TestDb::with_c_locale().await;
    let comparison = TestDb::with_comparison_locale().await;

    assert_eq!(
        tie_break_order(&c).await,
        tie_break_order(&comparison).await,
        "the tag tie-break must not depend on the cluster's default collation",
    );
}

#[tokio::test]
async fn test_missing_embeddings_ordering_agrees_across_cluster_locales() {
    let c = TestDb::with_c_locale().await;
    let comparison = TestDb::with_comparison_locale().await;

    assert_eq!(
        missing_embeddings_order(&c).await,
        missing_embeddings_order(&comparison).await,
        "reindex selection order must not depend on the cluster's default collation",
    );
}
