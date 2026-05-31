use tribal_db::{EmbeddingProfileRepository, PgEmbeddingProfileRepository};
use tribal_test_utils::{a_new_embedding_profile, test_context};

#[tokio::test]
async fn test_mark_failed_transitions_building_then_is_idempotent() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingProfileRepository;

    // The repository always inserts in the `building` state.
    let profile = repo
        .insert(&mut txn, &a_new_embedding_profile().build())
        .await
        .expect("insert profile");

    assert!(
        repo.mark_failed(&mut txn, profile.id())
            .await
            .expect("mark_failed"),
        "a building profile transitions to failed",
    );
    // No longer building.
    assert!(
        repo.find_building(&mut txn)
            .await
            .expect("find_building")
            .is_none(),
    );
    // Idempotent: a second call is a no-op (no longer building).
    assert!(
        !repo
            .mark_failed(&mut txn, profile.id())
            .await
            .expect("mark_failed again"),
    );
}

#[tokio::test]
async fn test_mark_superseded_only_from_failed() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgEmbeddingProfileRepository;

    let profile = repo
        .insert(&mut txn, &a_new_embedding_profile().build())
        .await
        .expect("insert profile");

    // A building profile cannot be superseded directly.
    assert!(
        !repo
            .mark_superseded(&mut txn, profile.id())
            .await
            .expect("mark_superseded while building"),
    );

    repo.mark_failed(&mut txn, profile.id())
        .await
        .expect("mark_failed");
    assert!(
        repo.mark_superseded(&mut txn, profile.id())
            .await
            .expect("mark_superseded"),
        "a failed profile is superseded on prune",
    );
    // Idempotent.
    assert!(
        !repo
            .mark_superseded(&mut txn, profile.id())
            .await
            .expect("mark_superseded again"),
    );
}
