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
