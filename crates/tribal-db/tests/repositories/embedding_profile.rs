use tribal_db::{EmbeddingProfileRepository, PgEmbeddingProfileRepository};
use tribal_domain::EmbeddingProfileId;
use tribal_test_utils::{TestDb, a_new_embedding_profile};

#[tokio::test]
async fn test_find_by_id_returns_profile_in_any_state_and_none_when_absent() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
    let repo = PgEmbeddingProfileRepository;

    // A freshly inserted profile is `building`; `find_by_id` returns it
    // regardless of state (unlike `find_active`, which is `complete`-only).
    let profile = repo
        .insert(&mut txn, &a_new_embedding_profile().build())
        .await
        .expect("insert profile");

    let found = repo
        .find_by_id(&mut txn, profile.id())
        .await
        .expect("find_by_id")
        .expect("the inserted profile is found");
    assert_eq!(found.id(), profile.id());

    // An id that names no row resolves to `None`.
    assert!(
        repo.find_by_id(&mut txn, EmbeddingProfileId::new())
            .await
            .expect("find_by_id missing")
            .is_none(),
    );
}

#[tokio::test]
async fn test_mark_failed_transitions_building_then_is_idempotent() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");
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
