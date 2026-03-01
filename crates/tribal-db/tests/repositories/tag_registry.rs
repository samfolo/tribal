use tribal_db::{PgTagRegistryRepository, TagRegistryRepository};
use tribal_test_utils::{shift_tag_registry_timestamp, test_context};

// ---------------------------------------------------------------------------
// upsert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_upsert_inserts_new_tag() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTagRegistryRepository;

    let entry = repo.upsert(&mut txn, "rust").await.expect("upsert");

    assert_eq!(entry.tag(), "rust");
}

#[tokio::test]
async fn test_upsert_existing_tag_is_idempotent() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTagRegistryRepository;

    let first = repo.upsert(&mut txn, "rust").await.expect("upsert 1");
    let second = repo.upsert(&mut txn, "rust").await.expect("upsert 2");

    assert_eq!(first.tag(), second.tag());
    assert_eq!(first.first_seen_at(), second.first_seen_at());
}

// ---------------------------------------------------------------------------
// batch_upsert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_batch_upsert_mix_of_new_and_existing() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTagRegistryRepository;

    let pre_existing = repo.upsert(&mut txn, "existing").await.expect("pre-insert");

    let tags = vec!["existing".to_owned(), "alpha".to_owned(), "beta".to_owned()];
    let entries = repo
        .batch_upsert(&mut txn, &tags)
        .await
        .expect("batch_upsert");

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].tag(), "alpha");
    assert_eq!(entries[1].tag(), "beta");
    assert_eq!(entries[2].tag(), "existing");
    assert_eq!(
        entries[2].first_seen_at(),
        pre_existing.first_seen_at(),
        "pre-existing tag's first_seen_at must be preserved"
    );
}

#[tokio::test]
async fn test_batch_upsert_empty_returns_empty() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTagRegistryRepository;

    let entries = repo
        .batch_upsert(&mut txn, &[])
        .await
        .expect("batch_upsert");

    assert!(entries.is_empty());
}

// ---------------------------------------------------------------------------
// find_all
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_all_returns_complete_registry() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTagRegistryRepository;

    repo.upsert(&mut txn, "zebra").await.expect("upsert");
    repo.upsert(&mut txn, "apple").await.expect("upsert");
    repo.upsert(&mut txn, "mango").await.expect("upsert");

    let all = repo.find_all(&mut txn).await.expect("find_all");

    assert_eq!(all.len(), 3);
    assert_eq!(all[0].tag(), "apple");
    assert_eq!(all[1].tag(), "mango");
    assert_eq!(all[2].tag(), "zebra");
}

#[tokio::test]
async fn test_find_all_empty_registry() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTagRegistryRepository;

    let all = repo.find_all(&mut txn).await.expect("find_all");

    assert!(all.is_empty());
}

// ---------------------------------------------------------------------------
// increment_usage_count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_increment_usage_count_increments_existing_tags() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTagRegistryRepository;

    repo.upsert(&mut txn, "rust").await.expect("upsert");
    repo.upsert(&mut txn, "python").await.expect("upsert");

    repo.increment_usage_count(&mut txn, &["rust".to_owned(), "python".to_owned()])
        .await
        .expect("increment");

    let all = repo.find_all(&mut txn).await.expect("find_all");
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].usage_count(), 1);
    assert_eq!(all[1].usage_count(), 1);

    // Second increment — should go to 2.
    repo.increment_usage_count(&mut txn, &["rust".to_owned()])
        .await
        .expect("increment");

    let all = repo.find_all(&mut txn).await.expect("find_all");
    let rust = all.iter().find(|e| e.tag() == "rust").expect("find rust");
    assert_eq!(rust.usage_count(), 2);
}

#[tokio::test]
async fn test_increment_usage_count_sets_last_seen_at() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTagRegistryRepository;

    let entry = repo.upsert(&mut txn, "rust").await.expect("upsert");
    assert!(
        entry.last_seen_at().is_none(),
        "last_seen_at should be None before first usage",
    );

    repo.increment_usage_count(&mut txn, &["rust".to_owned()])
        .await
        .expect("increment");

    let all = repo.find_all(&mut txn).await.expect("find_all");
    let rust = all.iter().find(|e| e.tag() == "rust").expect("find rust");
    assert!(
        rust.last_seen_at().is_some(),
        "last_seen_at should be set after increment",
    );
}

#[tokio::test]
async fn test_increment_usage_count_advances_last_seen_at() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTagRegistryRepository;

    repo.upsert(&mut txn, "rust").await.expect("upsert");
    repo.increment_usage_count(&mut txn, &["rust".to_owned()])
        .await
        .expect("first increment");

    // Backdate last_seen_at so the second increment produces a
    // distinguishable timestamp (now() is fixed within a transaction).
    shift_tag_registry_timestamp(
        &mut txn,
        "last_seen_at",
        "rust",
        chrono::Duration::hours(-1),
    )
    .await;

    let all = repo.find_all(&mut txn).await.expect("find_all");
    let backdated = all
        .iter()
        .find(|e| e.tag() == "rust")
        .expect("find rust")
        .last_seen_at()
        .expect("last_seen_at after backdate");

    repo.increment_usage_count(&mut txn, &["rust".to_owned()])
        .await
        .expect("second increment");

    let all = repo.find_all(&mut txn).await.expect("find_all");
    let advanced = all
        .iter()
        .find(|e| e.tag() == "rust")
        .expect("find rust")
        .last_seen_at()
        .expect("last_seen_at after second increment");

    assert!(
        advanced > backdated,
        "last_seen_at should advance: {advanced} > {backdated}",
    );
}

#[tokio::test]
async fn test_increment_usage_count_ignores_unknown_tags() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTagRegistryRepository;

    // No tags in registry — should succeed without error.
    repo.increment_usage_count(&mut txn, &["nonexistent".to_owned()])
        .await
        .expect("increment should not fail for unknown tags");
}
