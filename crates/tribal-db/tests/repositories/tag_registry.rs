use tribal_db::{PgTagRegistryRepository, TagRegistryRepository};
use tribal_test_utils::test_context;

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

    repo.upsert(&mut txn, "existing").await.expect("pre-insert");

    let tags = vec![
        "existing".to_owned(),
        "alpha".to_owned(),
        "beta".to_owned(),
    ];
    let entries = repo.batch_upsert(&mut txn, &tags).await.expect("batch_upsert");

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].tag(), "alpha");
    assert_eq!(entries[1].tag(), "beta");
    assert_eq!(entries[2].tag(), "existing");
}

#[tokio::test]
async fn test_batch_upsert_empty_returns_empty() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgTagRegistryRepository;

    let entries = repo.batch_upsert(&mut txn, &[]).await.expect("batch_upsert");

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
