//! Behavioural tests for the graph-identity repository.

use tribal_db::{GraphIdentityRepository, PgGraphIdentityRepository};
use tribal_test_utils::TestDb;

#[tokio::test]
async fn test_get_returns_the_one_seeded_identity() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    let first = PgGraphIdentityRepository
        .get(&mut txn)
        .await
        .expect("read graph identity");
    let second = PgGraphIdentityRepository
        .get(&mut txn)
        .await
        .expect("read graph identity again");

    assert_eq!(first, second, "the identity is stable across reads");
    assert!(
        first.to_string().starts_with("graph_"),
        "the identity serialises with the graph prefix, got {first}"
    );
}
