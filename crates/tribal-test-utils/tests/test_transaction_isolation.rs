//! Integration test: [`TestTransaction`] provides isolation via rollback.
//!
//! This test lives in `tests/` (separate binary) because it requires a
//! Docker daemon and a real pgvector container. It validates that writes
//! within a [`TestTransaction`] are invisible to subsequent transactions.

#[tokio::test]
async fn test_transaction_rolls_back_on_drop() {
    let ctx = tribal_test_utils::TestDb::new().await;

    // Insert a row inside a TestTransaction, then drop it.
    {
        let mut txn = ctx.begin().await.expect("should begin test transaction");

        sqlx::query(
            "INSERT INTO principals (principal_key, display_name)
             VALUES ('rollback-test', 'Rollback Test')",
        )
        .execute(&mut *txn)
        .await
        .expect("insert should succeed within transaction");

        // Verify the row is visible within the transaction.
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM principals WHERE principal_key = 'rollback-test'")
                .fetch_one(&mut *txn)
                .await
                .expect("count query should succeed");

        assert_eq!(count.0, 1, "row should be visible within the transaction");

        // txn is dropped here — transaction rolls back.
    }

    // Verify the row is NOT visible outside the transaction.
    let count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM principals WHERE principal_key = 'rollback-test'")
            .fetch_one(ctx.pool())
            .await
            .expect("count query should succeed on pool");

    assert_eq!(
        count.0, 0,
        "row should not exist after transaction rollback"
    );
}

#[tokio::test]
async fn test_transaction_isolates_concurrent_tests() {
    let ctx = tribal_test_utils::TestDb::new().await;

    // Two transactions should not see each other's writes.
    let mut txn_a = ctx.begin().await.expect("should begin transaction A");
    let mut txn_b = ctx.begin().await.expect("should begin transaction B");

    sqlx::query(
        "INSERT INTO principals (principal_key, display_name)
         VALUES ('isolation-a', 'Agent A')",
    )
    .execute(&mut *txn_a)
    .await
    .expect("insert into txn_a should succeed");

    // txn_b should not see txn_a's insert.
    let count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM principals WHERE principal_key = 'isolation-a'")
            .fetch_one(&mut *txn_b)
            .await
            .expect("count query in txn_b should succeed");

    assert_eq!(
        count.0, 0,
        "transaction B should not see transaction A's uncommitted writes",
    );
}
