//! Integration test: full [`TestContext`] lifecycle.
//!
//! This test lives in `tests/` (separate binary) because it requires a
//! Docker daemon and starts a real pgvector container via testcontainers.
//! It validates the complete lifecycle: container start, migration, and
//! pool creation.

use sqlx::Row;

#[tokio::test]
async fn test_context_starts_container_and_runs_migrations() {
    let ctx = tribal_test_utils::test_context().await;

    // Verify the pool is connected by running a simple query.
    let row = sqlx::query("SELECT 1 AS value")
        .fetch_one(ctx.pool())
        .await
        .expect("should execute query against test database");

    let value: i32 = row.get("value");
    assert_eq!(value, 1);
}

#[tokio::test]
async fn test_context_migrations_create_expected_tables() {
    let ctx = tribal_test_utils::test_context().await;

    let row = sqlx::query(
        "SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'principals'
        ) AS table_exists",
    )
    .fetch_one(ctx.pool())
    .await
    .expect("should query information_schema");

    let exists: bool = row.get("table_exists");
    assert!(exists, "principals table should exist after migration");
}

#[tokio::test]
async fn test_context_migrations_enable_pgvector_extension() {
    let ctx = tribal_test_utils::test_context().await;

    let row = sqlx::query(
        "SELECT EXISTS (
            SELECT FROM pg_extension WHERE extname = 'vector'
        ) AS ext_exists",
    )
    .fetch_one(ctx.pool())
    .await
    .expect("should query pg_extension");

    let exists: bool = row.get("ext_exists");
    assert!(exists, "vector extension should be enabled after migration");
}
