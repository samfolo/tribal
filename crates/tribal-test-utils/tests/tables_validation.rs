//! Validates that `APPLICATION_TABLES` matches the live database schema.
//!
//! Catches both missing tables (migration added but constant not
//! updated) and stale entries (in constant but not in schema).

use sqlx::Row;
use tribal_db::APPLICATION_TABLES;

#[tokio::test]
async fn test_application_tables_matches_schema() {
    let ctx = tribal_test_utils::test_context().await;

    let rows = sqlx::query(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'public' \
           AND table_name != '_sqlx_migrations' \
         ORDER BY table_name",
    )
    .fetch_all(ctx.pool())
    .await
    .expect("should query information_schema");

    let schema_tables: Vec<String> = rows.iter().map(|r| r.get("table_name")).collect();
    let constant_tables: Vec<&str> = APPLICATION_TABLES.to_vec();

    assert_eq!(
        constant_tables, schema_tables,
        "APPLICATION_TABLES must exactly match public schema tables \
         (excluding _sqlx_migrations)",
    );
}
