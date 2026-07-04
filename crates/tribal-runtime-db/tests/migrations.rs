//! The runtime-database migration lineage applies from empty to head and is
//! idempotent on re-run.

mod support;

use sqlx::Row;

#[tokio::test]
async fn migrations_apply_from_empty_to_head_and_are_idempotent() {
    let db = support::provision().await;

    // The schema is present: both job-plane tables exist.
    let tables: Vec<String> = sqlx::query(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename",
    )
    .fetch_all(db.pool())
    .await
    .expect("read the table catalogue")
    .into_iter()
    .map(|row| row.get::<String, _>("tablename"))
    .filter(|name| name != "_sqlx_migrations")
    .collect();
    assert_eq!(tables, vec!["run_job".to_owned(), "tenant_slot".to_owned()]);

    // Re-running the migrator changes nothing — the lineage is idempotent.
    tribal_runtime_db::MIGRATOR
        .run(db.pool())
        .await
        .expect("second migrate run is a no-op");
}
