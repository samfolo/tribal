use tribal_db::{MIGRATOR, MigrationHeadStatus, MigrationRepository, PgMigrationRepository};
use tribal_test_utils::TestDb;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the binary's compile-time head migration version.
///
/// # Panics
///
/// Panics if `MIGRATOR` contains no migrations, which would indicate a
/// build problem rather than a test failure.
fn binary_head() -> i64 {
    MIGRATOR
        .iter()
        .last()
        .expect("MIGRATOR contains at least one migration")
        .version
}

// ---------------------------------------------------------------------------
// current_head_matches
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_current_head_matches_returns_matches_when_versions_agree() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");

    let status = PgMigrationRepository
        .current_head_matches(&mut txn, binary_head())
        .await
        .expect("current_head_matches");

    assert_eq!(status, MigrationHeadStatus::Matches);
}

#[tokio::test]
async fn test_current_head_matches_returns_behind_when_expected_newer_than_db() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");

    let head = binary_head();
    let expected = head + 1;

    let status = PgMigrationRepository
        .current_head_matches(&mut txn, expected)
        .await
        .expect("current_head_matches");

    assert_eq!(
        status,
        MigrationHeadStatus::Behind {
            expected,
            found: head,
        }
    );
}

#[tokio::test]
async fn test_current_head_matches_returns_ahead_when_db_newer_than_expected() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");

    let head = binary_head();
    let synthetic = head + 1;

    PgMigrationRepository
        .insert_synthetic_row_for_test(&mut txn, synthetic)
        .await
        .expect("insert synthetic future migration");

    let status = PgMigrationRepository
        .current_head_matches(&mut txn, head)
        .await
        .expect("current_head_matches");

    assert_eq!(
        status,
        MigrationHeadStatus::Ahead {
            expected: head,
            found: synthetic,
        }
    );
}

#[tokio::test]
async fn test_current_head_matches_returns_behind_with_found_zero_when_table_empty() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");

    PgMigrationRepository
        .truncate_migrations_table_for_test(&mut txn)
        .await
        .expect("truncate _sqlx_migrations");

    let status = PgMigrationRepository
        .current_head_matches(&mut txn, binary_head())
        .await
        .expect("current_head_matches");

    assert_eq!(
        status,
        MigrationHeadStatus::Behind {
            expected: binary_head(),
            found: 0,
        },
    );
}

#[tokio::test]
async fn test_current_head_matches_returns_missing_table_when_table_absent() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");

    PgMigrationRepository
        .drop_migrations_table_for_test(&mut txn)
        .await
        .expect("drop _sqlx_migrations");

    let status = PgMigrationRepository
        .current_head_matches(&mut txn, binary_head())
        .await
        .expect("current_head_matches");

    assert_eq!(status, MigrationHeadStatus::MissingTable);
}
