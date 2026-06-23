use tribal_db::{AdvisoryLockRepository, PgAdvisoryLockRepository};
use tribal_test_utils::TestDb;

// Each test uses a distinct, test-scoped lock id (the `test` ASCII prefix plus
// a per-test suffix) so concurrently-running tests never contend on the same
// advisory lock across their pooled connections. Cross-session contention
// semantics (shared coexists, exclusive blocks) are covered by the reindex
// cutover race tests, which drive two real connections.
const LOCK_SHARED: i64 = 0x7465_7374_0000_0001;
const LOCK_EXCLUSIVE: i64 = 0x7465_7374_0000_0002;
const LOCK_TRY: i64 = 0x7465_7374_0000_0003;
const LOCK_REENTRANT: i64 = 0x7465_7374_0000_0004;

#[tokio::test]
async fn test_acquire_shared_xact_succeeds_in_transaction() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");

    PgAdvisoryLockRepository
        .acquire_shared_xact(&mut txn, LOCK_SHARED)
        .await
        .expect("shared lock acquired");
}

#[tokio::test]
async fn test_acquire_exclusive_xact_succeeds_in_transaction() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");

    PgAdvisoryLockRepository
        .acquire_exclusive_xact(&mut txn, LOCK_EXCLUSIVE)
        .await
        .expect("exclusive lock acquired");
}

#[tokio::test]
async fn test_try_acquire_exclusive_xact_granted_when_uncontended() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");

    let granted = PgAdvisoryLockRepository
        .try_acquire_exclusive_xact(&mut txn, LOCK_TRY)
        .await
        .expect("try-acquire");
    assert!(granted, "an uncontended exclusive lock should be granted");
}

#[tokio::test]
async fn test_same_session_does_not_self_conflict() {
    // A session that already holds the shared lock can still take the exclusive
    // lock on the same id: advisory locks conflict only between sessions.
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin_test");

    PgAdvisoryLockRepository
        .acquire_shared_xact(&mut txn, LOCK_REENTRANT)
        .await
        .expect("shared lock acquired");
    let granted = PgAdvisoryLockRepository
        .try_acquire_exclusive_xact(&mut txn, LOCK_REENTRANT)
        .await
        .expect("try-acquire");
    assert!(
        granted,
        "the same session must not block itself on the exclusive lock",
    );
}
