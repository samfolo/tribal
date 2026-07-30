use tribal_db::{AdvisoryLockRepository, PgAdvisoryLockRepository};
use tribal_test_utils::TestDb;

// Each test uses a distinct, test-scoped lock id (the `test` ASCII prefix plus
// a per-test suffix) so concurrently-running tests never contend on the same
// advisory lock across their pooled connections.
const LOCK_SHARED: i64 = 0x7465_7374_0000_0001;
const LOCK_EXCLUSIVE: i64 = 0x7465_7374_0000_0002;
const LOCK_TRY: i64 = 0x7465_7374_0000_0003;
const LOCK_REENTRANT: i64 = 0x7465_7374_0000_0004;
const LOCK_SESSION_EXCLUSIVE: i64 = 0x7465_7374_0000_0005;
const LOCK_SESSION_SHARED: i64 = 0x7465_7374_0000_0006;
const LOCK_XACT_SHARED_TRY: i64 = 0x7465_7374_0000_0007;

#[tokio::test]
async fn test_acquire_shared_xact_succeeds_in_transaction() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    PgAdvisoryLockRepository
        .acquire_shared_xact(&mut txn, LOCK_SHARED)
        .await
        .expect("shared lock acquired");
}

#[tokio::test]
async fn test_acquire_exclusive_xact_succeeds_in_transaction() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    PgAdvisoryLockRepository
        .acquire_exclusive_xact(&mut txn, LOCK_EXCLUSIVE)
        .await
        .expect("exclusive lock acquired");
}

#[tokio::test]
async fn test_try_acquire_exclusive_xact_granted_when_uncontended() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

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
    let mut txn = ctx.begin().await.expect("begin");

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

#[tokio::test]
async fn test_session_exclusive_excludes_across_sessions_until_released() {
    let ctx = TestDb::new().await;
    let mut holder = ctx.raw_connection().await.expect("holder session");
    let mut rival = ctx.raw_connection().await.expect("rival session");

    let granted = PgAdvisoryLockRepository
        .try_acquire_exclusive(&mut holder, LOCK_SESSION_EXCLUSIVE)
        .await
        .expect("try-acquire exclusive");
    assert!(granted);

    let contended = PgAdvisoryLockRepository
        .try_acquire_exclusive(&mut rival, LOCK_SESSION_EXCLUSIVE)
        .await
        .expect("rival try-acquire exclusive");
    assert!(!contended, "a second session must be refused while held");
    let shared_contended = PgAdvisoryLockRepository
        .try_acquire_shared(&mut rival, LOCK_SESSION_EXCLUSIVE)
        .await
        .expect("rival try-acquire shared");
    assert!(
        !shared_contended,
        "a shared acquisition must be refused under an exclusive holder"
    );

    let released = PgAdvisoryLockRepository
        .release_exclusive(&mut holder, LOCK_SESSION_EXCLUSIVE)
        .await
        .expect("release exclusive");
    assert!(released);

    let after_release = PgAdvisoryLockRepository
        .try_acquire_shared(&mut rival, LOCK_SESSION_EXCLUSIVE)
        .await
        .expect("rival try-acquire shared after release");
    assert!(
        after_release,
        "release must free the lock for other sessions"
    );
    let cleaned = PgAdvisoryLockRepository
        .release_shared(&mut rival, LOCK_SESSION_EXCLUSIVE)
        .await
        .expect("release shared");
    assert!(cleaned);
}

#[tokio::test]
async fn test_session_shared_coexists_and_refuses_exclusive() {
    let ctx = TestDb::new().await;
    let mut first = ctx.raw_connection().await.expect("first session");
    let mut second = ctx.raw_connection().await.expect("second session");
    let mut rival = ctx.raw_connection().await.expect("rival session");

    for conn in [&mut first, &mut second] {
        let granted = PgAdvisoryLockRepository
            .try_acquire_shared(conn, LOCK_SESSION_SHARED)
            .await
            .expect("try-acquire shared");
        assert!(granted, "shared holders coexist");
    }

    let contended = PgAdvisoryLockRepository
        .try_acquire_exclusive(&mut rival, LOCK_SESSION_SHARED)
        .await
        .expect("rival try-acquire exclusive");
    assert!(!contended, "exclusive must be refused under shared holders");

    for conn in [&mut first, &mut second] {
        let released = PgAdvisoryLockRepository
            .release_shared(conn, LOCK_SESSION_SHARED)
            .await
            .expect("release shared");
        assert!(released);
    }

    let after_release = PgAdvisoryLockRepository
        .try_acquire_exclusive(&mut rival, LOCK_SESSION_SHARED)
        .await
        .expect("rival try-acquire exclusive after releases");
    assert!(
        after_release,
        "the last shared release frees the exclusive path"
    );
}

#[tokio::test]
async fn test_try_acquire_shared_xact_refused_while_exclusive_held() {
    let ctx = TestDb::new().await;
    let mut holder = ctx.raw_connection().await.expect("holder session");

    let granted = PgAdvisoryLockRepository
        .try_acquire_exclusive(&mut holder, LOCK_XACT_SHARED_TRY)
        .await
        .expect("try-acquire exclusive");
    assert!(granted);

    let mut txn = ctx.begin().await.expect("begin");
    let refused = PgAdvisoryLockRepository
        .try_acquire_shared_xact(&mut txn, LOCK_XACT_SHARED_TRY)
        .await
        .expect("try-acquire shared xact");
    assert!(
        !refused,
        "a shared xact acquisition must be refused under an exclusive session holder"
    );

    PgAdvisoryLockRepository
        .release_exclusive(&mut holder, LOCK_XACT_SHARED_TRY)
        .await
        .expect("release exclusive");
    let after_release = PgAdvisoryLockRepository
        .try_acquire_shared_xact(&mut txn, LOCK_XACT_SHARED_TRY)
        .await
        .expect("try-acquire shared xact after release");
    assert!(after_release, "release must free the shared xact path");
}
