//! Task lifecycle helpers for test setup.
//!
//! Functions in this module manipulate task state that sits outside the
//! normal repository layer: backdating heartbeats, overriding retry
//! counts, etc.  They use raw `sqlx::query()` to avoid coupling to the
//! production repository API.

use sqlx::PgConnection;
use tribal_domain::TaskId;

// ---------------------------------------------------------------------------
// backdate_task_heartbeat
// ---------------------------------------------------------------------------

/// Backdates a task's `heartbeat_at` by the given duration.
///
/// Intended for simulating stale heartbeats in tests that exercise
/// the reclaim sweep and startup reclaim paths.
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn backdate_task_heartbeat(
    conn: &mut PgConnection,
    id: TaskId,
    duration: std::time::Duration,
) {
    let secs = duration.as_secs_f64();
    sqlx::query("UPDATE tasks SET heartbeat_at = now() - make_interval(secs => $1) WHERE id = $2")
        .bind(secs)
        .bind(id.inner())
        .execute(&mut *conn)
        .await
        .expect("lifecycle: backdate task heartbeat");
}

// ---------------------------------------------------------------------------
// set_retry_count
// ---------------------------------------------------------------------------

/// Overwrites a task's `retry_count`.
///
/// Intended for tests that need to position a task at or near the retry
/// budget boundary before exercising failure or reclaim paths.
///
/// # Panics
///
/// Panics if `count` exceeds `i32::MAX` or if the database query fails.
pub async fn set_retry_count(conn: &mut PgConnection, id: TaskId, count: u32) {
    let count_i32 = i32::try_from(count).expect("retry count exceeds i32");
    sqlx::query("UPDATE tasks SET retry_count = $1 WHERE id = $2")
        .bind(count_i32)
        .bind(id.inner())
        .execute(&mut *conn)
        .await
        .expect("lifecycle: set retry count");
}
