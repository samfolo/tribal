//! Lifecycle helpers for test setup.
//!
//! Functions in this module manipulate state that sits outside the
//! normal repository layer: shifting timestamps, overriding retry
//! counts, truncating tables, etc.  They use raw `sqlx::query()` to
//! avoid coupling to the production repository API.

use chrono::Duration;
use sqlx::PgConnection;
use tribal_db::APPLICATION_TABLES;
use tribal_domain::{JobId, RelationBatchId, TaskId, TaskStatus, TaskType};

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Panics if `value` is not a valid SQL identifier (`[a-zA-Z_][a-zA-Z0-9_]*`).
fn assert_identifier(value: &str, label: &str) {
    assert!(
        !value.is_empty()
            && value.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
        "lifecycle: {label} is not a valid SQL identifier: {value}",
    );
}

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

// ---------------------------------------------------------------------------
// truncate_all_tables
// ---------------------------------------------------------------------------

/// Truncates every application table, cascading foreign keys.
///
/// Uses the `APPLICATION_TABLES` constant to build a single
/// `TRUNCATE … CASCADE` statement.
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn truncate_all_tables(conn: &mut PgConnection) {
    let tables = APPLICATION_TABLES.join(", ");
    let sql = format!("TRUNCATE {tables} CASCADE");
    sqlx::query(&sql)
        .execute(&mut *conn)
        .await
        .expect("lifecycle: truncate all tables");
}

// ---------------------------------------------------------------------------
// shift_timestamp
// ---------------------------------------------------------------------------

/// Shifts a timestamp column by the given offset.
///
/// Positive offsets move the timestamp forward; negative offsets move
/// it backward.  The `where_column` parameter controls which column
/// is used in the `WHERE` clause.
///
/// # Panics
///
/// Panics if `table` is not in `APPLICATION_TABLES`, if `set_column`
/// or `where_column` is not a valid SQL identifier, or if the database
/// query fails.
pub async fn shift_timestamp(
    conn: &mut PgConnection,
    table: &str,
    set_column: &str,
    where_column: &str,
    where_value: uuid::Uuid,
    offset: Duration,
) {
    assert!(
        APPLICATION_TABLES.contains(&table),
        "shift_timestamp: unknown table {table}",
    );
    assert_identifier(set_column, "set_column");
    assert_identifier(where_column, "where_column");
    #[allow(clippy::cast_precision_loss)]
    let secs = offset.num_seconds() as f64;
    let sql = format!(
        "UPDATE {table} \
         SET {set_column} = {set_column} + make_interval(secs => $1) \
         WHERE {where_column} = $2",
    );
    sqlx::query(&sql)
        .bind(secs)
        .bind(where_value)
        .execute(&mut *conn)
        .await
        .expect("lifecycle: shift timestamp");
}

// ---------------------------------------------------------------------------
// shift_timestamp_by_id
// ---------------------------------------------------------------------------

/// Shifts a timestamp column on a row identified by its `id`.
///
/// Convenience wrapper around [`shift_timestamp`] with
/// `where_column = "id"`.
///
/// # Panics
///
/// Panics if `table` is not in `APPLICATION_TABLES`, if `column` is
/// not a valid SQL identifier, or if the database query fails.
pub async fn shift_timestamp_by_id(
    conn: &mut PgConnection,
    table: &str,
    column: &str,
    id: uuid::Uuid,
    offset: Duration,
) {
    shift_timestamp(conn, table, column, "id", id, offset).await;
}

// ---------------------------------------------------------------------------
// shift_relations_timestamp_by_batch
// ---------------------------------------------------------------------------

/// Shifts `created_at` on knowledge item relations matching a batch ID.
///
/// Convenience wrapper around [`shift_timestamp`] for the standing.rs
/// pattern that backdates relations by batch.
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn shift_relations_timestamp_by_batch(
    conn: &mut PgConnection,
    batch_id: RelationBatchId,
    offset: Duration,
) {
    shift_timestamp(
        conn,
        "knowledge_item_relations",
        "created_at",
        "relation_batch_id",
        *batch_id.inner(),
        offset,
    )
    .await;
}

// ---------------------------------------------------------------------------
// shift_tag_registry_timestamp
// ---------------------------------------------------------------------------

/// Shifts a timestamp column on a tag registry entry.
///
/// Convenience wrapper for the tag registry's text primary key,
/// which cannot use the UUID-based [`shift_timestamp`].
///
/// # Panics
///
/// Panics if `column` is not a valid SQL identifier, or if the database
/// query fails.
pub async fn shift_tag_registry_timestamp(
    conn: &mut PgConnection,
    column: &str,
    tag: &str,
    offset: Duration,
) {
    assert_identifier(column, "column");
    #[allow(clippy::cast_precision_loss)]
    let secs = offset.num_seconds() as f64;
    let sql = format!(
        "UPDATE tag_registry \
         SET {column} = {column} + make_interval(secs => $1) \
         WHERE tag = $2",
    );
    sqlx::query(&sql)
        .bind(secs)
        .bind(tag)
        .execute(&mut *conn)
        .await
        .expect("lifecycle: shift tag registry timestamp");
}

// ---------------------------------------------------------------------------
// set_timestamp
// ---------------------------------------------------------------------------

/// Sets a timestamp column to an absolute value on a row identified
/// by its `id`.
///
/// # Panics
///
/// Panics if `table` is not in `APPLICATION_TABLES`, if `column` is
/// not a valid SQL identifier, or if the database query fails.
pub async fn set_timestamp(
    conn: &mut PgConnection,
    table: &str,
    column: &str,
    id: uuid::Uuid,
    ts: chrono::DateTime<chrono::Utc>,
) {
    assert!(
        APPLICATION_TABLES.contains(&table),
        "set_timestamp: unknown table {table}",
    );
    assert_identifier(column, "column");
    let sql = format!("UPDATE {table} SET {column} = $1 WHERE id = $2");
    sqlx::query(&sql)
        .bind(ts)
        .bind(id)
        .execute(&mut *conn)
        .await
        .expect("lifecycle: set timestamp");
}

// ---------------------------------------------------------------------------
// count_tasks_by_status
// ---------------------------------------------------------------------------

/// Counts tasks with a given status.
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn count_tasks_by_status(conn: &mut PgConnection, status: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM tasks WHERE status = $1")
        .bind(status)
        .fetch_one(&mut *conn)
        .await
        .expect("lifecycle: count tasks by status")
}

// ---------------------------------------------------------------------------
// count_prompt_versions
// ---------------------------------------------------------------------------

/// Counts rows in the `prompt_versions` table.
///
/// Used by setup-flow tests to assert that the `tribal setup` command
/// does not upsert prompts (that responsibility belongs to `tribal
/// serve`).
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn count_prompt_versions(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM prompt_versions")
        .fetch_one(&mut *conn)
        .await
        .expect("lifecycle: count prompt versions")
}

// ---------------------------------------------------------------------------
// set_task_status_by_job
// ---------------------------------------------------------------------------

/// Sets the status of all tasks matching a job ID and task type.
///
/// Intended for test setup where tasks need to be positioned in a
/// specific terminal state without going through the normal processing
/// pipeline (e.g. simulating the reclaim-sweep gap).
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn set_task_status_by_job(
    conn: &mut PgConnection,
    job_id: JobId,
    task_type: TaskType,
    status: TaskStatus,
) {
    sqlx::query(
        "UPDATE tasks SET status = $1 \
         WHERE job_id = $2 AND task_type = $3",
    )
    .bind(status.as_str())
    .bind(job_id.inner())
    .bind(task_type.as_str())
    .execute(&mut *conn)
    .await
    .expect("lifecycle: set task status by job");
}
