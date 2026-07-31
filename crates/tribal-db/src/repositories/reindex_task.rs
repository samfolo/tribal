//! Reindex task repository: the lease protocol for a reindex run's work.
//!
//! Derived from the ingestion [`task`](super::task) lease behaviour, not the
//! literal SQL re-pointed: claim via `FOR UPDATE SKIP LOCKED` and
//! retry-with-backoff are identical in spirit, but the table is
//! `reindex_tasks`, the live state is `pending`, the retry budget is the
//! `max_attempts` column (not a call-site value), and a permanent-class failure
//! is routed to `reindex_quarantine` rather than a task state. The reindex
//! driver claims and processes one task per cycle synchronously, so it neither
//! refreshes a heartbeat nor sweeps for stale leases; a crashed worker's task
//! is re-done by the set-difference catch-up, not lease reclaim. Uses raw
//! `sqlx::query()` for the CTE-based atomic operations and `CASE` retry logic.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{
    EmbeddingErrorClass, ReindexEntityKind, ReindexRunId, ReindexTask, ReindexTaskId,
    ReindexTaskState,
};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "reindex_run_id",
    "kind",
    "target_ref",
    "state",
    "attempt",
    "max_attempts",
    "available_at",
    "claim_token",
    "claimed_by",
    "claimed_at",
    "heartbeat_at",
    "last_error",
    "last_error_class",
    "created_at",
    "updated_at",
    "completed_at",
]);

const UNKNOWN_KIND_IN_DB: &str = "unrecognised reindex task kind in database: schema mismatch";
const UNKNOWN_STATE_IN_DB: &str = "unrecognised reindex task state in database: schema mismatch";
const UNKNOWN_ERROR_CLASS_IN_DB: &str =
    "unrecognised reindex error class in database: schema mismatch";
const ATTEMPT_OVERFLOW: &str = "negative attempt in database: data corruption";
const MAX_ATTEMPTS_OVERFLOW: &str = "negative max_attempts in database: data corruption";

/// A count of reindex tasks in a given state within a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReindexTaskStateCount {
    /// The task state.
    pub state: ReindexTaskState,
    /// The number of tasks in this state.
    pub count: i64,
}

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for enrolling a reindex task.
///
/// `id`, `state`, `attempt`, `max_attempts`, `available_at`, and the timestamps
/// are server-defaulted. `target_ref` is a `range:<lo>..<hi>` backfill key or an
/// `item:<uuid>`/`tag:<text>` catch-up singleton.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewReindexTask {
    /// The run this task belongs to.
    pub reindex_run_id: ReindexRunId,
    /// Whether this task embeds items or tags.
    pub kind: ReindexEntityKind,
    /// The batch range key or catch-up singleton reference.
    pub target_ref: String,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for reindex tasks.
#[async_trait]
pub trait ReindexTaskRepository {
    /// Idempotently enrols a task, keyed by `(reindex_run_id, kind,
    /// target_ref)`. Returns `1` if newly enrolled, `0` if it already existed.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn upsert(&self, conn: &mut PgConnection, new: &NewReindexTask) -> Result<u64, DbError>;

    /// Finds a task by id.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: ReindexTaskId,
    ) -> Result<Option<ReindexTask>, DbError>;

    /// Atomically claims up to `limit` available `pending` tasks for the owner,
    /// scoped to `reindex_run_id` so a later run never claims a prior aborted
    /// run's leftover pending tasks, and admitted only while the parent run is
    /// live — an observed abort yields nothing further to claim.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn claim(
        &self,
        conn: &mut PgConnection,
        reindex_run_id: ReindexRunId,
        limit: u32,
        claimed_by: &str,
    ) -> Result<Vec<ReindexTask>, DbError>;

    /// Marks a claimed task `completed`. Returns the affected row count.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn complete(
        &self,
        conn: &mut PgConnection,
        id: ReindexTaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError>;

    /// Records a transient failure: increments `attempt`, requeues with the
    /// caller's backoff or dead-letters past `max_attempts`. Returns the
    /// affected row count.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn fail(
        &self,
        conn: &mut PgConnection,
        id: ReindexTaskId,
        claim_token: uuid::Uuid,
        available_at: DateTime<Utc>,
        error_class: EmbeddingErrorClass,
        error_message: &str,
    ) -> Result<u64, DbError>;

    /// Counts a run's tasks grouped by state.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn count_by_state(
        &self,
        conn: &mut PgConnection,
        reindex_run_id: ReindexRunId,
    ) -> Result<Vec<ReindexTaskStateCount>, DbError>;

    /// The earliest instant a pending task waiting out a retry backoff
    /// becomes claimable, or `None` when nothing is deferred.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn retry_wait(
        &self,
        conn: &mut PgConnection,
        reindex_run_id: ReindexRunId,
    ) -> Result<Option<DateTime<Utc>>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`ReindexTaskRepository`].
pub struct PgReindexTaskRepository;

#[async_trait]
impl ReindexTaskRepository for PgReindexTaskRepository {
    async fn upsert(&self, conn: &mut PgConnection, new: &NewReindexTask) -> Result<u64, DbError> {
        let result = sqlx::query(
            "INSERT INTO reindex_tasks (reindex_run_id, kind, target_ref, state) \
             VALUES ($1, $2, $3, 'pending') \
             ON CONFLICT (reindex_run_id, kind, target_ref) DO NOTHING",
        )
        .bind(new.reindex_run_id.inner())
        .bind(new.kind.as_str())
        .bind(&new.target_ref)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("enrolling reindex task {}", new.target_ref),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: ReindexTaskId,
    ) -> Result<Option<ReindexTask>, DbError> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM reindex_tasks WHERE id = $1"
        ))
        .bind(id.inner())
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("finding reindex task {id}"),
            source: e,
        })?;

        Ok(row.as_ref().map(map_reindex_task_row))
    }

    async fn claim(
        &self,
        conn: &mut PgConnection,
        reindex_run_id: ReindexRunId,
        limit: u32,
        claimed_by: &str,
    ) -> Result<Vec<ReindexTask>, DbError> {
        let sql = format!(
            "WITH claimable AS ( \
                 SELECT t.id FROM reindex_tasks t \
                 JOIN reindex_runs r ON r.id = t.reindex_run_id \
                 WHERE t.reindex_run_id = $3 \
                   AND r.state IN ('queued', 'running') \
                   AND t.state = 'pending' AND t.available_at <= now() \
                 ORDER BY t.available_at, t.created_at \
                 LIMIT $1 \
                 FOR UPDATE OF t SKIP LOCKED \
             ) \
             UPDATE reindex_tasks t \
             SET state = 'claimed', \
                 claim_token = gen_random_uuid(), \
                 claimed_by = $2, \
                 claimed_at = now(), \
                 heartbeat_at = now(), \
                 updated_at = now() \
             FROM claimable c \
             WHERE t.id = c.id \
             RETURNING {columns}",
            columns = COLUMNS.qualified("t"),
        );

        let rows = sqlx::query(&sql)
            .bind(i64::from(limit))
            .bind(claimed_by)
            .bind(reindex_run_id.inner())
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("claiming up to {limit} reindex tasks for run {reindex_run_id}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_reindex_task_row).collect())
    }

    async fn complete(
        &self,
        conn: &mut PgConnection,
        id: ReindexTaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            "UPDATE reindex_tasks \
             SET state = 'completed', claim_token = NULL, completed_at = now(), updated_at = now() \
             WHERE id = $1 AND claim_token = $2 AND state = 'claimed'",
        )
        .bind(id.inner())
        .bind(claim_token)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("completing reindex task {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn fail(
        &self,
        conn: &mut PgConnection,
        id: ReindexTaskId,
        claim_token: uuid::Uuid,
        available_at: DateTime<Utc>,
        error_class: EmbeddingErrorClass,
        error_message: &str,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            "UPDATE reindex_tasks \
             SET attempt = attempt + 1, \
                 state = CASE \
                     WHEN attempt + 1 > max_attempts THEN 'dead_letter' \
                     ELSE 'pending' \
                 END, \
                 available_at = CASE \
                     WHEN attempt + 1 > max_attempts THEN available_at \
                     ELSE $3 \
                 END, \
                 claim_token = NULL, \
                 claimed_by = NULL, \
                 claimed_at = NULL, \
                 heartbeat_at = NULL, \
                 last_error_class = $4, \
                 last_error = $5, \
                 updated_at = now() \
             WHERE id = $1 AND claim_token = $2 AND state = 'claimed'",
        )
        .bind(id.inner())
        .bind(claim_token)
        .bind(available_at)
        .bind(error_class.as_str())
        .bind(error_message)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("failing reindex task {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn count_by_state(
        &self,
        conn: &mut PgConnection,
        reindex_run_id: ReindexRunId,
    ) -> Result<Vec<ReindexTaskStateCount>, DbError> {
        let rows = sqlx::query(
            "SELECT state, COUNT(*) AS count FROM reindex_tasks \
             WHERE reindex_run_id = $1 GROUP BY state",
        )
        .bind(reindex_run_id.inner())
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("counting reindex tasks for run {reindex_run_id}"),
            source: e,
        })?;

        Ok(rows
            .iter()
            .map(|r| ReindexTaskStateCount {
                state: r
                    .get::<String, _>("state")
                    .parse::<ReindexTaskState>()
                    .expect(UNKNOWN_STATE_IN_DB),
                count: r.get::<i64, _>("count"),
            })
            .collect())
    }

    async fn retry_wait(
        &self,
        conn: &mut PgConnection,
        reindex_run_id: ReindexRunId,
    ) -> Result<Option<DateTime<Utc>>, DbError> {
        let row = sqlx::query(
            "SELECT MIN(available_at) AS resume_at \
             FROM reindex_tasks \
             WHERE reindex_run_id = $1 \
               AND state = 'pending' AND available_at > now()",
        )
        .bind(reindex_run_id.inner())
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("aggregating reindex retry waits for run {reindex_run_id}"),
            source: e,
        })?;

        Ok(row.get::<Option<DateTime<Utc>>, _>("resume_at"))
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

fn map_reindex_task_row(r: &sqlx::postgres::PgRow) -> ReindexTask {
    ReindexTask::builder()
        .id(ReindexTaskId::from(r.get::<uuid::Uuid, _>("id")))
        .reindex_run_id(ReindexRunId::from(r.get::<uuid::Uuid, _>("reindex_run_id")))
        .kind(
            r.get::<String, _>("kind")
                .parse::<ReindexEntityKind>()
                .expect(UNKNOWN_KIND_IN_DB),
        )
        .target_ref(r.get::<String, _>("target_ref"))
        .state(
            r.get::<String, _>("state")
                .parse::<ReindexTaskState>()
                .expect(UNKNOWN_STATE_IN_DB),
        )
        .attempt(u32::try_from(r.get::<i32, _>("attempt")).expect(ATTEMPT_OVERFLOW))
        .max_attempts(u32::try_from(r.get::<i32, _>("max_attempts")).expect(MAX_ATTEMPTS_OVERFLOW))
        .available_at(r.get("available_at"))
        .claim_token(r.get::<Option<uuid::Uuid>, _>("claim_token"))
        .claimed_by(r.get::<Option<String>, _>("claimed_by"))
        .claimed_at(r.get("claimed_at"))
        .heartbeat_at(r.get("heartbeat_at"))
        .last_error(r.get::<Option<String>, _>("last_error"))
        .last_error_class(r.get::<Option<String>, _>("last_error_class").map(|s| {
            s.parse::<EmbeddingErrorClass>()
                .expect(UNKNOWN_ERROR_CLASS_IN_DB)
        }))
        .created_at(r.get("created_at"))
        .updated_at(r.get("updated_at"))
        .completed_at(r.get("completed_at"))
        .build()
}
