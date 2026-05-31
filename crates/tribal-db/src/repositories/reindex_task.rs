//! Reindex task repository: the lease protocol for a reindex run's work.
//!
//! Derived from the ingestion [`task`](super::task) lease behaviour, not the
//! literal SQL re-pointed: claim via `FOR UPDATE SKIP LOCKED`, heartbeat, stale
//! reclaim, and retry-with-backoff are identical in spirit, but the table is
//! `reindex_tasks`, the live state is `pending`, the retry budget is the
//! `max_attempts` column (not a call-site value), and a permanent-class failure
//! is routed to `reindex_quarantine` rather than a task state. Uses raw
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
use crate::{DbError, ReclaimOutcome};

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

    /// Atomically claims up to `limit` available `pending` tasks for the owner.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn claim(
        &self,
        conn: &mut PgConnection,
        limit: u32,
        claimed_by: &str,
    ) -> Result<Vec<ReindexTask>, DbError>;

    /// Refreshes a claimed task's heartbeat. Returns the affected row count
    /// (`0` if the claim token no longer matches).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn heartbeat(
        &self,
        conn: &mut PgConnection,
        id: ReindexTaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError>;

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

    /// Reclaims tasks whose heartbeat has expired, requeuing or dead-lettering
    /// them. Reclaim backoff is pre-increment (`2^attempt`), one step below the
    /// inline-failure backoff so reclaimed work retries sooner.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn reclaim_stale(
        &self,
        conn: &mut PgConnection,
        timeout_seconds: u32,
        limit: u32,
        error_class: EmbeddingErrorClass,
        error_message: &str,
        flat_backoff_seconds: Option<u32>,
    ) -> Result<ReclaimOutcome, DbError>;

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
        limit: u32,
        claimed_by: &str,
    ) -> Result<Vec<ReindexTask>, DbError> {
        let sql = format!(
            "WITH claimable AS ( \
                 SELECT id FROM reindex_tasks \
                 WHERE state = 'pending' AND available_at <= now() \
                 ORDER BY available_at, created_at \
                 LIMIT $1 \
                 FOR UPDATE SKIP LOCKED \
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
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("claiming up to {limit} reindex tasks"),
                source: e,
            })?;

        Ok(rows.iter().map(map_reindex_task_row).collect())
    }

    async fn heartbeat(
        &self,
        conn: &mut PgConnection,
        id: ReindexTaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            "UPDATE reindex_tasks \
             SET heartbeat_at = now(), updated_at = now() \
             WHERE id = $1 AND claim_token = $2 AND state = 'claimed'",
        )
        .bind(id.inner())
        .bind(claim_token)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("heartbeat for reindex task {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
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

    async fn reclaim_stale(
        &self,
        conn: &mut PgConnection,
        timeout_seconds: u32,
        limit: u32,
        error_class: EmbeddingErrorClass,
        error_message: &str,
        flat_backoff_seconds: Option<u32>,
    ) -> Result<ReclaimOutcome, DbError> {
        let rows = sqlx::query(
            "WITH stale AS ( \
                 SELECT id, attempt, max_attempts FROM reindex_tasks \
                 WHERE state = 'claimed' \
                   AND heartbeat_at < now() - make_interval(secs => $1::double precision) \
                 ORDER BY heartbeat_at ASC \
                 LIMIT $2 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             UPDATE reindex_tasks t \
             SET attempt = s.attempt + 1, \
                 state = CASE \
                     WHEN s.attempt + 1 > s.max_attempts THEN 'dead_letter' \
                     ELSE 'pending' \
                 END, \
                 available_at = CASE \
                     WHEN s.attempt + 1 > s.max_attempts THEN t.available_at \
                     WHEN $5 IS NOT NULL THEN now() + make_interval(secs => $5) \
                     ELSE now() + make_interval( \
                         secs => power(2, s.attempt)::double precision \
                     ) \
                 END, \
                 claim_token = NULL, \
                 claimed_by = NULL, \
                 claimed_at = NULL, \
                 heartbeat_at = NULL, \
                 last_error_class = $3, \
                 last_error = $4, \
                 updated_at = now() \
             FROM stale s \
             WHERE t.id = s.id \
             RETURNING t.state",
        )
        .bind(f64::from(timeout_seconds))
        .bind(i64::from(limit))
        .bind(error_class.as_str())
        .bind(error_message)
        .bind(flat_backoff_seconds.map(f64::from))
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "reclaiming stale reindex tasks".to_owned(),
            source: e,
        })?;

        let mut requeued: u64 = 0;
        let mut dead_lettered: u64 = 0;
        for row in &rows {
            match row.get::<String, _>("state").as_str() {
                "pending" => requeued += 1,
                "dead_letter" => dead_lettered += 1,
                other => debug_assert!(false, "unexpected reclaim state: {other}"),
            }
        }

        Ok(ReclaimOutcome {
            requeued,
            dead_lettered,
        })
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
