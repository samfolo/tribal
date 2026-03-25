//! Task repository: trait definition and Postgres implementation.
//!
//! Tasks are the unit of work in the ingest pipeline.  Claiming uses an
//! atomic CTE with `FOR UPDATE SKIP LOCKED`.  Retry uses exponential
//! backoff; dead-letter shelves tasks that exhaust their retry budget.
//!
//! Uses raw `sqlx::query()` because CTE-based atomic operations
//! (`claim`, `reclaim_stale`) and conditional `CASE` expressions
//! (`fail`) are unsupported by the compile-time macro.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{JobId, Task, TaskErrorKind, TaskId, TaskStatus, TaskType};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "job_id",
    "task_type",
    "status",
    "batch_index",
    "claim_token",
    "available_at",
    "claimed_by",
    "claimed_at",
    "heartbeat_at",
    "retry_count",
    "error_kind",
    "error_message",
    "created_at",
    "updated_at",
]);

const UNKNOWN_TASK_TYPE_IN_DB: &str = "unrecognised task type in database — schema mismatch";
const UNKNOWN_TASK_STATUS_IN_DB: &str = "unrecognised task status in database — schema mismatch";
const UNKNOWN_TASK_ERROR_KIND_IN_DB: &str =
    "unrecognised task error kind in database — schema mismatch";
const BATCH_INDEX_EXCEEDS_I32: &str = "batch_index exceeds i32::MAX";
const BATCH_INDEX_OVERFLOW: &str = "negative batch_index in database — data corruption";
const RETRY_COUNT_OVERFLOW: &str = "negative retry_count in database — data corruption";
const MAX_RETRIES_EXCEEDS_I32: &str = "max_retries exceeds i32::MAX";
const UPSERT_REQUIRES_SINGLETON: &str =
    "upsert is only valid for singleton task types (extraction, relation)";

/// Result of a [`TaskRepository::reclaim_stale`] operation, breaking
/// down the total affected rows by their post-reclaim status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimOutcome {
    /// Tasks reset to `queued` for another attempt.
    pub requeued: u64,
    /// Tasks moved to `dead_letter` (retry budget exhausted).
    pub dead_lettered: u64,
}

/// A count of tasks for a given `(task_type, status)` combination.
///
/// Returned by [`TaskRepository::count_by_status`] for periodic
/// queue health gauge reporting.
#[derive(Debug, Clone)]
pub struct TaskStatusCount {
    /// The task type.
    pub task_type: TaskType,
    /// The task status.
    pub status: TaskStatus,
    /// The number of tasks matching this combination.
    pub count: i64,
}

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new task.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `status`, `available_at`, `created_at`, `updated_at`) are
/// produced by Postgres via `DEFAULT` clauses and returned via
/// `RETURNING {COLUMNS}`.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewTask {
    /// The job this task belongs to.
    pub job_id: JobId,
    /// The type of work this task performs.
    pub task_type: TaskType,
    /// Triage batch index (required for triage tasks, must be `None` otherwise).
    #[builder(default)]
    pub batch_index: Option<u32>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for tasks.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.  Claiming and reclaiming use
/// atomic CTEs for concurrency safety.
#[async_trait]
pub trait TaskRepository {
    /// Inserts a new task and returns the fully populated domain type.
    ///
    /// Surfaces [`DbError::UniqueViolation`] when the partial unique
    /// indexes on singleton or triage tasks are violated.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] on duplicate task conflict.
    /// Returns [`DbError::QueryFailed`] on other database errors.
    async fn insert(&self, conn: &mut PgConnection, new_task: &NewTask) -> Result<Task, DbError>;

    /// Finds a task by its ID.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no task with the given ID exists.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(&self, conn: &mut PgConnection, id: TaskId) -> Result<Task, DbError>;

    /// Finds all tasks for a job, ordered by `created_at` ascending
    /// with `id` as tiebreaker.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_job_id(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Vec<Task>, DbError>;

    /// Atomically claims up to `limit` queued tasks.
    ///
    /// Uses a CTE with `FOR UPDATE SKIP LOCKED` for concurrency safety.
    /// Only tasks with `available_at <= now()` and `status = 'queued'`
    /// are eligible.  Returns the claimed tasks with their new
    /// `claim_token`, `claimed_by`, `claimed_at`, and `heartbeat_at` set.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn claim(
        &self,
        conn: &mut PgConnection,
        limit: u32,
        claimed_by: &str,
    ) -> Result<Vec<Task>, DbError>;

    /// Updates the heartbeat timestamp for a claimed task.
    ///
    /// Guarded by `claim_token` — only the current owner can heartbeat.
    /// Returns the number of rows affected (0 if the token does not
    /// match or the task is no longer claimed).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn heartbeat(
        &self,
        conn: &mut PgConnection,
        id: TaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError>;

    /// Marks a task as completed, guarded by `claim_token`.
    ///
    /// Clears the `claim_token` but retains `claimed_by`, `claimed_at`,
    /// and `heartbeat_at` for audit.  Returns the number of rows affected
    /// (0 if the token does not match or the task is not claimed).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn complete(
        &self,
        conn: &mut PgConnection,
        id: TaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError>;

    /// Fails a task: re-queues with retry backoff or dead-letters.
    ///
    /// Increments `retry_count`.  If the post-increment count exceeds
    /// `max_retries`, the task transitions to `dead_letter` with the
    /// current `available_at` preserved.  Otherwise, the task returns to
    /// `queued` with `available_at` set to the caller-provided value
    /// (which should include backoff and jitter).  Clears all claim
    /// fields (`claim_token`, `claimed_by`, `claimed_at`, `heartbeat_at`).
    ///
    /// Returns the number of rows affected (0 if the token does not
    /// match or the task is not claimed).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    #[allow(clippy::too_many_arguments)]
    async fn fail(
        &self,
        conn: &mut PgConnection,
        id: TaskId,
        claim_token: uuid::Uuid,
        max_retries: u32,
        available_at: DateTime<Utc>,
        error_kind: TaskErrorKind,
        error_message: &str,
    ) -> Result<u64, DbError>;

    /// Reclaims stale tasks whose heartbeat has lapsed.
    ///
    /// Sweeps tasks with `status = 'claimed'` and
    /// `heartbeat_at < now() - timeout_seconds`.  Tasks within their
    /// retry budget are re-queued with either a flat backoff (when
    /// `flat_backoff_seconds` is `Some`) or exponential backoff
    /// (`2^retry_count` seconds, when `None`).  Tasks exceeding
    /// `max_retries` are dead-lettered.
    ///
    /// Returns a [`ReclaimOutcome`] with the count of requeued and
    /// dead-lettered tasks.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    #[allow(clippy::too_many_arguments)]
    async fn reclaim_stale(
        &self,
        conn: &mut PgConnection,
        timeout_seconds: u32,
        max_retries: u32,
        limit: u32,
        error_kind: TaskErrorKind,
        error_message: &str,
        flat_backoff_seconds: Option<u32>,
    ) -> Result<ReclaimOutcome, DbError>;

    /// Counts sibling tasks of the given type and statuses for a job,
    /// excluding the specified task.
    ///
    /// The exclusion is a defensive guard for callers that run this
    /// check within the same transaction that transitions the excluded
    /// task.  On the same connection the updated status is already
    /// visible, but the exclusion ensures correctness regardless of
    /// statement ordering within the transaction.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn count_siblings_by_status(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
        task_type: TaskType,
        statuses: &[TaskStatus],
        exclude_task_id: TaskId,
    ) -> Result<i64, DbError>;

    /// Inserts a task idempotently using `ON CONFLICT DO NOTHING`.
    ///
    /// Returns the number of rows affected: `1` if the task was
    /// created, `0` if it already exists (matched by the
    /// `idx_tasks_unique_singleton` partial unique index).  Unlike
    /// [`insert`], this keeps the enclosing transaction healthy on
    /// conflict.
    ///
    /// Only applicable to singleton task types (`extraction`,
    /// `relation`) — the `ON CONFLICT` clause references the partial
    /// unique index on `(job_id, task_type) WHERE task_type IN
    /// ('extraction', 'relation')`.  Triage tasks use a separate
    /// index that includes `batch_index`.
    ///
    /// # Panics
    ///
    /// Panics if `new_task.task_type` is not `Extraction` or `Relation`,
    /// or if `new_task.batch_index` is `Some`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn upsert(&self, conn: &mut PgConnection, new_task: &NewTask) -> Result<u64, DbError>;

    /// Counts tasks grouped by `(task_type, status)`.
    ///
    /// Returns all combinations present in the database.  Used by
    /// the periodic queue health gauge task to set gauge values.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn count_by_status(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Vec<TaskStatusCount>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`TaskRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgTaskRepository;

#[async_trait]
impl TaskRepository for PgTaskRepository {
    async fn insert(&self, conn: &mut PgConnection, new_task: &NewTask) -> Result<Task, DbError> {
        let batch_index_i32 = new_task
            .batch_index
            .map(|v| i32::try_from(v).expect(BATCH_INDEX_EXCEEDS_I32));

        let sql = format!(
            "INSERT INTO tasks (job_id, task_type, batch_index) \
             VALUES ($1, $2, $3) \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(new_task.job_id.inner())
            .bind(new_task.task_type.as_str())
            .bind(batch_index_i32)
            .fetch_one(&mut *conn)
            .await;

        match row {
            Ok(r) => Ok(map_task_row(&r)),
            Err(e) => {
                if let Some(uv) = super::common::constraint::try_into_unique_violation(&e) {
                    Err(uv)
                } else {
                    Err(DbError::QueryFailed {
                        context: "inserting task".to_owned(),
                        source: e,
                    })
                }
            }
        }
    }

    async fn find_by_id(&self, conn: &mut PgConnection, id: TaskId) -> Result<Task, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM tasks WHERE id = $1");

        let row = sqlx::query(&sql)
            .bind(id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding task by id {id}"),
                source: e,
            })?
            .ok_or_else(|| DbError::NotFound {
                entity: "task",
                id: id.to_string(),
            })?;

        Ok(map_task_row(&row))
    }

    async fn find_by_job_id(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Vec<Task>, DbError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM tasks WHERE job_id = $1 \
             ORDER BY created_at ASC, id ASC",
        );

        let rows = sqlx::query(&sql)
            .bind(job_id.inner())
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding tasks for job {job_id}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_task_row).collect())
    }

    async fn claim(
        &self,
        conn: &mut PgConnection,
        limit: u32,
        claimed_by: &str,
    ) -> Result<Vec<Task>, DbError> {
        let limit_i64 = i64::from(limit);

        let sql = format!(
            "WITH claimable AS ( \
                 SELECT id FROM tasks \
                 WHERE status = 'queued' AND available_at <= now() \
                 ORDER BY available_at, created_at \
                 LIMIT $1 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             UPDATE tasks t \
             SET status = 'claimed', \
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
            .bind(limit_i64)
            .bind(claimed_by)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("claiming up to {limit} tasks"),
                source: e,
            })?;

        Ok(rows.iter().map(map_task_row).collect())
    }

    async fn heartbeat(
        &self,
        conn: &mut PgConnection,
        id: TaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            "UPDATE tasks \
             SET heartbeat_at = now(), updated_at = now() \
             WHERE id = $1 AND claim_token = $2 AND status = 'claimed'",
        )
        .bind(id.inner())
        .bind(claim_token)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("heartbeat for task {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn complete(
        &self,
        conn: &mut PgConnection,
        id: TaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            "UPDATE tasks \
             SET status = 'completed', claim_token = NULL, updated_at = now() \
             WHERE id = $1 AND claim_token = $2 AND status = 'claimed'",
        )
        .bind(id.inner())
        .bind(claim_token)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("completing task {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn fail(
        &self,
        conn: &mut PgConnection,
        id: TaskId,
        claim_token: uuid::Uuid,
        max_retries: u32,
        available_at: DateTime<Utc>,
        error_kind: TaskErrorKind,
        error_message: &str,
    ) -> Result<u64, DbError> {
        let max_retries_i32 = i32::try_from(max_retries).expect(MAX_RETRIES_EXCEEDS_I32);

        let result = sqlx::query(
            "UPDATE tasks \
             SET retry_count = retry_count + 1, \
                 status = CASE \
                     WHEN retry_count + 1 > $3 THEN 'dead_letter' \
                     ELSE 'queued' \
                 END, \
                 available_at = CASE \
                     WHEN retry_count + 1 > $3 THEN available_at \
                     ELSE $4 \
                 END, \
                 claim_token = NULL, \
                 claimed_by = NULL, \
                 claimed_at = NULL, \
                 heartbeat_at = NULL, \
                 error_kind = $5, \
                 error_message = $6, \
                 updated_at = now() \
             WHERE id = $1 AND claim_token = $2 AND status = 'claimed'",
        )
        .bind(id.inner())
        .bind(claim_token)
        .bind(max_retries_i32)
        .bind(available_at)
        .bind(error_kind.as_str())
        .bind(error_message)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("failing task {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn reclaim_stale(
        &self,
        conn: &mut PgConnection,
        timeout_seconds: u32,
        max_retries: u32,
        limit: u32,
        error_kind: TaskErrorKind,
        error_message: &str,
        flat_backoff_seconds: Option<u32>,
    ) -> Result<ReclaimOutcome, DbError> {
        let timeout_f64 = f64::from(timeout_seconds);
        let max_retries_i32 = i32::try_from(max_retries).expect(MAX_RETRIES_EXCEEDS_I32);
        let limit_i64 = i64::from(limit);
        let error_kind_str = error_kind.as_str();
        let flat_backoff_f64 = flat_backoff_seconds.map(f64::from);

        // Backoff: when `flat_backoff_seconds` is provided, all
        // requeued tasks use that flat delay.  Otherwise, exponential
        // backoff `2^retry_count` is applied using the pre-increment
        // count (so the first reclaim backoff is `2^0 = 1s`).  This
        // is intentionally lower than the Rust-side `backoff_duration`
        // which uses the post-increment count (`2^1 = 2s` for first
        // inline failure) — reclaim recovery should retry sooner.
        let rows = sqlx::query(
            "WITH stale AS ( \
                 SELECT id, retry_count FROM tasks \
                 WHERE status = 'claimed' \
                   AND heartbeat_at < now() - make_interval(secs => $1::double precision) \
                 ORDER BY heartbeat_at ASC \
                 LIMIT $3 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             UPDATE tasks t \
             SET retry_count = s.retry_count + 1, \
                 status = CASE \
                     WHEN s.retry_count + 1 > $2 THEN 'dead_letter' \
                     ELSE 'queued' \
                 END, \
                 available_at = CASE \
                     WHEN s.retry_count + 1 > $2 THEN t.available_at \
                     WHEN $6 IS NOT NULL \
                         THEN now() + make_interval(secs => $6) \
                     ELSE now() + make_interval( \
                         secs => power(2, s.retry_count)::double precision \
                     ) \
                 END, \
                 claim_token = NULL, \
                 claimed_by = NULL, \
                 claimed_at = NULL, \
                 heartbeat_at = NULL, \
                 error_kind = $4, \
                 error_message = $5, \
                 updated_at = now() \
             FROM stale s \
             WHERE t.id = s.id \
             RETURNING t.status",
        )
        .bind(timeout_f64)
        .bind(max_retries_i32)
        .bind(limit_i64)
        .bind(error_kind_str)
        .bind(error_message)
        .bind(flat_backoff_f64)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "reclaiming stale tasks".to_owned(),
            source: e,
        })?;

        let mut requeued: u64 = 0;
        let mut dead_lettered: u64 = 0;
        for row in &rows {
            match row.get::<String, _>("status").as_str() {
                "queued" => requeued += 1,
                "dead_letter" => dead_lettered += 1,
                other => debug_assert!(false, "unexpected reclaim status: {other}"),
            }
        }

        Ok(ReclaimOutcome {
            requeued,
            dead_lettered,
        })
    }

    async fn count_siblings_by_status(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
        task_type: TaskType,
        statuses: &[TaskStatus],
        exclude_task_id: TaskId,
    ) -> Result<i64, DbError> {
        let status_strings: Vec<&str> = statuses.iter().map(TaskStatus::as_str).collect();

        sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks \
             WHERE job_id = $1 \
               AND task_type = $2 \
               AND status = ANY($3::text[]) \
               AND id != $4",
        )
        .bind(job_id.inner())
        .bind(task_type.as_str())
        .bind(&status_strings)
        .bind(exclude_task_id.inner())
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("counting {task_type} siblings for job {job_id}"),
            source: e,
        })
    }

    async fn upsert(&self, conn: &mut PgConnection, new_task: &NewTask) -> Result<u64, DbError> {
        assert!(
            matches!(
                new_task.task_type,
                TaskType::Extraction | TaskType::Relation,
            ),
            "{UPSERT_REQUIRES_SINGLETON}",
        );
        assert!(
            new_task.batch_index.is_none(),
            "singleton tasks must not have a batch_index",
        );

        let batch_index_i32 = new_task
            .batch_index
            .map(|v| i32::try_from(v).expect(BATCH_INDEX_EXCEEDS_I32));

        let result = sqlx::query(
            "INSERT INTO tasks (job_id, task_type, batch_index) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (job_id, task_type) \
                 WHERE task_type IN ('extraction', 'relation') \
             DO NOTHING",
        )
        .bind(new_task.job_id.inner())
        .bind(new_task.task_type.as_str())
        .bind(batch_index_i32)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!(
                "upserting {} task for job {}",
                new_task.task_type, new_task.job_id,
            ),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn count_by_status(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Vec<TaskStatusCount>, DbError> {
        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT task_type, status, COUNT(*) \
             FROM tasks \
             GROUP BY task_type, status",
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "counting tasks by status".to_owned(),
            source: e,
        })?;

        Ok(rows
            .into_iter()
            .map(|(task_type, status, count)| TaskStatusCount {
                task_type: task_type.parse::<TaskType>().expect(UNKNOWN_TASK_TYPE_IN_DB),
                status: status.parse::<TaskStatus>().expect(UNKNOWN_TASK_STATUS_IN_DB),
                count,
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from a task query into a [`Task`].
fn map_task_row(r: &sqlx::postgres::PgRow) -> Task {
    Task::builder()
        .id(TaskId::from(r.get::<uuid::Uuid, _>("id")))
        .job_id(JobId::from(r.get::<uuid::Uuid, _>("job_id")))
        .task_type(
            r.get::<String, _>("task_type")
                .parse::<TaskType>()
                .expect(UNKNOWN_TASK_TYPE_IN_DB),
        )
        .status(
            r.get::<String, _>("status")
                .parse::<TaskStatus>()
                .expect(UNKNOWN_TASK_STATUS_IN_DB),
        )
        .batch_index(
            r.get::<Option<i32>, _>("batch_index")
                .map(|v| u32::try_from(v).expect(BATCH_INDEX_OVERFLOW)),
        )
        .claim_token(r.get("claim_token"))
        .available_at(r.get("available_at"))
        .claimed_by(r.get("claimed_by"))
        .claimed_at(r.get("claimed_at"))
        .heartbeat_at(r.get("heartbeat_at"))
        .retry_count(u32::try_from(r.get::<i32, _>("retry_count")).expect(RETRY_COUNT_OVERFLOW))
        .error_kind(r.get::<Option<String>, _>("error_kind").map(|s| {
            s.parse::<TaskErrorKind>()
                .expect(UNKNOWN_TASK_ERROR_KIND_IN_DB)
        }))
        .error_message(r.get("error_message"))
        .created_at(r.get("created_at"))
        .updated_at(r.get("updated_at"))
        .build()
}

// ---------------------------------------------------------------------------
// Test helpers (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "test-helpers")]
impl PgTaskRepository {
    /// Inserts a task with a caller-controlled status, bypassing the
    /// production `insert()` which always creates `queued` tasks.
    ///
    /// # Panics
    ///
    /// Panics if `batch_index` exceeds `i32::MAX`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    pub async fn insert_for_test(
        &self,
        conn: &mut PgConnection,
        new_task: &NewTask,
        status: TaskStatus,
    ) -> Result<Task, DbError> {
        let batch_index_i32 = new_task
            .batch_index
            .map(|v| i32::try_from(v).expect(BATCH_INDEX_EXCEEDS_I32));

        let sql = format!(
            "INSERT INTO tasks (job_id, task_type, batch_index, status) \
             VALUES ($1, $2, $3, $4) \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(new_task.job_id.inner())
            .bind(new_task.task_type.as_str())
            .bind(batch_index_i32)
            .bind(status.as_str())
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "inserting task (test helper)".to_owned(),
                source: e,
            })?;

        Ok(map_task_row(&row))
    }
}
