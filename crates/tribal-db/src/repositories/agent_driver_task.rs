//! Agent driver task repository: the lease protocol for the driver family.
//!
//! Derived from the reindex task lease behaviour: claim via `FOR UPDATE
//! SKIP LOCKED`, claim-token CAS on every worker-held mutation. Unlike the
//! launched families, no retry-or-dead-letter CASE lives in SQL: the
//! disposition decision is a domain function, and the runtime calls the
//! explicit outcome method it decided on (`complete`, `requeue`,
//! `dead_letter`). Uses runtime `sqlx::query()` for the CTE-based claim
//! and TEXT-encoded domain enums.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{
    AgentDriverTask, AgentDriverTaskId, AgentDriverTaskKind, AgentDriverTaskState, AgentThreadId,
};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "thread_id",
    "kind",
    "state",
    "attempt",
    "max_attempts",
    "available_at",
    "claim_token",
    "claimed_by",
    "claimed_at",
    "heartbeat_at",
    "last_error",
    "created_at",
    "updated_at",
    "completed_at",
]);

const UNKNOWN_KIND_IN_DB: &str = "unrecognised driver task kind in database: schema mismatch";
const UNKNOWN_STATE_IN_DB: &str = "unrecognised driver task state in database: schema mismatch";
const ATTEMPT_OVERFLOW: &str = "negative attempt in database: data corruption";
const ATTEMPT_EXCEEDS_I32: &str = "attempt exceeds i32 range";
const MAX_ATTEMPTS_OVERFLOW: &str = "negative max_attempts in database: data corruption";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for enrolling a driver task.
///
/// `state`, `attempt`, `max_attempts`, `available_at`, and the
/// timestamps are server-defaulted: a new row is pending and immediately
/// available with the standard retry budget. The id is server-defaulted
/// too unless the caller supplies one. The thread/driver pair writer
/// must, because the thread row names the driver task's id under the
/// deferred foreign key before the task exists.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewAgentDriverTask {
    /// A client-generated id, for the pair writer's deferred reference.
    #[builder(default)]
    pub id: Option<AgentDriverTaskId>,
    /// The thread this row drives or works for.
    pub thread_id: AgentThreadId,
    /// What this row executes.
    pub kind: AgentDriverTaskKind,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for agent driver tasks.
#[async_trait]
pub trait AgentDriverTaskRepository {
    /// Enrols a driver task, pending and immediately available.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewAgentDriverTask,
    ) -> Result<AgentDriverTask, DbError>;

    /// Finds a driver task by id.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
    ) -> Result<Option<AgentDriverTask>, DbError>;

    /// Lists a thread's driver tasks, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_thread_id(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
    ) -> Result<Vec<AgentDriverTask>, DbError>;

    /// Atomically claims up to `limit` available pending rows.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn claim(
        &self,
        conn: &mut PgConnection,
        limit: u32,
        claimed_by: &str,
    ) -> Result<Vec<AgentDriverTask>, DbError>;

    /// Marks a claimed row completed. Returns the affected row count
    /// (zero means the lease was lost).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn complete(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError>;

    /// Re-queues a claimed row after a retryable failure, with the
    /// disposition's incremented attempt and the caller's backoff.
    /// Returns the affected row count (zero means the lease was lost).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn requeue(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        claim_token: uuid::Uuid,
        attempt: u32,
        available_at: DateTime<Utc>,
        error_message: &str,
    ) -> Result<u64, DbError>;

    /// Dead-letters a claimed row. Returns the affected row count (zero
    /// means the lease was lost).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn dead_letter(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        claim_token: uuid::Uuid,
        error_message: &str,
    ) -> Result<u64, DbError>;

    /// Verifies the caller still holds the claim, taking a shared lock
    /// that pins the lease for the calling transaction: the claim-token
    /// guard for transactions that write no driver-task column. Returns
    /// `true` while the lease holds.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn holds_claim(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        claim_token: uuid::Uuid,
    ) -> Result<bool, DbError>;

    /// Pumps the heartbeat under the claim guard. Returns the affected
    /// row count (zero means the lease was lost).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn heartbeat(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError>;

    /// Locks one stale claimed row (`FOR UPDATE SKIP LOCKED`) whose
    /// heartbeat lapsed beyond `timeout_seconds`, for the reclaim pass.
    /// Returns the row with its claim token intact, so the reclaimer's
    /// guarded disposition targets exactly the lease it judged stale.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn lock_stale(
        &self,
        conn: &mut PgConnection,
        timeout_seconds: u32,
    ) -> Result<Option<AgentDriverTask>, DbError>;

    /// Terminally dispositions an unclaimed live row under a row lock —
    /// the cancel transaction's disposal of pending deferred work. The
    /// `claim_token IS NULL` requirement is the locked-unclaimed guard:
    /// a row a worker holds is never disposed of from outside. Returns
    /// the affected row count.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn dispose_unclaimed(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        error_message: &str,
    ) -> Result<u64, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`AgentDriverTaskRepository`].
pub struct PgAgentDriverTaskRepository;

#[async_trait]
impl AgentDriverTaskRepository for PgAgentDriverTaskRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewAgentDriverTask,
    ) -> Result<AgentDriverTask, DbError> {
        let sql = format!(
            "INSERT INTO agent_driver_tasks (id, thread_id, kind) \
             VALUES (COALESCE($1, gen_random_uuid()), $2, $3) RETURNING {COLUMNS}"
        );
        let row = sqlx::query(&sql)
            .bind(new.id.map(|id| *id.inner()))
            .bind(new.thread_id.inner())
            .bind(new.kind.as_str())
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("enrolling driver task for thread {}", new.thread_id),
                source: e,
            })?;

        Ok(map_agent_driver_task_row(&row))
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
    ) -> Result<Option<AgentDriverTask>, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM agent_driver_tasks WHERE id = $1");
        let row = sqlx::query(&sql)
            .bind(id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding driver task {id}"),
                source: e,
            })?;

        Ok(row.as_ref().map(map_agent_driver_task_row))
    }

    async fn find_by_thread_id(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
    ) -> Result<Vec<AgentDriverTask>, DbError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM agent_driver_tasks WHERE thread_id = $1 ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id.inner())
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("listing driver tasks for thread {thread_id}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_agent_driver_task_row).collect())
    }

    async fn claim(
        &self,
        conn: &mut PgConnection,
        limit: u32,
        claimed_by: &str,
    ) -> Result<Vec<AgentDriverTask>, DbError> {
        let sql = format!(
            "WITH claimable AS ( \
                 SELECT id FROM agent_driver_tasks \
                 WHERE state = 'pending' AND available_at <= now() \
                 ORDER BY available_at, created_at \
                 LIMIT $1 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             UPDATE agent_driver_tasks t \
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
                context: format!("claiming up to {limit} driver tasks"),
                source: e,
            })?;

        Ok(rows.iter().map(map_agent_driver_task_row).collect())
    }

    async fn complete(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            "UPDATE agent_driver_tasks \
             SET state = 'completed', claim_token = NULL, claimed_by = NULL, \
                 claimed_at = NULL, heartbeat_at = NULL, \
                 completed_at = now(), updated_at = now() \
             WHERE id = $1 AND claim_token = $2 AND state = 'claimed'",
        )
        .bind(id.inner())
        .bind(claim_token)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("completing driver task {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn requeue(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        claim_token: uuid::Uuid,
        attempt: u32,
        available_at: DateTime<Utc>,
        error_message: &str,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            "UPDATE agent_driver_tasks \
             SET state = 'pending', attempt = $3, available_at = $4, \
                 claim_token = NULL, claimed_by = NULL, claimed_at = NULL, \
                 heartbeat_at = NULL, last_error = $5, updated_at = now() \
             WHERE id = $1 AND claim_token = $2 AND state = 'claimed'",
        )
        .bind(id.inner())
        .bind(claim_token)
        .bind(i32::try_from(attempt).expect(ATTEMPT_EXCEEDS_I32))
        .bind(available_at)
        .bind(error_message)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("re-queueing driver task {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn dead_letter(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        claim_token: uuid::Uuid,
        error_message: &str,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            "UPDATE agent_driver_tasks \
             SET state = 'dead_letter', claim_token = NULL, claimed_by = NULL, \
                 claimed_at = NULL, heartbeat_at = NULL, last_error = $3, \
                 completed_at = now(), updated_at = now() \
             WHERE id = $1 AND claim_token = $2 AND state = 'claimed'",
        )
        .bind(id.inner())
        .bind(claim_token)
        .bind(error_message)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("dead-lettering driver task {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn holds_claim(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        claim_token: uuid::Uuid,
    ) -> Result<bool, DbError> {
        let held: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM agent_driver_tasks WHERE id = $1 AND claim_token = $2 FOR SHARE",
        )
        .bind(id.inner())
        .bind(claim_token)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("verifying the lease on driver task {id}"),
            source: e,
        })?;

        Ok(held.is_some())
    }

    async fn heartbeat(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        claim_token: uuid::Uuid,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            "UPDATE agent_driver_tasks \
             SET heartbeat_at = now(), updated_at = now() \
             WHERE id = $1 AND claim_token = $2 AND state = 'claimed'",
        )
        .bind(id.inner())
        .bind(claim_token)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("heartbeat for driver task {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn lock_stale(
        &self,
        conn: &mut PgConnection,
        timeout_seconds: u32,
    ) -> Result<Option<AgentDriverTask>, DbError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM agent_driver_tasks \
             WHERE state = 'claimed' \
               AND heartbeat_at < now() - make_interval(secs => $1) \
             ORDER BY heartbeat_at \
             LIMIT 1 \
             FOR UPDATE SKIP LOCKED"
        );
        let row = sqlx::query(&sql)
            .bind(f64::from(timeout_seconds))
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "locking a stale driver task".to_owned(),
                source: e,
            })?;

        Ok(row.as_ref().map(map_agent_driver_task_row))
    }

    async fn dispose_unclaimed(
        &self,
        conn: &mut PgConnection,
        id: AgentDriverTaskId,
        error_message: &str,
    ) -> Result<u64, DbError> {
        // The inner SELECT ... FOR UPDATE is the locked-unclaimed guard:
        // the row is locked before the state write, and a claimed row
        // (token held by a live worker) is never selected.
        let terminal = terminal_driver_state_literals().join(", ");
        let sql = format!(
            "WITH target AS ( \
                 SELECT id FROM agent_driver_tasks \
                 WHERE id = $1 AND claim_token IS NULL \
                   AND state NOT IN ({terminal}) \
                 FOR UPDATE \
             ) \
             UPDATE agent_driver_tasks t \
             SET state = 'dead_letter', last_error = $2, \
                 completed_at = now(), updated_at = now() \
             FROM target \
             WHERE t.id = target.id",
        );
        let result = sqlx::query(&sql)
            .bind(id.inner())
            .bind(error_message)
            .execute(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("disposing of unclaimed driver task {id}"),
                source: e,
            })?;

        Ok(result.rows_affected())
    }
}

/// The quoted SQL literals for the terminal driver-task states, derived
/// from `ALL` so a new state joins the set by construction. Interpolated,
/// not bound, so the planner matches a partial index's literal predicate;
/// the single source the disposal and the sweep's orphan and stuck-relating
/// scans share.
pub(super) fn terminal_driver_state_literals() -> Vec<String> {
    AgentDriverTaskState::ALL
        .iter()
        .filter(|state| state.is_terminal())
        .map(|state| format!("'{}'", state.as_str()))
        .collect()
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

fn map_agent_driver_task_row(r: &sqlx::postgres::PgRow) -> AgentDriverTask {
    AgentDriverTask::builder()
        .id(AgentDriverTaskId::from(r.get::<uuid::Uuid, _>("id")))
        .thread_id(AgentThreadId::from(r.get::<uuid::Uuid, _>("thread_id")))
        .kind(
            r.get::<String, _>("kind")
                .parse::<AgentDriverTaskKind>()
                .expect(UNKNOWN_KIND_IN_DB),
        )
        .state(
            r.get::<String, _>("state")
                .parse::<AgentDriverTaskState>()
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
        .created_at(r.get("created_at"))
        .updated_at(r.get("updated_at"))
        .completed_at(r.get("completed_at"))
        .build()
}
