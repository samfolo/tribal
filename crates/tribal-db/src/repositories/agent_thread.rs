//! Agent thread repository: the thread store's guarded transitions.
//!
//! Status moves exist only as compare-and-set methods naming their `from`
//! status; zero affected rows is the CAS-miss signal every §8 retry loop
//! is built on, with the table CHECKs as backstop. Methods that leave
//! `suspended` clear the suspension payload and wake instant in the same
//! statement, so the suspended-has-cause CHECK can never trip. The
//! cancellation intent write is seq-free and idempotent. Uses runtime
//! `sqlx::query()` because rows carry TEXT-encoded domain enums and JSONB
//! payloads.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{
    AgentBindingVersionId, AgentDriverTaskId, AgentThread, AgentThreadId, AgentThreadStatus,
    AgentThreadSuspension, AgentThreadTerminal, ExecutionSpend, PrincipalId, TaskId, TaskType,
};
use typed_builder::TypedBuilder;

use super::common::{columns::Columns, constraint::try_into_unique_violation};
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "parent_thread_id",
    "pipeline_stage",
    "binding_version_id",
    "stage_task_id",
    "driver_task_id",
    "principal_id",
    "status",
    "suspension",
    "cancel_requested_at",
    "cancel_requested_by",
    "recovery_attempts",
    "format_version",
    "wake_at",
    "fidelity",
    "execution_spend",
    "completed_at",
    "created_at",
    "updated_at",
]);

const UNKNOWN_STAGE_IN_DB: &str = "unrecognised pipeline stage in database: schema mismatch";
const UNKNOWN_STATUS_IN_DB: &str = "unrecognised thread status in database: schema mismatch";
const MALFORMED_SUSPENSION_IN_DB: &str = "malformed suspension payload in database: format drift";
const RECOVERY_OVERFLOW: &str = "negative recovery_attempts in database: data corruption";
const FORMAT_VERSION_OVERFLOW: &str = "negative format_version in database: data corruption";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// The one driving task a thread is created with.
///
/// The schema XORs the two columns; this enum makes the invalid shapes
/// (both, neither) unrepresentable at the input layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrivingTaskRef {
    /// Driven by a launched stage task.
    Stage(TaskId),
    /// Driven by a driver-family row.
    Driver(AgentDriverTaskId),
}

/// Input for creating a thread.
///
/// `id`, `status` (queued), counters, and timestamps are
/// server-defaulted.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewAgentThread {
    /// The parent thread, for delegation lineage.
    #[builder(default)]
    pub parent_thread_id: Option<AgentThreadId>,
    /// The pipeline stage this thread executes.
    pub pipeline_stage: TaskType,
    /// The binding version this thread is admitted under.
    pub binding_version_id: AgentBindingVersionId,
    /// The one driving task.
    pub driving_task: DrivingTaskRef,
    /// The principal this run is attributed and metered to.
    pub principal_id: PrincipalId,
    /// The serialisation shape of the thread's owned structures.
    pub format_version: u32,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for agent threads.
#[async_trait]
pub trait AgentThreadRepository {
    /// Creates a thread, queued.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] when the stage task already
    /// drives a thread, or [`DbError::QueryFailed`] on other database
    /// errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewAgentThread,
    ) -> Result<AgentThread, DbError>;

    /// Finds a thread by id.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
    ) -> Result<Option<AgentThread>, DbError>;

    /// Finds the thread a stage task drives.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_stage_task(
        &self,
        conn: &mut PgConnection,
        stage_task_id: TaskId,
    ) -> Result<Option<AgentThread>, DbError>;

    /// Locks a thread row (`FOR UPDATE`) and returns its current state.
    /// Every resolution append locks the thread before its completeness
    /// check, and every transition locks before deriving a seq.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn lock(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
    ) -> Result<Option<AgentThread>, DbError>;

    /// CAS to `running` from `from`, clearing any suspension payload and
    /// wake instant in the same statement. Returns the affected row count
    /// (zero is the CAS miss).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn mark_running(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
        from: AgentThreadStatus,
    ) -> Result<u64, DbError>;

    /// CAS from `running` to `suspended` with the typed cause, refusing
    /// to commit over a durable cancellation intent: the `WHERE` clause
    /// requires `cancel_requested_at IS NULL`, so a worker whose suspend
    /// returns zero rows performs the cancel transaction at that boundary
    /// instead.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn suspend(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
        suspension: &AgentThreadSuspension,
        wake_at: Option<DateTime<Utc>>,
    ) -> Result<u64, DbError>;

    /// CAS to a terminal status from `from`, stamping `completed_at` and
    /// clearing any suspension payload and wake instant. Returns the
    /// affected row count (zero is the CAS miss).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn complete(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
        terminal: AgentThreadTerminal,
        from: AgentThreadStatus,
    ) -> Result<u64, DbError>;

    /// Durably records a cancellation intent: an idempotent, seq-free
    /// write to columns no record commit touches, so it can never be lost
    /// to a racing commit. Recording an intent on a terminal thread is
    /// harmless dead data.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn record_cancel_intent(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
        requested_by: &str,
    ) -> Result<(), DbError>;

    /// Advances the accumulated recovery-cycle counter, returning the new
    /// value. The counter never resets.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors, including a
    /// vanished row.
    async fn increment_recovery_attempts(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
    ) -> Result<u32, DbError>;

    /// Replaces the committed-record spend projection.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn set_execution_spend(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
        spend: &ExecutionSpend,
    ) -> Result<(), DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`AgentThreadRepository`].
pub struct PgAgentThreadRepository;

#[async_trait]
impl AgentThreadRepository for PgAgentThreadRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewAgentThread,
    ) -> Result<AgentThread, DbError> {
        let (stage_task_id, driver_task_id) = match new.driving_task {
            DrivingTaskRef::Stage(id) => (Some(id.inner().to_owned()), None),
            DrivingTaskRef::Driver(id) => (None, Some(id.inner().to_owned())),
        };

        let sql = format!(
            "INSERT INTO agent_threads \
             (parent_thread_id, pipeline_stage, binding_version_id, stage_task_id, \
              driver_task_id, principal_id, format_version) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             RETURNING {COLUMNS}"
        );
        let row = sqlx::query(&sql)
            .bind(new.parent_thread_id.map(|id| id.inner().to_owned()))
            .bind(new.pipeline_stage.as_str())
            .bind(new.binding_version_id.inner())
            .bind(stage_task_id)
            .bind(driver_task_id)
            .bind(new.principal_id.inner())
            .bind(i32::try_from(new.format_version).expect(FORMAT_VERSION_OVERFLOW))
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| {
                try_into_unique_violation(&e).unwrap_or_else(|| DbError::QueryFailed {
                    context: format!("creating {} thread", new.pipeline_stage),
                    source: e,
                })
            })?;

        Ok(map_agent_thread_row(&row))
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
    ) -> Result<Option<AgentThread>, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM agent_threads WHERE id = $1");
        let row = sqlx::query(&sql)
            .bind(id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding thread {id}"),
                source: e,
            })?;

        Ok(row.as_ref().map(map_agent_thread_row))
    }

    async fn find_by_stage_task(
        &self,
        conn: &mut PgConnection,
        stage_task_id: TaskId,
    ) -> Result<Option<AgentThread>, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM agent_threads WHERE stage_task_id = $1");
        let row = sqlx::query(&sql)
            .bind(stage_task_id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding the thread driven by task {stage_task_id}"),
                source: e,
            })?;

        Ok(row.as_ref().map(map_agent_thread_row))
    }

    async fn lock(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
    ) -> Result<Option<AgentThread>, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM agent_threads WHERE id = $1 FOR UPDATE");
        let row = sqlx::query(&sql)
            .bind(id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("locking thread {id}"),
                source: e,
            })?;

        Ok(row.as_ref().map(map_agent_thread_row))
    }

    async fn mark_running(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
        from: AgentThreadStatus,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            "UPDATE agent_threads \
             SET status = 'running', suspension = NULL, wake_at = NULL, updated_at = now() \
             WHERE id = $1 AND status = $2",
        )
        .bind(id.inner())
        .bind(from.as_str())
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("marking thread {id} running"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn suspend(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
        suspension: &AgentThreadSuspension,
        wake_at: Option<DateTime<Utc>>,
    ) -> Result<u64, DbError> {
        let payload = serde_json::to_value(suspension).map_err(|e| DbError::QueryFailed {
            context: format!("serialising suspension for thread {id}"),
            source: sqlx::Error::Encode(Box::new(e)),
        })?;

        let result = sqlx::query(
            "UPDATE agent_threads \
             SET status = 'suspended', suspension = $2, wake_at = $3, updated_at = now() \
             WHERE id = $1 AND status = 'running' AND cancel_requested_at IS NULL",
        )
        .bind(id.inner())
        .bind(payload)
        .bind(wake_at)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("suspending thread {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn complete(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
        terminal: AgentThreadTerminal,
        from: AgentThreadStatus,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            "UPDATE agent_threads \
             SET status = $2, suspension = NULL, wake_at = NULL, \
                 completed_at = now(), updated_at = now() \
             WHERE id = $1 AND status = $3",
        )
        .bind(id.inner())
        .bind(AgentThreadStatus::from(terminal).as_str())
        .bind(from.as_str())
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("completing thread {id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn record_cancel_intent(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
        requested_by: &str,
    ) -> Result<(), DbError> {
        sqlx::query(
            "UPDATE agent_threads \
             SET cancel_requested_at = COALESCE(cancel_requested_at, now()), \
                 cancel_requested_by = COALESCE(cancel_requested_by, $2), \
                 updated_at = now() \
             WHERE id = $1",
        )
        .bind(id.inner())
        .bind(requested_by)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("recording cancel intent on thread {id}"),
            source: e,
        })?;

        Ok(())
    }

    async fn increment_recovery_attempts(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
    ) -> Result<u32, DbError> {
        let row = sqlx::query(
            "UPDATE agent_threads \
             SET recovery_attempts = recovery_attempts + 1, updated_at = now() \
             WHERE id = $1 \
             RETURNING recovery_attempts",
        )
        .bind(id.inner())
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("advancing recovery attempts on thread {id}"),
            source: e,
        })?;

        Ok(u32::try_from(row.get::<i32, _>("recovery_attempts")).expect(RECOVERY_OVERFLOW))
    }

    async fn set_execution_spend(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
        spend: &ExecutionSpend,
    ) -> Result<(), DbError> {
        let payload = serde_json::to_value(spend).map_err(|e| DbError::QueryFailed {
            context: format!("serialising spend for thread {id}"),
            source: sqlx::Error::Encode(Box::new(e)),
        })?;

        sqlx::query(
            "UPDATE agent_threads SET execution_spend = $2, updated_at = now() WHERE id = $1",
        )
        .bind(id.inner())
        .bind(payload)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("recording spend on thread {id}"),
            source: e,
        })?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

fn map_agent_thread_row(r: &sqlx::postgres::PgRow) -> AgentThread {
    AgentThread::builder()
        .id(AgentThreadId::from(r.get::<uuid::Uuid, _>("id")))
        .parent_thread_id(
            r.get::<Option<uuid::Uuid>, _>("parent_thread_id")
                .map(AgentThreadId::from),
        )
        .pipeline_stage(
            r.get::<String, _>("pipeline_stage")
                .parse::<TaskType>()
                .expect(UNKNOWN_STAGE_IN_DB),
        )
        .binding_version_id(AgentBindingVersionId::from(
            r.get::<uuid::Uuid, _>("binding_version_id"),
        ))
        .stage_task_id(
            r.get::<Option<uuid::Uuid>, _>("stage_task_id")
                .map(TaskId::from),
        )
        .driver_task_id(
            r.get::<Option<uuid::Uuid>, _>("driver_task_id")
                .map(AgentDriverTaskId::from),
        )
        .principal_id(PrincipalId::from(r.get::<uuid::Uuid, _>("principal_id")))
        .status(
            r.get::<String, _>("status")
                .parse::<AgentThreadStatus>()
                .expect(UNKNOWN_STATUS_IN_DB),
        )
        .suspension(
            r.get::<Option<serde_json::Value>, _>("suspension")
                .map(|v| {
                    serde_json::from_value::<AgentThreadSuspension>(v)
                        .expect(MALFORMED_SUSPENSION_IN_DB)
                }),
        )
        .cancel_requested_at(r.get("cancel_requested_at"))
        .cancel_requested_by(r.get::<Option<String>, _>("cancel_requested_by"))
        .recovery_attempts(
            u32::try_from(r.get::<i32, _>("recovery_attempts")).expect(RECOVERY_OVERFLOW),
        )
        .format_version(
            u32::try_from(r.get::<i32, _>("format_version")).expect(FORMAT_VERSION_OVERFLOW),
        )
        .wake_at(r.get("wake_at"))
        .fidelity(r.get::<Option<serde_json::Value>, _>("fidelity"))
        .execution_spend(r.get::<Option<serde_json::Value>, _>("execution_spend"))
        .completed_at(r.get("completed_at"))
        .created_at(r.get("created_at"))
        .updated_at(r.get("updated_at"))
        .build()
}
