//! Agent thread repository: the thread store's guarded transitions.
//!
//! Status moves exist only as compare-and-set methods naming their `from`
//! status; zero affected rows is the CAS-miss signal callers act on. Methods
//! that leave `suspended` clear the suspension payload and wake instant in the
//! same statement, so the suspended-has-cause CHECK can never trip.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{
    AgentBindingVersionId, AgentDriverTaskId, AgentThread, AgentThreadId, AgentThreadStage,
    AgentThreadStatus, AgentThreadSuspension, AgentThreadTerminal, ExecutionSpend, JobId,
    PrincipalId, TaskId, TaskType,
};
use typed_builder::TypedBuilder;

use super::{
    agent_driver_task::terminal_driver_state_literals,
    common::{columns::Columns, constraint::try_into_unique_violation},
};
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
    "job_id",
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
    Stage(TaskId),
    Driver(AgentDriverTaskId),
}

impl std::fmt::Display for DrivingTaskRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stage(id) => write!(f, "stage task {id}"),
            Self::Driver(id) => write!(f, "driver task {id}"),
        }
    }
}

/// Input for creating a thread.
///
/// `id`, `status` (queued), counters, and timestamps are server-defaulted.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewAgentThread {
    #[builder(default)]
    pub parent_thread_id: Option<AgentThreadId>,
    pub stage: AgentThreadStage,
    /// Absent for a managed thread, which carries no binding.
    #[builder(default)]
    pub binding_version_id: Option<AgentBindingVersionId>,
    pub driving_task: DrivingTaskRef,
    /// Absent for a managed thread's account-level automation.
    #[builder(default)]
    pub principal_id: Option<PrincipalId>,
    #[builder(default)]
    pub job_id: Option<JobId>,
    /// The serialisation shape of the thread's owned structures.
    pub format_version: u32,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Criteria for one thread prune pass.
#[derive(Debug, Clone)]
pub struct ThreadPruneCriteria {
    pub completed_before: DateTime<Utc>,
    pub stage: Option<TaskType>,
    /// Include a candidate's terminal descendants in the deletion; a live
    /// descendant still refuses the whole subtree.
    pub cascade: bool,
}

/// What one prune pass concluded (or would conclude, for a dry run).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThreadPruneOutcome {
    pub pruned: u64,
    /// Candidate roots refused: a live descendant, or any descendant without
    /// the cascade flag.
    pub refused: u64,
}

/// Data access operations for agent threads.
#[async_trait]
pub trait AgentThreadRepository {
    /// Creates a thread, queued.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] when the driving task already
    /// drives a thread (the stage and driver sides each hold their own unique
    /// fence), or [`DbError::QueryFailed`] on other database errors.
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
    async fn find_by_stage_task_id(
        &self,
        conn: &mut PgConnection,
        stage_task_id: TaskId,
    ) -> Result<Option<AgentThread>, DbError>;

    /// Lists the threads a job's stage tasks drive, ordered by creation time
    /// with ties broken by id.
    ///
    /// Reaches threads through the stage-task linkage, so driver-driven threads
    /// (delegated children) belong to their parent's lineage, not the job's
    /// stage roster, and are excluded.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_job_id(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Vec<AgentThread>, DbError>;

    /// Locks a thread row (`FOR UPDATE`) and returns its current state.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn lock(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
    ) -> Result<Option<AgentThread>, DbError>;

    /// CAS to `running` from `from`, clearing any suspension payload and wake
    /// instant in the same statement. Returns the affected row count (zero is
    /// the CAS miss).
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

    /// CAS from `running` to `suspended` with the typed cause, refusing to
    /// commit over a durable cancellation intent (the `WHERE` requires
    /// `cancel_requested_at IS NULL`) so a worker whose suspend returns zero
    /// rows performs the cancel transaction at that boundary instead.
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
    /// clearing any suspension payload and wake instant. Returns the affected
    /// row count (zero is the CAS miss).
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

    /// Durably records a cancellation intent against an existing thread,
    /// returning [`DbError::NotFound`] for an unknown id. The idempotent,
    /// seq-free write touches columns no record commit touches, so it can
    /// never be lost to a racing commit.
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
    /// Returns [`DbError::QueryFailed`] on database errors, including a vanished
    /// row.
    async fn increment_recovery_attempts(
        &self,
        conn: &mut PgConnection,
        id: AgentThreadId,
    ) -> Result<u32, DbError>;

    /// Finds suspended stage-driven threads whose wake instant has elapsed and
    /// that carry no cancellation intent, the availability sweep's timer
    /// predicate. `SKIP LOCKED` sheds rows a rival scan holds.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_due_timer_wakes(
        &self,
        conn: &mut PgConnection,
        limit: u32,
    ) -> Result<Vec<AgentThread>, DbError>;

    /// Finds live threads carrying a durable cancellation intent, the sweep's
    /// cancel-fallback predicate. Rows are locked with `SKIP LOCKED`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_cancel_intents(
        &self,
        conn: &mut PgConnection,
        limit: u32,
    ) -> Result<Vec<AgentThread>, DbError>;

    /// Finds suspended relation threads of still-relating jobs whose resolver
    /// has died. Rows are locked with `SKIP LOCKED`.
    ///
    /// `wake_at IS NULL` spares a budget-suspended thread (which carries a wake
    /// instant for its own recheck), and a live descendant child thread or
    /// driver task spares a healthy verifier-suspended thread the driver loop
    /// is still re-driving.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_stuck_relating_threads(
        &self,
        conn: &mut PgConnection,
        limit: u32,
    ) -> Result<Vec<AgentThread>, DbError>;

    /// Records cancellation intents on a terminal parent's live direct
    /// children, the post-commit fast path of the cancellation cascade. One
    /// level is exhaustive because a verifier binding is flat (a one-shot
    /// launches no child of its own), and the orphan janitor heals any deeper
    /// tree a nested delegation could add. Only non-terminal children without
    /// an intent are touched. Returns the count written.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn cascade_cancel_to_children(
        &self,
        conn: &mut PgConnection,
        parent_id: AgentThreadId,
        requested_by: &str,
    ) -> Result<u64, DbError>;

    /// Records cancellation intents on non-terminal threads whose only resolver
    /// has gone terminal: the parent (the convergent half of the cancellation
    /// cascade, healing a crash between a parent's terminal commit and its
    /// fast-path cascade) or the thread's own driver task. Rows are locked with
    /// `SKIP LOCKED`. Returns the count written.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn cascade_cancel_to_orphans(
        &self,
        conn: &mut PgConnection,
        limit: u32,
        requested_by: &str,
    ) -> Result<u64, DbError>;

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

    /// Counts the prune candidates a pass would refuse: terminal,
    /// criteria-matching roots holding a live descendant, or any descendant at
    /// all without the cascade flag.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn count_refused_prune_roots(
        &self,
        conn: &mut PgConnection,
        criteria: &ThreadPruneCriteria,
    ) -> Result<u64, DbError>;

    /// Deletes the prunable threads and their records in one statement:
    /// terminal, criteria-matching roots without refused descendants,
    /// plus (cascading) the terminal descendants of accepted roots.
    /// Spend rows survive with their thread references nulled. Returns
    /// the deleted thread count.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn prune_threads(
        &self,
        conn: &mut PgConnection,
        criteria: &ThreadPruneCriteria,
    ) -> Result<u64, DbError>;
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
              driver_task_id, principal_id, job_id, format_version) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             RETURNING {COLUMNS}"
        );
        let row = sqlx::query(&sql)
            .bind(new.parent_thread_id.map(|id| id.inner().to_owned()))
            .bind(new.stage.as_str())
            .bind(new.binding_version_id.map(|id| id.inner().to_owned()))
            .bind(stage_task_id)
            .bind(driver_task_id)
            .bind(new.principal_id.map(|id| id.inner().to_owned()))
            .bind(new.job_id.map(|id| id.inner().to_owned()))
            .bind(i32::try_from(new.format_version).expect(FORMAT_VERSION_OVERFLOW))
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| {
                try_into_unique_violation(&e).unwrap_or_else(|| DbError::QueryFailed {
                    context: format!("creating {} thread", new.stage),
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

    async fn find_by_stage_task_id(
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

    async fn find_by_job_id(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Vec<AgentThread>, DbError> {
        let sql = format!(
            "SELECT {} FROM agent_threads t \
             JOIN tasks ON tasks.id = t.stage_task_id \
             WHERE tasks.job_id = $1 \
             ORDER BY t.created_at, t.id",
            COLUMNS.qualified("t"),
        );
        let rows = sqlx::query(&sql)
            .bind(job_id.inner())
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("listing the threads of job {job_id}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_agent_thread_row).collect())
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
        let result = sqlx::query(
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

        // A cancel against an unknown id must not report success.
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound {
                entity: "agent_thread",
                id: id.to_string(),
            });
        }

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

    async fn find_due_timer_wakes(
        &self,
        conn: &mut PgConnection,
        limit: u32,
    ) -> Result<Vec<AgentThread>, DbError> {
        // An intent-carrying thread belongs to the cancel predicate: waking it
        // would burn a turn the cancellation immediately discards. The scan is
        // stage-only because the resolve transaction re-queues a stage task,
        // and waking a driver-driven thread without its driver half would
        // strand it running.
        let sql = format!(
            "SELECT {COLUMNS} FROM agent_threads \
             WHERE status = 'suspended' AND wake_at IS NOT NULL AND wake_at <= now() \
               AND cancel_requested_at IS NULL \
               AND stage_task_id IS NOT NULL \
             ORDER BY wake_at \
             LIMIT $1 \
             FOR UPDATE SKIP LOCKED"
        );
        let rows = sqlx::query(&sql)
            .bind(i64::from(limit))
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "scanning for due timer wakes".to_owned(),
                source: e,
            })?;

        Ok(rows.iter().map(map_agent_thread_row).collect())
    }

    async fn find_cancel_intents(
        &self,
        conn: &mut PgConnection,
        limit: u32,
    ) -> Result<Vec<AgentThread>, DbError> {
        // The terminal set is interpolated as literals rather than bound so the
        // planner can match the partial cancel-intent index's literal predicate.
        let terminal = terminal_status_literals();
        let sql = format!(
            "SELECT {COLUMNS} FROM agent_threads \
             WHERE cancel_requested_at IS NOT NULL \
               AND status NOT IN ({terminal}) \
             ORDER BY cancel_requested_at \
             LIMIT $1 \
             FOR UPDATE SKIP LOCKED",
            terminal = terminal.join(", "),
        );
        let rows = sqlx::query(&sql)
            .bind(i64::from(limit))
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "scanning for pending cancel intents".to_owned(),
                source: e,
            })?;

        Ok(rows.iter().map(map_agent_thread_row).collect())
    }

    async fn find_stuck_relating_threads(
        &self,
        conn: &mut PgConnection,
        limit: u32,
    ) -> Result<Vec<AgentThread>, DbError> {
        // The job-relating check rides the driving task because the thread row
        // carries no job id.
        let child_live = thread_is_live("child");
        let descendant_driver_live = driver_task_is_live("dt");
        let sql = format!(
            "SELECT {COLUMNS} FROM agent_threads \
             WHERE pipeline_stage = 'relation' \
               AND status = 'suspended' \
               AND cancel_requested_at IS NULL \
               AND wake_at IS NULL \
               AND stage_task_id IN ( \
                   SELECT t.id FROM tasks t \
                   JOIN jobs j ON j.id = t.job_id \
                   WHERE j.status = 'relating' \
               ) \
               AND NOT EXISTS ( \
                   SELECT 1 FROM agent_threads child \
                   WHERE child.parent_thread_id = agent_threads.id \
                     AND {child_live} \
               ) \
               AND NOT EXISTS ( \
                   SELECT 1 FROM agent_driver_tasks dt \
                   JOIN agent_threads child ON child.driver_task_id = dt.id \
                   WHERE child.parent_thread_id = agent_threads.id \
                     AND {descendant_driver_live} \
               ) \
             ORDER BY updated_at \
             LIMIT $1 \
             FOR UPDATE SKIP LOCKED",
        );
        let rows = sqlx::query(&sql)
            .bind(i64::from(limit))
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "scanning for stranded relation threads".to_owned(),
                source: e,
            })?;

        Ok(rows.iter().map(map_agent_thread_row).collect())
    }

    async fn cascade_cancel_to_children(
        &self,
        conn: &mut PgConnection,
        parent_id: AgentThreadId,
        requested_by: &str,
    ) -> Result<u64, DbError> {
        let terminal = terminal_status_literals();
        let sql = format!(
            "UPDATE agent_threads \
             SET cancel_requested_at = now(), cancel_requested_by = $2, updated_at = now() \
             WHERE parent_thread_id = $1 \
               AND cancel_requested_at IS NULL \
               AND status NOT IN ({terminal})",
            terminal = terminal.join(", "),
        );
        let result = sqlx::query(&sql)
            .bind(parent_id.inner())
            .bind(requested_by)
            .execute(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("cascading cancellation to children of thread {parent_id}"),
                source: e,
            })?;

        Ok(result.rows_affected())
    }

    async fn cascade_cancel_to_orphans(
        &self,
        conn: &mut PgConnection,
        limit: u32,
        requested_by: &str,
    ) -> Result<u64, DbError> {
        // A live thread is orphaned when its only resolver (its parent or its
        // own driver task) has gone terminal, so no live actor will resolve it
        // and the cancel fallback disposes the written intent. The orphan test
        // is the negation of the liveness predicate the stuck-relating sweep
        // composes, so the two share one definition of "live". The CTE locks
        // only the child rows it writes under `SKIP LOCKED`.
        let child_live = thread_is_live("child");
        let parent_live = thread_is_live("parent");
        let driver_live = driver_task_is_live("dt");
        let sql = format!(
            "WITH orphan AS ( \
                 SELECT child.id FROM agent_threads child \
                 LEFT JOIN agent_threads parent ON parent.id = child.parent_thread_id \
                 LEFT JOIN agent_driver_tasks dt ON dt.id = child.driver_task_id \
                 WHERE child.cancel_requested_at IS NULL \
                   AND {child_live} \
                   AND (NOT ({parent_live}) OR NOT ({driver_live})) \
                 ORDER BY child.updated_at \
                 LIMIT $1 \
                 FOR UPDATE OF child SKIP LOCKED \
             ) \
             UPDATE agent_threads c \
             SET cancel_requested_at = now(), cancel_requested_by = $2, updated_at = now() \
             FROM orphan \
             WHERE c.id = orphan.id"
        );
        let result = sqlx::query(&sql)
            .bind(i64::from(limit))
            .bind(requested_by)
            .execute(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "cascading cancellation to orphaned descendants".to_owned(),
                source: e,
            })?;

        Ok(result.rows_affected())
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

    async fn count_refused_prune_roots(
        &self,
        conn: &mut PgConnection,
        criteria: &ThreadPruneCriteria,
    ) -> Result<u64, DbError> {
        let stage = criteria.stage.map(|stage| stage.as_str().to_owned());
        let count: i64 = sqlx::query_scalar(
            "WITH RECURSIVE candidates AS ( \
                 SELECT id FROM agent_threads \
                 WHERE completed_at IS NOT NULL AND completed_at < $1 \
                   AND ($2::text IS NULL OR pipeline_stage = $2) \
             ), \
             descendants AS ( \
                 SELECT c.id AS root, t.id, t.completed_at \
                 FROM agent_threads t JOIN candidates c ON t.parent_thread_id = c.id \
                 UNION ALL \
                 SELECT d.root, t.id, t.completed_at \
                 FROM agent_threads t JOIN descendants d ON t.parent_thread_id = d.id \
             ) \
             SELECT COUNT(DISTINCT root) FROM descendants \
             WHERE completed_at IS NULL OR NOT $3",
        )
        .bind(criteria.completed_before)
        .bind(stage)
        .bind(criteria.cascade)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "counting refused prune roots".to_owned(),
            source: e,
        })?;

        Ok(u64::try_from(count).unwrap_or_default())
    }

    async fn prune_threads(
        &self,
        conn: &mut PgConnection,
        criteria: &ThreadPruneCriteria,
    ) -> Result<u64, DbError> {
        let stage = criteria.stage.map(|stage| stage.as_str().to_owned());
        // One statement so the self-referential parent FK is checked at
        // statement end and a whole terminal subtree deletes together.
        let result = sqlx::query(
            "WITH RECURSIVE candidates AS ( \
                 SELECT id FROM agent_threads \
                 WHERE completed_at IS NOT NULL AND completed_at < $1 \
                   AND ($2::text IS NULL OR pipeline_stage = $2) \
             ), \
             descendants AS ( \
                 SELECT c.id AS root, t.id, t.completed_at \
                 FROM agent_threads t JOIN candidates c ON t.parent_thread_id = c.id \
                 UNION ALL \
                 SELECT d.root, t.id, t.completed_at \
                 FROM agent_threads t JOIN descendants d ON t.parent_thread_id = d.id \
             ), \
             refused AS ( \
                 SELECT DISTINCT root FROM descendants \
                 WHERE completed_at IS NULL OR NOT $3 \
             ), \
             prunable AS ( \
                 SELECT id FROM candidates \
                 WHERE id NOT IN (SELECT root FROM refused) \
                 UNION \
                 SELECT d.id FROM descendants d \
                 WHERE $3 \
                   AND d.root NOT IN (SELECT root FROM refused) \
                   AND d.completed_at IS NOT NULL \
             ), \
             deleted_records AS ( \
                 DELETE FROM agent_thread_records \
                 WHERE thread_id IN (SELECT id FROM prunable) \
             ) \
             DELETE FROM agent_threads WHERE id IN (SELECT id FROM prunable)",
        )
        .bind(criteria.completed_before)
        .bind(stage)
        .bind(criteria.cascade)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "pruning terminal threads".to_owned(),
            source: e,
        })?;

        Ok(result.rows_affected())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// The quoted SQL literals for the terminal thread statuses, derived from
/// `ALL` so a new status joins the set by construction. Interpolated, not
/// bound, so the planner can match a partial index's literal predicate.
fn terminal_status_literals() -> Vec<String> {
    AgentThreadStatus::ALL
        .iter()
        .filter(|status| status.is_terminal())
        .map(|status| format!("'{}'", status.as_str()))
        .collect()
}

/// SQL predicate: the aliased `agent_threads` row holds a live (non-terminal)
/// status. The single definition of thread liveness the stuck-relating sweep
/// and the orphan cascade both compose, so the two cannot drift on what "live"
/// means.
fn thread_is_live(alias: &str) -> String {
    format!(
        "{alias}.status NOT IN ({})",
        terminal_status_literals().join(", ")
    )
}

/// SQL predicate: the aliased `agent_driver_tasks` row holds a live
/// (non-terminal) state, the driver-task companion to [`thread_is_live`].
fn driver_task_is_live(alias: &str) -> String {
    format!(
        "{alias}.state NOT IN ({})",
        terminal_driver_state_literals().join(", ")
    )
}

fn map_agent_thread_row(r: &sqlx::postgres::PgRow) -> AgentThread {
    AgentThread::builder()
        .id(AgentThreadId::from(r.get::<uuid::Uuid, _>("id")))
        .parent_thread_id(
            r.get::<Option<uuid::Uuid>, _>("parent_thread_id")
                .map(AgentThreadId::from),
        )
        .stage(
            r.get::<String, _>("pipeline_stage")
                .parse::<AgentThreadStage>()
                .expect(UNKNOWN_STAGE_IN_DB),
        )
        .binding_version_id(
            r.get::<Option<uuid::Uuid>, _>("binding_version_id")
                .map(AgentBindingVersionId::from),
        )
        .stage_task_id(
            r.get::<Option<uuid::Uuid>, _>("stage_task_id")
                .map(TaskId::from),
        )
        .driver_task_id(
            r.get::<Option<uuid::Uuid>, _>("driver_task_id")
                .map(AgentDriverTaskId::from),
        )
        .principal_id(
            r.get::<Option<uuid::Uuid>, _>("principal_id")
                .map(PrincipalId::from),
        )
        .job_id(r.get::<Option<uuid::Uuid>, _>("job_id").map(JobId::from))
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
