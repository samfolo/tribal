//! Job repository: trait definition and Postgres implementation.
//!
//! Jobs track ingest pipeline runs.  The repository provides insert,
//! lookup, status transition, batch sizing, and batch commit operations.
//! All mutations use `RETURNING {COLUMNS}` to produce the updated domain
//! type atomically.
//!
//! Uses raw `sqlx::query()` because job status transitions bind and
//! parse domain enums as TEXT, and the compile-time macro cannot
//! type-check these casts.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{
    EpisodeId, Job, JobId, JobOutcome, JobStatus, PrincipalId, ProjectId, PromptVersionId,
    RelationBatchId,
};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "correlation_id",
    "project_id",
    "principal_id",
    "actor_id",
    "status",
    "outcome",
    "batch_size",
    "committed_batch_id",
    "source_context",
    "raw_input",
    "extraction_original_count",
    "error_message",
    "extraction_system_prompt_version_id",
    "extraction_user_prompt_version_id",
    "triage_system_prompt_version_id",
    "triage_user_prompt_version_id",
    "relation_system_prompt_version_id",
    "relation_user_prompt_version_id",
    "trace_context",
    "completed_at",
    "created_at",
    "updated_at",
]);

const UNKNOWN_JOB_STATUS_IN_DB: &str = "unrecognised job status in database — schema mismatch";
const UNKNOWN_JOB_OUTCOME_IN_DB: &str = "unrecognised job outcome in database — schema mismatch";
const BATCH_SIZE_EXCEEDS_I32: &str = "batch_size exceeds i32::MAX";
const EXTRACTION_ORIGINAL_COUNT_EXCEEDS_I32: &str = "extraction_original_count exceeds i32::MAX";
const BATCH_SIZE_OVERFLOW: &str = "negative batch_size in database — data corruption";
const EXTRACTION_ORIGINAL_COUNT_OVERFLOW: &str =
    "negative extraction_original_count in database — data corruption";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new job.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `status`, `created_at`, `updated_at`) are produced by Postgres
/// via `DEFAULT` clauses and returned via `RETURNING {COLUMNS}`.
#[derive(Debug, TypedBuilder)]
pub struct NewJob {
    /// Episode grouping (correlation key).
    #[builder(default)]
    pub correlation_id: Option<EpisodeId>,
    /// The project this job belongs to.
    pub project_id: ProjectId,
    /// The principal who initiated this job.
    pub principal_id: PrincipalId,
    /// The immediate caller, if operating on behalf of another principal.
    #[builder(default)]
    pub actor_id: Option<PrincipalId>,
    /// Source context (opaque JSONB).
    pub source_context: serde_json::Value,
    /// Verbatim text from ingestion; primary input to extraction.
    pub raw_input: String,
    /// Extraction system prompt version at job creation time.
    pub extraction_system_prompt_version_id: PromptVersionId,
    /// Extraction user prompt version at job creation time.
    pub extraction_user_prompt_version_id: PromptVersionId,
    /// Triage system prompt version at job creation time.
    pub triage_system_prompt_version_id: PromptVersionId,
    /// Triage user prompt version at job creation time.
    pub triage_user_prompt_version_id: PromptVersionId,
    /// Relation system prompt version at job creation time.
    pub relation_system_prompt_version_id: PromptVersionId,
    /// Relation user prompt version at job creation time.
    pub relation_user_prompt_version_id: PromptVersionId,
    /// W3C traceparent for distributed tracing.
    #[builder(default)]
    pub trace_context: Option<String>,
}

/// Input for transitioning a job's status.
///
/// Callers supply the new status and, for terminal states, the outcome
/// and optional error details.  The repository applies the transition
/// atomically and returns the updated job.
#[derive(Debug, TypedBuilder)]
pub struct JobStatusTransition {
    /// The new lifecycle status.
    pub status: JobStatus,
    /// The outcome — required for terminal states.
    #[builder(default)]
    pub outcome: Option<JobOutcome>,
    /// Error message — required for `Failed` status.
    #[builder(default)]
    pub error_message: Option<String>,
    /// Completion timestamp — set for terminal transitions.
    #[builder(default)]
    pub completed_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for jobs.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait JobRepository {
    /// Inserts a new job and returns the fully populated domain type.
    ///
    /// The job is created with `Queued` status and no outcome.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn insert(&self, conn: &mut PgConnection, new_job: &NewJob) -> Result<Job, DbError>;

    /// Finds a job by its ID.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no job with the given ID exists.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(&self, conn: &mut PgConnection, id: JobId) -> Result<Job, DbError>;

    /// Finds all jobs for a project, ordered by `created_at` descending
    /// with `id` as tiebreaker.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_project_id(
        &self,
        conn: &mut PgConnection,
        project_id: ProjectId,
    ) -> Result<Vec<Job>, DbError>;

    /// Transitions a job's status and returns the updated job.
    ///
    /// Sets `updated_at` to `now()` atomically.  Terminal transitions
    /// must supply appropriate outcome and `error_message` fields to
    /// satisfy database CHECK constraints.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no job with the given ID exists.
    /// Returns [`DbError::QueryFailed`] on database errors (including
    /// CHECK constraint violations for invalid transitions).
    async fn update_status(
        &self,
        conn: &mut PgConnection,
        id: JobId,
        transition: &JobStatusTransition,
    ) -> Result<Job, DbError>;

    /// Sets the batch size and extraction original count, returning the
    /// updated job.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no job with the given ID exists.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn update_batch_size(
        &self,
        conn: &mut PgConnection,
        id: JobId,
        batch_size: u32,
        extraction_original_count: u32,
    ) -> Result<Job, DbError>;

    /// Conditionally sets the committed batch ID.
    ///
    /// The update only takes effect when `committed_batch_id` is currently
    /// `NULL`. Returns `Some(job)` on success, or `None` if the batch ID
    /// was already set (idempotency hit).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no job with the given ID exists.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn set_committed_batch_id(
        &self,
        conn: &mut PgConnection,
        id: JobId,
        batch_id: RelationBatchId,
    ) -> Result<Option<Job>, DbError>;

    /// Transitions jobs to `Failed` when they have dead-lettered
    /// extraction or relation tasks, and the job is not already
    /// terminal.  Returns the IDs of the transitioned jobs.
    ///
    /// Idempotent — already-failed or completed jobs are skipped.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn fail_stale_dead_lettered_jobs(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Vec<JobId>, DbError>;

    /// Detects jobs stuck in `Triaging` where at least one triage task
    /// exists, all triage tasks have reached terminal state, and no
    /// relation task exists.
    ///
    /// Jobs with zero triage tasks are excluded — these represent data
    /// integrity issues rather than missed fan-ins.
    ///
    /// Returns the IDs of stuck jobs.  The caller is responsible for
    /// creating relation tasks and transitioning job status.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_stuck_triaging_jobs(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Vec<JobId>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`JobRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgJobRepository;

#[async_trait]
impl JobRepository for PgJobRepository {
    async fn insert(&self, conn: &mut PgConnection, new_job: &NewJob) -> Result<Job, DbError> {
        let sql = format!(
            "INSERT INTO jobs \
                 (correlation_id, project_id, principal_id, actor_id, \
                  source_context, raw_input, \
                  extraction_system_prompt_version_id, extraction_user_prompt_version_id, \
                  triage_system_prompt_version_id, triage_user_prompt_version_id, \
                  relation_system_prompt_version_id, relation_user_prompt_version_id, \
                  trace_context) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(new_job.correlation_id.map(|id| *id.inner()))
            .bind(new_job.project_id.inner())
            .bind(new_job.principal_id.inner())
            .bind(new_job.actor_id.map(|id| *id.inner()))
            .bind(&new_job.source_context)
            .bind(&new_job.raw_input)
            .bind(new_job.extraction_system_prompt_version_id.inner())
            .bind(new_job.extraction_user_prompt_version_id.inner())
            .bind(new_job.triage_system_prompt_version_id.inner())
            .bind(new_job.triage_user_prompt_version_id.inner())
            .bind(new_job.relation_system_prompt_version_id.inner())
            .bind(new_job.relation_user_prompt_version_id.inner())
            .bind(&new_job.trace_context)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "inserting job".to_owned(),
                source: e,
            })?;

        Ok(map_job_row(&row))
    }

    async fn find_by_id(&self, conn: &mut PgConnection, id: JobId) -> Result<Job, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM jobs WHERE id = $1");

        let row = sqlx::query(&sql)
            .bind(id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding job by id {id}"),
                source: e,
            })?
            .ok_or_else(|| DbError::NotFound {
                entity: "job",
                id: id.to_string(),
            })?;

        Ok(map_job_row(&row))
    }

    async fn find_by_project_id(
        &self,
        conn: &mut PgConnection,
        project_id: ProjectId,
    ) -> Result<Vec<Job>, DbError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM jobs WHERE project_id = $1 \
             ORDER BY created_at DESC, id DESC",
        );

        let rows = sqlx::query(&sql)
            .bind(project_id.inner())
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding jobs for project {project_id}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_job_row).collect())
    }

    async fn update_status(
        &self,
        conn: &mut PgConnection,
        id: JobId,
        transition: &JobStatusTransition,
    ) -> Result<Job, DbError> {
        let sql = format!(
            "UPDATE jobs \
             SET status = $2, outcome = $3, error_message = $4, \
                 completed_at = $5, updated_at = now() \
             WHERE id = $1 \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(id.inner())
            .bind(transition.status.as_str())
            .bind(transition.outcome.map(|o| o.as_str().to_owned()))
            .bind(&transition.error_message)
            .bind(transition.completed_at)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("updating job status for {id}"),
                source: e,
            })?
            .ok_or_else(|| DbError::NotFound {
                entity: "job",
                id: id.to_string(),
            })?;

        Ok(map_job_row(&row))
    }

    async fn update_batch_size(
        &self,
        conn: &mut PgConnection,
        id: JobId,
        batch_size: u32,
        extraction_original_count: u32,
    ) -> Result<Job, DbError> {
        let batch_size_i32 = i32::try_from(batch_size).expect(BATCH_SIZE_EXCEEDS_I32);
        let original_count_i32 =
            i32::try_from(extraction_original_count).expect(EXTRACTION_ORIGINAL_COUNT_EXCEEDS_I32);

        let sql = format!(
            "UPDATE jobs \
             SET batch_size = $2, extraction_original_count = $3, \
                 updated_at = now() \
             WHERE id = $1 \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(id.inner())
            .bind(batch_size_i32)
            .bind(original_count_i32)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("updating batch size for job {id}"),
                source: e,
            })?
            .ok_or_else(|| DbError::NotFound {
                entity: "job",
                id: id.to_string(),
            })?;

        Ok(map_job_row(&row))
    }

    async fn set_committed_batch_id(
        &self,
        conn: &mut PgConnection,
        id: JobId,
        batch_id: RelationBatchId,
    ) -> Result<Option<Job>, DbError> {
        let sql = format!(
            "UPDATE jobs \
             SET committed_batch_id = $2, updated_at = now() \
             WHERE id = $1 AND committed_batch_id IS NULL \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(id.inner())
            .bind(batch_id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("setting committed batch id for job {id}"),
                source: e,
            })?;

        if let Some(r) = row {
            return Ok(Some(map_job_row(&r)));
        }

        // Zero rows updated — distinguish "already set" from "not found".
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)")
            .bind(id.inner())
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("checking job existence for {id}"),
                source: e,
            })?;

        if exists {
            Ok(None)
        } else {
            Err(DbError::NotFound {
                entity: "job",
                id: id.to_string(),
            })
        }
    }

    async fn fail_stale_dead_lettered_jobs(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Vec<JobId>, DbError> {
        let rows = sqlx::query(
            "UPDATE jobs \
             SET status = 'failed', \
                 outcome = 'failure', \
                 error_message = 'task dead-lettered during reclaim', \
                 completed_at = now(), \
                 updated_at = now() \
             WHERE id IN ( \
                 SELECT DISTINCT t.job_id FROM tasks t \
                 WHERE t.status = 'dead_letter' \
                 AND t.task_type IN ('extraction', 'relation') \
             ) \
             AND status NOT IN ('failed', 'completed') \
             RETURNING id",
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "failing jobs with dead-lettered tasks".to_owned(),
            source: e,
        })?;

        Ok(rows
            .iter()
            .map(|r| JobId::from(r.get::<uuid::Uuid, _>("id")))
            .collect())
    }

    async fn find_stuck_triaging_jobs(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Vec<JobId>, DbError> {
        let rows = sqlx::query(
            "SELECT j.id FROM jobs j \
             WHERE j.status = 'triaging' \
               AND EXISTS ( \
                   SELECT 1 FROM tasks t \
                   WHERE t.job_id = j.id \
                     AND t.task_type = 'triage' \
               ) \
               AND NOT EXISTS ( \
                   SELECT 1 FROM tasks t \
                   WHERE t.job_id = j.id \
                     AND t.task_type = 'triage' \
                     AND t.status NOT IN ('completed', 'dead_letter') \
               ) \
               AND NOT EXISTS ( \
                   SELECT 1 FROM tasks t \
                   WHERE t.job_id = j.id \
                     AND t.task_type = 'relation' \
               )",
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "finding stuck triaging jobs".to_owned(),
            source: e,
        })?;

        Ok(rows
            .iter()
            .map(|r| JobId::from(r.get::<uuid::Uuid, _>("id")))
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from a job query into a [`Job`].
fn map_job_row(r: &sqlx::postgres::PgRow) -> Job {
    Job::builder()
        .id(JobId::from(r.get::<uuid::Uuid, _>("id")))
        .correlation_id(
            r.get::<Option<uuid::Uuid>, _>("correlation_id")
                .map(EpisodeId::from),
        )
        .project_id(ProjectId::from(r.get::<uuid::Uuid, _>("project_id")))
        .principal_id(PrincipalId::from(r.get::<uuid::Uuid, _>("principal_id")))
        .actor_id(
            r.get::<Option<uuid::Uuid>, _>("actor_id")
                .map(PrincipalId::from),
        )
        .status(
            r.get::<String, _>("status")
                .parse::<JobStatus>()
                .expect(UNKNOWN_JOB_STATUS_IN_DB),
        )
        .outcome(
            r.get::<Option<String>, _>("outcome")
                .map(|s| s.parse::<JobOutcome>().expect(UNKNOWN_JOB_OUTCOME_IN_DB)),
        )
        .batch_size(
            r.get::<Option<i32>, _>("batch_size")
                .map(|v| u32::try_from(v).expect(BATCH_SIZE_OVERFLOW)),
        )
        .committed_batch_id(
            r.get::<Option<uuid::Uuid>, _>("committed_batch_id")
                .map(RelationBatchId::from),
        )
        .source_context(r.get("source_context"))
        .raw_input(r.get("raw_input"))
        .extraction_original_count(
            r.get::<Option<i32>, _>("extraction_original_count")
                .map(|v| u32::try_from(v).expect(EXTRACTION_ORIGINAL_COUNT_OVERFLOW)),
        )
        .error_message(r.get("error_message"))
        .extraction_system_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("extraction_system_prompt_version_id"),
        ))
        .extraction_user_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("extraction_user_prompt_version_id"),
        ))
        .triage_system_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("triage_system_prompt_version_id"),
        ))
        .triage_user_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("triage_user_prompt_version_id"),
        ))
        .relation_system_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("relation_system_prompt_version_id"),
        ))
        .relation_user_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("relation_user_prompt_version_id"),
        ))
        .trace_context(r.get("trace_context"))
        .completed_at(r.get("completed_at"))
        .created_at(r.get("created_at"))
        .updated_at(r.get("updated_at"))
        .build()
}

// ---------------------------------------------------------------------------
// Test helpers (feature-gated)
// ---------------------------------------------------------------------------

/// Test-only overrides for job fields that the production `insert()`
/// does not expose.
///
/// `Default` produces a queued job with no extras.  Use `TypedBuilder`
/// for specific overrides:
///
/// ```ignore
/// JobStateOverride::builder()
///     .status(JobStatus::Completed)
///     .outcome(Some(JobOutcome::Success))
///     .committed_batch_id(Some(batch_id))
///     .build()
/// ```
#[cfg(feature = "test-helpers")]
#[derive(Debug, Clone, TypedBuilder)]
pub struct JobStateOverride {
    #[builder(default = JobStatus::Queued)]
    pub status: JobStatus,
    #[builder(default)]
    pub outcome: Option<JobOutcome>,
    #[builder(default)]
    pub committed_batch_id: Option<RelationBatchId>,
    #[builder(default)]
    pub error_message: Option<String>,
}

#[cfg(feature = "test-helpers")]
impl Default for JobStateOverride {
    fn default() -> Self {
        Self {
            status: JobStatus::Queued,
            outcome: None,
            committed_batch_id: None,
            error_message: None,
        }
    }
}

#[cfg(feature = "test-helpers")]
impl PgJobRepository {
    /// Inserts a job with caller-controlled status, outcome, batch ID,
    /// and error message — fields the production `insert()` does not
    /// expose.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    pub async fn insert_for_test(
        &self,
        conn: &mut PgConnection,
        new_job: &NewJob,
        overrides: &JobStateOverride,
    ) -> Result<Job, DbError> {
        let sql = format!(
            "INSERT INTO jobs \
                 (correlation_id, project_id, principal_id, actor_id, \
                  source_context, raw_input, \
                  extraction_system_prompt_version_id, extraction_user_prompt_version_id, \
                  triage_system_prompt_version_id, triage_user_prompt_version_id, \
                  relation_system_prompt_version_id, relation_user_prompt_version_id, \
                  trace_context, status, outcome, committed_batch_id, \
                  error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(new_job.correlation_id.map(|id| *id.inner()))
            .bind(new_job.project_id.inner())
            .bind(new_job.principal_id.inner())
            .bind(new_job.actor_id.map(|id| *id.inner()))
            .bind(&new_job.source_context)
            .bind(&new_job.raw_input)
            .bind(new_job.extraction_system_prompt_version_id.inner())
            .bind(new_job.extraction_user_prompt_version_id.inner())
            .bind(new_job.triage_system_prompt_version_id.inner())
            .bind(new_job.triage_user_prompt_version_id.inner())
            .bind(new_job.relation_system_prompt_version_id.inner())
            .bind(new_job.relation_user_prompt_version_id.inner())
            .bind(&new_job.trace_context)
            .bind(overrides.status.as_str())
            .bind(overrides.outcome.map(|o| o.as_str().to_owned()))
            .bind(overrides.committed_batch_id.map(|id| *id.inner()))
            .bind(&overrides.error_message)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "inserting job (test helper)".to_owned(),
                source: e,
            })?;

        Ok(map_job_row(&row))
    }
}
