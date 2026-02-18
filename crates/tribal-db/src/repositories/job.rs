//! Job repository: trait definition and Postgres implementation.
//!
//! Jobs track ingest pipeline runs.  The repository provides insert,
//! lookup, status transition, batch sizing, and batch commit operations.
//! All mutations use `RETURNING *` to produce the updated domain type
//! atomically.
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

use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

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
/// via `DEFAULT` clauses and returned via `RETURNING *`.
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
    /// Extraction prompt version at job creation time.
    pub extraction_prompt_version_id: PromptVersionId,
    /// Triage prompt version at job creation time.
    pub triage_prompt_version_id: PromptVersionId,
    /// Relation prompt version at job creation time.
    pub relation_prompt_version_id: PromptVersionId,
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

    /// Sets the committed batch ID, returning the updated job.
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
    ) -> Result<Job, DbError>;
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
        let row = sqlx::query(
            "INSERT INTO jobs \
                 (correlation_id, project_id, principal_id, actor_id, \
                  source_context, extraction_prompt_version_id, \
                  triage_prompt_version_id, relation_prompt_version_id, \
                  trace_context) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             RETURNING *",
        )
        .bind(new_job.correlation_id.map(|id| *id.inner()))
        .bind(new_job.project_id.inner())
        .bind(new_job.principal_id.inner())
        .bind(new_job.actor_id.map(|id| *id.inner()))
        .bind(&new_job.source_context)
        .bind(new_job.extraction_prompt_version_id.inner())
        .bind(new_job.triage_prompt_version_id.inner())
        .bind(new_job.relation_prompt_version_id.inner())
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
        let row = sqlx::query("SELECT * FROM jobs WHERE id = $1")
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
        let rows = sqlx::query(
            "SELECT * FROM jobs WHERE project_id = $1 ORDER BY created_at DESC, id DESC",
        )
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
        let row = sqlx::query(
            "UPDATE jobs \
             SET status = $2, outcome = $3, error_message = $4, \
                 completed_at = $5, updated_at = now() \
             WHERE id = $1 \
             RETURNING *",
        )
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

        let row = sqlx::query(
            "UPDATE jobs \
             SET batch_size = $2, extraction_original_count = $3, \
                 updated_at = now() \
             WHERE id = $1 \
             RETURNING *",
        )
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
    ) -> Result<Job, DbError> {
        let row = sqlx::query(
            "UPDATE jobs \
             SET committed_batch_id = $2, updated_at = now() \
             WHERE id = $1 \
             RETURNING *",
        )
        .bind(id.inner())
        .bind(batch_id.inner())
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("setting committed batch id for job {id}"),
            source: e,
        })?
        .ok_or_else(|| DbError::NotFound {
            entity: "job",
            id: id.to_string(),
        })?;

        Ok(map_job_row(&row))
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
        .extraction_original_count(
            r.get::<Option<i32>, _>("extraction_original_count")
                .map(|v| u32::try_from(v).expect(EXTRACTION_ORIGINAL_COUNT_OVERFLOW)),
        )
        .error_message(r.get("error_message"))
        .extraction_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("extraction_prompt_version_id"),
        ))
        .triage_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("triage_prompt_version_id"),
        ))
        .relation_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("relation_prompt_version_id"),
        ))
        .trace_context(r.get("trace_context"))
        .completed_at(r.get("completed_at"))
        .created_at(r.get("created_at"))
        .updated_at(r.get("updated_at"))
        .build()
}
