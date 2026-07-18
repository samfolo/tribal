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
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{
    EpisodeId, ExtractionCommitOutcome, InferenceIdentity, Job, JobId, JobOutcome, JobStatus,
    PrincipalId, ProjectId, PromptVersionId, RelationBatchId, SourceContextV1, TaskStatus,
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
    "ingest_idempotency_key",
    "extraction_original_count",
    "error_message",
    "extraction_system_prompt_version_id",
    "extraction_user_prompt_version_id",
    "triage_system_prompt_version_id",
    "triage_user_prompt_version_id",
    "relation_system_prompt_version_id",
    "relation_user_prompt_version_id",
    "system_fingerprint_hash",
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
#[derive(Debug, Clone, TypedBuilder)]
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
    /// Producer-supplied key converging retries of one logical ingest.
    #[builder(default)]
    pub ingest_idempotency_key: Option<uuid::Uuid>,
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
    /// SHA-256 hash referencing the active system fingerprint.
    pub system_fingerprint_hash: String,
    /// W3C traceparent for distributed tracing.
    #[builder(default)]
    pub trace_context: Option<String>,
}

/// Input for transitioning a job's status.
///
/// Callers supply the new status and, for terminal states, the outcome
/// and optional error details.  The repository applies the transition
/// atomically and returns the updated job.
#[derive(Debug, Clone, TypedBuilder)]
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

    /// Applies a status transition to a live job; a terminal job is a
    /// silent no-op. Returns `Ok(None)` for the no-op — zero rows is
    /// success, distinguishable from job-not-found — so a late terminal
    /// commit against a completed job commits its transaction instead of
    /// erroring. Invalid transitions are rejected by the table CHECKs.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no job with the given ID exists.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn update_status_if_live(
        &self,
        conn: &mut PgConnection,
        id: JobId,
        transition: &JobStatusTransition,
    ) -> Result<Option<Job>, DbError>;

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

    /// Commits the extraction identity into the job's stored source
    /// context: sets it when absent, accepts an identical value as a
    /// no-op, and refuses a differing one. Locks the job row for the
    /// remainder of the caller's transaction.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no job with the given ID exists.
    /// Returns [`DbError::SourceContextUnreadable`] when the stored
    /// context does not parse as the typed V1 shape.
    /// Returns [`DbError::SourceContextRejected`] when a differing
    /// identity is already committed.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn set_extraction_identity(
        &self,
        conn: &mut PgConnection,
        id: JobId,
        identity: &InferenceIdentity,
    ) -> Result<ExtractionCommitOutcome, DbError>;

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
                  source_context, raw_input, ingest_idempotency_key, \
                  extraction_system_prompt_version_id, extraction_user_prompt_version_id, \
                  triage_system_prompt_version_id, triage_user_prompt_version_id, \
                  relation_system_prompt_version_id, relation_user_prompt_version_id, \
                  system_fingerprint_hash, trace_context) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(new_job.correlation_id.map(|id| *id.inner()))
            .bind(new_job.project_id.inner())
            .bind(new_job.principal_id.inner())
            .bind(new_job.actor_id.map(|id| *id.inner()))
            .bind(&new_job.source_context)
            .bind(&new_job.raw_input)
            .bind(new_job.ingest_idempotency_key)
            .bind(new_job.extraction_system_prompt_version_id.inner())
            .bind(new_job.extraction_user_prompt_version_id.inner())
            .bind(new_job.triage_system_prompt_version_id.inner())
            .bind(new_job.triage_user_prompt_version_id.inner())
            .bind(new_job.relation_system_prompt_version_id.inner())
            .bind(new_job.relation_user_prompt_version_id.inner())
            .bind(&new_job.system_fingerprint_hash)
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

    async fn update_status_if_live(
        &self,
        conn: &mut PgConnection,
        id: JobId,
        transition: &JobStatusTransition,
    ) -> Result<Option<Job>, DbError> {
        let terminal: Vec<&str> = JobStatus::ALL
            .iter()
            .filter(|status| status.is_terminal())
            .map(JobStatus::as_str)
            .collect();

        let sql = format!(
            "UPDATE jobs \
             SET status = $2, outcome = $3, error_message = $4, \
                 completed_at = $5, updated_at = now() \
             WHERE id = $1 AND NOT (status = ANY($6::text[])) \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(id.inner())
            .bind(transition.status.as_str())
            .bind(transition.outcome.map(|o| o.as_str().to_owned()))
            .bind(&transition.error_message)
            .bind(transition.completed_at)
            .bind(&terminal)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("updating job status for {id}"),
                source: e,
            })?;

        if let Some(row) = row {
            return Ok(Some(map_job_row(&row)));
        }

        // Zero rows: a terminal job (success, no-op) or a missing one.
        let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM jobs WHERE id = $1)")
            .bind(id.inner())
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("checking job {id} after a guarded status update"),
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

    async fn set_extraction_identity(
        &self,
        conn: &mut PgConnection,
        id: JobId,
        identity: &InferenceIdentity,
    ) -> Result<ExtractionCommitOutcome, DbError> {
        let stored: serde_json::Value =
            sqlx::query_scalar("SELECT source_context FROM jobs WHERE id = $1 FOR UPDATE")
                .bind(id.inner())
                .fetch_optional(&mut *conn)
                .await
                .map_err(|e| DbError::QueryFailed {
                    context: format!("locking source context for job {id}"),
                    source: e,
                })?
                .ok_or_else(|| DbError::NotFound {
                    entity: "job",
                    id: id.to_string(),
                })?;

        let mut context: SourceContextV1 =
            serde_json::from_value(stored).map_err(|e| DbError::SourceContextUnreadable {
                job_id: id.to_string(),
                detail: e.to_string(),
            })?;

        let outcome = context
            .commit_extraction_identity(identity)
            .map_err(|source| DbError::SourceContextRejected {
                job_id: id.to_string(),
                source: Box::new(source),
            })?;

        if outcome == ExtractionCommitOutcome::Recorded {
            let written =
                serde_json::to_value(&context).map_err(|e| DbError::SourceContextUnreadable {
                    job_id: id.to_string(),
                    detail: e.to_string(),
                })?;
            sqlx::query("UPDATE jobs SET source_context = $2, updated_at = now() WHERE id = $1")
                .bind(id.inner())
                .bind(written)
                .execute(&mut *conn)
                .await
                .map_err(|e| DbError::QueryFailed {
                    context: format!("committing extraction identity for job {id}"),
                    source: e,
                })?;
        }

        Ok(outcome)
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
        let job_terminal: Vec<String> = JobStatus::ALL
            .iter()
            .filter(|status| status.is_terminal())
            .map(|status| format!("'{}'", status.as_str()))
            .collect();
        let sql = format!(
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
             AND status NOT IN ({job_terminal}) \
             RETURNING id",
            job_terminal = job_terminal.join(", "),
        );
        let rows =
            sqlx::query(&sql)
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
        // The live-sibling predicate is NOT-in-terminal, derived from the
        // task vocabulary: a blocked triage task (suspended thread) holds
        // the fan-in back through this authority path exactly as it does
        // through the in-commit count.
        let task_terminal: Vec<String> = TaskStatus::ALL
            .iter()
            .filter(|status| status.is_terminal())
            .map(|status| format!("'{}'", status.as_str()))
            .collect();
        let sql = format!(
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
                     AND t.status NOT IN ({task_terminal}) \
               ) \
               AND NOT EXISTS ( \
                   SELECT 1 FROM tasks t \
                   WHERE t.job_id = j.id \
                     AND t.task_type = 'relation' \
               )",
            task_terminal = task_terminal.join(", "),
        );
        let rows =
            sqlx::query(&sql)
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
// IngestJobRepository
// ---------------------------------------------------------------------------

/// Most recent-listing rows returned in one page.
const RECENT_LIMIT_CAP: u16 = 50;

/// Unicode scalar values a listing preview carries.
const PREVIEW_SCALAR_LIMIT: usize = 160;

/// Outcome of idempotency-arbitrated job admission.
#[derive(Debug, Clone)]
pub enum IngestInsertOutcome {
    /// No key was supplied, or the key was unclaimed: a job was created.
    Inserted(Job),
    /// The key already names a job with this project and raw input.
    Existing(Job),
    /// The key already names a job whose project or raw input differ.
    Conflict,
}

/// Cursor into the `(created_at DESC, id DESC)` recent-listing order,
/// carried on the wire as base64url-without-padding JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecentIngestionCursor {
    /// Creation instant of the last row the caller has seen.
    pub created_at: DateTime<Utc>,
    /// Identifier of that row, breaking creation-instant ties.
    pub job_id: JobId,
}

impl RecentIngestionCursor {
    /// Encodes the cursor for the wire.
    #[must_use]
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).expect("cursor serialises to JSON");
        URL_SAFE_NO_PAD.encode(json)
    }

    /// Decodes a wire cursor.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::InvalidCursor`] when the value is not the
    /// base64url JSON this type emits.
    pub fn decode(raw: &str) -> Result<Self, DbError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|e| DbError::InvalidCursor {
                detail: format!("not base64url: {e}"),
            })?;
        serde_json::from_slice(&bytes).map_err(|e| DbError::InvalidCursor {
            detail: format!("not a recent-ingestion cursor: {e}"),
        })
    }
}

/// Query for the principal-scoped recent-ingestion listing.
#[derive(Debug, Clone, Default)]
pub struct RecentIngestionsQuery {
    /// Restrict to one project.
    pub project_id: Option<ProjectId>,
    /// Restrict to these statuses; empty means all.
    pub statuses: Vec<JobStatus>,
    /// Return rows strictly before this position.
    pub before: Option<RecentIngestionCursor>,
    /// Requested page size; the repository caps it.
    pub limit: u16,
}

/// One listed ingestion: monitoring fields plus a bounded preview.
#[derive(Debug, Clone)]
pub struct RecentIngestionSummary {
    /// The job's identifier.
    pub job_id: JobId,
    /// The project the ingest resolved to.
    pub project_id: ProjectId,
    /// Current lifecycle status.
    pub status: JobStatus,
    /// Terminal outcome, when the job has one.
    pub outcome: Option<JobOutcome>,
    /// Whitespace-collapsed raw-input preview, at most
    /// [`PREVIEW_SCALAR_LIMIT`] scalar values.
    pub preview: String,
    /// When the job was created.
    pub created_at: DateTime<Utc>,
    /// When the job last changed.
    pub updated_at: DateTime<Utc>,
}

/// One page of the recent-ingestion listing.
#[derive(Debug, Clone)]
pub struct RecentIngestionPage {
    /// The page's rows, newest first.
    pub ingestions: Vec<RecentIngestionSummary>,
    /// Position to resume from, absent on the last page.
    pub next_cursor: Option<RecentIngestionCursor>,
}

/// The narrowed data-plane capability handed to the MCP boundary: no
/// unscoped job read exists on it, so a handler cannot reach another
/// principal's rows even by mistake. Workers keep the wider internal
/// [`JobRepository`].
#[async_trait]
pub trait IngestJobRepository: Send + Sync {
    /// Admits an ingest with database-side idempotency arbitration: a
    /// keyless job inserts as today, an unclaimed key inserts and claims,
    /// a claimed key resolves to the existing job when project and raw
    /// input match exactly, and conflicts otherwise — without echoing
    /// content.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn insert_or_resolve_idempotency(
        &self,
        conn: &mut PgConnection,
        job: &NewJob,
    ) -> Result<IngestInsertOutcome, DbError>;

    /// Finds a job the principal owns. A missing and a foreign job are
    /// the same [`DbError::NotFound`], so existence never leaks.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] for a missing or foreign job.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id_for_principal(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
        principal_id: PrincipalId,
    ) -> Result<Job, DbError>;

    /// Lists the principal's ingestions in `(created_at DESC, id DESC)`
    /// order with keyset pagination; the page size is capped at
    /// [`RECENT_LIMIT_CAP`].
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn list_recent_for_principal(
        &self,
        conn: &mut PgConnection,
        principal_id: PrincipalId,
        query: &RecentIngestionsQuery,
    ) -> Result<RecentIngestionPage, DbError>;
}

#[async_trait]
impl IngestJobRepository for PgJobRepository {
    async fn insert_or_resolve_idempotency(
        &self,
        conn: &mut PgConnection,
        job: &NewJob,
    ) -> Result<IngestInsertOutcome, DbError> {
        let Some(key) = job.ingest_idempotency_key else {
            return Ok(IngestInsertOutcome::Inserted(self.insert(conn, job).await?));
        };

        let sql = format!(
            "INSERT INTO jobs \
                 (correlation_id, project_id, principal_id, actor_id, \
                  source_context, raw_input, ingest_idempotency_key, \
                  extraction_system_prompt_version_id, extraction_user_prompt_version_id, \
                  triage_system_prompt_version_id, triage_user_prompt_version_id, \
                  relation_system_prompt_version_id, relation_user_prompt_version_id, \
                  system_fingerprint_hash, trace_context) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) \
             ON CONFLICT (principal_id, ingest_idempotency_key) \
                 WHERE ingest_idempotency_key IS NOT NULL \
                 DO NOTHING \
             RETURNING {COLUMNS}",
        );

        let inserted = sqlx::query(&sql)
            .bind(job.correlation_id.map(|id| *id.inner()))
            .bind(job.project_id.inner())
            .bind(job.principal_id.inner())
            .bind(job.actor_id.map(|id| *id.inner()))
            .bind(&job.source_context)
            .bind(&job.raw_input)
            .bind(key)
            .bind(job.extraction_system_prompt_version_id.inner())
            .bind(job.extraction_user_prompt_version_id.inner())
            .bind(job.triage_system_prompt_version_id.inner())
            .bind(job.triage_user_prompt_version_id.inner())
            .bind(job.relation_system_prompt_version_id.inner())
            .bind(job.relation_user_prompt_version_id.inner())
            .bind(&job.system_fingerprint_hash)
            .bind(&job.trace_context)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("admitting ingest under idempotency key {key}"),
                source: e,
            })?;

        if let Some(row) = inserted {
            return Ok(IngestInsertOutcome::Inserted(map_job_row(&row)));
        }

        let sql = format!(
            "SELECT {COLUMNS} FROM jobs \
             WHERE principal_id = $1 AND ingest_idempotency_key = $2",
        );
        let row = sqlx::query(&sql)
            .bind(job.principal_id.inner())
            .bind(key)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("resolving ingest idempotency key {key}"),
                source: e,
            })?
            .ok_or_else(|| DbError::NotFound {
                entity: "job",
                id: key.to_string(),
            })?;
        let existing = map_job_row(&row);

        if existing.project_id() == job.project_id && existing.raw_input() == job.raw_input {
            Ok(IngestInsertOutcome::Existing(existing))
        } else {
            Ok(IngestInsertOutcome::Conflict)
        }
    }

    async fn find_by_id_for_principal(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
        principal_id: PrincipalId,
    ) -> Result<Job, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM jobs WHERE id = $1 AND principal_id = $2");

        let row = sqlx::query(&sql)
            .bind(job_id.inner())
            .bind(principal_id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding job {job_id} for principal"),
                source: e,
            })?
            .ok_or_else(|| DbError::NotFound {
                entity: "job",
                id: job_id.to_string(),
            })?;

        Ok(map_job_row(&row))
    }

    async fn list_recent_for_principal(
        &self,
        conn: &mut PgConnection,
        principal_id: PrincipalId,
        query: &RecentIngestionsQuery,
    ) -> Result<RecentIngestionPage, DbError> {
        let limit = query.limit.clamp(1, RECENT_LIMIT_CAP);

        let mut sql = format!("SELECT {COLUMNS} FROM jobs WHERE principal_id = $1");
        let mut next_bind = 2;
        if query.project_id.is_some() {
            sql.push_str(&format!(" AND project_id = ${next_bind}"));
            next_bind += 1;
        }
        if !query.statuses.is_empty() {
            sql.push_str(&format!(" AND status = ANY(${next_bind}::text[])"));
            next_bind += 1;
        }
        if query.before.is_some() {
            sql.push_str(&format!(
                " AND (created_at, id) < (${}, ${})",
                next_bind,
                next_bind + 1
            ));
            next_bind += 2;
        }
        sql.push_str(&format!(
            " ORDER BY created_at DESC, id DESC LIMIT ${next_bind}"
        ));

        let mut q = sqlx::query(&sql).bind(principal_id.inner());
        if let Some(project_id) = query.project_id {
            q = q.bind(*project_id.inner());
        }
        if !query.statuses.is_empty() {
            let statuses: Vec<String> = query
                .statuses
                .iter()
                .map(|s| s.as_str().to_owned())
                .collect();
            q = q.bind(statuses);
        }
        if let Some(before) = query.before {
            q = q.bind(before.created_at).bind(*before.job_id.inner());
        }
        // One row past the page decides whether a next cursor exists.
        q = q.bind(i64::from(limit) + 1);

        let rows = q
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "listing recent ingestions".to_owned(),
                source: e,
            })?;

        let mut jobs: Vec<Job> = rows.iter().map(map_job_row).collect();
        let has_more = jobs.len() > usize::from(limit);
        jobs.truncate(usize::from(limit));

        let next_cursor = if has_more {
            jobs.last().map(|job| RecentIngestionCursor {
                created_at: job.created_at(),
                job_id: job.id(),
            })
        } else {
            None
        };

        let ingestions = jobs
            .into_iter()
            .map(|job| RecentIngestionSummary {
                job_id: job.id(),
                project_id: job.project_id(),
                status: job.status(),
                outcome: job.outcome(),
                preview: preview_of(job.raw_input()),
                created_at: job.created_at(),
                updated_at: job.updated_at(),
            })
            .collect();

        Ok(RecentIngestionPage {
            ingestions,
            next_cursor,
        })
    }
}

/// Collapses whitespace runs to single spaces and bounds the result at
/// [`PREVIEW_SCALAR_LIMIT`] Unicode scalar values.
fn preview_of(raw_input: &str) -> String {
    let collapsed = raw_input.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.chars().take(PREVIEW_SCALAR_LIMIT).collect()
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
        .ingest_idempotency_key(r.get("ingest_idempotency_key"))
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
        .system_fingerprint_hash(r.get("system_fingerprint_hash"))
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
                  system_fingerprint_hash, trace_context, status, outcome, \
                  committed_batch_id, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18) \
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
            .bind(&new_job.system_fingerprint_hash)
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
