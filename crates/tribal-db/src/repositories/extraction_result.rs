//! Extraction result repository: trait definition and Postgres implementation.
//!
//! Stores the structured output of the extraction stage (candidates and
//! relation hints) as JSONB. One row per job.
//!
//! The `extraction_results` write MUST occur in the same transaction as
//! triage task creation and the claim_token-guarded task completion.
//! This is load-bearing — extracting it to a separate transaction would
//! remove the ownership guard and risk silent data corruption on stale
//! worker commits.
//!
//! Uses raw `sqlx::query()` because the `candidates` and `relation_hints`
//! columns are opaque JSONB values.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{ExtractionResult, ExtractionResultId, JobId};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "job_id",
    "candidates",
    "relation_hints",
    "created_at",
]);

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating an extraction result.
///
/// Server-generated fields (`id`, `created_at`) are produced by Postgres
/// defaults.
#[derive(Debug, TypedBuilder)]
pub struct NewExtractionResult {
    /// The job this extraction result belongs to.
    pub job_id: JobId,
    /// JSONB array of candidate objects.
    pub candidates: serde_json::Value,
    /// JSONB array of relation hint objects.
    #[builder(default = serde_json::json!([]))]
    pub relation_hints: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for extraction results.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait ExtractionResultRepository {
    /// Inserts an extraction result for a job.
    ///
    /// A unique constraint on `job_id` ensures at most one result per
    /// job. If a unique violation fires, it means a previous extraction
    /// attempt fully committed and the current attempt is spurious — the
    /// `claim_token` guard should have prevented this.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] if a result already exists
    /// for this job (indicates an ownership model violation).
    /// Returns [`DbError::QueryFailed`] on other database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewExtractionResult,
    ) -> Result<ExtractionResult, DbError>;

    /// Finds the extraction result for a job, or `None` if extraction
    /// has not yet completed.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_job_id(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Option<ExtractionResult>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`ExtractionResultRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgExtractionResultRepository;

#[async_trait]
impl ExtractionResultRepository for PgExtractionResultRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewExtractionResult,
    ) -> Result<ExtractionResult, DbError> {
        let sql = format!(
            "INSERT INTO extraction_results \
                 (job_id, candidates, relation_hints) \
             VALUES ($1, $2, $3) \
             RETURNING {COLUMNS}",
        );

        let result = sqlx::query(&sql)
            .bind(new.job_id.inner())
            .bind(&new.candidates)
            .bind(&new.relation_hints)
            .fetch_one(&mut *conn)
            .await;

        match result {
            Ok(row) => Ok(map_extraction_result_row(&row)),
            Err(e) => {
                if let Some(uv) = super::common::constraint::try_into_unique_violation(&e) {
                    Err(uv)
                } else {
                    Err(DbError::QueryFailed {
                        context: "inserting extraction result".to_owned(),
                        source: e,
                    })
                }
            }
        }
    }

    async fn find_by_job_id(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Option<ExtractionResult>, DbError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM extraction_results WHERE job_id = $1",
        );

        let row = sqlx::query(&sql)
            .bind(job_id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding extraction result for job {job_id}"),
                source: e,
            })?;

        Ok(row.as_ref().map(map_extraction_result_row))
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from an extraction result query into an
/// [`ExtractionResult`].
fn map_extraction_result_row(r: &sqlx::postgres::PgRow) -> ExtractionResult {
    ExtractionResult::builder()
        .id(ExtractionResultId::from(r.get::<uuid::Uuid, _>("id")))
        .job_id(JobId::from(r.get::<uuid::Uuid, _>("job_id")))
        .candidates(r.get("candidates"))
        .relation_hints(r.get("relation_hints"))
        .created_at(r.get("created_at"))
        .build()
}
