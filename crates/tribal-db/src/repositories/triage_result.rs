//! Triage result repository: trait definition and Postgres implementation.
//!
//! Triage results use a discriminated union at the database level: the
//! `outcome_type` column stores `'created'`, `'duplicate'`, or `'failed'`,
//! with conditional nullability enforced by CHECK constraints.  The
//! repository flattens [`TriageOutcome`] into these columns on insert and
//! reconstructs it on read.
//!
//! The `similar_items` field is left as an empty `Vec` — similar items
//! live in the `triage_similar_item_decisions` table and are composed at
//! a higher layer.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{
    ItemObservationId, JobId, KnowledgeItemId, TriageOutcome, TriageResult, TriageResultId,
};
use typed_builder::TypedBuilder;

use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const UNKNOWN_OUTCOME_TYPE_IN_DB: &str =
    "unrecognised triage outcome type in database — schema mismatch";
const BATCH_INDEX_EXCEEDS_I32: &str = "batch_index exceeds i32::MAX";
const BATCH_INDEX_OVERFLOW: &str = "negative batch_index in database — data corruption";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new triage result.
///
/// The [`TriageOutcome`] enum is flattened into separate database columns
/// by the repository implementation.  Server-generated fields (`id`,
/// `created_at`) are produced by Postgres defaults.
#[derive(Debug, TypedBuilder)]
pub struct NewTriageResult {
    /// The job this result belongs to.
    pub job_id: JobId,
    /// The candidate's position in the extraction batch.
    pub batch_index: u32,
    /// The triage outcome for this candidate.
    pub outcome: TriageOutcome,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for triage results.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait TriageResultRepository {
    /// Inserts a triage result and returns it with server-generated `id`
    /// and `created_at`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] if a result already exists
    /// for the given `(job_id, batch_index)` pair.
    /// Returns [`DbError::QueryFailed`] on other database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewTriageResult,
    ) -> Result<TriageResult, DbError>;

    /// Returns all triage results for a job ordered by `batch_index`.
    ///
    /// Returns an empty `Vec` when no results exist for the job.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_job_id(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Vec<TriageResult>, DbError>;

    /// Returns the triage result for a specific `(job_id, batch_index)`,
    /// or `None` if no such result exists.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_job_id_and_batch_index(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
        batch_index: u32,
    ) -> Result<Option<TriageResult>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`TriageResultRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgTriageResultRepository;

#[async_trait]
impl TriageResultRepository for PgTriageResultRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewTriageResult,
    ) -> Result<TriageResult, DbError> {
        let batch_index_i32 = i32::try_from(new.batch_index).expect(BATCH_INDEX_EXCEEDS_I32);

        let (outcome_type, knowledge_item_id, observation_id, matched_item_id, error_message, retryable) =
            flatten_outcome(&new.outcome);

        let result = sqlx::query(
            "INSERT INTO triage_results \
                 (job_id, batch_index, outcome_type, knowledge_item_id, \
                  observation_id, matched_item_id, error_message, retryable) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             RETURNING *",
        )
        .bind(new.job_id.inner())
        .bind(batch_index_i32)
        .bind(outcome_type)
        .bind(knowledge_item_id)
        .bind(observation_id)
        .bind(matched_item_id)
        .bind(error_message)
        .bind(retryable)
        .fetch_one(&mut *conn)
        .await;

        match result {
            Ok(row) => Ok(map_triage_result_row(&row)),
            Err(e) => {
                if let Some(uv) = super::common::constraint::try_into_unique_violation(&e) {
                    Err(uv)
                } else {
                    Err(DbError::QueryFailed {
                        context: "inserting triage result".to_owned(),
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
    ) -> Result<Vec<TriageResult>, DbError> {
        let rows = sqlx::query(
            "SELECT * FROM triage_results \
             WHERE job_id = $1 \
             ORDER BY batch_index",
        )
        .bind(job_id.inner())
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("finding triage results for job {job_id}"),
            source: e,
        })?;

        Ok(rows.iter().map(map_triage_result_row).collect())
    }

    async fn find_by_job_id_and_batch_index(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
        batch_index: u32,
    ) -> Result<Option<TriageResult>, DbError> {
        let batch_index_i32 = i32::try_from(batch_index).expect(BATCH_INDEX_EXCEEDS_I32);

        let row = sqlx::query(
            "SELECT * FROM triage_results \
             WHERE job_id = $1 AND batch_index = $2",
        )
        .bind(job_id.inner())
        .bind(batch_index_i32)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!(
                "finding triage result for job {job_id} batch_index {batch_index}"
            ),
            source: e,
        })?;

        Ok(row.as_ref().map(map_triage_result_row))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Flattens a [`TriageOutcome`] enum into the database column values.
fn flatten_outcome(
    outcome: &TriageOutcome,
) -> (
    &'static str,
    Option<uuid::Uuid>,
    Option<uuid::Uuid>,
    Option<uuid::Uuid>,
    Option<&str>,
    Option<bool>,
) {
    match outcome {
        TriageOutcome::Created { item_id } => (
            "created",
            Some(*item_id.inner()),
            None,
            None,
            None,
            None,
        ),
        TriageOutcome::Duplicate {
            observation_id,
            matched_item_id,
        } => (
            "duplicate",
            None,
            Some(*observation_id.inner()),
            Some(*matched_item_id.inner()),
            None,
            None,
        ),
        TriageOutcome::Failed {
            error_message,
            retryable,
        } => (
            "failed",
            None,
            None,
            None,
            Some(error_message.as_str()),
            Some(*retryable),
        ),
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from a triage result query into a [`TriageResult`].
fn map_triage_result_row(r: &sqlx::postgres::PgRow) -> TriageResult {
    let outcome_type = r.get::<String, _>("outcome_type");
    let outcome = match outcome_type.as_str() {
        "created" => TriageOutcome::Created {
            item_id: KnowledgeItemId::from(
                r.get::<uuid::Uuid, _>("knowledge_item_id"),
            ),
        },
        "duplicate" => TriageOutcome::Duplicate {
            observation_id: ItemObservationId::from(
                r.get::<uuid::Uuid, _>("observation_id"),
            ),
            matched_item_id: KnowledgeItemId::from(
                r.get::<uuid::Uuid, _>("matched_item_id"),
            ),
        },
        "failed" => TriageOutcome::Failed {
            error_message: r.get("error_message"),
            retryable: r.get("retryable"),
        },
        _ => panic!("{UNKNOWN_OUTCOME_TYPE_IN_DB}: {outcome_type}"),
    };

    TriageResult::builder()
        .id(TriageResultId::from(r.get::<uuid::Uuid, _>("id")))
        .job_id(JobId::from(r.get::<uuid::Uuid, _>("job_id")))
        .batch_index(
            u32::try_from(r.get::<i32, _>("batch_index")).expect(BATCH_INDEX_OVERFLOW),
        )
        .outcome(outcome)
        .created_at(r.get("created_at"))
        .build()
}
