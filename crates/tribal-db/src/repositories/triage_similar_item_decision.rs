//! Triage similar item decision repository: trait definition and Postgres
//! implementation.
//!
//! Records the triage agent's per-candidate reasoning for each similar
//! existing item.  Decisions are batch-inserted (one call per candidate)
//! using a multi-row `INSERT ... SELECT * FROM UNNEST(...)` statement.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{
    JobId, KnowledgeItemId, RelationSuggestion, TriageSimilarItemDecision,
    TriageSimilarItemDecisionId,
};
use typed_builder::TypedBuilder;

use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const UNKNOWN_RELATION_SUGGESTION_IN_DB: &str =
    "unrecognised relation suggestion in database — schema mismatch";
const BATCH_INDEX_EXCEEDS_I32: &str = "batch_index exceeds i32::MAX";
const BATCH_INDEX_OVERFLOW: &str = "negative batch_index in database — data corruption";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new triage similar item decision.
///
/// Server-generated fields (`id`, `created_at`) are produced by Postgres
/// defaults and returned via `RETURNING *`.
#[derive(Debug, TypedBuilder)]
pub struct NewTriageSimilarItemDecision {
    /// The job this decision belongs to.
    pub job_id: JobId,
    /// The candidate's position in the extraction batch.
    pub batch_index: u32,
    /// The existing item that was compared against.
    pub matched_item_id: KnowledgeItemId,
    /// Cosine similarity score.
    pub similarity_score: f32,
    /// The agent's suggested relation classification.
    pub suggested_relation: RelationSuggestion,
    /// The agent's reasoning for the classification.
    pub justification_text: String,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for triage similar item decisions.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait TriageSimilarItemDecisionRepository {
    /// Inserts multiple decisions in a single statement and returns them
    /// with server-generated `id` and `created_at`.
    ///
    /// An empty input slice returns an empty `Vec` without issuing a query.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] if a decision already exists
    /// for a given `(job_id, batch_index, matched_item_id)` triple.
    /// Returns [`DbError::QueryFailed`] on other database errors.
    async fn batch_insert(
        &self,
        conn: &mut PgConnection,
        batch: &[NewTriageSimilarItemDecision],
    ) -> Result<Vec<TriageSimilarItemDecision>, DbError>;

    /// Returns all decisions for a job ordered by `batch_index` then
    /// `created_at`.
    ///
    /// Returns an empty `Vec` when no decisions exist for the job.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_job_id(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Vec<TriageSimilarItemDecision>, DbError>;

    /// Returns decisions for a specific `(job_id, batch_index)` ordered
    /// by `created_at`.
    ///
    /// Returns an empty `Vec` when no decisions exist for that batch index.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_job_id_and_batch_index(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
        batch_index: u32,
    ) -> Result<Vec<TriageSimilarItemDecision>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`TriageSimilarItemDecisionRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgTriageSimilarItemDecisionRepository;

#[async_trait]
impl TriageSimilarItemDecisionRepository for PgTriageSimilarItemDecisionRepository {
    async fn batch_insert(
        &self,
        conn: &mut PgConnection,
        batch: &[NewTriageSimilarItemDecision],
    ) -> Result<Vec<TriageSimilarItemDecision>, DbError> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let job_ids: Vec<uuid::Uuid> = batch.iter().map(|d| *d.job_id.inner()).collect();
        let batch_indices: Vec<i32> = batch
            .iter()
            .map(|d| i32::try_from(d.batch_index).expect(BATCH_INDEX_EXCEEDS_I32))
            .collect();
        let matched_item_ids: Vec<uuid::Uuid> =
            batch.iter().map(|d| *d.matched_item_id.inner()).collect();
        let similarity_scores: Vec<f32> = batch.iter().map(|d| d.similarity_score).collect();
        let suggested_relations: Vec<String> = batch
            .iter()
            .map(|d| d.suggested_relation.as_str().to_owned())
            .collect();
        let justification_texts: Vec<String> =
            batch.iter().map(|d| d.justification_text.clone()).collect();

        let result = sqlx::query(
            "INSERT INTO triage_similar_item_decisions \
                 (job_id, batch_index, matched_item_id, similarity_score, \
                  suggested_relation, justification_text) \
             SELECT * FROM UNNEST(\
                 $1::uuid[], $2::int[], $3::uuid[], $4::real[], \
                 $5::text[], $6::text[]) \
             RETURNING *",
        )
        .bind(&job_ids)
        .bind(&batch_indices)
        .bind(&matched_item_ids)
        .bind(&similarity_scores)
        .bind(&suggested_relations)
        .bind(&justification_texts)
        .fetch_all(&mut *conn)
        .await;

        match result {
            Ok(rows) => Ok(rows
                .into_iter()
                .map(|r| map_triage_similar_item_decision_row(&r))
                .collect()),
            Err(e) => {
                if let Some(uv) = super::common::constraint::try_into_unique_violation(&e) {
                    Err(uv)
                } else {
                    Err(DbError::QueryFailed {
                        context: format!(
                            "batch inserting {} triage similar item decisions",
                            batch.len()
                        ),
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
    ) -> Result<Vec<TriageSimilarItemDecision>, DbError> {
        let rows = sqlx::query(
            "SELECT * FROM triage_similar_item_decisions \
             WHERE job_id = $1 \
             ORDER BY batch_index, created_at",
        )
        .bind(job_id.inner())
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("finding triage similar item decisions for job {job_id}"),
            source: e,
        })?;

        Ok(rows
            .iter()
            .map(map_triage_similar_item_decision_row)
            .collect())
    }

    async fn find_by_job_id_and_batch_index(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
        batch_index: u32,
    ) -> Result<Vec<TriageSimilarItemDecision>, DbError> {
        let batch_index_i32 = i32::try_from(batch_index).expect(BATCH_INDEX_EXCEEDS_I32);

        let rows = sqlx::query(
            "SELECT * FROM triage_similar_item_decisions \
             WHERE job_id = $1 AND batch_index = $2 \
             ORDER BY created_at",
        )
        .bind(job_id.inner())
        .bind(batch_index_i32)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!(
                "finding triage similar item decisions for job {job_id} \
                 batch_index {batch_index}"
            ),
            source: e,
        })?;

        Ok(rows
            .iter()
            .map(map_triage_similar_item_decision_row)
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from a triage similar item decision query into a
/// [`TriageSimilarItemDecision`].
fn map_triage_similar_item_decision_row(r: &sqlx::postgres::PgRow) -> TriageSimilarItemDecision {
    TriageSimilarItemDecision::builder()
        .id(TriageSimilarItemDecisionId::from(
            r.get::<uuid::Uuid, _>("id"),
        ))
        .job_id(JobId::from(r.get::<uuid::Uuid, _>("job_id")))
        .batch_index(u32::try_from(r.get::<i32, _>("batch_index")).expect(BATCH_INDEX_OVERFLOW))
        .matched_item_id(KnowledgeItemId::from(
            r.get::<uuid::Uuid, _>("matched_item_id"),
        ))
        .similarity_score(r.get::<f32, _>("similarity_score"))
        .suggested_relation(
            r.get::<String, _>("suggested_relation")
                .parse::<RelationSuggestion>()
                .expect(UNKNOWN_RELATION_SUGGESTION_IN_DB),
        )
        .justification_text(r.get("justification_text"))
        .created_at(r.get("created_at"))
        .build()
}
