//! Retrieval feedback repository: trait definition and Postgres implementation.
//!
//! Retrieval feedback records are self-sufficient after trace rotation —
//! each record includes query text, item IDs, model, and policy version
//! so it remains a usable eval dataset without the trace.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{
    FeedbackRating, KnowledgeItemId, PrincipalId, RetrievalFeedback, RetrievalFeedbackId,
};
use typed_builder::TypedBuilder;

use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const UNKNOWN_FEEDBACK_RATING_IN_DB: &str =
    "unrecognised feedback rating in database — schema mismatch";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new retrieval feedback record.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING`.
#[derive(Debug, TypedBuilder)]
pub struct NewRetrievalFeedback {
    /// The trace identifier for correlating with retrieval sessions.
    pub trace_id: String,
    /// The query text used in the retrieval session.
    pub query_text: String,
    /// The embedding model used for the retrieval.
    pub embedding_model: String,
    /// Knowledge item IDs returned by the discovery query.
    #[builder(default)]
    pub returned_item_ids: Vec<KnowledgeItemId>,
    /// Anchor item IDs explored during graph traversal.
    #[builder(default)]
    pub explored_anchor_ids: Vec<KnowledgeItemId>,
    /// The system policy version at the time of retrieval.
    #[builder(default)]
    pub policy_version: Option<String>,
    /// The principal who provided this feedback.
    pub principal_id: PrincipalId,
    /// The overall rating of the retrieval experience.
    pub rating: FeedbackRating,
    /// Optional free-text reasoning for the rating.
    #[builder(default)]
    pub notes: Option<String>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for retrieval feedback records.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.  Retrieval feedback is
/// append-only.
#[async_trait]
pub trait RetrievalFeedbackRepository {
    /// Inserts a retrieval feedback record and returns the populated
    /// domain type.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewRetrievalFeedback,
    ) -> Result<RetrievalFeedback, DbError>;

    /// Finds a retrieval feedback record by its ID.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no row matches.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: RetrievalFeedbackId,
    ) -> Result<RetrievalFeedback, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`RetrievalFeedbackRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgRetrievalFeedbackRepository;

/// Maps a raw `sqlx::Row` from a retrieval feedback query into a
/// [`RetrievalFeedback`].
fn map_retrieval_feedback_row(r: &sqlx::postgres::PgRow) -> RetrievalFeedback {
    RetrievalFeedback::builder()
        .id(RetrievalFeedbackId::from(r.get::<uuid::Uuid, _>("id")))
        .trace_id(r.get("trace_id"))
        .query_text(r.get("query_text"))
        .embedding_model(r.get("embedding_model"))
        .returned_item_ids(
            r.get::<Vec<uuid::Uuid>, _>("returned_item_ids")
                .into_iter()
                .map(KnowledgeItemId::from)
                .collect(),
        )
        .explored_anchor_ids(
            r.get::<Vec<uuid::Uuid>, _>("explored_anchor_ids")
                .into_iter()
                .map(KnowledgeItemId::from)
                .collect(),
        )
        .policy_version(r.get("policy_version"))
        .principal_id(PrincipalId::from(r.get::<uuid::Uuid, _>("principal_id")))
        .rating(
            r.get::<String, _>("rating")
                .parse::<FeedbackRating>()
                .expect(UNKNOWN_FEEDBACK_RATING_IN_DB),
        )
        .notes(r.get("notes"))
        .created_at(r.get("created_at"))
        .build()
}

const COLUMNS: &str = "id, trace_id, query_text, embedding_model, \
                        returned_item_ids, explored_anchor_ids, \
                        policy_version, principal_id, rating, notes, created_at";

#[async_trait]
impl RetrievalFeedbackRepository for PgRetrievalFeedbackRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewRetrievalFeedback,
    ) -> Result<RetrievalFeedback, DbError> {
        let returned_ids: Vec<uuid::Uuid> = new
            .returned_item_ids
            .iter()
            .map(|id| *id.inner())
            .collect();
        let explored_ids: Vec<uuid::Uuid> = new
            .explored_anchor_ids
            .iter()
            .map(|id| *id.inner())
            .collect();

        let sql = format!(
            "INSERT INTO retrieval_feedback \
                 (trace_id, query_text, embedding_model, \
                  returned_item_ids, explored_anchor_ids, \
                  policy_version, principal_id, rating, notes) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(&new.trace_id)
            .bind(&new.query_text)
            .bind(&new.embedding_model)
            .bind(&returned_ids)
            .bind(&explored_ids)
            .bind(&new.policy_version)
            .bind(new.principal_id.inner())
            .bind(new.rating.as_str())
            .bind(&new.notes)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "inserting retrieval feedback".to_owned(),
                source: e,
            })?;

        Ok(map_retrieval_feedback_row(&row))
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: RetrievalFeedbackId,
    ) -> Result<RetrievalFeedback, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM retrieval_feedback WHERE id = $1");

        let row = sqlx::query(&sql)
            .bind(id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding retrieval feedback {id}"),
                source: e,
            })?
            .ok_or_else(|| DbError::NotFound {
                entity: "retrieval_feedback",
                id: id.to_string(),
            })?;

        Ok(map_retrieval_feedback_row(&row))
    }
}
