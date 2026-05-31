//! Reindex quarantine repository: the durable permanent-failure relation.
//!
//! A `permanent`-class item or tag is recorded here and excluded from every §6
//! set-difference, so one bad entity is durably skipped rather than re-swept
//! forever. Keyed by `(target_profile_id, kind, entity_ref)` so re-enrolment is
//! idempotent and the exclusion is a direct join.

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_domain::{EmbeddingErrorClass, EmbeddingProfileId, ReindexEntityKind, ReindexRunId};
use typed_builder::TypedBuilder;

use crate::DbError;

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for recording a permanently-failed entity.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewReindexQuarantine {
    /// The run that observed the failure.
    pub reindex_run_id: ReindexRunId,
    /// The profile the entity could not be embedded into.
    pub target_profile_id: EmbeddingProfileId,
    /// Whether the entity is an item or a tag.
    pub kind: ReindexEntityKind,
    /// The knowledge item id (item) or tag text (tag).
    pub entity_ref: String,
    /// The failure class (always permanent for quarantine).
    pub error_class: EmbeddingErrorClass,
    /// The failure message, if any.
    #[builder(default)]
    pub error_message: Option<String>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for reindex quarantine.
#[async_trait]
pub trait ReindexQuarantineRepository {
    /// Idempotently records a quarantined entity. Returns `true` if newly
    /// inserted (so the caller increments the run tally only once).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn record(
        &self,
        conn: &mut PgConnection,
        new: &NewReindexQuarantine,
    ) -> Result<bool, DbError>;

    /// Counts the entities quarantined against a profile.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn count_for_profile(
        &self,
        conn: &mut PgConnection,
        target_profile_id: EmbeddingProfileId,
    ) -> Result<i64, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`ReindexQuarantineRepository`].
pub struct PgReindexQuarantineRepository;

#[async_trait]
impl ReindexQuarantineRepository for PgReindexQuarantineRepository {
    async fn record(
        &self,
        conn: &mut PgConnection,
        new: &NewReindexQuarantine,
    ) -> Result<bool, DbError> {
        let inserted: Option<uuid::Uuid> = sqlx::query_scalar(
            "INSERT INTO reindex_quarantine \
                 (reindex_run_id, target_profile_id, kind, entity_ref, error_class, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (target_profile_id, kind, entity_ref) DO NOTHING \
             RETURNING id",
        )
        .bind(new.reindex_run_id.inner())
        .bind(new.target_profile_id.inner())
        .bind(new.kind.as_str())
        .bind(&new.entity_ref)
        .bind(new.error_class.as_str())
        .bind(&new.error_message)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("quarantining {} {}", new.kind, new.entity_ref),
            source: e,
        })?;

        Ok(inserted.is_some())
    }

    async fn count_for_profile(
        &self,
        conn: &mut PgConnection,
        target_profile_id: EmbeddingProfileId,
    ) -> Result<i64, DbError> {
        sqlx::query_scalar("SELECT COUNT(*) FROM reindex_quarantine WHERE target_profile_id = $1")
            .bind(target_profile_id.inner())
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("counting quarantine for profile {target_profile_id}"),
                source: e,
            })
    }
}
