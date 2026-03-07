//! Item observation repository: trait definition and Postgres implementation.
//!
//! Item observations are append-only records of re-encountering an existing
//! knowledge item.  No UPDATE or DELETE operations are provided; observation
//! weight is computed at query time.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{ItemObservation, ItemObservationId, KnowledgeItemId, PrincipalId, SourceType};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "knowledge_item_id",
    "principal_id",
    "source_type",
    "observed_at",
]);

const UNKNOWN_SOURCE_TYPE_IN_DB: &str = "unrecognised source type in database — schema mismatch";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new item observation.
///
/// Server-generated fields (`id`, `observed_at`) are produced by Postgres
/// defaults and returned via `RETURNING {COLUMNS}`.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewItemObservation {
    /// The knowledge item that was re-observed.
    pub knowledge_item_id: KnowledgeItemId,
    /// The principal who captured this observation.
    pub principal_id: PrincipalId,
    /// How the re-observation was captured.
    pub source_type: SourceType,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for item observations.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait ItemObservationRepository {
    /// Inserts an observation and returns it with server-generated `id`
    /// and `observed_at`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewItemObservation,
    ) -> Result<ItemObservation, DbError>;

    /// Returns all observations for a knowledge item ordered by `observed_at`.
    ///
    /// Returns an empty `Vec` when no observations exist.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_knowledge_item_id(
        &self,
        conn: &mut PgConnection,
        knowledge_item_id: KnowledgeItemId,
    ) -> Result<Vec<ItemObservation>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`ItemObservationRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgItemObservationRepository;

#[async_trait]
impl ItemObservationRepository for PgItemObservationRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewItemObservation,
    ) -> Result<ItemObservation, DbError> {
        let sql = format!(
            "INSERT INTO item_observations \
                 (knowledge_item_id, principal_id, source_type) \
             VALUES ($1, $2, $3) \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(new.knowledge_item_id.inner())
            .bind(new.principal_id.inner())
            .bind(new.source_type.as_str())
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "inserting item observation".to_owned(),
                source: e,
            })?;

        Ok(map_item_observation_row(&row))
    }

    async fn find_by_knowledge_item_id(
        &self,
        conn: &mut PgConnection,
        knowledge_item_id: KnowledgeItemId,
    ) -> Result<Vec<ItemObservation>, DbError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM item_observations \
             WHERE knowledge_item_id = $1 \
             ORDER BY observed_at",
        );

        let rows = sqlx::query(&sql)
            .bind(knowledge_item_id.inner())
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding observations for knowledge item {knowledge_item_id}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_item_observation_row).collect())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from an item observation query into an
/// [`ItemObservation`].
fn map_item_observation_row(r: &sqlx::postgres::PgRow) -> ItemObservation {
    ItemObservation::builder()
        .id(ItemObservationId::from(r.get::<uuid::Uuid, _>("id")))
        .knowledge_item_id(KnowledgeItemId::from(
            r.get::<uuid::Uuid, _>("knowledge_item_id"),
        ))
        .principal_id(PrincipalId::from(r.get::<uuid::Uuid, _>("principal_id")))
        .source_type(
            r.get::<String, _>("source_type")
                .parse::<SourceType>()
                .expect(UNKNOWN_SOURCE_TYPE_IN_DB),
        )
        .observed_at(r.get("observed_at"))
        .build()
}
