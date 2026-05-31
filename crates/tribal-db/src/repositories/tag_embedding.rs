//! Tag embedding repository: trait definition and Postgres implementation.
//!
//! Stores embedding vectors for tag registry entries, keyed by the embedding
//! profile that produced them, enabling pgvector semantic similarity search
//! during tag resolution. The similarity query inlines the active profile's
//! UUID and dimension as literals so the per-profile partial `halfvec` HNSW
//! index is planner-pickable. Uses raw `sqlx::query()` because
//! `pgvector::HalfVector` is not handled by the compile-time `sqlx::query!`
//! macro.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{EmbeddingProfileId, TagSimilarityResult};

use super::common::{columns::Columns, halfvec::to_halfvec};
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const INSERT_COLUMNS: Columns = Columns(&["tag", "embedding_profile_id", "model", "embedding"]);

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new tag embedding.
#[derive(Debug, typed_builder::TypedBuilder)]
pub struct NewTagEmbedding {
    /// The canonical tag string (must already exist in `tag_registry`).
    pub tag: String,
    /// The embedding profile that produced this vector.
    pub embedding_profile_id: EmbeddingProfileId,
    /// The embedding model name (denormalised lineage).
    pub model: String,
    /// The embedding vector.
    pub embedding: Vec<f32>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for tag embeddings.
///
/// All methods take `&mut PgConnection` as an explicit executor, keeping the
/// repository pool-agnostic.
#[async_trait]
pub trait TagEmbeddingRepository {
    /// Inserts tag embeddings with
    /// `ON CONFLICT (tag, embedding_profile_id) DO NOTHING` semantics for
    /// concurrent-safe upserts.
    ///
    /// An empty input slice returns without issuing a query.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn batch_upsert(
        &self,
        conn: &mut PgConnection,
        embeddings: &[NewTagEmbedding],
    ) -> Result<(), DbError>;

    /// Returns tags whose embeddings, within the given profile, are similar to
    /// the query vector at or above the threshold.
    ///
    /// Results are ordered for deterministic selection: `similarity`
    /// descending, then `usage_count` descending, then `tag` alphabetically.
    /// `dimensions` must be the profile's dimension so the cast matches its
    /// partial index.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn similarity_search(
        &self,
        conn: &mut PgConnection,
        embedding: &[f32],
        embedding_profile_id: EmbeddingProfileId,
        dimensions: u32,
        threshold: f64,
        limit: u32,
    ) -> Result<Vec<TagSimilarityResult>, DbError>;

    /// Returns tag names from the registry that have no embedding for the
    /// given profile. The profile-keyed set-difference driving backfill and
    /// the reindex tag sweep.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_tags_missing_embeddings(
        &self,
        conn: &mut PgConnection,
        embedding_profile_id: EmbeddingProfileId,
    ) -> Result<Vec<String>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`TagEmbeddingRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgTagEmbeddingRepository;

#[async_trait]
impl TagEmbeddingRepository for PgTagEmbeddingRepository {
    async fn batch_upsert(
        &self,
        conn: &mut PgConnection,
        embeddings: &[NewTagEmbedding],
    ) -> Result<(), DbError> {
        if embeddings.is_empty() {
            return Ok(());
        }

        let mut tags = Vec::with_capacity(embeddings.len());
        let mut profile_ids = Vec::with_capacity(embeddings.len());
        let mut models = Vec::with_capacity(embeddings.len());
        let mut vectors = Vec::with_capacity(embeddings.len());

        for e in embeddings {
            tags.push(e.tag.as_str());
            profile_ids.push(*e.embedding_profile_id.inner());
            models.push(e.model.as_str());
            vectors.push(to_halfvec(&e.embedding));
        }

        let sql = format!(
            "INSERT INTO tag_embeddings ({INSERT_COLUMNS}) \
             SELECT * FROM UNNEST($1::text[], $2::uuid[], $3::text[], $4::halfvec[]) \
             ON CONFLICT (tag, embedding_profile_id) DO NOTHING"
        );

        sqlx::query(&sql)
            .bind(&tags)
            .bind(&profile_ids)
            .bind(&models)
            .bind(&vectors)
            .execute(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("batch upserting {} tag embeddings", embeddings.len()),
                source: e,
            })?;

        Ok(())
    }

    async fn similarity_search(
        &self,
        conn: &mut PgConnection,
        embedding: &[f32],
        embedding_profile_id: EmbeddingProfileId,
        dimensions: u32,
        threshold: f64,
        limit: u32,
    ) -> Result<Vec<TagSimilarityResult>, DbError> {
        // The profile UUID and dimension are inlined as literals (the UUID
        // rendered from a typed value, never untrusted text) so the query
        // predicate implies the partial index predicate and the cast matches
        // the index expression — both conditions for the partial HNSW index to
        // be pickable. The similarity vector stays a bind parameter.
        let profile = embedding_profile_id.inner();
        let distance = format!("te.embedding::halfvec({dimensions}) <=> $1::halfvec({dimensions})");
        let sql = format!(
            "SELECT te.tag, \
                    1.0 - ({distance}) AS similarity, \
                    tr.usage_count \
             FROM tag_embeddings te \
             INNER JOIN tag_registry tr ON tr.tag = te.tag \
             WHERE te.embedding_profile_id = '{profile}'::uuid \
               AND 1.0 - ({distance}) >= $2 \
             ORDER BY {distance} ASC, \
                      tr.usage_count DESC, \
                      te.tag ASC \
             LIMIT $3"
        );

        let rows = sqlx::query(&sql)
            .bind(to_halfvec(embedding))
            .bind(threshold)
            .bind(i64::from(limit))
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "tag embedding similarity search".to_owned(),
                source: e,
            })?;

        Ok(rows.iter().map(map_tag_similarity_row).collect())
    }

    async fn find_tags_missing_embeddings(
        &self,
        conn: &mut PgConnection,
        embedding_profile_id: EmbeddingProfileId,
    ) -> Result<Vec<String>, DbError> {
        let rows = sqlx::query(
            "SELECT tr.tag \
             FROM tag_registry tr \
             LEFT JOIN tag_embeddings te \
                 ON te.tag = tr.tag AND te.embedding_profile_id = $1 \
             WHERE te.tag IS NULL \
             ORDER BY tr.tag",
        )
        .bind(embedding_profile_id.inner())
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "finding tags missing embeddings".to_owned(),
            source: e,
        })?;

        Ok(rows.iter().map(|r| r.get("tag")).collect())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw row from the similarity search query into a [`TagSimilarityResult`].
fn map_tag_similarity_row(r: &sqlx::postgres::PgRow) -> TagSimilarityResult {
    TagSimilarityResult::new(
        r.get("tag"),
        r.get::<f64, _>("similarity"),
        r.get::<i32, _>("usage_count"),
    )
}
