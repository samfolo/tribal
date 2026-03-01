//! Tag embedding repository: trait definition and Postgres implementation.
//!
//! Stores embedding vectors for tag registry entries, enabling pgvector
//! semantic similarity search during tag resolution.  Uses raw
//! `sqlx::query()` because `pgvector::Vector` is not handled by the
//! compile-time `sqlx::query!` macro.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::TagSimilarityResult;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const INSERT_COLUMNS: Columns = Columns(&["tag", "model", "dimensions", "embedding"]);
const DIMENSIONS_EXCEEDS_I32: &str = "dimensions exceeds i32::MAX";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new tag embedding.
#[derive(Debug, typed_builder::TypedBuilder)]
pub struct NewTagEmbedding {
    /// The canonical tag string (must already exist in `tag_registry`).
    pub tag: String,
    /// The embedding model name.
    pub model: String,
    /// The number of dimensions in the embedding vector.
    pub dimensions: u32,
    /// The embedding vector.
    pub embedding: Vec<f32>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for tag embeddings.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait TagEmbeddingRepository {
    /// Inserts tag embeddings with `ON CONFLICT (tag, model) DO NOTHING`
    /// semantics for concurrent-safe upserts.
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

    /// Returns tags whose embeddings are similar to the given vector,
    /// filtered to results above the threshold.
    ///
    /// Results are ordered for deterministic selection: `similarity`
    /// descending, then `usage_count` descending, then `tag` alphabetically.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn similarity_search(
        &self,
        conn: &mut PgConnection,
        embedding: &[f32],
        model: &str,
        threshold: f64,
        limit: u32,
    ) -> Result<Vec<TagSimilarityResult>, DbError>;

    /// Returns tag names from the registry that have no embedding for the
    /// given model.  Used by the startup backfill step.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_tags_missing_embeddings(
        &self,
        conn: &mut PgConnection,
        model: &str,
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

        let sql = format!(
            "INSERT INTO tag_embeddings ({INSERT_COLUMNS}) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (tag, model) DO NOTHING"
        );

        for new in embeddings {
            let dimensions = i32::try_from(new.dimensions).expect(DIMENSIONS_EXCEEDS_I32);
            let vector = pgvector::Vector::from(new.embedding.clone());

            sqlx::query(&sql)
                .bind(&new.tag)
                .bind(&new.model)
                .bind(dimensions)
                .bind(vector)
                .execute(&mut *conn)
                .await
                .map_err(|e| DbError::QueryFailed {
                    context: format!("upserting tag embedding for {:?}", new.tag),
                    source: e,
                })?;
        }

        Ok(())
    }

    async fn similarity_search(
        &self,
        conn: &mut PgConnection,
        embedding: &[f32],
        model: &str,
        threshold: f64,
        limit: u32,
    ) -> Result<Vec<TagSimilarityResult>, DbError> {
        let query_vector = pgvector::Vector::from(embedding.to_vec());

        let rows = sqlx::query(
            "SELECT te.tag, \
                    1.0 - (te.embedding <=> $1::vector) AS similarity, \
                    tr.usage_count \
             FROM tag_embeddings te \
             INNER JOIN tag_registry tr ON tr.tag = te.tag \
             WHERE te.model = $2 \
               AND 1.0 - (te.embedding <=> $1::vector) > $3 \
             ORDER BY te.embedding <=> $1::vector ASC, \
                      tr.usage_count DESC, \
                      te.tag ASC \
             LIMIT $4",
        )
        .bind(query_vector)
        .bind(model)
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
        model: &str,
    ) -> Result<Vec<String>, DbError> {
        let rows = sqlx::query(
            "SELECT tr.tag \
             FROM tag_registry tr \
             LEFT JOIN tag_embeddings te \
                 ON te.tag = tr.tag AND te.model = $1 \
             WHERE te.tag IS NULL \
             ORDER BY tr.tag",
        )
        .bind(model)
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
