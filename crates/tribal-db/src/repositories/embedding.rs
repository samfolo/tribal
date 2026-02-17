//! Embedding repository: trait definition and Postgres implementation.
//!
//! Embeddings are stored separately from knowledge items to support model
//! hot-swapping and re-embedding.  The repository enforces a one-embedding-
//! per-(item, model) invariant via a unique index.  Both methods use raw
//! `sqlx::query()` because `pgvector::Vector` is not handled by the
//! compile-time `sqlx::query!` macro.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{Embedding, EmbeddingId, KnowledgeItemId};
use typed_builder::TypedBuilder;

use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DIMENSIONS_EXCEEDS_I32: &str = "dimensions exceeds i32::MAX";
const DIMENSIONS_OVERFLOW: &str = "negative dimensions in database — data corruption";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new embedding.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING`.
#[derive(Debug, TypedBuilder)]
pub struct NewEmbedding {
    /// The knowledge item this embedding represents.
    pub knowledge_item_id: KnowledgeItemId,
    /// The embedding model name (e.g. `"nomic-embed-text:v1.5"`).
    pub model: String,
    /// The number of dimensions in the embedding vector.
    pub dimensions: u32,
    /// The embedding vector.
    pub embedding: Vec<f32>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for embeddings.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait EmbeddingRepository {
    /// Inserts a new embedding and returns the fully populated domain type.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] if an embedding for the same
    /// `(knowledge_item_id, model)` pair already exists.
    /// Returns [`DbError::QueryFailed`] on other database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewEmbedding,
    ) -> Result<Embedding, DbError>;

    /// Finds an embedding by knowledge item ID and model.
    ///
    /// Returns `None` if no embedding exists for the given pair.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_knowledge_item_id(
        &self,
        conn: &mut PgConnection,
        knowledge_item_id: KnowledgeItemId,
        model: &str,
    ) -> Result<Option<Embedding>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`EmbeddingRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgEmbeddingRepository;

#[async_trait]
impl EmbeddingRepository for PgEmbeddingRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewEmbedding,
    ) -> Result<Embedding, DbError> {
        let dimensions = i32::try_from(new.dimensions).expect(DIMENSIONS_EXCEEDS_I32);
        let vector = pgvector::Vector::from(new.embedding.clone());

        let row = sqlx::query(
            "INSERT INTO embeddings (knowledge_item_id, model, dimensions, embedding) \
             VALUES ($1, $2, $3, $4) \
             RETURNING id, knowledge_item_id, model, dimensions, created_at",
        )
        .bind(new.knowledge_item_id.inner())
        .bind(&new.model)
        .bind(dimensions)
        .bind(vector)
        .fetch_one(&mut *conn)
        .await;

        match row {
            Ok(r) => Ok(Embedding::builder()
                .id(EmbeddingId::from(r.get::<uuid::Uuid, _>("id")))
                .knowledge_item_id(KnowledgeItemId::from(
                    r.get::<uuid::Uuid, _>("knowledge_item_id"),
                ))
                .model(r.get::<String, _>("model"))
                .dimensions(
                    u32::try_from(r.get::<i32, _>("dimensions")).expect(DIMENSIONS_OVERFLOW),
                )
                .embedding(new.embedding.clone())
                .created_at(r.get("created_at"))
                .build()),
            Err(e) => {
                if let Some(uv) = super::common::constraint::try_into_unique_violation(&e) {
                    Err(uv)
                } else {
                    Err(DbError::QueryFailed {
                        context: "inserting embedding".to_owned(),
                        source: e,
                    })
                }
            }
        }
    }

    async fn find_by_knowledge_item_id(
        &self,
        conn: &mut PgConnection,
        knowledge_item_id: KnowledgeItemId,
        model: &str,
    ) -> Result<Option<Embedding>, DbError> {
        let row = sqlx::query(
            "SELECT id, knowledge_item_id, model, dimensions, embedding, created_at \
             FROM embeddings \
             WHERE knowledge_item_id = $1 AND model = $2",
        )
        .bind(knowledge_item_id.inner())
        .bind(model)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!(
                "finding embedding by knowledge item id {knowledge_item_id} and model '{model}'"
            ),
            source: e,
        })?;

        Ok(row.map(|r| {
            let vector: pgvector::Vector = r.get("embedding");
            Embedding::builder()
                .id(EmbeddingId::from(r.get::<uuid::Uuid, _>("id")))
                .knowledge_item_id(KnowledgeItemId::from(
                    r.get::<uuid::Uuid, _>("knowledge_item_id"),
                ))
                .model(r.get::<String, _>("model"))
                .dimensions(
                    u32::try_from(r.get::<i32, _>("dimensions")).expect(DIMENSIONS_OVERFLOW),
                )
                .embedding(vector.to_vec())
                .created_at(r.get("created_at"))
                .build()
        }))
    }
}
