//! Embedding repository: trait definition and Postgres implementation.
//!
//! Embeddings are stored separately from knowledge items and keyed by the
//! embedding profile that produced them. The `uq_embedding_item_profile`
//! unique constraint enforces one row per `(knowledge_item, profile)`. Both
//! methods use raw `sqlx::query()` because `pgvector::HalfVector` is not
//! handled by the compile-time `sqlx::query!` macro.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{Embedding, EmbeddingId, EmbeddingProfileId, KnowledgeItemId};
use typed_builder::TypedBuilder;

use super::common::halfvec::{to_f32_vec, to_halfvec};
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DIMENSIONS_EXCEEDS_U32: &str = "embedding dimension exceeds u32::MAX";
const DIMENSIONS_OVERFLOW: &str = "negative dimensions in database — data corruption";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new embedding.
///
/// Contains only caller-provided fields. Server-generated values (`id`,
/// `created_at`) are produced by Postgres via `DEFAULT` clauses and returned
/// via `RETURNING`. `model` is denormalised lineage; the profile is identity.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewEmbedding {
    /// The knowledge item this embedding represents.
    pub knowledge_item_id: KnowledgeItemId,
    /// The embedding profile that produced this vector.
    pub embedding_profile_id: EmbeddingProfileId,
    /// The embedding model name (e.g. `"nomic-embed-text:v1.5"`).
    pub model: String,
    /// The embedding vector.
    pub embedding: Vec<f32>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for embeddings.
///
/// All methods take `&mut PgConnection` as an explicit executor, keeping the
/// repository pool-agnostic.
#[async_trait]
pub trait EmbeddingRepository {
    /// Inserts a new embedding and returns the fully populated domain type.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] if an embedding for the same
    /// `(knowledge_item_id, embedding_profile_id)` pair already exists.
    /// Returns [`DbError::QueryFailed`] on other database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewEmbedding,
    ) -> Result<Embedding, DbError>;

    /// Finds an embedding by knowledge item ID and embedding profile.
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
        embedding_profile_id: EmbeddingProfileId,
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
        let dimensions = u32::try_from(new.embedding.len()).expect(DIMENSIONS_EXCEEDS_U32);

        let row = sqlx::query(
            "INSERT INTO embeddings (knowledge_item_id, embedding_profile_id, model, embedding) \
             VALUES ($1, $2, $3, $4) \
             RETURNING id, knowledge_item_id, embedding_profile_id, model, created_at",
        )
        .bind(new.knowledge_item_id.inner())
        .bind(new.embedding_profile_id.inner())
        .bind(&new.model)
        .bind(to_halfvec(&new.embedding))
        .fetch_one(&mut *conn)
        .await;

        match row {
            Ok(r) => Ok(Embedding::builder()
                .id(EmbeddingId::from(r.get::<uuid::Uuid, _>("id")))
                .knowledge_item_id(KnowledgeItemId::from(
                    r.get::<uuid::Uuid, _>("knowledge_item_id"),
                ))
                .embedding_profile_id(EmbeddingProfileId::from(
                    r.get::<uuid::Uuid, _>("embedding_profile_id"),
                ))
                .model(r.get::<String, _>("model"))
                .dimensions(dimensions)
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
        embedding_profile_id: EmbeddingProfileId,
    ) -> Result<Option<Embedding>, DbError> {
        let row = sqlx::query(
            "SELECT id, knowledge_item_id, embedding_profile_id, model, embedding, \
                    vector_dims(embedding) AS dimensions, created_at \
             FROM embeddings \
             WHERE knowledge_item_id = $1 AND embedding_profile_id = $2",
        )
        .bind(knowledge_item_id.inner())
        .bind(embedding_profile_id.inner())
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!(
                "finding embedding by knowledge item id {knowledge_item_id} \
                 and profile {embedding_profile_id}"
            ),
            source: e,
        })?;

        Ok(row.map(|r| {
            let vector: pgvector::HalfVector = r.get("embedding");
            Embedding::builder()
                .id(EmbeddingId::from(r.get::<uuid::Uuid, _>("id")))
                .knowledge_item_id(KnowledgeItemId::from(
                    r.get::<uuid::Uuid, _>("knowledge_item_id"),
                ))
                .embedding_profile_id(EmbeddingProfileId::from(
                    r.get::<uuid::Uuid, _>("embedding_profile_id"),
                ))
                .model(r.get::<String, _>("model"))
                .dimensions(
                    u32::try_from(r.get::<i32, _>("dimensions")).expect(DIMENSIONS_OVERFLOW),
                )
                .embedding(to_f32_vec(&vector))
                .created_at(r.get("created_at"))
                .build()
        }))
    }
}
