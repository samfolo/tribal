//! Embedding entity — vector representations of knowledge items.
//!
//! Multiple embeddings per item are allowed (different models). The
//! system config has an `active_embedding_model`; all queries filter
//! by the active model to enable zero-downtime migration.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{EmbeddingId, KnowledgeItemId};

/// A vector embedding of a knowledge item's content.
///
/// Produced by the embedding model during triage. Multiple embeddings
/// per item are supported for model migration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct Embedding {
    /// Unique identifier with `emb_` prefix.
    id: EmbeddingId,
    /// The knowledge item this embedding represents.
    knowledge_item_id: KnowledgeItemId,
    /// The embedding model name (e.g. `"nomic-embed-text:v1.5"`).
    model: String,
    /// The number of dimensions in the embedding vector.
    dimensions: u32,
    /// The embedding vector.
    embedding: Vec<f32>,
    /// When this embedding was created.
    created_at: DateTime<Utc>,
}

impl Embedding {
    /// Returns the embedding identifier.
    pub fn id(&self) -> EmbeddingId {
        self.id
    }

    /// Returns the knowledge item this embedding represents.
    pub fn knowledge_item_id(&self) -> KnowledgeItemId {
        self.knowledge_item_id
    }

    /// Returns the embedding model name.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns the number of dimensions.
    pub fn dimensions(&self) -> u32 {
        self.dimensions
    }

    /// Returns the embedding vector.
    pub fn embedding(&self) -> &[f32] {
        &self.embedding
    }

    /// Returns when this embedding was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
