//! Embedding entity — vector representations of knowledge items.
//!
//! Each row belongs to exactly one embedding profile and is written once
//! per `(knowledge_item, profile)`. The `model` is denormalised lineage;
//! the profile is the identity key. Reads filter by the active profile so a
//! model migration is a zero-downtime reindex.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{EmbeddingId, EmbeddingProfileId, KnowledgeItemId};

/// A vector embedding of a knowledge item's content.
///
/// Produced by the embedding model during triage and keyed by the profile
/// that produced it. `dimensions` is derived from the stored vector on read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
#[allow(clippy::struct_field_names)]
pub struct Embedding {
    /// Unique identifier with `emb_` prefix.
    id: EmbeddingId,
    /// The knowledge item this embedding represents.
    knowledge_item_id: KnowledgeItemId,
    /// The embedding profile that produced this vector.
    embedding_profile_id: EmbeddingProfileId,
    /// The embedding model name (denormalised lineage, e.g.
    /// `"nomic-embed-text:v1.5"`).
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

    /// Returns the embedding profile that produced this vector.
    pub fn embedding_profile_id(&self) -> EmbeddingProfileId {
        self.embedding_profile_id
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
