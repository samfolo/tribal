use chrono::Utc;
use tribal_db::NewEmbedding;
use tribal_domain::{Embedding, EmbeddingId, EmbeddingProfileId, KnowledgeItemId};

define_factory! {
    /// Factory for [`Embedding`] instances.
    pub struct EmbeddingFactory for Embedding {
        id: EmbeddingId = EmbeddingId::new(),
        knowledge_item_id: KnowledgeItemId = KnowledgeItemId::new(),
        embedding_profile_id: EmbeddingProfileId = EmbeddingProfileId::new(),
        model: String = "nomic-embed-text:v1.5".to_owned(),
        dimensions: u32 = 768,
        embedding: Vec<f32> = vec![0.1; 768],
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns an [`EmbeddingFactory`] with sensible defaults.
pub fn an_embedding() -> EmbeddingFactory {
    EmbeddingFactory::new()
}

define_factory! {
    /// Factory for [`NewEmbedding`] instances used in repository insert operations.
    ///
    /// `embedding_profile_id` defaults to a fresh value; insert tests must
    /// override it with an existing profile's id to satisfy the foreign key.
    pub struct NewEmbeddingFactory for NewEmbedding {
        knowledge_item_id: KnowledgeItemId = KnowledgeItemId::new(),
        embedding_profile_id: EmbeddingProfileId = EmbeddingProfileId::new(),
        model: String = "nomic-embed-text:v1.5".to_owned(),
        embedding: Vec<f32> = vec![0.1; 768],
    }
}

/// Returns a [`NewEmbeddingFactory`] with sensible defaults.
pub fn a_new_embedding() -> NewEmbeddingFactory {
    NewEmbeddingFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let e = an_embedding().build();
        assert_eq!(e.model(), "nomic-embed-text:v1.5");
        assert_eq!(e.dimensions(), 768);
        assert_eq!(e.embedding().len(), 768);
    }

    #[test]
    fn test_new_embedding_builds_with_defaults() {
        let new = a_new_embedding().build();
        assert_eq!(new.model, "nomic-embed-text:v1.5");
        assert_eq!(new.embedding.len(), 768);
    }
}
