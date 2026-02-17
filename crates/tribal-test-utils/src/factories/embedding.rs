use chrono::Utc;
use tribal_db::NewEmbedding;
use tribal_domain::{Embedding, EmbeddingId, KnowledgeItemId};

define_factory! {
    /// Factory for [`Embedding`] instances.
    pub struct EmbeddingFactory for Embedding {
        id: EmbeddingId = EmbeddingId::new(),
        knowledge_item_id: KnowledgeItemId = KnowledgeItemId::new(),
        model: String = "nomic-embed-text:v1.5".to_owned(),
        dimensions: u32 = 384,
        embedding: Vec<f32> = vec![0.0; 384],
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns an [`EmbeddingFactory`] with sensible defaults.
pub fn a_embedding() -> EmbeddingFactory {
    EmbeddingFactory::new()
}

define_factory! {
    /// Factory for [`NewEmbedding`] instances used in repository insert operations.
    pub struct NewEmbeddingFactory for NewEmbedding {
        knowledge_item_id: KnowledgeItemId = KnowledgeItemId::new(),
        model: String = "nomic-embed-text:v1.5".to_owned(),
        dimensions: u32 = 768,
        embedding: Vec<f32> = vec![0.0; 768],
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
        let e = a_embedding().build();
        assert_eq!(e.model(), "nomic-embed-text:v1.5");
        assert_eq!(e.dimensions(), 384);
        assert_eq!(e.embedding().len(), 384);
    }

    #[test]
    fn test_new_embedding_builds_with_defaults() {
        let new = a_new_embedding().build();
        assert_eq!(new.model, "nomic-embed-text:v1.5");
        assert_eq!(new.dimensions, 768);
        assert_eq!(new.embedding.len(), 768);
    }
}
