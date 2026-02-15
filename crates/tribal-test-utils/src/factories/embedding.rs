use chrono::Utc;
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
}
