use tribal_db::NewTagEmbedding;
use tribal_domain::EmbeddingProfileId;

define_factory! {
    /// Factory for [`NewTagEmbedding`] instances.
    ///
    /// `embedding_profile_id` defaults to a fresh value; insert tests must
    /// override it with an existing profile's id to satisfy the foreign key.
    pub struct NewTagEmbeddingFactory for NewTagEmbedding {
        tag: String = "test-tag".to_owned(),
        embedding_profile_id: EmbeddingProfileId = EmbeddingProfileId::new(),
        model: String = "test-model".to_owned(),
        embedding: Vec<f32> = vec![0.1; 768],
    }
}

/// Returns a [`NewTagEmbeddingFactory`] with sensible defaults.
pub fn a_new_tag_embedding() -> NewTagEmbeddingFactory {
    NewTagEmbeddingFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let t = a_new_tag_embedding().build();
        assert_eq!(t.tag, "test-tag");
        assert_eq!(t.model, "test-model");
        assert_eq!(t.embedding.len(), 768);
    }
}
