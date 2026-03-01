use tribal_db::NewTagEmbedding;

define_factory! {
    /// Factory for [`NewTagEmbedding`] instances.
    pub struct NewTagEmbeddingFactory for NewTagEmbedding {
        tag: String = "test-tag".to_owned(),
        model: String = "test-model".to_owned(),
        dimensions: u32 = 768,
        embedding: Vec<f32> = vec![0.0; 768],
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
        assert_eq!(t.dimensions, 768);
        assert_eq!(t.embedding.len(), 768);
    }
}
