//! Tag similarity result from pgvector semantic search.

/// A tag embedding similarity result from pgvector.
///
/// Returned by tag embedding similarity search during semantic tag
/// resolution. Includes the usage count for deterministic tie-breaking.
#[derive(Debug, Clone, PartialEq)]
pub struct TagSimilarityResult {
    /// The canonical tag string from the registry.
    tag: String,
    /// Cosine similarity score (-1.0–1.0).
    similarity: f64,
    /// Number of times this tag has been resolved to.
    usage_count: i32,
}

impl TagSimilarityResult {
    /// Creates a new tag similarity result.
    pub fn new(tag: String, similarity: f64, usage_count: i32) -> Self {
        Self {
            tag,
            similarity,
            usage_count,
        }
    }

    /// Returns the canonical tag string.
    pub fn tag(&self) -> &str {
        &self.tag
    }

    /// Returns the cosine similarity score.
    pub fn similarity(&self) -> f64 {
        self.similarity
    }

    /// Returns the usage count for this tag.
    pub fn usage_count(&self) -> i32 {
        self.usage_count
    }
}
