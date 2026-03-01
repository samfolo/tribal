//! Embedding purpose classification.
//!
//! Distinguishes whether an embedding call was for indexing a candidate
//! knowledge item, embedding a query during retrieval, or embedding a
//! tag for semantic tag resolution.

use serde::{Deserialize, Serialize};

/// The purpose of an embedding call.
///
/// Only applicable when the pipeline stage is `Embedding`. Distinguishes
/// write-path (indexing candidates), read-path (embedding queries), and
/// tag resolution (embedding tags for semantic matching).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingPurpose {
    /// Embedding a candidate knowledge item for indexing.
    Candidate,
    /// Embedding a query for semantic search.
    Query,
    /// Embedding a tag for semantic tag resolution.
    Tag,
}

enum_text_conversions!(EmbeddingPurpose {
    EmbeddingPurpose::Candidate => "candidate",
    EmbeddingPurpose::Query => "query",
    EmbeddingPurpose::Tag => "tag",
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_embedding_purpose_serde_roundtrip, EmbeddingPurpose {
        EmbeddingPurpose::Candidate => "candidate",
        EmbeddingPurpose::Query => "query",
        EmbeddingPurpose::Tag => "tag",
    });

    enum_text_tests!(test_embedding_purpose_text_roundtrip, EmbeddingPurpose {
        EmbeddingPurpose::Candidate => "candidate",
        EmbeddingPurpose::Query => "query",
        EmbeddingPurpose::Tag => "tag",
    });
}
