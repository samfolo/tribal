//! Embedding purpose classification.
//!
//! Distinguishes whether an embedding call was for indexing a candidate
//! knowledge item or for embedding a query during retrieval.

use serde::{Deserialize, Serialize};

/// The purpose of an embedding call.
///
/// Only applicable when the pipeline stage is `Embedding`. Distinguishes
/// write-path (indexing candidates) from read-path (embedding queries).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingPurpose {
    /// Embedding a candidate knowledge item for indexing.
    Candidate,
    /// Embedding a query for semantic search.
    Query,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enum_serde_tests;

    enum_serde_tests!(test_embedding_purpose_serde_roundtrip, EmbeddingPurpose {
        EmbeddingPurpose::Candidate => "candidate",
        EmbeddingPurpose::Query => "query",
    });
}
