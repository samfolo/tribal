//! Embedding purpose classification: what an embedding call served.

use serde::{Deserialize, Serialize};

/// The purpose of an embedding call.
///
/// Only applicable when the pipeline stage is `Embedding`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingPurpose {
    /// Embedding a candidate knowledge item for indexing.
    Candidate,
    /// Embedding a query for semantic search.
    Query,
    /// Embedding a tag for semantic tag resolution.
    Tag,
    /// Probing a provider: verifying reachability or resolving embedding
    /// geometry with a canonical input.
    Probe,
}

enum_text_conversions!(EmbeddingPurpose {
    EmbeddingPurpose::Candidate => "candidate",
    EmbeddingPurpose::Query => "query",
    EmbeddingPurpose::Tag => "tag",
    EmbeddingPurpose::Probe => "probe",
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_embedding_purpose_serde_roundtrip, EmbeddingPurpose {
        EmbeddingPurpose::Candidate => "candidate",
        EmbeddingPurpose::Query => "query",
        EmbeddingPurpose::Tag => "tag",
        EmbeddingPurpose::Probe => "probe",
    });

    enum_text_tests!(test_embedding_purpose_text_roundtrip, EmbeddingPurpose {
        EmbeddingPurpose::Candidate => "candidate",
        EmbeddingPurpose::Query => "query",
        EmbeddingPurpose::Tag => "tag",
        EmbeddingPurpose::Probe => "probe",
    });
}
