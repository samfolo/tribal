//! Embedding-call error classification.
//!
//! Partitions embedding failures by how the reindex worker should react. The
//! single source for both the `tribal-inference` retry classifier and the
//! `reindex_tasks.last_error_class` column; the string form drives the database
//! `CHECK`.

use serde::{Deserialize, Serialize};

/// How an embedding-call failure should be handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingErrorClass {
    /// HTTP 429: rate limited; respect `Retry-After`.
    RateLimited,
    /// HTTP 529: provider overloaded; back off and retry.
    Overloaded,
    /// 5xx, network, or timeout: retry with exponential backoff to a cap.
    Transient,
    /// 400 shape error, 401, 403, 404 model-not-found: not retried; quarantined.
    Permanent,
}

enum_text_conversions!(EmbeddingErrorClass {
    EmbeddingErrorClass::RateLimited => "rate_limited",
    EmbeddingErrorClass::Overloaded => "overloaded",
    EmbeddingErrorClass::Transient => "transient",
    EmbeddingErrorClass::Permanent => "permanent",
});

impl EmbeddingErrorClass {
    /// Returns `true` when a task of this class should be retried rather than
    /// quarantined.
    #[must_use]
    pub fn is_retryable(self) -> bool {
        match self {
            Self::RateLimited | Self::Overloaded | Self::Transient => true,
            Self::Permanent => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_embedding_error_class_serde_roundtrip, EmbeddingErrorClass {
        EmbeddingErrorClass::RateLimited => "rate_limited",
        EmbeddingErrorClass::Overloaded => "overloaded",
        EmbeddingErrorClass::Transient => "transient",
        EmbeddingErrorClass::Permanent => "permanent",
    });

    enum_text_tests!(test_embedding_error_class_text_roundtrip, EmbeddingErrorClass {
        EmbeddingErrorClass::RateLimited => "rate_limited",
        EmbeddingErrorClass::Overloaded => "overloaded",
        EmbeddingErrorClass::Transient => "transient",
        EmbeddingErrorClass::Permanent => "permanent",
    });

    #[test]
    fn test_permanent_is_not_retryable() {
        assert!(!EmbeddingErrorClass::Permanent.is_retryable());
        assert!(EmbeddingErrorClass::Transient.is_retryable());
        assert!(EmbeddingErrorClass::RateLimited.is_retryable());
        assert!(EmbeddingErrorClass::Overloaded.is_retryable());
    }
}
