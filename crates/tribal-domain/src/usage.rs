//! Token usage and latency types for inference calls.
//!
//! These types are produced by provider implementations alongside
//! responses and consumed downstream for ledger persistence as
//! [`TokenUsage`](crate::TokenUsage) records.

use std::time::Duration;

use crate::EmbeddingPurpose;

/// Token usage and latency for an LLM completion call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionUsage {
    /// The inference provider name (e.g. `"ollama"`, `"anthropic"`).
    pub provider: String,
    /// The model identifier.
    pub model: String,
    /// Total input tokens consumed, including cached tokens.
    pub input_tokens: u32,
    /// Number of output tokens generated.
    pub output_tokens: u32,
    /// Number of cache-read tokens (subset of `input_tokens`).
    pub cache_read_tokens: u32,
    /// Number of cache-write tokens.
    pub cache_write_tokens: u32,
    /// Total tokens (`input_tokens + output_tokens`).
    pub total_tokens: u32,
    /// Wall-clock latency of the provider call.
    pub latency: Duration,
}

/// Token usage and latency for an embedding generation call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingUsage {
    /// The inference provider name (e.g. `"ollama"`, `"anthropic"`).
    pub provider: String,
    /// The model identifier.
    pub model: String,
    /// Total tokens consumed.
    pub total_tokens: u32,
    /// Wall-clock latency of the provider call.
    pub latency: Duration,
}

/// Unified usage type spanning both completion and embedding calls.
///
/// Enables downstream code (e.g. token usage persistence) to handle
/// both usage shapes uniformly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Usage {
    /// Usage from an LLM completion call.
    Completion {
        /// The completion usage data.
        usage: CompletionUsage,
    },
    /// Usage from an embedding generation call.
    Embedding {
        /// The embedding usage data.
        usage: EmbeddingUsage,
        /// The domain-level purpose of this embedding call.
        purpose: EmbeddingPurpose,
    },
}

impl From<CompletionUsage> for Usage {
    fn from(usage: CompletionUsage) -> Self {
        Self::Completion { usage }
    }
}

impl From<(EmbeddingUsage, EmbeddingPurpose)> for Usage {
    fn from((usage, purpose): (EmbeddingUsage, EmbeddingPurpose)) -> Self {
        Self::Embedding { usage, purpose }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_completion_usage() -> CompletionUsage {
        CompletionUsage {
            provider: "ollama".to_owned(),
            model: "llama3".to_owned(),
            input_tokens: 100,
            output_tokens: 50,
            cache_read_tokens: 10,
            cache_write_tokens: 20,
            total_tokens: 150,
            latency: Duration::from_millis(500),
        }
    }

    fn an_embedding_usage() -> EmbeddingUsage {
        EmbeddingUsage {
            provider: "ollama".to_owned(),
            model: "nomic-embed-text".to_owned(),
            total_tokens: 25,
            latency: Duration::from_millis(80),
        }
    }

    #[test]
    fn test_completion_usage_equality() {
        let a = a_completion_usage();
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_embedding_usage_equality() {
        let a = an_embedding_usage();
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_from_completion_usage() {
        let usage = a_completion_usage();
        let unified = Usage::from(usage.clone());
        assert_eq!(unified, Usage::Completion { usage });
    }

    #[test]
    fn test_from_embedding_usage() {
        let usage = an_embedding_usage();
        let unified = Usage::from((usage.clone(), EmbeddingPurpose::Candidate));
        assert_eq!(
            unified,
            Usage::Embedding {
                usage,
                purpose: EmbeddingPurpose::Candidate,
            },
        );
    }
}
