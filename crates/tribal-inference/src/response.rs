//! Response types for inference provider calls.
//!
//! All types have public fields and derive `Debug, Clone, PartialEq`.
//! Serialisation is intentionally omitted — it is a concern of concrete
//! provider implementations.

use crate::usage::{CompletionUsage, EmbeddingUsage};

/// The response from an LLM completion call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionResponse {
    /// The generated text.
    pub text: String,
    /// Token usage and latency for this call.
    pub usage: CompletionUsage,
}

/// The response from an embedding generation call.
///
/// Dimensionality is derived from `vector.len()` — there is no
/// separate field because no provider API reports it independently.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingResponse {
    /// The embedding vector.
    pub vector: Vec<f32>,
    /// Token usage and latency for this call.
    pub usage: EmbeddingUsage,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_completion_response_equality() {
        let usage = CompletionUsage {
            provider: "ollama".to_owned(),
            model: "llama3".to_owned(),
            input_tokens: 10,
            output_tokens: 5,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            total_tokens: 15,
            latency: Duration::from_millis(200),
        };
        let a = CompletionResponse {
            text: "hello".to_owned(),
            usage,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_embedding_response_equality() {
        let usage = EmbeddingUsage {
            provider: "ollama".to_owned(),
            model: "nomic-embed-text".to_owned(),
            total_tokens: 5,
            latency: Duration::from_millis(50),
        };
        let a = EmbeddingResponse {
            vector: vec![0.1, 0.2, 0.3],
            usage,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
