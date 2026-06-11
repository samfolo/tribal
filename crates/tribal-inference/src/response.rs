//! Response types for embedding provider calls.
//!
//! Completion responses are the domain type
//! [`CompletionResponse`](tribal_domain::CompletionResponse); only the
//! embedding response shape is provider-layer-specific. Serialisation is
//! intentionally omitted — it is a concern of concrete provider
//! implementations.

use tribal_domain::EmbeddingUsage;

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
