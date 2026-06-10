//! The completed result of an LLM completion call.

use crate::CompletionUsage;

/// The response from an LLM completion call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionResponse {
    /// The generated text.
    pub text: String,
    /// Token usage and latency for this call.
    pub usage: CompletionUsage,
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
}
