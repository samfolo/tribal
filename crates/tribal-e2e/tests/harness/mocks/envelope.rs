use serde_json::{Value, json};
use tribal_config::ProviderKind;

// ---------------------------------------------------------------------------
// Completion envelope
// ---------------------------------------------------------------------------

/// Wraps stage content in the provider-specific chat response format.
#[must_use]
pub fn wrap_completion(content: &Value, provider: ProviderKind) -> Value {
    match provider {
        ProviderKind::Ollama => json!({
            "message": { "role": "assistant", "content": content.to_string() },
            "prompt_eval_count": 10,
            "eval_count": 5,
            "total_duration": 200_000_000_u64,
            "load_duration": 20_000_000_u64,
        }),
        ProviderKind::Anthropic => json!({
            "id": "msg_test",
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "text", "text": content.to_string() }],
            "model": "claude-haiku-4-5-20251001",
            "stop_reason": "end_turn",
            "usage": { "input_tokens": 10, "output_tokens": 5 },
        }),
        ProviderKind::OpenAi => json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "model": "gpt-4o-mini",
            "choices": [{ "index": 0, "message": {
                "role": "assistant",
                "content": content.to_string(),
            }, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15 },
        }),
    }
}

// ---------------------------------------------------------------------------
// Embedding envelope
// ---------------------------------------------------------------------------

/// Wraps an embedding vector in the provider-specific embed response format.
///
/// # Panics
///
/// Panics if `provider` is `Anthropic` — Anthropic does not provide an
/// embedding service.
#[must_use]
pub fn wrap_embedding(vector: &[f32], provider: ProviderKind) -> Value {
    match provider {
        ProviderKind::Ollama => json!({
            "embeddings": [vector],
            "prompt_eval_count": 5,
            "total_duration": 100_000_000_u64,
            "load_duration": 10_000_000_u64,
        }),
        ProviderKind::OpenAi => json!({
            "object": "list",
            "data": [{
                "object": "embedding",
                "embedding": vector,
                "index": 0,
            }],
            "model": "text-embedding-3-small",
            "usage": { "prompt_tokens": 5, "total_tokens": 5 },
        }),
        ProviderKind::Anthropic => {
            panic!("Anthropic does not provide an embedding service")
        }
    }
}

// ---------------------------------------------------------------------------
// Provider endpoint paths
// ---------------------------------------------------------------------------

/// Returns the chat endpoint path for the given provider.
#[must_use]
pub fn chat_path(provider: ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Ollama => "/api/chat",
        ProviderKind::OpenAi => "/v1/chat/completions",
        ProviderKind::Anthropic => "/v1/messages",
    }
}

/// Returns the embedding endpoint path for the given provider.
///
/// # Panics
///
/// Panics if `provider` is `Anthropic`.
#[must_use]
pub fn embed_path(provider: ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Ollama => "/api/embed",
        ProviderKind::OpenAi => "/v1/embeddings",
        ProviderKind::Anthropic => panic!("Anthropic does not provide an embedding service"),
    }
}

/// Returns the tags/models endpoint path for the given provider, if any.
///
/// `Ollama` has a dedicated model listing endpoint used for probing.
/// Other providers probe via their inference endpoint.
#[must_use]
pub fn tags_path(provider: ProviderKind) -> Option<&'static str> {
    match provider {
        ProviderKind::Ollama => Some("/api/tags"),
        ProviderKind::OpenAi | ProviderKind::Anthropic => None,
    }
}

/// Returns a deterministic embedding vector of the given dimensionality.
#[must_use]
pub fn fixed_embedding_vector(dimensions: u32) -> Vec<f32> {
    (0..dimensions).map(|i| (i as f32 * 0.001).sin()).collect()
}
