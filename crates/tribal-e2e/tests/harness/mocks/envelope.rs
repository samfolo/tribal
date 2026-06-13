use serde_json::{Value, json};
use tribal_domain::ProviderKind;

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
// Streaming completion envelope
// ---------------------------------------------------------------------------

/// Frames a stage turn as the provider's native streaming wire body and
/// its content type. The agentic loop's only inference entry is the
/// streaming path, so a loop stage's mock must answer with the chunked
/// body the provider's accumulator folds, not the buffered envelope.
///
/// A fixture carrying a `tool_calls` array (each `{name, arguments}`)
/// frames a tool-call turn; any other fixture frames its JSON as the
/// assistant content, exactly as the buffered envelope does.
///
/// # Panics
///
/// Panics for providers other than Ollama: the agentic E2E runs the
/// Ollama wire (the harness default), and the SSE framing for the other
/// providers is already exercised by the inference-tier streaming tests,
/// so leaving it unbuilt here keeps the harness free of untested wire
/// code.
#[must_use]
pub fn wrap_completion_stream(fixture: &Value, provider: ProviderKind) -> (String, &'static str) {
    match provider {
        ProviderKind::Ollama => (ollama_stream_body(fixture), "application/x-ndjson"),
        ProviderKind::OpenAi | ProviderKind::Anthropic => panic!(
            "the agentic E2E streams the Ollama wire format; streaming framing for {provider:?} \
             is covered by the inference-tier tests and intentionally unbuilt in this harness",
        ),
    }
}

/// Builds the Ollama NDJSON body: one content-or-tool-call chunk, then a
/// terminal `done` chunk carrying the usage the accumulator reads.
fn ollama_stream_body(fixture: &Value) -> String {
    let message = if let Some(calls) = fixture.get("tool_calls").and_then(Value::as_array) {
        let tool_calls: Vec<Value> = calls
            .iter()
            .map(|call| {
                json!({
                    "function": {
                        "name": call.get("name").and_then(Value::as_str).unwrap_or_default(),
                        "arguments": call.get("arguments").cloned().unwrap_or(json!({})),
                    }
                })
            })
            .collect();
        json!({ "role": "assistant", "content": "", "tool_calls": tool_calls })
    } else {
        json!({ "role": "assistant", "content": fixture.to_string() })
    };
    let first = json!({ "model": "test", "message": message, "done": false });
    let done = json!({
        "model": "test",
        "message": { "role": "assistant", "content": "" },
        "done": true,
        "done_reason": "stop",
        "prompt_eval_count": 10,
        "eval_count": 5,
    });
    format!("{first}\n{done}\n")
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
        ProviderKind::Ollama => tribal_inference::OLLAMA_CHAT_PATH,
        ProviderKind::OpenAi => tribal_inference::OPENAI_CHAT_PATH,
        ProviderKind::Anthropic => tribal_inference::ANTHROPIC_MESSAGES_PATH,
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
        ProviderKind::Ollama => tribal_inference::OLLAMA_EMBED_PATH,
        ProviderKind::OpenAi => tribal_inference::OPENAI_EMBED_PATH,
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
        ProviderKind::Ollama => Some(tribal_inference::OLLAMA_TAGS_PATH),
        ProviderKind::OpenAi | ProviderKind::Anthropic => None,
    }
}

/// Returns a deterministic embedding vector of the given dimensionality.
#[must_use]
pub fn fixed_embedding_vector(dimensions: u32) -> Vec<f32> {
    (0..dimensions).map(|i| (i as f32 * 0.001).sin()).collect()
}
