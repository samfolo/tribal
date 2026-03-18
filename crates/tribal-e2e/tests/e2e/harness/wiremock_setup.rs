use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Embedding model name used by test fixtures.
pub const EMBEDDING_MODEL: &str = "nomic-embed-text:v1.5";

/// Extraction model name used by test fixtures.
pub const EXTRACTION_MODEL: &str = "llama3.1:70b";

/// Triage and relation model name used by test fixtures.
pub const SMALL_MODEL: &str = "llama3.1:8b";

/// Dimensionality of the embedding vector returned by mocks.
pub const EMBEDDING_DIMENSIONS: usize = 768;

// ---------------------------------------------------------------------------
// Tag mocks
// ---------------------------------------------------------------------------

/// Mounts a `GET /api/tags` handler returning the given model name.
pub async fn mount_tags_mock(server: &MockServer, model_name: &str) {
    let body = json!({ "models": [{ "name": model_name }] });
    Mock::given(method("GET"))
        .and(path("/api/tags"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

// ---------------------------------------------------------------------------
// Embedding mocks
// ---------------------------------------------------------------------------

/// Returns a deterministic 768-dimension embedding vector.
#[must_use]
pub fn fixed_embedding_vector() -> Vec<f32> {
    (0..EMBEDDING_DIMENSIONS)
        .map(|i| (i as f32 * 0.001).sin())
        .collect()
}

/// Mounts a `POST /api/embed` handler returning a fixed embedding vector.
pub async fn mount_embed_mock(server: &MockServer) {
    let body = json!({
        "embeddings": [fixed_embedding_vector()],
        "prompt_eval_count": 5,
        "total_duration": 100_000_000_u64,
        "load_duration": 10_000_000_u64,
    });
    Mock::given(method("POST"))
        .and(path("/api/embed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

// ---------------------------------------------------------------------------
// Chat mocks
// ---------------------------------------------------------------------------

/// Wraps stage output JSON inside an `Ollama` `/api/chat` response envelope.
fn chat_envelope(content: &Value) -> Value {
    json!({
        "message": {
            "role": "assistant",
            "content": content.to_string(),
        },
        "prompt_eval_count": 10,
        "eval_count": 5,
        "total_duration": 200_000_000_u64,
        "load_duration": 20_000_000_u64,
    })
}

/// Mounts a `POST /api/chat` handler returning a single canned response.
pub async fn mount_chat_mock(server: &MockServer, response_content: &Value) {
    Mock::given(method("POST"))
        .and(path("/api/chat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(chat_envelope(response_content)))
        .mount(server)
        .await;
}

/// Mounts a sequence of `POST /api/chat` responses consumed in order.
///
/// Uses `up_to_n_times(1)` with LIFO registration — the last-registered
/// mock is matched first, so `responses[0]` is consumed first.
pub async fn mount_chat_sequence(server: &MockServer, responses: &[Value]) {
    for response in responses.iter().rev() {
        Mock::given(method("POST"))
            .and(path("/api/chat"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_envelope(response)))
            .up_to_n_times(1)
            .mount(server)
            .await;
    }
}

// ---------------------------------------------------------------------------
// Stage response fixtures
// ---------------------------------------------------------------------------

/// Builds an extraction stage response with the given candidates.
///
/// Each candidate is `(kind, content, tags)`.
#[must_use]
pub fn extraction_response(
    candidates: &[(&str, &str, &[&str])],
    relation_hints: &[(usize, usize, &str)],
) -> Value {
    let candidates: Vec<Value> = candidates
        .iter()
        .map(|(kind, content, tags)| {
            json!({
                "kind": kind,
                "content": content,
                "suggested_tags": tags,
                "suggested_references": [],
            })
        })
        .collect();

    let hints: Vec<Value> = relation_hints
        .iter()
        .map(|(src, tgt, hint_type)| {
            json!({
                "source_index": src,
                "target_index": tgt,
                "hint_type": hint_type,
            })
        })
        .collect();

    json!({
        "candidates": candidates,
        "relation_hints": hints,
    })
}

/// Builds a Novel triage response.
#[must_use]
pub fn novel_triage_response() -> Value {
    json!({
        "outcome": { "decision": "created" },
        "similar_item_decisions": [],
    })
}

/// Builds a Duplicate triage response referencing an existing item.
#[must_use]
pub fn duplicate_triage_response(matched_item_id: &str) -> Value {
    json!({
        "outcome": {
            "decision": "duplicate",
            "matched_item_id": matched_item_id,
        },
        "similar_item_decisions": [],
    })
}

/// Builds a relation stage response with the given edges.
///
/// Each edge is `(source_batch_index, target_batch_index, relation_type)`.
#[must_use]
pub fn relation_response(edges: &[(usize, usize, &str)]) -> Value {
    let relations: Vec<Value> = edges
        .iter()
        .map(|(src, tgt, rel_type)| {
            json!({
                "source": { "kind": "batch_index", "batch_index": src },
                "target": { "kind": "batch_index", "batch_index": tgt },
                "relation_type": rel_type,
            })
        })
        .collect();

    json!({ "relations": relations })
}
