//! Response JSON fixtures shared across worker integration tests.

/// Builds an extraction-output JSON string from candidates and hints.
pub(super) fn extraction_response_json(
    candidates: &[tribal_domain::Candidate],
    hints: &[tribal_domain::RelationHint],
) -> String {
    serde_json::json!({
        "candidates": serde_json::to_value(candidates).unwrap(),
        "relation_hints": serde_json::to_value(hints).unwrap(),
    })
    .to_string()
}

/// Builds a triage Novel response JSON string.
pub(super) fn triage_novel_response_json() -> String {
    serde_json::json!({
        "outcome": { "decision": "created" },
        "similar_item_decisions": [],
    })
    .to_string()
}

/// Builds a triage Duplicate response JSON string referencing the similar
/// item at the given zero-based context index.
pub(super) fn triage_duplicate_response_json(matched_index: u32) -> String {
    serde_json::json!({
        "outcome": {
            "decision": "duplicate",
            "matched_item": { "kind": "context_index", "context_index": matched_index },
        },
        "similar_item_decisions": [],
    })
    .to_string()
}

/// Builds a relation response JSON string with `context_index`
/// targets — a `supports` edge from index 0 to each subsequent index.
pub(super) fn context_index_relation_response_json(batch_size: usize) -> String {
    let mut relations = Vec::new();

    for i in 1..batch_size {
        relations.push(serde_json::json!({
            "source": { "kind": "context_index", "context_index": 0 },
            "target": { "kind": "context_index", "context_index": i },
            "relation_type": "supports",
            "justification": "Test relation",
        }));
    }

    serde_json::json!({ "relations": relations }).to_string()
}
