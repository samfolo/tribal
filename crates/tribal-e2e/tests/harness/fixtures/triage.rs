use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Spec types
// ---------------------------------------------------------------------------

/// Specifies a similar item classification in a triage response, referencing
/// the similar item by its zero-based index in the prompt's numbered list.
pub struct SimilarItemSpec<'a> {
    pub context_index: u32,
    pub suggested_relation: &'a str,
    pub justification: &'a str,
}

// ---------------------------------------------------------------------------
// Convenience constructors
// ---------------------------------------------------------------------------

/// Creates a Novel triage fixture builder.
#[must_use]
pub fn novel() -> TriageFixtureBuilder {
    TriageFixtureBuilder {
        decision: "created".to_owned(),
        matched_index: None,
        similar_items: Vec::new(),
    }
}

/// Creates a Duplicate triage fixture builder referencing the similar item
/// at the given zero-based context index.
#[must_use]
pub fn duplicate(matched_index: u32) -> TriageFixtureBuilder {
    TriageFixtureBuilder {
        decision: "duplicate".to_owned(),
        matched_index: Some(matched_index),
        similar_items: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builds a triage stage fixture in the wire format expected by
/// `crates/tribal-worker/src/parsing/triage.rs`.
pub struct TriageFixtureBuilder {
    decision: String,
    matched_index: Option<u32>,
    similar_items: Vec<Value>,
}

impl TriageFixtureBuilder {
    /// Adds a similar item classification.
    #[must_use]
    pub fn similar_item(mut self, item: SimilarItemSpec<'_>) -> Self {
        self.similar_items.push(json!({
            "item": { "kind": "context_index", "context_index": item.context_index },
            "suggested_relation": item.suggested_relation,
            "justification": item.justification,
        }));
        self
    }

    /// Produces the fixture as `serde_json::Value`.
    #[must_use]
    pub fn build(self) -> Value {
        let mut outcome = json!({ "decision": self.decision });
        if let Some(index) = self.matched_index {
            outcome["matched_item"] = json!({ "kind": "context_index", "context_index": index });
        }

        json!({
            "outcome": outcome,
            "similar_item_decisions": self.similar_items,
        })
    }
}
