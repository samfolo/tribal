use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Spec types
// ---------------------------------------------------------------------------

/// Specifies a similar item classification in a triage response.
pub struct SimilarItemSpec<'a> {
    pub item_id: &'a str,
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
        matched_item_id: None,
        similar_items: Vec::new(),
    }
}

/// Creates a Duplicate triage fixture builder referencing an existing item.
#[must_use]
pub fn duplicate(matched_item_id: &str) -> TriageFixtureBuilder {
    TriageFixtureBuilder {
        decision: "duplicate".to_owned(),
        matched_item_id: Some(matched_item_id.to_owned()),
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
    matched_item_id: Option<String>,
    similar_items: Vec<Value>,
}

impl TriageFixtureBuilder {
    /// Adds a similar item classification.
    #[must_use]
    pub fn similar_item(mut self, item: SimilarItemSpec<'_>) -> Self {
        self.similar_items.push(json!({
            "item_id": item.item_id,
            "suggested_relation": item.suggested_relation,
            "justification": item.justification,
        }));
        self
    }

    /// Produces the fixture as `serde_json::Value`.
    #[must_use]
    pub fn build(self) -> Value {
        let mut outcome = json!({ "decision": self.decision });
        if let Some(id) = &self.matched_item_id {
            outcome["matched_item_id"] = json!(id);
        }

        json!({
            "outcome": outcome,
            "similar_item_decisions": self.similar_items,
        })
    }
}
