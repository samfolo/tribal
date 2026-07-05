use std::sync::Arc;

use serde_json::{Value, json};
use wiremock::Request;

use crate::harness::mocks::MockResponse;

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

/// How a similar item is located in the triage prompt's numbered list.
enum SimilarItemRef {
    /// A fixed zero-based position.
    Index(u32),
    /// A unique substring of the item's content, resolved to its position from
    /// the request body at serve time.
    Content(String),
}

struct SimilarItemEntry {
    item: SimilarItemRef,
    suggested_relation: String,
    justification: String,
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
    similar_items: Vec<SimilarItemEntry>,
}

impl TriageFixtureBuilder {
    /// Adds a similar item classification referenced by its zero-based index
    /// in the prompt's numbered list.
    #[must_use]
    pub fn similar_item(mut self, item: &SimilarItemSpec<'_>) -> Self {
        self.similar_items.push(SimilarItemEntry {
            item: SimilarItemRef::Index(item.context_index),
            suggested_relation: item.suggested_relation.to_owned(),
            justification: item.justification.to_owned(),
        });
        self
    }

    /// Adds a similar item referenced by a unique substring of its content
    /// rather than by position. The index is resolved from the triage request
    /// at serve time via [`build_resolved`](Self::build_resolved), so the
    /// fixture does not depend on the non-deterministic ordering of
    /// equal-distance semantic-search results.
    #[must_use]
    pub fn similar_item_by_content(
        mut self,
        content_substring: &str,
        suggested_relation: &str,
        justification: &str,
    ) -> Self {
        self.similar_items.push(SimilarItemEntry {
            item: SimilarItemRef::Content(content_substring.to_owned()),
            suggested_relation: suggested_relation.to_owned(),
            justification: justification.to_owned(),
        });
        self
    }

    /// Produces the fixture as `serde_json::Value`.
    ///
    /// # Panics
    ///
    /// Panics if any similar item was added via
    /// [`similar_item_by_content`](Self::similar_item_by_content): resolving a
    /// content reference needs the request body, so such fixtures must go
    /// through [`build_resolved`](Self::build_resolved).
    #[must_use]
    pub fn build(self) -> Value {
        let similar_items: Vec<Value> = self
            .similar_items
            .iter()
            .map(|entry| {
                let index = match &entry.item {
                    SimilarItemRef::Index(index) => *index,
                    SimilarItemRef::Content(substring) => panic!(
                        "similar_item_by_content({substring:?}) needs build_resolved(), not build()"
                    ),
                };
                similar_item_json(index, &entry.suggested_relation, &entry.justification)
            })
            .collect();
        triage_response(&self.decision, self.matched_index, &similar_items)
    }

    /// Produces a [`MockResponse`] whose content-referenced similar items are
    /// resolved to their numbered positions from the triage request body at
    /// serve time. Index-referenced items pass through unchanged.
    #[must_use]
    pub fn build_resolved(self) -> MockResponse {
        MockResponse::DynamicFixture(Arc::new(move |request: &Request| {
            let body = String::from_utf8_lossy(&request.body);
            let similar_items: Vec<Value> = self
                .similar_items
                .iter()
                .map(|entry| {
                    let index = match &entry.item {
                        SimilarItemRef::Index(index) => *index,
                        SimilarItemRef::Content(substring) => {
                            resolve_context_index(&body, substring)
                        }
                    };
                    similar_item_json(index, &entry.suggested_relation, &entry.justification)
                })
                .collect();
            triage_response(&self.decision, self.matched_index, &similar_items)
        }))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn similar_item_json(context_index: u32, suggested_relation: &str, justification: &str) -> Value {
    json!({
        "item": { "kind": "context_index", "context_index": context_index },
        "suggested_relation": suggested_relation,
        "justification": justification,
    })
}

fn triage_response(decision: &str, matched_index: Option<u32>, similar_items: &[Value]) -> Value {
    let mut outcome = json!({ "decision": decision });
    if let Some(index) = matched_index {
        outcome["matched_item"] = json!({ "kind": "context_index", "context_index": index });
    }
    json!({
        "outcome": outcome,
        "similar_item_decisions": similar_items,
    })
}

/// Resolves the zero-based index of the `### Item N` block whose content
/// contains `substring`, by scanning the rendered triage prompt in the request
/// body. The triage user prompt numbers each similar item as `### Item N`
/// (`crates/tribal-worker/src/prompt/triage.rs`), so the nearest such header
/// before the matched content gives its index.
///
/// # Panics
///
/// Panics if no item block contains `substring`, or its header cannot be
/// parsed — both indicate a test-setup error, not something to paper over.
fn resolve_context_index(body: &str, substring: &str) -> u32 {
    const HEADER: &str = "### Item ";
    let content_pos = body.find(substring).unwrap_or_else(|| {
        panic!("triage request body does not contain similar-item content {substring:?}")
    });
    let header_pos = body[..content_pos].rfind(HEADER).unwrap_or_else(|| {
        panic!("no '{HEADER}N' header precedes content {substring:?} in the triage request")
    });
    let digits: String = body[header_pos + HEADER.len()..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits
        .parse()
        .unwrap_or_else(|_| panic!("could not parse the index from the '{HEADER}' header"))
}
