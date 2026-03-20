use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Spec types
// ---------------------------------------------------------------------------

/// Specifies a candidate in an extraction response.
pub struct CandidateSpec {
    kind: String,
    content: String,
    tags: Vec<String>,
    references: Vec<Value>,
}

/// Specifies a suggested reference for a candidate.
pub struct ReferenceSpec<'a> {
    pub kind: &'a str,
    pub value: &'a str,
    pub description: Option<&'a str>,
}

/// Specifies a relation hint between candidates in the same batch.
pub struct RelationHintSpec {
    source: usize,
    target: usize,
    hint_type: String,
}

// ---------------------------------------------------------------------------
// Convenience constructors
// ---------------------------------------------------------------------------

/// Creates a candidate spec with the given kind and content.
#[must_use]
pub fn candidate(kind: &str, content: &str) -> CandidateSpec {
    CandidateSpec {
        kind: kind.to_owned(),
        content: content.to_owned(),
        tags: Vec::new(),
        references: Vec::new(),
    }
}

/// Creates a relation hint between two batch indices.
///
/// Argument order reads as a sentence: `hint(1, "derived_from", 0)`
/// → "batch item 1 derived_from batch item 0".
#[must_use]
pub fn hint(source: usize, hint_type: &str, target: usize) -> RelationHintSpec {
    RelationHintSpec {
        source,
        target,
        hint_type: hint_type.to_owned(),
    }
}

impl CandidateSpec {
    /// Sets the suggested tags for this candidate.
    #[must_use]
    pub fn tags(mut self, tags: &[&str]) -> Self {
        self.tags = tags.iter().map(|t| (*t).to_owned()).collect();
        self
    }

    /// Adds a suggested reference to this candidate.
    #[must_use]
    pub fn reference(mut self, r: ReferenceSpec<'_>) -> Self {
        let mut obj = json!({
            "reference_type": r.kind,
            "value": r.value,
        });
        if let Some(desc) = r.description {
            obj["description"] = json!(desc);
        }
        self.references.push(obj);
        self
    }

    fn to_json(&self) -> Value {
        json!({
            "kind": self.kind,
            "content": self.content,
            "suggested_tags": self.tags,
            "suggested_references": self.references,
        })
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Namespace for the extraction fixture builder.
///
/// Produces `serde_json::Value` in the wire format expected by
/// `crates/tribal-worker/src/parsing/extraction.rs`.
pub struct ExtractionFixture;

impl ExtractionFixture {
    #[must_use]
    pub fn builder() -> ExtractionFixtureBuilder {
        ExtractionFixtureBuilder {
            candidates: Vec::new(),
            relation_hints: Vec::new(),
        }
    }
}

pub struct ExtractionFixtureBuilder {
    candidates: Vec<CandidateSpec>,
    relation_hints: Vec<RelationHintSpec>,
}

impl ExtractionFixtureBuilder {
    /// Adds a candidate to the extraction response.
    #[must_use]
    pub fn candidate(mut self, spec: CandidateSpec) -> Self {
        self.candidates.push(spec);
        self
    }

    /// Adds a relation hint between candidates.
    #[must_use]
    pub fn relation_hint(mut self, spec: RelationHintSpec) -> Self {
        self.relation_hints.push(spec);
        self
    }

    /// Produces the fixture as `serde_json::Value`.
    #[must_use]
    pub fn build(self) -> Value {
        let candidates: Vec<Value> = self.candidates.iter().map(CandidateSpec::to_json).collect();
        let hints: Vec<Value> = self
            .relation_hints
            .iter()
            .map(|h| {
                json!({
                    "source_index": h.source,
                    "target_index": h.target,
                    "hint_type": h.hint_type,
                })
            })
            .collect();

        json!({
            "candidates": candidates,
            "relation_hints": hints,
        })
    }
}
