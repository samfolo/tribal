use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Spec types
// ---------------------------------------------------------------------------

/// Specifies a relation edge in a relation response.
pub struct EdgeSpec {
    source: usize,
    target: usize,
    relation_type: String,
    justification: Option<String>,
}

// ---------------------------------------------------------------------------
// Convenience constructors
// ---------------------------------------------------------------------------

/// Creates an edge between two items by context index.
///
/// Argument order reads as a sentence: `relate(0, "supports", 1)`
/// → "item 0 supports item 1".
#[must_use]
pub fn relate(source: usize, relation_type: &str, target: usize) -> EdgeSpec {
    EdgeSpec {
        source,
        target,
        relation_type: relation_type.to_owned(),
        justification: None,
    }
}

impl EdgeSpec {
    /// Sets the justification for this edge.
    #[must_use]
    pub fn justification(mut self, justification: &str) -> Self {
        self.justification = Some(justification.to_owned());
        self
    }
}

fn target_json(index: usize) -> Value {
    json!({ "kind": "context_index", "context_index": index })
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Namespace for the relation fixture builder.
///
/// Produces `serde_json::Value` in the wire format expected by
/// `crates/tribal-worker/src/parsing/relation.rs`.
pub struct RelationFixture;

impl RelationFixture {
    #[must_use]
    pub fn builder() -> RelationFixtureBuilder {
        RelationFixtureBuilder { edges: Vec::new() }
    }
}

pub struct RelationFixtureBuilder {
    edges: Vec<EdgeSpec>,
}

impl RelationFixtureBuilder {
    /// Adds an edge to the relation response.
    #[must_use]
    pub fn edge(mut self, spec: EdgeSpec) -> Self {
        self.edges.push(spec);
        self
    }

    /// Produces the fixture as `serde_json::Value`.
    #[must_use]
    pub fn build(self) -> Value {
        let relations: Vec<Value> = self
            .edges
            .iter()
            .map(|e| {
                let mut val = json!({
                    "source": target_json(e.source),
                    "target": target_json(e.target),
                    "relation_type": e.relation_type,
                });
                if let Some(j) = &e.justification {
                    val["justification"] = json!(j);
                }
                val
            })
            .collect();

        json!({ "relations": relations })
    }
}
