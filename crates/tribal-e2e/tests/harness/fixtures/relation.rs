use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Spec types
// ---------------------------------------------------------------------------

/// Specifies a relation edge in a relation response.
pub struct EdgeSpec {
    source: RelationTargetSpec,
    target: RelationTargetSpec,
    relation_type: String,
    justification: Option<String>,
}

enum RelationTargetSpec {
    BatchIndex(usize),
    ItemId(String),
}

// ---------------------------------------------------------------------------
// Convenience constructors
// ---------------------------------------------------------------------------

/// Creates an edge between two candidates in the same batch.
///
/// Argument order reads as a sentence: `intra_batch(0, "supports", 1)`
/// → "batch item 0 supports batch item 1".
#[must_use]
pub fn intra_batch(source: usize, relation_type: &str, target: usize) -> EdgeSpec {
    EdgeSpec {
        source: RelationTargetSpec::BatchIndex(source),
        target: RelationTargetSpec::BatchIndex(target),
        relation_type: relation_type.to_owned(),
        justification: None,
    }
}

/// Creates an edge from a batch candidate to an existing knowledge item.
///
/// Argument order reads as a sentence: `to_existing(0, "supports", id)`
/// → "batch item 0 supports existing item".
#[must_use]
pub fn to_existing(source_index: usize, relation_type: &str, target_id: &str) -> EdgeSpec {
    EdgeSpec {
        source: RelationTargetSpec::BatchIndex(source_index),
        target: RelationTargetSpec::ItemId(target_id.to_owned()),
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

impl RelationTargetSpec {
    fn to_json(&self) -> Value {
        match self {
            Self::BatchIndex(i) => json!({ "kind": "batch_index", "batch_index": i }),
            Self::ItemId(id) => json!({ "kind": "item_id", "item_id": id }),
        }
    }
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
                let mut edge = json!({
                    "source": e.source.to_json(),
                    "target": e.target.to_json(),
                    "relation_type": e.relation_type,
                });
                if let Some(j) = &e.justification {
                    edge["justification"] = json!(j);
                }
                edge
            })
            .collect();

        json!({ "relations": relations })
    }
}
