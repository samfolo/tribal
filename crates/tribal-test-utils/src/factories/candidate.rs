//! Factories for extraction candidate and relation hint types.
//!
//! These are hand-written (not `define_factory!`) because `Candidate`
//! and `RelationHint` have private fields and no `TypedBuilder` —
//! they are constructed via serde deserialisation only.

use tribal_domain::{Candidate, KnowledgeKind, RelationHint, RelationHintType};

// ---------------------------------------------------------------------------
// CandidateFactory
// ---------------------------------------------------------------------------

/// Factory for [`Candidate`] instances.
#[derive(Debug, Clone)]
pub struct CandidateFactory {
    kind: KnowledgeKind,
    content: String,
    suggested_tags: Vec<String>,
    suggested_references: Vec<serde_json::Value>,
}

impl Default for CandidateFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl CandidateFactory {
    /// Creates a new factory with sensible defaults.
    pub fn new() -> Self {
        Self {
            kind: KnowledgeKind::Fact,
            content: "test candidate content".to_owned(),
            suggested_tags: vec!["test-tag".to_owned()],
            suggested_references: vec![],
        }
    }

    /// Overrides the knowledge kind.
    #[must_use]
    pub fn kind(mut self, value: KnowledgeKind) -> Self {
        self.kind = value;
        self
    }

    /// Overrides the content.
    #[must_use]
    pub fn content(mut self, value: String) -> Self {
        self.content = value;
        self
    }

    /// Overrides the suggested tags.
    #[must_use]
    pub fn suggested_tags(mut self, value: Vec<String>) -> Self {
        self.suggested_tags = value;
        self
    }

    /// Overrides the suggested references.
    #[must_use]
    pub fn suggested_references(mut self, value: Vec<serde_json::Value>) -> Self {
        self.suggested_references = value;
        self
    }

    /// Builds a [`Candidate`] from the current factory state.
    ///
    /// # Panics
    ///
    /// Panics if the assembled JSON cannot be deserialised into a
    /// `Candidate`.
    pub fn build(self) -> Candidate {
        serde_json::from_value(serde_json::json!({
            "kind": self.kind,
            "content": self.content,
            "suggested_tags": self.suggested_tags,
            "suggested_references": self.suggested_references,
        }))
        .expect("CandidateFactory produces valid JSON")
    }
}

/// Returns a [`CandidateFactory`] with sensible defaults.
pub fn a_candidate() -> CandidateFactory {
    CandidateFactory::new()
}

// ---------------------------------------------------------------------------
// RelationHintFactory
// ---------------------------------------------------------------------------

/// Factory for [`RelationHint`] instances.
#[derive(Debug, Clone)]
pub struct RelationHintFactory {
    source_index: u32,
    target_index: u32,
    hint_type: RelationHintType,
}

impl Default for RelationHintFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RelationHintFactory {
    /// Creates a new factory with sensible defaults.
    pub fn new() -> Self {
        Self {
            source_index: 0,
            target_index: 1,
            hint_type: RelationHintType::DerivedFrom,
        }
    }

    /// Overrides the source index.
    #[must_use]
    pub fn source_index(mut self, value: u32) -> Self {
        self.source_index = value;
        self
    }

    /// Overrides the target index.
    #[must_use]
    pub fn target_index(mut self, value: u32) -> Self {
        self.target_index = value;
        self
    }

    /// Overrides the hint type.
    #[must_use]
    pub fn hint_type(mut self, value: RelationHintType) -> Self {
        self.hint_type = value;
        self
    }

    /// Builds a [`RelationHint`] from the current factory state.
    ///
    /// # Panics
    ///
    /// Panics if the assembled JSON cannot be deserialised into a
    /// `RelationHint`.
    pub fn build(self) -> RelationHint {
        serde_json::from_value(serde_json::json!({
            "source_index": self.source_index,
            "target_index": self.target_index,
            "hint_type": self.hint_type,
        }))
        .expect("RelationHintFactory produces valid JSON")
    }
}

/// Returns a [`RelationHintFactory`] with sensible defaults.
pub fn a_relation_hint() -> RelationHintFactory {
    RelationHintFactory::new()
}

// ---------------------------------------------------------------------------
// Serialisation helpers
// ---------------------------------------------------------------------------

/// Serialises a slice of candidates to a JSON value.
///
/// # Panics
///
/// Panics if serialisation fails.
pub fn candidates_json(candidates: &[Candidate]) -> serde_json::Value {
    serde_json::to_value(candidates).expect("candidates serialise to JSON")
}

/// Serialises a slice of relation hints to a JSON value.
///
/// # Panics
///
/// Panics if serialisation fails.
pub fn relation_hints_json(hints: &[RelationHint]) -> serde_json::Value {
    serde_json::to_value(hints).expect("relation hints serialise to JSON")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candidate_builds_with_defaults() {
        let candidate = a_candidate().build();
        assert_eq!(candidate.kind(), KnowledgeKind::Fact);
        assert_eq!(candidate.content(), "test candidate content");
        assert_eq!(candidate.suggested_tags(), &["test-tag".to_owned()]);
        assert!(candidate.suggested_references().is_empty());
    }

    #[test]
    fn test_relation_hint_builds_with_defaults() {
        let hint = a_relation_hint().build();
        assert_eq!(hint.source_index(), 0);
        assert_eq!(hint.target_index(), 1);
        assert_eq!(hint.hint_type(), RelationHintType::DerivedFrom);
    }

    #[test]
    fn test_candidates_json_serialises_slice() {
        let candidates = vec![a_candidate().build()];
        let json = candidates_json(&candidates);
        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_relation_hints_json_serialises_slice() {
        let hints = vec![a_relation_hint().build()];
        let json = relation_hints_json(&hints);
        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 1);
    }
}
