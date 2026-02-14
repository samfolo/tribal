use serde::{Deserialize, Serialize};

/// The type of a committed relationship between two knowledge items.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationKind {
    /// Source provides evidence for, validates, or reinforces target.
    Supports,
    /// Source challenges, undermines, or conflicts with target; both items
    /// may remain valid in different contexts.
    Contradicts,
    /// Source replaces target with updated understanding; stronger than
    /// `Contradicts` — the old item is retired.
    Supersedes,
    /// Source was produced using target as an input to its formulation;
    /// exists to support traceability, not reliability.
    DerivedFrom,
}

/// The triage agent's classification of a similar item before any
/// relationship is created.
///
/// Overlaps with [`RelationKind`] but is distinct: `Supersedes` and
/// `DerivedFrom` are never suggested by triage, and `Unrelated` is never
/// stored as a relation row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationSuggestion {
    /// Similar item corroborates the candidate.
    Supports,
    /// Similar item conflicts with the candidate.
    Contradicts,
    /// Semantic similarity is incidental; no meaningful relationship.
    Unrelated,
}

/// The type of an intra-batch relation hint emitted by the extraction agent.
///
/// Currently a single variant; the enum exists as an extension point for
/// future hint types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationHintType {
    /// Intra-batch derivation hint from the extraction agent.
    DerivedFrom,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enum_serde_tests;

    enum_serde_tests!(test_relation_kind_serde_roundtrip, RelationKind {
        RelationKind::Supports => "supports",
        RelationKind::Contradicts => "contradicts",
        RelationKind::Supersedes => "supersedes",
        RelationKind::DerivedFrom => "derived_from",
    });

    enum_serde_tests!(test_relation_suggestion_serde_roundtrip, RelationSuggestion {
        RelationSuggestion::Supports => "supports",
        RelationSuggestion::Contradicts => "contradicts",
        RelationSuggestion::Unrelated => "unrelated",
    });

    enum_serde_tests!(test_relation_hint_type_serde_roundtrip, RelationHintType {
        RelationHintType::DerivedFrom => "derived_from",
    });
}
