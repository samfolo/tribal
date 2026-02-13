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

    #[test]
    fn test_relation_kind_serde_roundtrip() {
        let variants = [
            (RelationKind::Supports, "\"supports\""),
            (RelationKind::Contradicts, "\"contradicts\""),
            (RelationKind::Supersedes, "\"supersedes\""),
            (RelationKind::DerivedFrom, "\"derived_from\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: RelationKind =
                serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_relation_suggestion_serde_roundtrip() {
        let variants = [
            (RelationSuggestion::Supports, "\"supports\""),
            (RelationSuggestion::Contradicts, "\"contradicts\""),
            (RelationSuggestion::Unrelated, "\"unrelated\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: RelationSuggestion =
                serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_relation_hint_type_serde_roundtrip() {
        let variants = [(RelationHintType::DerivedFrom, "\"derived_from\"")];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: RelationHintType =
                serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }
}
