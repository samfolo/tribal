use serde::{Deserialize, Serialize};

/// The classification of a knowledge item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeKind {
    /// A discrete, verifiable piece of information.
    Fact,
    /// A rule of thumb or pattern derived from experience.
    Heuristic,
    /// A sequence of steps to accomplish something.
    Procedure,
    /// A record of a choice made and its rationale.
    DecisionRecord,
}

/// The confidence level of a knowledge item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// Manually confirmed or from an authoritative source.
    Verified,
    /// Extracted by a model from a conversation or document.
    Inferred,
    /// Flagged as potentially inaccurate or context-dependent.
    Uncertain,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_knowledge_kind_serde_roundtrip() {
        let variants = [
            (KnowledgeKind::Fact, "\"fact\""),
            (KnowledgeKind::Heuristic, "\"heuristic\""),
            (KnowledgeKind::Procedure, "\"procedure\""),
            (KnowledgeKind::DecisionRecord, "\"decision_record\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: KnowledgeKind =
                serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_confidence_serde_roundtrip() {
        let variants = [
            (Confidence::Verified, "\"verified\""),
            (Confidence::Inferred, "\"inferred\""),
            (Confidence::Uncertain, "\"uncertain\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: Confidence =
                serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }
}
