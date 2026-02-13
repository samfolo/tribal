use serde::{Deserialize, Serialize};

/// The direction of graph traversal relative to the anchor item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Follows `target_id = anchor`; "what do others assert about this item?"
    Inbound,
    /// Follows `source_id = anchor`; "what does this item assert about others?"
    Outbound,
    /// Full neighbourhood in all directions.
    Both,
}

/// Optional fields to include in a semantic discovery response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryField {
    /// Compute evidential standing profile.
    Standing,
    /// Include attached references.
    References,
}

/// Optional fields to include in a graph exploration response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplorationField {
    /// Compute evidential standing profile.
    Standing,
    /// Include attached references.
    References,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_direction_serde_roundtrip() {
        let variants = [
            (Direction::Inbound, "\"inbound\""),
            (Direction::Outbound, "\"outbound\""),
            (Direction::Both, "\"both\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: Direction =
                serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_discovery_field_serde_roundtrip() {
        let variants = [
            (DiscoveryField::Standing, "\"standing\""),
            (DiscoveryField::References, "\"references\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: DiscoveryField =
                serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_exploration_field_serde_roundtrip() {
        let variants = [
            (ExplorationField::Standing, "\"standing\""),
            (ExplorationField::References, "\"references\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: ExplorationField =
                serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }
}
