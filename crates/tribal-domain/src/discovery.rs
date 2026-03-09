use serde::{Deserialize, Serialize};
use strum::EnumIter;

/// The direction of graph traversal relative to the anchor item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumIter)]
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
    use crate::enum_serde_tests;

    enum_serde_tests!(test_direction_serde_roundtrip, Direction {
        Direction::Inbound => "inbound",
        Direction::Outbound => "outbound",
        Direction::Both => "both",
    });

    enum_serde_tests!(test_discovery_field_serde_roundtrip, DiscoveryField {
        DiscoveryField::Standing => "standing",
        DiscoveryField::References => "references",
    });

    enum_serde_tests!(test_exploration_field_serde_roundtrip, ExplorationField {
        ExplorationField::Standing => "standing",
        ExplorationField::References => "references",
    });
}
