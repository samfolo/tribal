//! Source type classification for item observations.

use serde::{Deserialize, Serialize};

/// The classification of how a re-observation was captured.
///
/// Distinct from `SourceContext` (the rich discriminated union stored as
/// JSONB on knowledge items). `SourceType` is a flat enum classifying the
/// observation source on [`ItemObservation`](crate::ItemObservation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    /// Captured by an agent during a tool-mediated interaction.
    AgentMediated,
    /// Manually captured by a human user.
    ManualCapture,
    /// Derived from system processing (e.g., synthesis, re-indexing).
    Derived,
    /// Captured by a file-watching mechanism.
    FileWatch,
}

enum_text_conversions!(SourceType {
    SourceType::AgentMediated => "agent_mediated",
    SourceType::ManualCapture => "manual_capture",
    SourceType::Derived => "derived",
    SourceType::FileWatch => "file_watch",
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_text_tests!(test_source_type_text_roundtrip, SourceType {
        SourceType::AgentMediated => "agent_mediated",
        SourceType::ManualCapture => "manual_capture",
        SourceType::Derived => "derived",
        SourceType::FileWatch => "file_watch",
    });

    enum_serde_tests!(test_source_type_serde_roundtrip, SourceType {
        SourceType::AgentMediated => "agent_mediated",
        SourceType::ManualCapture => "manual_capture",
        SourceType::Derived => "derived",
        SourceType::FileWatch => "file_watch",
    });
}
