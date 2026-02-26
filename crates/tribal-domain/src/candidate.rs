//! Candidate types — extracted knowledge item candidates and intra-batch
//! relation hints produced by the extraction stage.
//!
//! All types have private fields with accessors and are constructed via
//! serde deserialisation, not application code.

use serde::{Deserialize, Serialize};

use crate::{KnowledgeKind, RelationHintType};

// ---------------------------------------------------------------------------
// Candidate
// ---------------------------------------------------------------------------

/// A knowledge item candidate extracted from raw input.
///
/// Candidates are the primary output of the extraction stage. Each
/// candidate may progress through triage to become a committed
/// knowledge item, or be discarded as a duplicate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Candidate {
    /// Classification of the candidate knowledge.
    kind: KnowledgeKind,
    /// The knowledge content.
    content: String,
    /// Suggested categorisation tags. Always expected from the
    /// extraction prompt — an absent field is a parse error.
    suggested_tags: Vec<String>,
    /// Optional external references.
    #[serde(default)]
    suggested_references: Vec<SuggestedReference>,
}

impl Candidate {
    /// Returns the knowledge kind.
    pub fn kind(&self) -> KnowledgeKind {
        self.kind
    }

    /// Returns the knowledge content.
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Returns the suggested tags.
    pub fn suggested_tags(&self) -> &[String] {
        &self.suggested_tags
    }

    /// Returns the suggested references.
    pub fn suggested_references(&self) -> &[SuggestedReference] {
        &self.suggested_references
    }
}

// ---------------------------------------------------------------------------
// SuggestedReference
// ---------------------------------------------------------------------------

/// A suggested external reference for a candidate.
///
/// The `reference_type` is a free string — the LLM may produce values
/// outside the expected set. Validation and mapping to [`ReferenceKind`]
/// happens during triage, not extraction.
///
/// [`ReferenceKind`]: crate::ReferenceKind
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SuggestedReference {
    /// Free-form reference type (e.g. "url", "file_path").
    reference_type: String,
    /// The reference value.
    value: String,
    /// Optional human-readable context.
    #[serde(default)]
    description: Option<String>,
}

impl SuggestedReference {
    /// Returns the reference type.
    pub fn reference_type(&self) -> &str {
        &self.reference_type
    }

    /// Returns the reference value.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Returns the description, if present.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

// ---------------------------------------------------------------------------
// RelationHint
// ---------------------------------------------------------------------------

/// An intra-batch relation hint between two candidates by index.
///
/// Emitted by the extraction agent to indicate that one candidate
/// is derived from another within the same batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RelationHint {
    /// Index of the source candidate in the batch.
    source_index: u32,
    /// Index of the target candidate in the batch.
    target_index: u32,
    /// The type of relation hinted at.
    hint_type: RelationHintType,
}

impl RelationHint {
    /// Returns the source candidate index.
    pub fn source_index(&self) -> u32 {
        self.source_index
    }

    /// Returns the target candidate index.
    pub fn target_index(&self) -> u32 {
        self.target_index
    }

    /// Returns the relation hint type.
    pub fn hint_type(&self) -> RelationHintType {
        self.hint_type
    }
}
