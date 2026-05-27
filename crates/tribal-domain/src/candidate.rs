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

/// A single knowledge item extracted from the input.
///
/// The primary output of the extraction stage; each candidate is triaged
/// against existing knowledge before it is committed or discarded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Candidate {
    /// The kind of knowledge this item records.
    kind: KnowledgeKind,
    /// The captured tacit knowledge, as one self-contained claim.
    content: String,
    /// Categorisation tags for the item. Always present; an absent field is a
    /// parse error.
    suggested_tags: Vec<String>,
    /// External anchors the item refers to, used to connect it to related
    /// knowledge even when wording differs.
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

/// A suggested external reference for a candidate: an anchor (a URL, file path,
/// code symbol, or concept) that connects items even when their wording differs.
///
/// The `reference_type` is a free string; the LLM may produce values outside the
/// expected set, and mapping to [`ReferenceKind`] happens during triage, not
/// extraction.
///
/// [`ReferenceKind`]: crate::ReferenceKind
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "schema",
    derive(schemars::JsonSchema),
    schemars(
        description = "An external anchor a candidate refers to (a URL, file path, code symbol, or concept) that can connect items even when their wording differs."
    )
)]
pub struct SuggestedReference {
    /// What kind of anchor this is (e.g. `"url"`, `"file_path"`); free-form.
    reference_type: String,
    /// The anchor itself: the URL, path, symbol, or concept.
    value: String,
    /// Optional human-readable context for the anchor.
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
