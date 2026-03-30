//! Relation types and the knowledge item relation entity.
//!
//! Relationships are append-only and batch-committed via `relation_batch_id`.
//! New understanding is expressed by adding new relations, never by modifying
//! or deleting existing ones.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use strum::EnumIter;
use typed_builder::TypedBuilder;

use crate::{KnowledgeItemId, PrincipalId, RelationBatchId, RelationId};

/// The type of a committed relationship between two knowledge items.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumIter)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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

enum_text_conversions!(RelationKind {
    RelationKind::Supports => "supports",
    RelationKind::Contradicts => "contradicts",
    RelationKind::Supersedes => "supersedes",
    RelationKind::DerivedFrom => "derived_from",
});

/// The triage agent's classification of a similar item before any
/// relationship is created.
///
/// Overlaps with [`RelationKind`] but is distinct: `Supersedes` and
/// `DerivedFrom` are never suggested by triage, and `Unrelated` is never
/// stored as a relation row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RelationSuggestion {
    /// Similar item corroborates the candidate.
    Supports,
    /// Similar item conflicts with the candidate.
    Contradicts,
    /// Semantic similarity is incidental; no meaningful relationship.
    Unrelated,
}

impl RelationSuggestion {
    /// All variants in display order.
    pub const ALL: &[Self] = &[Self::Supports, Self::Contradicts, Self::Unrelated];
}

enum_text_conversions!(RelationSuggestion {
    RelationSuggestion::Supports => "supports",
    RelationSuggestion::Contradicts => "contradicts",
    RelationSuggestion::Unrelated => "unrelated",
});

/// The type of an intra-batch relation hint emitted by the extraction agent.
///
/// Currently a single variant; the enum exists as an extension point for
/// future hint types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RelationHintType {
    /// Intra-batch derivation hint from the extraction agent.
    DerivedFrom,
}

/// A committed relationship between two knowledge items.
///
/// Relationships are append-only and batch-committed. `source_id` is the
/// item asserting the relationship (typically newer); `target_id` is the
/// item being referenced. Bidirectional by query, unidirectional by storage.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
pub struct KnowledgeItemRelation {
    /// Unique identifier with `rel_` prefix.
    id: RelationId,
    /// Groups relations from one relation task attempt.
    relation_batch_id: RelationBatchId,
    /// The item asserting the relationship (typically newer).
    source_id: KnowledgeItemId,
    /// The item being referenced.
    target_id: KnowledgeItemId,
    /// The type of relationship.
    relation_type: RelationKind,
    /// The principal who created this relation.
    principal_id: PrincipalId,
    /// The agent's reasoning for creating this relation.
    #[builder(default)]
    justification: Option<String>,
    /// When this relation was created.
    created_at: DateTime<Utc>,
}

impl KnowledgeItemRelation {
    /// Returns the relation identifier.
    pub fn id(&self) -> RelationId {
        self.id
    }

    /// Returns the relation batch identifier.
    pub fn relation_batch_id(&self) -> RelationBatchId {
        self.relation_batch_id
    }

    /// Returns the source item (asserting the relationship).
    pub fn source_id(&self) -> KnowledgeItemId {
        self.source_id
    }

    /// Returns the target item (being referenced).
    pub fn target_id(&self) -> KnowledgeItemId {
        self.target_id
    }

    /// Returns the relation type.
    pub fn relation_type(&self) -> RelationKind {
        self.relation_type
    }

    /// Returns the principal who created this relation.
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Returns the agent's reasoning for creating this relation.
    pub fn justification(&self) -> Option<&str> {
        self.justification.as_deref()
    }

    /// Returns when this relation was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_text_tests!(test_relation_kind_text_roundtrip, RelationKind {
        RelationKind::Supports => "supports",
        RelationKind::Contradicts => "contradicts",
        RelationKind::Supersedes => "supersedes",
        RelationKind::DerivedFrom => "derived_from",
    });

    enum_serde_tests!(test_relation_kind_serde_roundtrip, RelationKind {
        RelationKind::Supports => "supports",
        RelationKind::Contradicts => "contradicts",
        RelationKind::Supersedes => "supersedes",
        RelationKind::DerivedFrom => "derived_from",
    });

    enum_text_tests!(test_relation_suggestion_text_roundtrip, RelationSuggestion {
        RelationSuggestion::Supports => "supports",
        RelationSuggestion::Contradicts => "contradicts",
        RelationSuggestion::Unrelated => "unrelated",
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
