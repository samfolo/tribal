//! Knowledge item types — classification, confidence, and the core entity.
//!
//! Knowledge items are immutable and append-only. No `updated_at` field.
//! Contradictions and evolving understanding are expressed through the
//! relation table, not by mutating existing items. An item's standing
//! is derived from the relationship graph at query time.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{EpisodeId, KnowledgeItemId, PrincipalId, ProjectId};

/// The classification of a knowledge item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "schema",
    derive(schemars::JsonSchema),
    schemars(description = "The kind of knowledge captured.")
)]
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

enum_text_conversions!(KnowledgeKind {
    KnowledgeKind::Fact => "fact",
    KnowledgeKind::Heuristic => "heuristic",
    KnowledgeKind::Procedure => "procedure",
    KnowledgeKind::DecisionRecord => "decision_record",
});

/// The confidence level of a knowledge item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// Manually confirmed or from an authoritative source.
    Verified,
    /// Extracted by a model from a conversation or document.
    Inferred,
    /// Flagged as potentially inaccurate or context-dependent.
    Uncertain,
}

enum_text_conversions!(Confidence {
    Confidence::Verified => "verified",
    Confidence::Inferred => "inferred",
    Confidence::Uncertain => "uncertain",
});

/// A knowledge item stored in the Tribal knowledge graph.
///
/// Knowledge items are immutable and append-only. Contradiction resolution
/// happens at query time, not at storage time.
///
/// # Invariants
///
/// - `content` is always non-empty.
/// - `principal_id` identifies who or what created this item.
/// - `source_context` is stored as opaque JSONB — no control flow depends
///   on its contents.
/// - `claim_context` is reserved and always `None`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct KnowledgeItem {
    /// Unique identifier with `ki_` prefix.
    id: KnowledgeItemId,
    /// The project this item belongs to.
    project_id: ProjectId,
    /// The identity (user or agent) that created this item.
    principal_id: PrincipalId,
    /// Classification of this knowledge.
    kind: KnowledgeKind,
    /// The knowledge content. Always non-empty.
    content: String,
    /// Free-form tags for categorisation. GIN-indexed in Postgres.
    #[builder(default)]
    tags: Vec<String>,
    /// Confidence level of this item.
    confidence: Confidence,
    /// Reserved for structured scope qualifiers (always `None`).
    #[builder(default)]
    claim_context: Option<serde_json::Value>,
    /// Discriminated union per source type (opaque JSONB).
    source_context: serde_json::Value,
    /// Groups co-extracted items (no Episode table — correlation key only).
    #[builder(default)]
    episode_id: Option<EpisodeId>,
    /// Git commit SHA anchoring this item to a point in time.
    #[builder(default)]
    capture_commit: Option<String>,
    /// Branch name (informational only — branches are ephemeral).
    #[builder(default)]
    capture_branch: Option<String>,
    /// When this item was created. Set once, never updated.
    created_at: DateTime<Utc>,
}

impl KnowledgeItem {
    /// Returns the knowledge item identifier.
    pub fn id(&self) -> KnowledgeItemId {
        self.id
    }

    /// Returns the project identifier.
    pub fn project_id(&self) -> ProjectId {
        self.project_id
    }

    /// Returns the principal who created this item.
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Returns the knowledge kind.
    pub fn kind(&self) -> KnowledgeKind {
        self.kind
    }

    /// Returns the knowledge content.
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Returns the tags.
    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    /// Returns the confidence level.
    pub fn confidence(&self) -> Confidence {
        self.confidence
    }

    /// Returns the claim context, if set.
    pub fn claim_context(&self) -> Option<&serde_json::Value> {
        self.claim_context.as_ref()
    }

    /// Returns the source context (opaque JSONB).
    pub fn source_context(&self) -> &serde_json::Value {
        &self.source_context
    }

    /// Returns the episode identifier, if set.
    pub fn episode_id(&self) -> Option<EpisodeId> {
        self.episode_id
    }

    /// Returns the capture commit SHA, if set.
    pub fn capture_commit(&self) -> Option<&str> {
        self.capture_commit.as_deref()
    }

    /// Returns the capture branch, if set.
    pub fn capture_branch(&self) -> Option<&str> {
        self.capture_branch.as_deref()
    }

    /// Returns when this item was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_knowledge_kind_serde_roundtrip, KnowledgeKind {
        KnowledgeKind::Fact => "fact",
        KnowledgeKind::Heuristic => "heuristic",
        KnowledgeKind::Procedure => "procedure",
        KnowledgeKind::DecisionRecord => "decision_record",
    });

    enum_serde_tests!(test_confidence_serde_roundtrip, Confidence {
        Confidence::Verified => "verified",
        Confidence::Inferred => "inferred",
        Confidence::Uncertain => "uncertain",
    });

    enum_text_tests!(test_knowledge_kind_text_roundtrip, KnowledgeKind {
        KnowledgeKind::Fact => "fact",
        KnowledgeKind::Heuristic => "heuristic",
        KnowledgeKind::Procedure => "procedure",
        KnowledgeKind::DecisionRecord => "decision_record",
    });

    enum_text_tests!(test_confidence_text_roundtrip, Confidence {
        Confidence::Verified => "verified",
        Confidence::Inferred => "inferred",
        Confidence::Uncertain => "uncertain",
    });
}
