//! Relation stage: LLM-based relation extraction and commit.

use serde::Deserialize;
use tribal_domain::{KnowledgeItemId, RelationKind};
use tribal_inference::InferenceError;

use super::StageOutput;
use crate::{error::StageError, worker::Worker};

// ---------------------------------------------------------------------------
// LLM output types
// ---------------------------------------------------------------------------

/// The deserialised output from the relation LLM call.
///
/// Lenient serde — unknown fields are silently ignored so the LLM
/// can return extra keys without breaking parsing.
#[derive(Debug, Clone, PartialEq, Deserialize, schemars::JsonSchema)]
pub(crate) struct RelationOutput {
    /// The complete set of relations to create for this job.
    pub relations: Vec<RelationEdge>,
}

/// A single directed relationship edge to create.
#[derive(Debug, Clone, PartialEq, Deserialize, schemars::JsonSchema)]
pub(crate) struct RelationEdge {
    /// The source item (the item asserting the relationship).
    pub source: RelationTarget,
    /// The target item.
    pub target: RelationTarget,
    /// The relationship type.
    pub relation_type: RelationKind,
    /// The agent's reasoning for this relationship.
    #[serde(default)]
    pub justification: Option<String>,
}

/// Identifies one end of a relationship edge.
///
/// The relation agent may reference items by their batch index (for
/// candidates created in this episode) or by their `KnowledgeItemId`
/// (for existing items found during triage similarity search).
/// The worker resolves batch indices to `KnowledgeItemId`s via triage
/// results before persisting.
///
/// Uses `#[serde(tag = "kind")]` (internally tagged) for explicit
/// discrimination.
#[derive(Debug, Clone, PartialEq, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind")]
pub(crate) enum RelationTarget {
    /// A candidate from the current episode, identified by its
    /// position in the extraction candidates array.
    /// Wire format: `{"kind": "batch_index", "batch_index": 2}`
    #[serde(rename = "batch_index")]
    BatchIndex { batch_index: u32 },
    /// An existing knowledge item, identified by ID.
    /// Wire format: `{"kind": "item_id", "item_id": "ki_..."}`
    #[serde(rename = "item_id")]
    ItemId { item_id: KnowledgeItemId },
}

// ---------------------------------------------------------------------------
// Stage implementation
// ---------------------------------------------------------------------------

impl Worker {
    /// Runs the relation stage for a task.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Provider`] — stub always fails.
    #[allow(clippy::unused_async)]
    pub(crate) async fn run_relation(
        &self,
        _job: &tribal_domain::Job,
        _task: &tribal_domain::Task,
        _deadline: tokio::time::Instant,
    ) -> Result<StageOutput, StageError> {
        // Replaced by ticket 4.5
        Err(StageError::Provider {
            context: "relation stage not yet implemented".into(),
            source: InferenceError::ProviderUnavailable {
                provider: "stub".into(),
                reason: "relation stage not yet implemented".into(),
            },
        })
    }
}
