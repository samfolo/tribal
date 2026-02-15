//! Triage entities — results, outcomes, similar items, and decisions.
//!
//! Triage classifies each candidate from extraction as novel, duplicate,
//! or failed. Results are stored as a struct with shared fields plus a
//! discriminated `TriageOutcome` enum. Similar-item decisions record
//! the agent's per-candidate reasoning for audit purposes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{
    ItemObservationId, JobId, KnowledgeItemId, RelationSuggestion, TriageResultId,
    TriageSimilarItemDecisionId,
};

// ---------------------------------------------------------------------------
// SimilarItem
// ---------------------------------------------------------------------------

/// A similar existing item found during triage semantic search.
///
/// Referenced as a repeated field on [`TriageResult`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimilarItem {
    /// The existing knowledge item that was found similar.
    item_id: KnowledgeItemId,
    /// Cosine similarity score.
    similarity_score: f32,
    /// The triage agent's suggested relation to the candidate.
    suggested_relation: RelationSuggestion,
}

impl SimilarItem {
    /// Creates a new similar item.
    pub fn new(
        item_id: KnowledgeItemId,
        similarity_score: f32,
        suggested_relation: RelationSuggestion,
    ) -> Self {
        Self {
            item_id,
            similarity_score,
            suggested_relation,
        }
    }

    /// Returns the similar item's identifier.
    pub fn item_id(&self) -> KnowledgeItemId {
        self.item_id
    }

    /// Returns the cosine similarity score.
    pub fn similarity_score(&self) -> f32 {
        self.similarity_score
    }

    /// Returns the suggested relation.
    pub fn suggested_relation(&self) -> RelationSuggestion {
        self.suggested_relation
    }
}

// ---------------------------------------------------------------------------
// TriageOutcome
// ---------------------------------------------------------------------------

/// The outcome of triaging a single candidate.
///
/// Discriminated union with struct variants carrying outcome-specific fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum TriageOutcome {
    /// Candidate was novel — a new knowledge item was created.
    Created {
        /// The newly created knowledge item.
        item_id: KnowledgeItemId,
    },
    /// Candidate was a duplicate of an existing item.
    Duplicate {
        /// The observation record linking to the existing item.
        observation_id: ItemObservationId,
        /// The existing item the candidate matched.
        matched_item_id: KnowledgeItemId,
    },
    /// Triage failed for this candidate.
    Failed {
        /// Description of the failure.
        error_message: String,
        /// Whether the failure is retryable.
        retryable: bool,
    },
}

// ---------------------------------------------------------------------------
// TriageResult
// ---------------------------------------------------------------------------

/// The result of triaging a single candidate in a batch.
///
/// Contains shared fields (identity, batch position, similar items found)
/// plus a discriminated [`TriageOutcome`] carrying outcome-specific data.
/// Idempotency is ensured by checking `(job_id, batch_index)` uniqueness.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct TriageResult {
    /// Unique identifier with `tr_` prefix.
    id: TriageResultId,
    /// The job this result belongs to.
    job_id: JobId,
    /// The candidate's position in the extraction batch.
    batch_index: u32,
    /// Similar existing items found during semantic search.
    #[builder(default)]
    similar_items: Vec<SimilarItem>,
    /// When this result was created.
    created_at: DateTime<Utc>,
    /// The triage outcome for this candidate.
    outcome: TriageOutcome,
}

impl TriageResult {
    /// Returns the triage result identifier.
    pub fn id(&self) -> TriageResultId {
        self.id
    }

    /// Returns the job identifier.
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Returns the batch index.
    pub fn batch_index(&self) -> u32 {
        self.batch_index
    }

    /// Returns the similar items found during triage.
    pub fn similar_items(&self) -> &[SimilarItem] {
        &self.similar_items
    }

    /// Returns when this result was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns the triage outcome.
    pub fn outcome(&self) -> &TriageOutcome {
        &self.outcome
    }
}

// ---------------------------------------------------------------------------
// TriageSimilarItemDecision
// ---------------------------------------------------------------------------

/// A triage agent's decision about a similar existing item.
///
/// Records the agent's reasoning for each `(candidate, similar_item)` pair.
/// Forms an audit chain: relation row -> `(job_id, batch_index)` ->
/// decisions -> justification -> job -> source context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct TriageSimilarItemDecision {
    /// Unique identifier with `tsd_` prefix.
    id: TriageSimilarItemDecisionId,
    /// The job this decision belongs to.
    job_id: JobId,
    /// The candidate's position in the extraction batch.
    batch_index: u32,
    /// The existing item that was compared against.
    matched_item_id: KnowledgeItemId,
    /// Cosine similarity score between candidate and matched item.
    similarity_score: f32,
    /// The agent's suggested relation classification.
    suggested_relation: RelationSuggestion,
    /// The agent's reasoning for the classification.
    justification_text: String,
    /// When this decision was recorded.
    created_at: DateTime<Utc>,
}

impl TriageSimilarItemDecision {
    /// Returns the decision identifier.
    pub fn id(&self) -> TriageSimilarItemDecisionId {
        self.id
    }

    /// Returns the job identifier.
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Returns the batch index.
    pub fn batch_index(&self) -> u32 {
        self.batch_index
    }

    /// Returns the matched item identifier.
    pub fn matched_item_id(&self) -> KnowledgeItemId {
        self.matched_item_id
    }

    /// Returns the cosine similarity score.
    pub fn similarity_score(&self) -> f32 {
        self.similarity_score
    }

    /// Returns the suggested relation classification.
    pub fn suggested_relation(&self) -> RelationSuggestion {
        self.suggested_relation
    }

    /// Returns the justification text.
    pub fn justification_text(&self) -> &str {
        &self.justification_text
    }

    /// Returns when this decision was recorded.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
