//! Triage stage: similarity search and LLM-based relevance scoring.

use tribal_db::{NewItemObservation, NewKnowledgeItem, NewTriageSimilarItemDecision};
use tribal_domain::{Candidate, Job, JobId, SuggestedReference, TagRegistryEntry, Task};
use tribal_inference::InferenceError;

use super::StageOutput;
use crate::{error::StageError, worker::Worker};

// ---------------------------------------------------------------------------
// TriageContext
// ---------------------------------------------------------------------------

/// Context assembled before running the triage stage.
#[allow(dead_code)]
pub(crate) struct TriageContext {
    /// The parent job.
    pub job: Job,
    /// The claimed task.
    pub task: Task,
    /// The candidate extracted for this batch index.
    pub candidate: Candidate,
    /// The candidate's position in the extraction batch.
    pub batch_index: u32,
    /// The current global tag registry.
    pub tag_registry: Vec<TagRegistryEntry>,
}

// ---------------------------------------------------------------------------
// Commit types
// ---------------------------------------------------------------------------

/// Domain effects produced by the triage stage, ready for transactional commit.
///
/// Carries raw components rather than pre-constructed DB types because
/// fields like `knowledge_item_id` are only available after INSERT
/// RETURNING inside the commit transaction.
#[allow(dead_code)]
pub(crate) struct TriageCommitData {
    /// The job this triage belongs to.
    pub job_id: JobId,
    /// The candidate's position in the extraction batch.
    pub batch_index: u32,
    /// The triage decision with associated data.
    pub decision: TriageCommitDecision,
    /// Per-similar-item decisions for audit persistence.
    pub similar_item_decisions: Vec<NewTriageSimilarItemDecision>,
}

/// The triage decision variant with associated commit data.
#[allow(dead_code)]
pub(crate) enum TriageCommitDecision {
    /// Novel candidate — create a new knowledge item with embedding and references.
    Novel {
        /// The knowledge item to insert.
        knowledge_item: Box<NewKnowledgeItem>,
        /// The candidate's embedding vector.
        embedding_vector: Vec<f32>,
        /// The embedding model used.
        embedding_model: String,
        /// References suggested by the extraction stage.
        suggested_references: Vec<SuggestedReference>,
        /// Tags not found in the registry, to be created via `batch_upsert`.
        new_tags: Vec<String>,
    },
    /// Duplicate candidate — record an observation against the matched item.
    Duplicate {
        /// The observation to insert.
        observation: NewItemObservation,
    },
    /// Idempotency skip — result already exists for this `(job_id, batch_index)`.
    NoOp,
}

// ---------------------------------------------------------------------------
// Stage implementation
// ---------------------------------------------------------------------------

impl Worker {
    /// Runs the triage stage for a task.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Provider`] — stub always fails.
    #[allow(clippy::unused_async)]
    pub(crate) async fn run_triage(
        &self,
        _job: &tribal_domain::Job,
        _task: &tribal_domain::Task,
        _deadline: tokio::time::Instant,
    ) -> Result<StageOutput, StageError> {
        // Replaced by ticket 4.4
        Err(StageError::Provider {
            context: "triage stage not yet implemented".into(),
            source: InferenceError::ProviderUnavailable {
                provider: "stub".into(),
                reason: "triage stage not yet implemented".into(),
            },
        })
    }
}
