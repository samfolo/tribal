//! Shared utilities and types for pipeline stage implementations.

use std::sync::Arc;

use tribal_db::{
    NewExtractionResult, NewItemObservation, NewKnowledgeItem, NewTask,
    NewTriageSimilarItemDecision, PgPromptVersionRepository, PgTagRegistryRepository,
    PromptVersionRepository, TagRegistryRepository,
};
use tribal_domain::{
    ProjectId, PromptVersion, PromptVersionId, SuggestedReference, TagRegistryEntry, span_attrs,
};
use tribal_inference::{EmbeddingProvider, Usage};

use crate::{error::StageError, tag_resolution::NewTagWithEmbedding, worker::Worker};

// ---------------------------------------------------------------------------
// StageOutput
// ---------------------------------------------------------------------------

/// Output of a successful stage execution, ready for commit.
pub(crate) struct StageOutput {
    /// The domain effects to commit transactionally.
    pub commit: StageCommit,
    /// Token usage records to persist.
    pub usages: Vec<Usage>,
}

// ---------------------------------------------------------------------------
// StageCommit
// ---------------------------------------------------------------------------

/// Domain effects produced by a stage, committed transactionally after
/// the stage completes.
pub(crate) enum StageCommit {
    /// Extraction stage effects.
    Extraction {
        /// The extraction result to insert.
        extraction_result: NewExtractionResult,
        /// Triage tasks to create (one per candidate in the batch).
        triage_tasks: Vec<NewTask>,
        /// Capped candidate count.
        batch_size: u32,
        /// Pre-cap candidate count.
        original_count: u32,
    },
    /// Triage stage effects.
    Triage {
        /// The project this triage belongs to.
        project_id: ProjectId,
        /// The triage decision with associated data.
        decision: TriageCommitDecision,
        /// Per-similar-item decisions for audit persistence.
        similar_item_decisions: Vec<NewTriageSimilarItemDecision>,
    },
    /// Relation stage effects.
    Relation {
        /// The relation decision with associated commit data.
        decision: super::relation::RelationCommitDecision,
    },
}

// ---------------------------------------------------------------------------
// TriageCommitDecision
// ---------------------------------------------------------------------------

/// The triage decision variant with associated commit data.
///
/// Carries raw components rather than pre-constructed DB types because
/// fields like `knowledge_item_id` are only available after INSERT
/// RETURNING inside the commit transaction.
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
        /// New tags with pre-computed embedding vectors for storage.
        new_tags: Vec<NewTagWithEmbedding>,
        /// Tags resolved to existing entries, for `usage_count` increment.
        resolved_tags: Vec<String>,
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
// Span recording
// ---------------------------------------------------------------------------

/// Records the system and user prompt version IDs on the current span.
pub(crate) fn record_prompt_version_ids(
    system_pv_id: PromptVersionId,
    user_pv_id: PromptVersionId,
) {
    let span = tracing::Span::current();
    span.record(
        span_attrs::LLM_SYSTEM_PROMPT_VERSION_ID,
        tracing::field::display(system_pv_id),
    );
    span.record(
        span_attrs::LLM_USER_PROMPT_VERSION_ID,
        tracing::field::display(user_pv_id),
    );
}

// ---------------------------------------------------------------------------
// Shared accessors
// ---------------------------------------------------------------------------

impl Worker {
    /// Returns a reference to the embedding provider.
    pub(crate) fn embedding_provider(&self) -> &Arc<dyn EmbeddingProvider> {
        &self.embedding_provider
    }
}

// ---------------------------------------------------------------------------
// Shared loaders
// ---------------------------------------------------------------------------

impl Worker {
    /// Loads the full tag registry from the database.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Database`] on pool or query failure.
    pub(crate) async fn load_tag_registry(
        &self,
        stage: &str,
    ) -> Result<Vec<TagRegistryEntry>, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for tag registry".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        PgTagRegistryRepository
            .find_all(&mut conn)
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "loading tag registry".into(),
                source: e,
            })
    }

    /// Loads a prompt version by ID from the database.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Database`] on pool or query failure.
    pub(crate) async fn load_prompt_version(
        &self,
        stage: &str,
        id: PromptVersionId,
    ) -> Result<PromptVersion, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for prompt version".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        PgPromptVersionRepository
            .find_by_id(&mut conn, id)
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "loading prompt version".into(),
                source: e,
            })
    }
}
