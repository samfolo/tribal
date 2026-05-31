//! Shared utilities and types for pipeline stage implementations.

use std::sync::Arc;

use tribal_db::{
    EmbeddingProfileRepository, NewExtractionResult, NewItemObservation, NewKnowledgeItem, NewTask,
    NewTriageSimilarItemDecision, PgEmbeddingProfileRepository, PgPromptVersionRepository,
    PgSystemFingerprintRepository, PgTagRegistryRepository, PromptVersionRepository,
    SystemFingerprintRepository, TagRegistryRepository,
};
use tribal_domain::{
    EmbeddingProfile, ProjectId, PromptVersion, PromptVersionId, SuggestedReference,
    SystemFingerprint, TagRegistryEntry, span_attrs,
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
// Active embedding profile
// ---------------------------------------------------------------------------

/// Loads the active embedding profile against the given connection.
///
/// Reads and writes embed against the active profile, so the read site, the
/// commit transaction, and tag resolution all resolve it through here. Taking
/// an explicit connection lets the commit path resolve it inside the
/// transaction's snapshot. First-boot provisioning completes a genesis profile
/// before the worker serves any task, so its absence is a consistency fault,
/// not an expected outcome.
///
/// # Errors
///
/// Returns [`StageError::Database`] on query failure or when no profile is
/// active.
pub(crate) async fn load_active_embedding_profile(
    conn: &mut sqlx::PgConnection,
    stage: &str,
) -> Result<EmbeddingProfile, StageError> {
    PgEmbeddingProfileRepository
        .find_active(conn)
        .await
        .map_err(|e| StageError::Database {
            stage: stage.into(),
            context: "loading active embedding profile".into(),
            source: e,
        })?
        .ok_or_else(|| StageError::Database {
            stage: stage.into(),
            context: "no active embedding profile".into(),
            source: tribal_db::DbError::NotFound {
                entity: "embedding_profile",
                id: "active".to_owned(),
            },
        })
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

    /// Loads the system fingerprint a job was created under, by content hash.
    ///
    /// The fingerprint records the effective sampling parameters the stage
    /// threads into its request, so they match what the job was fingerprinted
    /// under rather than the worker's current live config. Only the reconciled
    /// sampling parameters are sourced here: they carry a post-reconcile shape
    /// that must match the hash, whereas pass-through pipeline parameters
    /// (search limits, candidate caps) need no reconciliation and are read from
    /// live config.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Database`] on pool or query failure, or when no
    /// fingerprint matches the hash. The fingerprint is written before a job
    /// is enqueued, so its absence is a consistency fault, not an expected
    /// outcome.
    pub(crate) async fn load_system_fingerprint(
        &self,
        stage: &str,
        hash: &str,
    ) -> Result<SystemFingerprint, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for system fingerprint".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        PgSystemFingerprintRepository
            .find_by_hash(&mut conn, hash)
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "loading system fingerprint".into(),
                source: e,
            })?
            .ok_or_else(|| StageError::Database {
                stage: stage.into(),
                context: "system fingerprint not found".into(),
                source: tribal_db::DbError::NotFound {
                    entity: "system_fingerprint",
                    id: hash.to_owned(),
                },
            })
    }
}
