//! Shared utilities and types for pipeline stage implementations.

use tribal_db::{
    NewExtractionResult, NewTask, PgPromptVersionRepository, PgTagRegistryRepository,
    PromptVersionRepository, TagRegistryRepository,
};
use tribal_domain::{PromptVersion, PromptVersionId, TagRegistryEntry};
use tribal_inference::Usage;

use super::triage::TriageCommitData;
use crate::{error::StageError, worker::Worker};

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
    #[allow(dead_code)]
    Triage {
        /// The triage commit data.
        data: TriageCommitData,
    },
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
