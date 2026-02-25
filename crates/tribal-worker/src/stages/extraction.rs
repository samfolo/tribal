//! Extraction stage: LLM-based candidate extraction from raw input.

use tribal_db::{NewExtractionResult, NewTask};
use tribal_inference::{InferenceError, Usage};

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
#[allow(dead_code)]
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
}

// ---------------------------------------------------------------------------
// Stage implementation
// ---------------------------------------------------------------------------

impl Worker {
    /// Runs the extraction stage for a task.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Provider`] — stub always fails.
    #[allow(clippy::unused_async)]
    pub(crate) async fn run_extraction(
        &self,
        _job: &tribal_domain::Job,
        _task: &tribal_domain::Task,
    ) -> Result<StageOutput, StageError> {
        // Replaced by ticket 4.3
        Err(StageError::Provider {
            context: "extraction stage not yet implemented".into(),
            source: InferenceError::ProviderUnavailable {
                provider: "stub".into(),
                reason: "extraction stage not yet implemented".into(),
            },
        })
    }
}
