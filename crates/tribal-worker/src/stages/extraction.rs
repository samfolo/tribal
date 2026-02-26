//! Extraction stage: LLM-based candidate extraction from raw input.

use tribal_db::{NewExtractionResult, NewTask};
use tribal_inference::{CompletionRequest, InferenceError, Usage};

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
    pub(crate) async fn run_extraction(
        &self,
        _job: &tribal_domain::Job,
        _task: &tribal_domain::Task,
    ) -> Result<StageOutput, StageError> {
        // Calls provider to honour mock delays; replaced by ticket 4.3.
        let _ = self
            .extraction_provider()
            .complete(CompletionRequest {
                system: Some("extraction stub".into()),
                messages: vec![],
                temperature: None,
                max_tokens: None,
                response_format: None,
            })
            .await;

        Err(StageError::Provider {
            context: "extraction stage not yet implemented".into(),
            source: InferenceError::ProviderUnavailable {
                provider: "stub".into(),
                reason: "extraction stage not yet implemented".into(),
            },
        })
    }
}
