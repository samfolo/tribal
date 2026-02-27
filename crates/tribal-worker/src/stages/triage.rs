//! Triage stage: similarity search and LLM-based relevance scoring.

use tribal_inference::InferenceError;

use crate::{error::StageError, stages::StageOutput, worker::Worker};

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
