//! Relation stage: LLM-based relation extraction and commit.

use tribal_inference::InferenceError;

use super::StageOutput;
use crate::{error::StageError, worker::Worker};

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
