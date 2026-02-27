//! Extraction stage: LLM-based candidate extraction from raw input.

use std::sync::Arc;

use tracing::Instrument;
use tribal_db::{NewExtractionResult, NewTask};
use tribal_domain::{Candidate, Job, RelationHint, TagRegistryEntry, Task, TaskType, span_attrs};
use tribal_inference::Usage;

use crate::{
    common::clamp_to_u32,
    error::{SEMAPHORE_CLOSED, STAGE_EXTRACTION, StageError},
    parsing::parse_extraction_response,
    prompt::assemble_extraction_prompt,
    worker::Worker,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CANDIDATES_SERIALISE: &str = "candidates serialise to JSON";
const HINTS_SERIALISE: &str = "relation hints serialise to JSON";

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
}

// ---------------------------------------------------------------------------
// ExtractionContext
// ---------------------------------------------------------------------------

/// Context assembled before running the extraction stage.
#[allow(dead_code)]
pub(crate) struct ExtractionContext {
    /// The parent job.
    pub job: Job,
    /// The claimed task.
    pub task: Task,
    /// The current global tag registry.
    pub tag_registry: Vec<TagRegistryEntry>,
}

// ---------------------------------------------------------------------------
// Stage implementation
// ---------------------------------------------------------------------------

impl Worker {
    /// Runs the extraction stage for a task.
    ///
    /// Loads the prompt template and tag registry from the database,
    /// acquires a semaphore permit, assembles the prompt via Tera,
    /// calls the LLM, parses the response, caps candidates, and
    /// builds the [`StageOutput`] for commit.
    ///
    /// The `deadline` is the absolute instant by which the outer task
    /// timeout will fire.  Semaphore acquisition uses the remaining
    /// time budget so that `SemaphoreTimeout` is reported instead of
    /// a generic `Timeout` when permits are exhausted.
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] variant matching the failure mode:
    /// - [`StageError::Database`] for pool/repository failures.
    /// - [`StageError::SemaphoreTimeout`] if the provider semaphore
    ///   cannot be acquired within the remaining time budget.
    /// - [`StageError::TemplateRender`] if the prompt template is invalid.
    /// - [`StageError::Provider`] if the LLM call fails.
    /// - [`StageError::Parse`] if the LLM response cannot be parsed.
    ///
    /// # Panics
    ///
    /// Panics if the extraction provider key is not registered in the
    /// provider registry or if the semaphore is unexpectedly closed.
    pub(crate) async fn run_extraction(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
    ) -> Result<StageOutput, StageError> {
        let span = tracing::info_span!(
            "tribal.task.extraction",
            { span_attrs::TASK_ID } = %task.id(),
            { span_attrs::LLM_STAGE } = "extraction",
            { span_attrs::RETRY_COUNT } = task.retry_count(),
        );

        async {
            let tag_registry = self.load_tag_registry(STAGE_EXTRACTION).await?;

            let prompt_version = self
                .load_prompt_version(STAGE_EXTRACTION, job.extraction_prompt_version_id())
                .await?;

            let semaphore = self.extraction_semaphore();
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let _permit = tokio::time::timeout(remaining, Arc::clone(semaphore).acquire_owned())
                .await
                .map_err(|_| StageError::SemaphoreTimeout {
                    provider_key: format!("{:?}", self.extraction_key()),
                })?
                .expect(SEMAPHORE_CLOSED);

            let request = assemble_extraction_prompt(
                prompt_version.content(),
                job.raw_input(),
                &tag_registry,
            )?;

            if self.config().include_llm_content {
                tracing::debug!(
                    prompt = %request.system.as_deref().unwrap_or(""),
                    "extraction prompt assembled",
                );
            }

            let response = self
                .extraction_provider()
                .complete(request)
                .await
                .map_err(|e| StageError::Provider {
                    context: "extraction LLM call".into(),
                    source: e,
                })?;

            if self.config().include_llm_content {
                tracing::debug!(
                    response = %response.text,
                    "extraction response received",
                );
            }

            let output = {
                let _parse_span = tracing::info_span!("tribal.extraction.parse").entered();
                parse_extraction_response(&response)?
            };

            let original_count = clamp_to_u32(output.candidates.len());
            let max = self.config().max_candidates_per_job as usize;
            let capped_candidates: Vec<Candidate> =
                output.candidates.into_iter().take(max).collect();
            let batch_size = clamp_to_u32(capped_candidates.len());

            let capped_hints: Vec<RelationHint> = output
                .relation_hints
                .into_iter()
                .filter(|h: &RelationHint| {
                    h.source_index() < batch_size && h.target_index() < batch_size
                })
                .collect();

            let triage_tasks: Vec<NewTask> = (0..batch_size)
                .map(|i| {
                    NewTask::builder()
                        .job_id(task.job_id())
                        .task_type(TaskType::Triage)
                        .batch_index(Some(i))
                        .build()
                })
                .collect();

            let extraction_result = NewExtractionResult::builder()
                .job_id(task.job_id())
                .candidates(serde_json::to_value(&capped_candidates).expect(CANDIDATES_SERIALISE))
                .relation_hints(serde_json::to_value(&capped_hints).expect(HINTS_SERIALISE))
                .build();

            Ok(StageOutput {
                commit: StageCommit::Extraction {
                    extraction_result,
                    triage_tasks,
                    batch_size,
                    original_count,
                },
                usages: vec![Usage::Completion(response.usage)],
            })
        }
        .instrument(span)
        .await
    }
}
