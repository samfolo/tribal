//! Extraction stage: LLM-based candidate extraction from raw input.

use std::sync::Arc;

use serde::Deserialize;
use tribal_db::{
    NewExtractionResult, NewTask, PgPromptVersionRepository, PgTagRegistryRepository,
    PromptVersionRepository, TagRegistryRepository,
};
use tribal_domain::{Candidate, Job, RelationHint, TagRegistryEntry, Task, TaskType};
use tribal_inference::Usage;

use crate::{
    error::StageError,
    parsing::parse_extraction_response,
    prompt::assemble_extraction_prompt,
    worker::Worker,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const STAGE_EXTRACTION: &str = "extraction";
const SEMAPHORE_CLOSED: &str = "semaphore closed unexpectedly";

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
///
/// Groups the job, task, and tag registry into a single struct,
/// establishing the pattern for `TriageContext` and `RelationContext`.
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
// ExtractionOutput
// ---------------------------------------------------------------------------

/// The deserialised output from the extraction LLM call.
///
/// Lenient serde — unknown fields are silently ignored so the LLM
/// can return extra keys without breaking parsing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
pub(crate) struct ExtractionOutput {
    /// Extracted knowledge item candidates.
    pub candidates: Vec<Candidate>,
    /// Intra-batch relation hints between candidates.
    #[serde(default)]
    pub relation_hints: Vec<RelationHint>,
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
    /// # Errors
    ///
    /// Returns a [`StageError`] variant matching the failure mode:
    /// - [`StageError::Database`] for pool/repository failures.
    /// - [`StageError::SemaphoreTimeout`] if the provider semaphore
    ///   cannot be acquired within the task timeout.
    /// - [`StageError::TemplateRender`] if the prompt template is invalid.
    /// - [`StageError::Provider`] if the LLM call fails.
    /// - [`StageError::Parse`] if the LLM response cannot be parsed.
    ///
    /// # Panics
    ///
    /// Panics if the extraction provider key is not registered in the
    /// provider registry (startup configuration error) or if the
    /// semaphore is unexpectedly closed.
    pub(crate) async fn run_extraction(
        &self,
        job: &Job,
        task: &Task,
    ) -> Result<StageOutput, StageError> {
        // 1. Load tag registry.
        let tag_registry = {
            let mut conn = self.pool.acquire().await.map_err(|e| StageError::Database {
                stage: STAGE_EXTRACTION.into(),
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
                    stage: STAGE_EXTRACTION.into(),
                    context: "loading tag registry".into(),
                    source: e,
                })?
        };

        // 2. Load prompt template.
        let prompt_version = {
            let mut conn = self.pool.acquire().await.map_err(|e| StageError::Database {
                stage: STAGE_EXTRACTION.into(),
                context: "acquiring connection for prompt version".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
            PgPromptVersionRepository
                .find_by_id(&mut conn, job.extraction_prompt_version_id())
                .await
                .map_err(|e| StageError::Database {
                    stage: STAGE_EXTRACTION.into(),
                    context: "loading prompt version".into(),
                    source: e,
                })?
        };

        // 3. Acquire semaphore permit.
        let semaphore = self
            .provider_registry
            .semaphore(&self.extraction_key)
            .expect("extraction key registered at startup");
        let _permit = tokio::time::timeout(
            self.config.task_timeout(),
            Arc::clone(semaphore).acquire_owned(),
        )
        .await
        .map_err(|_| StageError::SemaphoreTimeout {
            provider_key: self.extraction_key.to_string(),
        })?
        .expect(SEMAPHORE_CLOSED);

        // 4. Assemble prompt.
        let request = assemble_extraction_prompt(
            prompt_version.content(),
            job.raw_input(),
            &tag_registry,
        )?;

        // 5. Call the LLM.
        let response = self
            .extraction_provider()
            .complete(request)
            .await
            .map_err(|e| StageError::Provider {
                context: "extraction LLM call".into(),
                source: e,
            })?;

        // 6. Parse response.
        let output = parse_extraction_response(&response)?;

        // 7. Cap candidates.
        #[allow(clippy::cast_possible_truncation)]
        let original_count = output.candidates.len() as u32;
        let max = self.config.max_candidates_per_job as usize;
        let capped_candidates: Vec<Candidate> =
            output.candidates.into_iter().take(max).collect();
        #[allow(clippy::cast_possible_truncation)]
        let batch_size = capped_candidates.len() as u32;

        // 8. Filter relation hints to only reference retained candidates.
        let capped_hints: Vec<RelationHint> = output
            .relation_hints
            .into_iter()
            .filter(|h| h.source_index() < batch_size && h.target_index() < batch_size)
            .collect();

        // 9. Build triage tasks.
        let triage_tasks: Vec<NewTask> = (0..batch_size)
            .map(|i| {
                NewTask::builder()
                    .job_id(task.job_id())
                    .task_type(TaskType::Triage)
                    .batch_index(Some(i))
                    .build()
            })
            .collect();

        // 10. Build extraction result.
        let extraction_result = NewExtractionResult::builder()
            .job_id(task.job_id())
            .candidates(
                serde_json::to_value(&capped_candidates).expect("candidates serialise to JSON"),
            )
            .relation_hints(
                serde_json::to_value(&capped_hints).expect("relation hints serialise to JSON"),
            )
            .build();

        // 11. Return stage output.
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
}
