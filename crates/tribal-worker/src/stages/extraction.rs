//! Extraction stage: LLM-based candidate extraction from raw input.

use std::sync::Arc;
use std::time::Instant;

use opentelemetry::KeyValue;
use tokio::sync::Semaphore;
use tribal_telemetry::{LABEL_MODEL, LABEL_PROVIDER, LABEL_PROVIDER_KEY, LABEL_STAGE};
use tracing::Instrument;
use tribal_common::clamp_to_u32;
use tribal_db::{NewExtractionResult, NewTask};
use tribal_domain::{Candidate, Job, RelationHint, TagRegistryEntry, Task, TaskType, span_attrs};
use tribal_inference::{InferenceProvider, ProviderKey, Usage};

use super::{StageCommit, StageOutput, record_prompt_version_ids};
use crate::{
    common::PARSE_PREVIEW_LENGTH,
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
const EXPECT_EXTRACTION_KEY: &str = "extraction key registered at startup";

// ---------------------------------------------------------------------------
// ExtractionContext
// ---------------------------------------------------------------------------

/// Context assembled before running the extraction stage.
pub(crate) struct ExtractionContext<'a> {
    /// The parent job.
    pub job: &'a Job,
    /// The claimed task.
    pub task: &'a Task,
    /// The current global tag registry.
    pub tag_registry: Vec<TagRegistryEntry>,
}

// ---------------------------------------------------------------------------
// Extraction accessors
// ---------------------------------------------------------------------------

impl Worker {
    /// Returns a reference to the extraction inference provider.
    pub(crate) fn extraction_provider(&self) -> &Arc<dyn InferenceProvider> {
        &self.extraction_provider
    }

    /// Returns the extraction provider key.
    pub(crate) fn extraction_key(&self) -> &ProviderKey {
        &self.extraction_key
    }

    /// Returns the extraction semaphore from the provider registry.
    ///
    /// # Panics
    ///
    /// Panics if the extraction key is not registered in the provider
    /// registry.
    pub(crate) fn extraction_semaphore(&self) -> &Arc<Semaphore> {
        self.provider_registry()
            .semaphore(self.extraction_key())
            .expect(EXPECT_EXTRACTION_KEY)
    }
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
            { span_attrs::LLM_SYSTEM_PROMPT_VERSION_ID } = tracing::field::Empty,
            { span_attrs::LLM_USER_PROMPT_VERSION_ID } = tracing::field::Empty,
        );

        async {
            let include_llm_content = self.include_llm_content();

            let tag_registry = self.load_tag_registry(STAGE_EXTRACTION).await?;
            let ctx = ExtractionContext {
                job,
                task,
                tag_registry,
            };

            let (system_pv, user_pv) = tokio::try_join!(
                self.load_prompt_version(
                    STAGE_EXTRACTION,
                    ctx.job.extraction_system_prompt_version_id()
                ),
                self.load_prompt_version(
                    STAGE_EXTRACTION,
                    ctx.job.extraction_user_prompt_version_id()
                ),
            )?;

            record_prompt_version_ids(
                ctx.job.extraction_system_prompt_version_id(),
                ctx.job.extraction_user_prompt_version_id(),
            );

            let semaphore = self.extraction_semaphore();
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let semaphore_start = Instant::now();
            let _permit = tokio::time::timeout(remaining, Arc::clone(semaphore).acquire_owned())
                .await
                .map_err(|_| StageError::SemaphoreTimeout {
                    provider_key: format!("{:?}", self.extraction_key()),
                })?
                .expect(SEMAPHORE_CLOSED);
            self.metrics().semaphore_acquire_wait_ms.record(
                semaphore_start.elapsed().as_secs_f64() * 1000.0,
                &[KeyValue::new(LABEL_PROVIDER_KEY, "extraction")],
            );

            let request = assemble_extraction_prompt(
                system_pv.content(),
                user_pv.content(),
                ctx.job.raw_input(),
                &ctx.tag_registry,
            )?;

            if include_llm_content {
                tracing::debug!(
                    system_prompt = %request.system.as_deref().unwrap_or(""),
                    user_prompt = %request.messages.first().map_or("", |m| m.content.as_str()),
                    "extraction prompt assembled",
                );
            }

            let provider_start = Instant::now();
            let response = self
                .extraction_provider()
                .complete(request)
                .await
                .map_err(|e| StageError::Provider {
                    context: "extraction LLM call".into(),
                    source: e,
                })?;
            let identity = self.extraction_provider().identity();
            self.metrics().provider_call_ms.record(
                provider_start.elapsed().as_secs_f64() * 1000.0,
                &[
                    KeyValue::new(LABEL_PROVIDER, identity.name.clone()),
                    KeyValue::new(LABEL_MODEL, identity.model.clone()),
                    KeyValue::new(LABEL_STAGE, "extraction"),
                ],
            );

            if include_llm_content {
                tracing::debug!(
                    response = %response.text,
                    "extraction response received",
                );
            }

            let output = parse_with_diagnostics(&response, include_llm_content)?;

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
                        .job_id(ctx.task.job_id())
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
                usages: vec![Usage::Completion {
                    usage: response.usage,
                }],
            })
        }
        .instrument(span)
        .await
    }
}

/// Parses the extraction response, emitting diagnostics on failure.
fn parse_with_diagnostics(
    response: &tribal_inference::CompletionResponse,
    include_llm_content: bool,
) -> Result<crate::parsing::ExtractionOutput, StageError> {
    let _parse_span = tracing::info_span!("tribal.extraction.parse").entered();
    parse_extraction_response(response).inspect_err(|_| {
        if include_llm_content {
            let preview: String = response.text.chars().take(PARSE_PREVIEW_LENGTH).collect();
            tracing::debug!(preview = %preview, "parse failure — raw LLM response");
        } else {
            tracing::debug!(
                response_length = response.text.len(),
                "parse failure — response details redacted",
            );
        }
    })
}
