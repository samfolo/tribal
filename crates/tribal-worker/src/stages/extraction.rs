//! Extraction stage: LLM-based candidate extraction from raw input.

use tracing::Instrument;
use tribal_agent_runtime::StageThread;
use tribal_common::clamp_to_u32;
use tribal_db::{NewExtractionResult, NewTask};
use tribal_domain::{
    Candidate, CompletionResponse, Job, RelationHint, TagRegistryEntry, Task, TaskType, span_attrs,
};
use tribal_inference::PermitWait;

use super::{
    StageCommit, StageTerminal, map_gateway_error, record_prompt_version_ids, stage_attribution,
};
use crate::{
    common::PARSE_PREVIEW_LENGTH,
    error::{STAGE_EXTRACTION, StageError},
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
// ExtractionContext
// ---------------------------------------------------------------------------

/// Context assembled before running the extraction stage.
pub(crate) struct ExtractionContext<'a> {
    pub job: &'a Job,
    pub task: &'a Task,
    pub tag_registry: Vec<TagRegistryEntry>,
}

// ---------------------------------------------------------------------------
// Stage implementation
// ---------------------------------------------------------------------------

impl Worker {
    /// Runs the extraction stage for a task, routed by the thread's
    /// recorded binding: the executor follows the binding, not the current
    /// configuration, so a resumed thread continues the way it started.
    pub(crate) async fn run_extraction(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
        stage_thread: &StageThread,
        pump: &crate::worker::heartbeat::WorkerHeartbeatPump,
    ) -> Result<Option<(StageCommit, StageTerminal)>, StageError> {
        match stage_thread.binding.definition().executor {
            tribal_domain::StageExecutorKind::OneShot => {
                self.run_extraction_one_shot(job, task, deadline, stage_thread)
                    .await
            }
            tribal_domain::StageExecutorKind::BuiltInLoop => {
                self.run_extraction_loop(job, task, deadline, stage_thread, pump)
                    .await
            }
            tribal_domain::StageExecutorKind::ExternalAgent => Err(StageError::BindingDerivation {
                stage: STAGE_EXTRACTION.into(),
                context: "the external-agent executor has no runner".into(),
            }),
        }
    }

    /// Runs the one-shot extraction turn for a task.
    async fn run_extraction_one_shot(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
        stage_thread: &StageThread,
    ) -> Result<Option<(StageCommit, StageTerminal)>, StageError> {
        let span = tracing::info_span!(
            "tribal.task.extraction",
            { span_attrs::TASK_ID } = %task.id(),
            { span_attrs::STAGE } = "extraction",
            { span_attrs::RETRY_COUNT } = task.retry_count(),
            { span_attrs::SYSTEM_PROMPT_VERSION_ID } = tracing::field::Empty,
            { span_attrs::USER_PROMPT_VERSION_ID } = tracing::field::Empty,
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

            let request = assemble_extraction_prompt(
                system_pv.content(),
                user_pv.content(),
                ctx.job.raw_input(),
                &ctx.tag_registry,
                &stage_thread.binding.definition().parameters,
            )?;

            if include_llm_content {
                tracing::debug!(
                    system_prompt = %request.system.as_deref().unwrap_or(""),
                    user_prompt = %request.messages.first().map_or("", |m| m.content()),
                    "extraction prompt assembled",
                );
            }

            // Extraction output stands alone (no positional references
            // into rendered context), so no resolution context is recorded.
            let Some(bracketed) = self
                .bracket_one_shot(
                    STAGE_EXTRACTION,
                    stage_thread,
                    ctx.job,
                    ctx.task,
                    request,
                    None,
                )
                .await?
            else {
                return Ok(None);
            };
            let request = bracketed.request;
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let response = self
                .gateway()
                .complete(
                    TaskType::Extraction,
                    request,
                    PermitWait::Bounded { limit: remaining },
                    &stage_attribution(ctx.job, ctx.task, &stage_thread.thread),
                )
                .await
                .map_err(|e| map_gateway_error("extraction LLM call", e))?;

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

            Ok(Some((
                StageCommit::Extraction {
                    extraction_result,
                    triage_tasks,
                    batch_size,
                    original_count,
                },
                StageTerminal::Completion(Some(response)),
            )))
        }
        .instrument(span)
        .await
    }
}

/// Parses the extraction response, emitting diagnostics on failure.
fn parse_with_diagnostics(
    response: &CompletionResponse,
    include_llm_content: bool,
) -> Result<crate::parsing::ExtractionOutput, StageError> {
    let _parse_span = tracing::info_span!("tribal.extraction.parse").entered();
    parse_extraction_response(response).inspect_err(|_| {
        if include_llm_content {
            let preview: String = response.text.chars().take(PARSE_PREVIEW_LENGTH).collect();
            tracing::debug!(preview = %preview, "parse failure: raw LLM response");
        } else {
            tracing::debug!(
                response_length = response.text.len(),
                "parse failure: response details redacted",
            );
        }
    })
}
