//! The agentic extraction stage: the loop executor's wiring.
//!
//! Extraction investigates nothing, so the loop is degenerate: the opening
//! is the raw input, the registry is empty, and the model submits its
//! candidates in one turn. The loop earns its place as the seam the
//! deferred source-grounding verifier plugs into (the `verifier_launch`
//! hook lives only in the turn loop), and as the bounce-and-retry path the
//! predicate validators correct a malformed submission through. The
//! committed shape (the extraction result and the triage fan-out) is the
//! one-shot's, so the two executors cannot drift on what they write.

use tracing::Instrument;
use tribal_agent_runtime::{
    LoopOutcome, RecheckPolicy, RecordedMessage, RenderedConversation, StageThread, ToolRegistry,
    TurnLoopDeps, recorded_conversation, run_turn_loop,
};
use tribal_common::clamp_to_u32;
use tribal_config::{DEFAULT_AGENTIC_RECHECK_BOUND, DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS};
use tribal_db::{NewExtractionResult, NewTask, PgPromptVersionRepository, PromptVersionRepository};
use tribal_domain::{
    AgentBinding, Candidate, Job, PromptClass, PromptRole, PromptStage, PromptVersion,
    RelationHint, Task, TaskType, span_attrs,
};

use super::{StageCommit, StageTerminal, common::attribution_with_prompts};
use crate::{
    common::PARSE_PREVIEW_LENGTH,
    error::{STAGE_EXTRACTION, StageError},
    parsing::ExtractionSubmission,
    prompt::assemble_extraction_loop_opening,
    stages::ExtractionSubmissionPipeline,
    tools::submit_candidates_descriptor,
    worker::{Worker, heartbeat::WorkerHeartbeatPump, map_runtime_error},
};

/// The loop's recorded prompt pair, resolved from the binding's hashes.
struct RecordedLoopPrompts {
    system: PromptVersion,
    user: PromptVersion,
}

impl Worker {
    /// Runs the extraction loop for a job: assembles the opening from the
    /// raw input, drives the runtime's turn loop with no tools, and maps the
    /// accepted submission onto the extraction commit.
    pub(super) async fn run_extraction_loop(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
        stage_thread: &StageThread,
        pump: &WorkerHeartbeatPump,
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
            let tag_registry = self.load_tag_registry(STAGE_EXTRACTION).await?;

            let prompts = self
                .recorded_extraction_loop_prompts(&stage_thread.binding)
                .await?;
            tracing::Span::current().record(
                span_attrs::SYSTEM_PROMPT_VERSION_ID,
                tracing::field::display(prompts.system.id()),
            );
            tracing::Span::current().record(
                span_attrs::USER_PROMPT_VERSION_ID,
                tracing::field::display(prompts.user.id()),
            );

            let attribution = attribution_with_prompts(
                job,
                task,
                &stage_thread.thread,
                prompts.system.id(),
                prompts.user.id(),
            );

            let opening_rendered = self
                .extraction_opening_or_replay(&stage_thread.thread, job, &tag_registry, &prompts)
                .await?;

            let registry = ToolRegistry::new();
            let pipeline =
                ExtractionSubmissionPipeline::new(self.config().max_candidates_per_job as usize);
            let submit_descriptor = submit_candidates_descriptor();
            let parameters = &stage_thread.binding.definition().parameters;

            let outcome = run_turn_loop(TurnLoopDeps {
                pool: self.pool(),
                gateway: self.gateway(),
                stage: TaskType::Extraction,
                thread: &stage_thread.thread,
                task_id: task.id(),
                claim_token: task.claim_token().ok_or(StageError::OwnershipLost)?,
                attempt: tribal_common::clamp_to_i32(task.retry_count()),
                attribution: &attribution,
                registry: &registry,
                submit_descriptor: &submit_descriptor,
                pipeline: &pipeline,
                pump,
                rendered: opening_rendered,
                budgets: crate::definition::current_stage_budgets(
                    TaskType::Extraction,
                    stage_thread.binding.definition().executor,
                    self.agents(),
                ),
                recheck: RecheckPolicy {
                    delay_seconds: DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS,
                    bound: DEFAULT_AGENTIC_RECHECK_BOUND,
                },
                temperature: crate::prompt::narrow_temperature(parameters.temperature),
                max_tokens: parameters.max_tokens,
                permit_deadline: deadline,
            })
            .await
            .map_err(|source| {
                map_runtime_error(STAGE_EXTRACTION, "running the extraction loop", source)
            })?;

            match outcome {
                LoopOutcome::Submitted(accepted) => {
                    let submission: ExtractionSubmission =
                        serde_json::from_value(accepted.payload.clone()).map_err(|_| {
                            let raw: String = accepted
                                .payload
                                .to_string()
                                .chars()
                                .take(PARSE_PREVIEW_LENGTH)
                                .collect();
                            StageError::Parse {
                                context: "re-reading the accepted submission".to_owned(),
                                raw_response: Some(raw),
                            }
                        })?;
                    let commit = Self::extraction_submission_commit(task, submission);
                    Ok(Some((commit, StageTerminal::Submission(accepted))))
                }
                LoopOutcome::Suspended => Ok(None),
                LoopOutcome::CancelIntent => {
                    self.dispose_intervened(task, &stage_thread.thread).await?;
                    Ok(None)
                }
                LoopOutcome::BudgetFailed { cause } => Err(StageError::BudgetExhausted {
                    stage: STAGE_EXTRACTION.into(),
                    context: cause.to_string(),
                }),
            }
        }
        .instrument(span)
        .await
    }

    /// The rendered opening for the raw input: a fresh claim builds it, a
    /// resume replays the committed one. Extraction references nothing
    /// positional, so the replay alone preserves what the model saw.
    async fn extraction_opening_or_replay(
        &self,
        thread: &tribal_domain::AgentThread,
        job: &Job,
        tag_registry: &[tribal_domain::TagRegistryEntry],
        prompts: &RecordedLoopPrompts,
    ) -> Result<RenderedConversation, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_EXTRACTION.into(),
                context: "acquiring connection to read the recorded opening".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let recorded = recorded_conversation(&mut conn, thread)
            .await
            .map_err(|source| {
                map_runtime_error(STAGE_EXTRACTION, "reading the recorded opening", source)
            })?;
        drop(conn);
        match recorded {
            Some(rendered) => Ok(rendered),
            None => Self::extraction_loop_opening(job, tag_registry, prompts),
        }
    }

    /// Assembles the loop's opening: the raw input and tag registry rendered
    /// through the binding's loop templates.
    fn extraction_loop_opening(
        job: &Job,
        tag_registry: &[tribal_domain::TagRegistryEntry],
        prompts: &RecordedLoopPrompts,
    ) -> Result<RenderedConversation, StageError> {
        let (rendered_system, rendered_user) = assemble_extraction_loop_opening(
            prompts.system.content(),
            prompts.user.content(),
            job.raw_input(),
            tag_registry,
        )?;
        Ok(RenderedConversation {
            system: Some(rendered_system),
            messages: vec![RecordedMessage {
                role: "user".to_owned(),
                content: rendered_user,
            }],
            system_prompt_version_id: Some(prompts.system.id()),
            user_prompt_version_id: Some(prompts.user.id()),
            resolution_context: None,
            response_schema: None,
        })
    }

    /// Maps an accepted submission onto the extraction commit: the candidates
    /// fan out one triage task each. The validators already enforced the cap
    /// and the hint indices, so the commit caps nothing and filters nothing.
    fn extraction_submission_commit(task: &Task, submission: ExtractionSubmission) -> StageCommit {
        let candidates: Vec<Candidate> = submission.candidates;
        let hints: Vec<RelationHint> = submission.relation_hints;
        let batch_size = clamp_to_u32(candidates.len());

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
            .candidates(serde_json::to_value(&candidates).expect("candidates serialise to JSON"))
            .relation_hints(serde_json::to_value(&hints).expect("relation hints serialise to JSON"))
            .build();

        StageCommit::Extraction {
            extraction_result,
            triage_tasks,
            batch_size,
            // The validators bounce an over-cap submission, so what commits
            // is what was extracted: the original and the committed counts
            // are equal.
            original_count: batch_size,
        }
    }

    /// Resolves the loop templates the binding's recorded hashes pin:
    /// execution truth, hot-reload-proof, resume-stable.
    async fn recorded_extraction_loop_prompts(
        &self,
        binding: &AgentBinding,
    ) -> Result<RecordedLoopPrompts, StageError> {
        let hashes = &binding.definition().prompt_hashes;
        let [system_hash, user_hash] = hashes.as_slice() else {
            return Err(StageError::BindingDerivation {
                stage: STAGE_EXTRACTION.into(),
                context: format!(
                    "the loop binding carries {} prompt hashes where two were recorded",
                    hashes.len(),
                ),
            });
        };

        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_EXTRACTION.into(),
                context: "acquiring connection for the loop prompts".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let system =
            resolve_extraction_loop_prompt(&mut conn, PromptRole::System, system_hash).await?;
        let user = resolve_extraction_loop_prompt(&mut conn, PromptRole::User, user_hash).await?;
        Ok(RecordedLoopPrompts { system, user })
    }
}

/// Resolves one recorded extraction loop prompt by its slot and content hash.
async fn resolve_extraction_loop_prompt(
    conn: &mut sqlx::PgConnection,
    role: PromptRole,
    hash: &str,
) -> Result<PromptVersion, StageError> {
    PgPromptVersionRepository
        .find_by_slot_and_hash(conn, PromptStage::Extraction, PromptClass::Loop, role, hash)
        .await
        .map_err(|source| StageError::Database {
            stage: STAGE_EXTRACTION.into(),
            context: "resolving a recorded loop prompt".into(),
            source,
        })?
        .ok_or_else(|| StageError::BindingDerivation {
            stage: STAGE_EXTRACTION.into(),
            context: format!(
                "no stored extraction loop {} prompt matches the binding's hash",
                role.as_str(),
            ),
        })
}
