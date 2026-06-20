//! The agentic triage stage: the loop executor's wiring.
//!
//! Context assembly opens with the candidate and the tag registry;
//! candidate similarity is acquired through the durable search tool the
//! loop exposes, which the must-search rule requires before a
//! classification, rendered through the loop templates the thread's
//! recorded binding pins. The runtime drives the turns; this module
//! supplies the tools, the submission pipeline, and the mapping from an
//! accepted submission onto the existing commit machinery. The committed
//! shapes (knowledge items, observations, similar-item decisions) are the
//! one-shot's own constructors, so the two executors cannot drift on what
//! they write.

use std::{collections::HashMap, sync::Arc};

use tracing::Instrument;
use tribal_agent_runtime::{
    LoopOutcome, RecheckPolicy, RecordedMessage, RenderedConversation, StageThread, ToolRegistry,
    TurnLoopDependencies, recorded_conversation, run_turn_loop,
};
use tribal_config::{DEFAULT_AGENTIC_RECHECK_BOUND, DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS};
use tribal_db::{NewTriageSimilarItemDecision, PgPromptVersionRepository, PromptVersionRepository};
use tribal_domain::{
    AgentBinding, Candidate, Job, JobId, KnowledgeItemId, PromptClass, PromptRole, PromptStage,
    PromptVersion, Task, TaskType, span_attrs,
};
use tribal_inference::{EmbeddingTarget, UsageAttribution};

use super::{
    StageCommit, StageTerminal, TriageCommitDecision,
    common::attribution_with_prompts,
    triage::{CandidateEmbedding, TriageContext, duplicate_decision},
};
use crate::{
    common::{EXPECT_BATCH_INDEX, PARSE_PREVIEW_LENGTH},
    definition::{current_stage_budgets, verifier_definition},
    error::{STAGE_TRIAGE, StageError},
    parsing::{TriageSubmission, TriageSubmissionDecision},
    prompt::{assemble_loop_opening, narrow_temperature},
    stages::{TriageSubmissionPipeline, VerifierContext, reconstruct_candidate_scores},
    tag_resolution,
    tools::{TriageToolset, build_triage_registry, submit_result_descriptor},
    worker::{Worker, heartbeat::WorkerHeartbeatPump, map_runtime_error},
};

/// The loop's recorded prompt pair, resolved from the binding's hashes.
struct RecordedLoopPrompts {
    system: PromptVersion,
    user: PromptVersion,
}

impl Worker {
    /// Runs the triage loop for a single candidate: assembles the
    /// opening, registers the stage's tools, drives the runtime's turn
    /// loop, and maps the outcome onto the stage contract.
    pub(super) async fn run_triage_loop(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
        stage_thread: &StageThread,
        pump: &WorkerHeartbeatPump,
    ) -> Result<Option<(StageCommit, StageTerminal)>, StageError> {
        let batch_index = task.batch_index().expect(EXPECT_BATCH_INDEX);

        let span = tracing::info_span!(
            "tribal.task.triage",
            { span_attrs::BATCH_INDEX } = batch_index,
            { span_attrs::TASK_ID } = %task.id(),
            { span_attrs::STAGE } = "triage",
            { span_attrs::RETRY_COUNT } = task.retry_count(),
            { span_attrs::SYSTEM_PROMPT_VERSION_ID } = tracing::field::Empty,
            { span_attrs::USER_PROMPT_VERSION_ID } = tracing::field::Empty,
        );

        async {
            if self.check_triage_idempotency(job.id(), batch_index).await? {
                return Ok(Some((
                    StageCommit::Triage {
                        project_id: job.project_id(),
                        decision: TriageCommitDecision::NoOp,
                        similar_item_decisions: vec![],
                    },
                    StageTerminal::Completion(None),
                )));
            }

            let candidate = self.load_triage_candidate(job.id(), batch_index).await?;
            let tag_registry = self.load_tag_registry(STAGE_TRIAGE).await?;
            let ctx = TriageContext {
                job,
                candidate,
                batch_index,
                tag_registry,
            };

            // The loop prompts follow the thread's recorded binding, not
            // the currently active set: a resumed thread renders and
            // attributes exactly what it was admitted under.
            let prompts = self.recorded_loop_prompts(&stage_thread.binding).await?;
            tracing::Span::current().record(
                span_attrs::SYSTEM_PROMPT_VERSION_ID,
                tracing::field::display(prompts.system.id()),
            );
            tracing::Span::current().record(
                span_attrs::USER_PROMPT_VERSION_ID,
                tracing::field::display(prompts.user.id()),
            );

            let active_profile = self.resolve_active_embedding(STAGE_TRIAGE).await?;
            let attribution = attribution_with_prompts(
                job,
                task,
                &stage_thread.thread,
                prompts.system.id(),
                prompts.user.id(),
            );

            let opening_rendered = self
                .loop_opening_or_replay(&stage_thread.thread, &ctx, &prompts)
                .await?;

            let registry = self.triage_tool_registry(
                &ctx,
                &active_profile,
                attribution.clone(),
                deadline,
                stage_thread,
            )?;
            let verifier = self
                .verifier_context(&stage_thread.binding, &ctx.candidate)
                .await?;
            let pipeline = TriageSubmissionPipeline::new(job.project_id()).with_verifier(verifier);
            let submit_descriptor = submit_result_descriptor();
            let parameters = &stage_thread.binding.definition().parameters;

            let outcome = run_turn_loop(TurnLoopDependencies {
                pool: self.pool(),
                gateway: self.gateway(),
                stage: TaskType::Triage,
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
                // Enforcement re-resolves from the current configuration
                // at every claim; the binding records admission-time
                // intent.
                budgets: current_stage_budgets(
                    TaskType::Triage,
                    stage_thread.binding.definition().executor,
                    self.agents(),
                ),
                recheck: RecheckPolicy {
                    delay_seconds: DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS,
                    bound: DEFAULT_AGENTIC_RECHECK_BOUND,
                },
                temperature: narrow_temperature(parameters.temperature),
                max_tokens: parameters.max_tokens,
                permit_deadline: deadline,
                recorder: self.metrics(),
            })
            .await
            .map_err(|source| map_runtime_error(STAGE_TRIAGE, "running the triage loop", source))?;

            match outcome {
                LoopOutcome::Submitted(accepted) => {
                    let submission: TriageSubmission =
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
                    let commit = self
                        .submission_commit(
                            &ctx,
                            &submission,
                            &stage_thread.thread,
                            &active_profile,
                            deadline,
                            &attribution,
                        )
                        .await?;
                    Ok(Some((commit, StageTerminal::Submission(accepted))))
                }
                LoopOutcome::Suspended => Ok(None),
                LoopOutcome::CancelIntent => {
                    self.dispose_intervened(task, &stage_thread.thread).await?;
                    Ok(None)
                }
                LoopOutcome::BudgetFailed { cause } => Err(StageError::BudgetExhausted {
                    stage: STAGE_TRIAGE.into(),
                    context: cause.to_string(),
                }),
            }
        }
        .instrument(span)
        .await
    }

    /// Assembles the loop's opening: the candidate and the tag registry
    /// rendered through the binding's loop templates, with no prefetched
    /// similar items. The candidate corpus is reached only through the
    /// candidate-search tool, so the model fetches what it needs and the
    /// decision-row scores are grounded in candidate similarity.
    fn loop_opening(
        ctx: &TriageContext<'_>,
        prompts: &RecordedLoopPrompts,
    ) -> Result<RenderedConversation, StageError> {
        let (rendered_system, rendered_user) = assemble_loop_opening(
            prompts.system.content(),
            prompts.user.content(),
            &ctx.candidate,
            &ctx.tag_registry,
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

    /// The rendered opening for a claim: a fresh claim builds it, a resume
    /// replays the committed one.
    ///
    /// The resume signal is the committed record log itself, not a recorded
    /// resolution context: with no opening prefetch there are no opening
    /// scores to record, and the decision-row scores are reconstructed from
    /// the candidate-search tool results at commit, which survive the
    /// resume unchanged.
    async fn loop_opening_or_replay(
        &self,
        thread: &tribal_domain::AgentThread,
        ctx: &TriageContext<'_>,
        prompts: &RecordedLoopPrompts,
    ) -> Result<RenderedConversation, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_TRIAGE.into(),
                context: "acquiring connection to read the recorded opening".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let recorded = recorded_conversation(&mut conn, thread)
            .await
            .map_err(|source| {
                map_runtime_error(STAGE_TRIAGE, "reading the recorded opening", source)
            })?;
        drop(conn);
        match recorded {
            Some(rendered) => Ok(rendered),
            None => Self::loop_opening(ctx, prompts),
        }
    }

    /// Maps an accepted submission onto the existing commit machinery:
    /// the same decisions, through the same constructors, as the
    /// one-shot path.
    async fn submission_commit(
        &self,
        ctx: &TriageContext<'_>,
        submission: &TriageSubmission,
        thread: &tribal_domain::AgentThread,
        profile: &tribal_domain::EmbeddingProfile,
        deadline: tokio::time::Instant,
        attribution: &UsageAttribution,
    ) -> Result<StageCommit, StageError> {
        // The decision-row scores are the candidate-similarity provenance
        // the must-search rule already validated, reconstructed from the
        // durable tool-result log so a resumed commit reads exactly what
        // the model reasoned over.
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_TRIAGE.into(),
                context: "acquiring connection to reconstruct candidate scores".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let scores = reconstruct_candidate_scores(&mut conn, thread)
            .await
            .map_err(|source| StageError::Database {
                stage: STAGE_TRIAGE.into(),
                context: "reconstructing candidate-search scores".into(),
                source,
            })?
            .scores;
        drop(conn);

        let embedding_target = EmbeddingTarget::from(profile);
        let decision = match &submission.decision {
            TriageSubmissionDecision::Novel => {
                let tag_data = tag_resolution::resolve_tags(
                    self.pool(),
                    ctx.candidate.suggested_tags(),
                    &ctx.tag_registry,
                    self.gateway(),
                    &embedding_target,
                    self.config().tag_similarity_threshold,
                    deadline,
                    attribution,
                )
                .await?;
                // The candidate-search tool embedded for its search; the
                // novel commit embeds for storage against the active
                // profile under the commit's flip-check, the embedding it
                // alone consumes.
                let vector = self
                    .embed_candidate(
                        ctx.candidate.content(),
                        &embedding_target,
                        deadline,
                        attribution,
                    )
                    .await?
                    .vector;
                self.novel_decision(
                    ctx.job,
                    &ctx.candidate,
                    CandidateEmbedding { vector, profile },
                    tag_data,
                )
            }
            TriageSubmissionDecision::Duplicate { matched_item_id } => {
                let matched: KnowledgeItemId =
                    matched_item_id.parse().map_err(|_| StageError::Parse {
                        context: "re-reading the accepted submission's matched id".to_owned(),
                        raw_response: Some(matched_item_id.clone()),
                    })?;
                duplicate_decision(ctx.job, matched)
            }
        };

        Ok(StageCommit::Triage {
            project_id: ctx.job.project_id(),
            decision,
            similar_item_decisions: decisions_from_submission(
                ctx.job.id(),
                ctx.batch_index,
                submission,
                &scores,
            ),
        })
    }

    /// The triage stage's tool registry: the six reads, scope captured
    /// at construction. Delegates to the shared builder the lockstep test
    /// pins against the hashed surface.
    fn triage_tool_registry(
        &self,
        ctx: &TriageContext<'_>,
        active_profile: &tribal_domain::EmbeddingProfile,
        attribution: UsageAttribution,
        deadline: tokio::time::Instant,
        stage_thread: &StageThread,
    ) -> Result<ToolRegistry, StageError> {
        build_triage_registry(&TriageToolset {
            candidate_content: ctx.candidate.content().to_owned(),
            project_id: ctx.job.project_id(),
            profile: active_profile.clone(),
            gateway: Arc::clone(self.gateway()),
            attribution,
            search_limit: self.config().triage_search_limit,
            deadline,
            job_id: ctx.job.id(),
            batch_index: ctx.batch_index,
            thread_id: stage_thread.thread.id(),
        })
        .map_err(|source| StageError::BindingDerivation {
            stage: STAGE_TRIAGE.into(),
            context: source.to_string(),
        })
    }

    /// Resolves the loop templates the binding's recorded hashes pin:
    /// execution truth, hot-reload-proof, resume-stable.
    async fn recorded_loop_prompts(
        &self,
        binding: &AgentBinding,
    ) -> Result<RecordedLoopPrompts, StageError> {
        let hashes = &binding.definition().prompt_hashes;
        let [system_hash, user_hash] = hashes.as_slice() else {
            return Err(StageError::BindingDerivation {
                stage: STAGE_TRIAGE.into(),
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
                stage: STAGE_TRIAGE.into(),
                context: "acquiring connection for the loop prompts".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let system = resolve_loop_prompt(&mut conn, PromptRole::System, system_hash).await?;
        let user = resolve_loop_prompt(&mut conn, PromptRole::User, user_hash).await?;
        Ok(RecordedLoopPrompts { system, user })
    }

    /// Resolves the verifier the recorded binding names, with the one-shot
    /// verifier binding the child runs under on the parent's model and
    /// endpoint. `None` when the binding records no verifier, leaving an
    /// accepted submission to commit on the validators alone.
    ///
    /// The verifier follows the parent's recorded binding, not live
    /// configuration: its prompts are part of the parent's hash, so a
    /// resumed parent runs the verifier it was admitted with and a config
    /// change between admission and resume cannot add, drop, or re-prompt
    /// it. Each launch pins its own verifier binding on the child it
    /// creates, so the child is resume-stable thereafter.
    async fn verifier_context(
        &self,
        binding: &AgentBinding,
        candidate: &Candidate,
    ) -> Result<Option<VerifierContext>, StageError> {
        let Some(verifier) = binding.definition().verifier.clone() else {
            return Ok(None);
        };

        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_TRIAGE.into(),
                context: "acquiring connection for the verifier binding".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;

        let system =
            resolve_verifier_prompt(&mut conn, PromptRole::System, &verifier.system_prompt_hash)
                .await?;
        let user = resolve_verifier_prompt(&mut conn, PromptRole::User, &verifier.user_prompt_hash)
            .await?;
        let definition = verifier_definition(
            binding.definition(),
            system.content_hash().to_owned(),
            user.content_hash().to_owned(),
        );
        let verifier_binding = tribal_agent_runtime::resolve_binding(&mut conn, &definition)
            .await
            .map_err(|source| {
                map_runtime_error(STAGE_TRIAGE, "resolving the verifier binding", source)
            })?;

        Ok(Some(VerifierContext {
            binding_version_id: verifier_binding.id(),
            system_template: system.content().to_owned(),
            system_prompt_version_id: system.id(),
            user_template: user.content().to_owned(),
            user_prompt_version_id: user.id(),
            candidate: candidate.clone(),
        }))
    }
}

/// Resolves one recorded loop prompt by its slot and content hash.
async fn resolve_loop_prompt(
    conn: &mut sqlx::PgConnection,
    role: PromptRole,
    hash: &str,
) -> Result<PromptVersion, StageError> {
    resolve_recorded_prompt(conn, PromptClass::Loop, role, hash).await
}

/// Resolves one recorded verifier prompt by its slot and content hash, the
/// hash the parent binding pins, so the verifier runs exactly the rubric
/// the binding names.
async fn resolve_verifier_prompt(
    conn: &mut sqlx::PgConnection,
    role: PromptRole,
    hash: &str,
) -> Result<PromptVersion, StageError> {
    resolve_recorded_prompt(conn, PromptClass::Verifier, role, hash).await
}

/// Resolves one recorded triage prompt by its class, role, and the content
/// hash the binding pins.
async fn resolve_recorded_prompt(
    conn: &mut sqlx::PgConnection,
    class: PromptClass,
    role: PromptRole,
    hash: &str,
) -> Result<PromptVersion, StageError> {
    PgPromptVersionRepository
        .find_by_slot_and_hash(conn, PromptStage::Triage, class, role, hash)
        .await
        .map_err(|source| StageError::Database {
            stage: STAGE_TRIAGE.into(),
            context: "resolving a recorded prompt".into(),
            source,
        })?
        .ok_or_else(|| StageError::BindingDerivation {
            stage: STAGE_TRIAGE.into(),
            context: format!(
                "no stored triage {class:?} {} prompt matches the binding's hash",
                role.as_str(),
            ),
        })
}

/// Builds similar-item decision rows from an accepted submission: every
/// assessment grounded in a candidate-search score. The must-search rule
/// guarantees a cited id carries one, so the lookup never misses for a
/// validated submission; the guard is defence, not an expected path.
fn decisions_from_submission(
    job_id: JobId,
    batch_index: u32,
    submission: &TriageSubmission,
    scores: &HashMap<String, f64>,
) -> Vec<NewTriageSimilarItemDecision> {
    submission
        .considered_items
        .iter()
        .filter_map(|item| {
            let Some(similarity) = scores.get(&item.item_id) else {
                tracing::warn!(
                    item_id = %item.item_id,
                    "an assessed item reached commit without a candidate-similarity score",
                );
                return None;
            };
            let Ok(matched_item_id) = item.item_id.parse::<KnowledgeItemId>() else {
                tracing::warn!(
                    item_id = %item.item_id,
                    "dropping an assessment whose validated id failed to parse",
                );
                return None;
            };
            // Similarity scores persist as REAL (f32), so narrowing here
            // matches the storage precision and loses nothing.
            #[allow(clippy::cast_possible_truncation)]
            let similarity_score = *similarity as f32;
            Some(
                NewTriageSimilarItemDecision::builder()
                    .job_id(job_id)
                    .batch_index(batch_index)
                    .matched_item_id(matched_item_id)
                    .similarity_score(similarity_score)
                    .suggested_relation(item.suggested_relation)
                    .justification_text(item.justification.clone())
                    .build(),
            )
        })
        .collect()
}
