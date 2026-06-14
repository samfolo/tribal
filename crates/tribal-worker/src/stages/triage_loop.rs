//! The agentic triage stage: the loop executor's wiring.
//!
//! Context assembly is the one-shot's — the candidate, the opening
//! semantic search, the tag registry — rendered through the loop
//! templates the thread's recorded binding pins. The runtime drives the
//! turns; this module supplies the tools, the submission pipeline, and
//! the mapping from an accepted submission onto the existing commit
//! machinery. The committed shapes (knowledge items, observations,
//! similar-item decisions) are the one-shot's own constructors, so the
//! two executors cannot drift on what they write.

use std::{collections::HashMap, sync::Arc};

use tracing::Instrument;
use tribal_agent_runtime::{
    LoopOutcome, RecheckPolicy, RecordedMessage, RenderedConversation, StageThread, ToolRegistry,
    TurnLoopDeps, recorded_conversation, run_turn_loop,
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
    triage::{CandidateEmbedding, CommitEmbedding, TriageContext, duplicate_decision},
};
use crate::{
    common::{EXPECT_BATCH_INDEX, PARSE_PREVIEW_LENGTH},
    error::{STAGE_TRIAGE, StageError},
    parsing::{TriageSubmission, TriageSubmissionDecision},
    prompt::{LoopSimilarItemContext, assemble_loop_opening},
    stages::{TriageSubmissionPipeline, VerifierContext},
    tag_resolution,
    tools::{TriageToolset, build_triage_registry, submit_result_descriptor},
    worker::{Worker, heartbeat::WorkerHeartbeatPump, map_runtime_error},
};

/// The loop's recorded prompt pair, resolved from the binding's hashes.
struct RecordedLoopPrompts {
    system: PromptVersion,
    user: PromptVersion,
}

/// The assembled opening: the rendered conversation, the opening
/// search's id-to-score map, and the candidate's embedding vector for a
/// novel commit.
struct LoopOpening {
    rendered: RenderedConversation,
    scores: HashMap<String, f64>,
    embedding_vector: Vec<f32>,
}

/// The graph-dependent context an accepted submission commits with: the
/// opening search's id-to-score map, backing the similar-item decision
/// rows. Recorded in the rendered conversation's resolution context at
/// first render so a resumed thread replays the scores the model saw
/// rather than re-searching a graph that may have moved. The candidate
/// embedding is deliberately not recorded: it is re-derived per claim
/// against the current active profile, so the committed vector and its
/// profile id stay consistent even across a reindex.
#[derive(serde::Serialize, serde::Deserialize)]
struct LoopCommitContext {
    scores: HashMap<String, f64>,
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

            let (opening_rendered, embedding_vector, opening_scores) = self
                .loop_opening_or_replay(
                    &stage_thread.thread,
                    &ctx,
                    &prompts,
                    &active_profile,
                    &attribution,
                    deadline,
                )
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

            let outcome = run_turn_loop(TurnLoopDeps {
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
                budgets: crate::definition::current_stage_budgets(
                    TaskType::Triage,
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
                            CommitEmbedding {
                                vector: embedding_vector,
                                profile: &active_profile,
                            },
                            &opening_scores,
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

    /// Assembles the loop's opening: the candidate embedded and
    /// searched exactly as the one-shot does, rendered through the
    /// binding's loop templates.
    async fn loop_opening(
        &self,
        ctx: &TriageContext<'_>,
        prompts: &RecordedLoopPrompts,
        active_profile: &tribal_domain::EmbeddingProfile,
        attribution: &UsageAttribution,
        deadline: tokio::time::Instant,
    ) -> Result<LoopOpening, StageError> {
        let embedding_target = EmbeddingTarget::from(active_profile);
        let embedding_response = self
            .embed_candidate(
                ctx.candidate.content(),
                &embedding_target,
                deadline,
                attribution,
            )
            .await?;
        let search_results = self
            .search_similar_items(&embedding_response.vector, active_profile, ctx.job)
            .await?;
        let similar_items: Vec<LoopSimilarItemContext> = search_results
            .iter()
            .map(LoopSimilarItemContext::from)
            .collect();
        // The opening's scores back the similar-item decision rows an
        // accepted submission produces; assessments of items found
        // through tools carry no candidate similarity and ride the
        // submission record instead.
        let scores: HashMap<String, f64> = search_results
            .iter()
            .map(|result| (result.item.id().to_string(), result.similarity))
            .collect();

        let (rendered_system, rendered_user) = assemble_loop_opening(
            prompts.system.content(),
            prompts.user.content(),
            &ctx.candidate,
            &similar_items,
            &ctx.tag_registry,
        )?;
        Ok(LoopOpening {
            rendered: RenderedConversation {
                system: Some(rendered_system),
                messages: vec![RecordedMessage {
                    role: "user".to_owned(),
                    content: rendered_user,
                }],
                system_prompt_version_id: Some(prompts.system.id()),
                user_prompt_version_id: Some(prompts.user.id()),
                resolution_context: None,
                response_schema: None,
            },
            scores,
            embedding_vector: embedding_response.vector,
        })
    }

    /// The rendered opening, the candidate embedding if one was produced
    /// here, and the opening scores. On a fresh claim the opening is built,
    /// its scores recorded into the conversation's resolution context, and
    /// its search embedding returned. A resume replays those scores and
    /// embeds nothing: the costly search is skipped, and the candidate
    /// embedding is deferred to the novel commit (its only consumer), so a
    /// budget-recheck wake re-suspends without an inference call and a
    /// resume that commits a duplicate never embeds at all. So a resumed
    /// commit carries the scores the model reasoned over, while the vector,
    /// when a novel commit needs one, is derived against the active profile
    /// rather than scored against a graph that has since moved or labelled
    /// with a profile it was not built under.
    async fn loop_opening_or_replay(
        &self,
        thread: &tribal_domain::AgentThread,
        ctx: &TriageContext<'_>,
        prompts: &RecordedLoopPrompts,
        active_profile: &tribal_domain::EmbeddingProfile,
        attribution: &UsageAttribution,
        deadline: tokio::time::Instant,
    ) -> Result<(RenderedConversation, Option<Vec<f32>>, HashMap<String, f64>), StageError> {
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
        if let Some(rendered) =
            recorded_conversation(&mut conn, thread)
                .await
                .map_err(|source| {
                    map_runtime_error(STAGE_TRIAGE, "reading the recorded opening", source)
                })?
        {
            let scores = match rendered.resolution_context.as_ref() {
                None => {
                    return Err(StageError::Parse {
                        context: "a resumed triage loop has no recorded opening context".to_owned(),
                        raw_response: None,
                    });
                }
                Some(value) => serde_json::from_value::<LoopCommitContext>(value.clone())
                    .map(|context| context.scores)
                    .map_err(|source| StageError::Parse {
                        context: format!(
                            "the resumed triage loop's recorded opening context is malformed: \
                             {source}"
                        ),
                        raw_response: None,
                    })?,
            };
            drop(conn);
            // The resume embeds nothing: scores are replayed, and the
            // candidate embedding is deferred to the novel commit that
            // alone consumes it.
            return Ok((rendered, None, scores));
        }
        drop(conn);

        let opening = self
            .loop_opening(ctx, prompts, active_profile, attribution, deadline)
            .await?;
        let mut rendered = opening.rendered;
        rendered.resolution_context = Some(
            serde_json::to_value(&LoopCommitContext {
                scores: opening.scores.clone(),
            })
            .map_err(|source| StageError::Parse {
                context: format!("recording the opening scores: {source}"),
                raw_response: None,
            })?,
        );
        Ok((rendered, Some(opening.embedding_vector), opening.scores))
    }

    /// Maps an accepted submission onto the existing commit machinery:
    /// the same decisions, through the same constructors, as the
    /// one-shot path.
    async fn submission_commit(
        &self,
        ctx: &TriageContext<'_>,
        submission: &TriageSubmission,
        embedding: CommitEmbedding<'_>,
        opening_scores: &HashMap<String, f64>,
        deadline: tokio::time::Instant,
        attribution: &UsageAttribution,
    ) -> Result<StageCommit, StageError> {
        let embedding_target = EmbeddingTarget::from(embedding.profile);
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
                // The fresh-claim opening already embedded for its search,
                // so its vector is reused here; a resume carried none and
                // embeds only now, where the novel commit finally consumes
                // it, against the active profile under the commit's
                // flip-check.
                let vector = match embedding.vector {
                    Some(vector) => vector,
                    None => {
                        self.embed_candidate(
                            ctx.candidate.content(),
                            &embedding_target,
                            deadline,
                            attribution,
                        )
                        .await?
                        .vector
                    }
                };
                self.novel_decision(
                    ctx.job,
                    &ctx.candidate,
                    CandidateEmbedding {
                        vector,
                        profile: embedding.profile,
                    },
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
                opening_scores,
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

    /// Resolves the loop templates the binding's recorded hashes pin —
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

    /// Resolves the verifier the binding configures, when the toggle is
    /// on: the active verifier templates and the one-shot verifier binding
    /// the child runs under, on the parent's model and endpoint. `None`
    /// when verification is disabled, leaving an accepted submission to
    /// commit on the validators alone.
    ///
    /// The verifier prompts follow the active set rather than the parent's
    /// recorded binding — they are no part of its hash (the parent binds
    /// the loop pair only) — and each launch pins its own verifier binding
    /// on the child it creates, so the child is resume-stable thereafter.
    async fn verifier_context(
        &self,
        binding: &AgentBinding,
        candidate: &Candidate,
    ) -> Result<Option<VerifierContext>, StageError> {
        // Unset enables the verifier: it is the loop's default safety net,
        // disabled only by an explicit `verifier: false`.
        if !self.agents().triage.verifier_enabled(true) {
            return Ok(None);
        }

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

        let system = self
            .active_verifier_prompt(&mut conn, PromptRole::System)
            .await?;
        let user = self
            .active_verifier_prompt(&mut conn, PromptRole::User)
            .await?;
        let definition = crate::definition::verifier_definition(
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

    /// Resolves the active verifier template for a role, erroring when the
    /// verifier is on but no template is live — a wiring fault, not a
    /// configuration the binding can run.
    async fn active_verifier_prompt(
        &self,
        conn: &mut sqlx::PgConnection,
        role: PromptRole,
    ) -> Result<PromptVersion, StageError> {
        let id = self
            .active_prompts()
            .version_id(PromptStage::Triage, PromptClass::Verifier, role)
            .await
            .ok_or_else(|| StageError::BindingDerivation {
                stage: STAGE_TRIAGE.into(),
                context: format!("no active triage verifier {} prompt", role.as_str()),
            })?;
        PgPromptVersionRepository
            .find_by_id(conn, id)
            .await
            .map_err(|source| StageError::Database {
                stage: STAGE_TRIAGE.into(),
                context: "loading an active verifier prompt".into(),
                source,
            })
    }
}

/// Resolves one recorded loop prompt by its slot and content hash.
async fn resolve_loop_prompt(
    conn: &mut sqlx::PgConnection,
    role: PromptRole,
    hash: &str,
) -> Result<PromptVersion, StageError> {
    PgPromptVersionRepository
        .find_by_slot_and_hash(conn, PromptStage::Triage, PromptClass::Loop, role, hash)
        .await
        .map_err(|source| StageError::Database {
            stage: STAGE_TRIAGE.into(),
            context: "resolving a recorded loop prompt".into(),
            source,
        })?
        .ok_or_else(|| StageError::BindingDerivation {
            stage: STAGE_TRIAGE.into(),
            context: format!(
                "no stored triage loop {} prompt matches the binding's hash",
                role.as_str(),
            ),
        })
}

/// Builds similar-item decision rows from an accepted submission: the
/// assessments whose items the opening search scored. Tool-discovered
/// assessments carry no candidate similarity for this table; they live
/// in full on the submission record.
fn decisions_from_submission(
    job_id: JobId,
    batch_index: u32,
    submission: &TriageSubmission,
    opening_scores: &HashMap<String, f64>,
) -> Vec<NewTriageSimilarItemDecision> {
    submission
        .considered_items
        .iter()
        .filter_map(|item| {
            let Some(similarity) = opening_scores.get(&item.item_id) else {
                tracing::debug!(
                    item_id = %item.item_id,
                    "assessment of a tool-discovered item rides the submission record only",
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
