//! Triage stage: similarity search and LLM-based relevance scoring.

use tracing::Instrument;
use tribal_agent_runtime::StageThread;
use tribal_db::{
    ExtractionResultRepository, KnowledgeItemRepository, NewItemObservation, NewKnowledgeItem,
    NewTriageSimilarItemDecision, PgExtractionResultRepository, PgKnowledgeItemRepository,
    PgTriageResultRepository, SemanticSearchParams, SemanticSearchResult, TriageResultRepository,
};
use tribal_domain::{
    Candidate, CompletionResponse, Confidence, EmbeddingProfile, EmbeddingPurpose, Job, JobId,
    KnowledgeItemId, PrincipalId, SourceType, StageExecutorKind, TagRegistryEntry, Task, TaskType,
    span_attrs,
};
use tribal_inference::{
    EmbeddingRequest, EmbeddingResponse, EmbeddingTarget, PermitWait, UsageAttribution,
};

use super::{
    StageCommit, StageTerminal, TriageCommitDecision, map_gateway_error, record_prompt_version_ids,
    stage_attribution,
};
use crate::{
    common::{EXPECT_BATCH_INDEX, PARSE_PREVIEW_LENGTH},
    error::{STAGE_TRIAGE, StageError},
    parsing::{
        SimilarItemClassification, TriageClassification, TriageDecision, TriageItemReference,
        parse_triage_response,
    },
    prompt::{SimilarItemContext, assemble_triage_prompt},
    tag_resolution::{self, ResolvedTags},
    worker::{Worker, map_runtime_error},
};

// ---------------------------------------------------------------------------
// TriageContext
// ---------------------------------------------------------------------------

/// Context assembled before running the triage stage.
pub(crate) struct TriageContext<'a> {
    /// The parent job.
    pub job: &'a Job,
    /// The candidate extracted for this batch index.
    pub candidate: Candidate,
    /// The candidate's position in the extraction batch.
    pub batch_index: u32,
    /// The current global tag registry.
    pub tag_registry: Vec<TagRegistryEntry>,
}

/// The candidate's pre-embedded vector and the active profile it was produced
/// against, carried together into the novel commit decision so the profile id
/// reaches the commit's flip-check.
pub(super) struct CandidateEmbedding<'a> {
    pub(super) vector: Vec<f32>,
    pub(super) profile: &'a EmbeddingProfile,
}

// ---------------------------------------------------------------------------
// Stage implementation
// ---------------------------------------------------------------------------

impl Worker {
    /// Runs the triage stage for a single candidate, routed by the
    /// thread's recorded binding: the executor follows the binding, not
    /// the current configuration, so a resumed thread continues the way
    /// it started.
    ///
    /// # Errors
    ///
    /// Propagates the routed executor's [`StageError`]s; an external
    /// executor on the recorded binding is a derivation fault. Nothing
    /// can produce one in this release.
    pub(crate) async fn run_triage(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
        stage_thread: &StageThread,
        pump: &crate::worker::heartbeat::WorkerHeartbeatPump,
    ) -> Result<Option<(StageCommit, StageTerminal)>, StageError> {
        match stage_thread.binding.definition().executor {
            StageExecutorKind::OneShot => {
                self.run_triage_one_shot(job, task, deadline, stage_thread)
                    .await
            }
            StageExecutorKind::BuiltInLoop => {
                self.run_triage_loop(job, task, deadline, stage_thread, pump)
                    .await
            }
            StageExecutorKind::ExternalAgent => Err(StageError::BindingDerivation {
                stage: STAGE_TRIAGE.into(),
                context: "the external-agent executor has no runner in this release".into(),
            }),
        }
    }

    /// Runs the one-shot triage turn for a single candidate.
    ///
    /// Loads the candidate from extraction results, generates an embedding,
    /// runs semantic search for similar items, calls the triage LLM for
    /// classification, resolves tags, and builds the commit data.
    ///
    /// Returns early with a no-op if a triage result already exists for
    /// the `(job_id, batch_index)` pair (idempotency guard).
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] variant matching the failure mode:
    /// - [`StageError::Database`] for pool/repository failures.
    /// - [`StageError::SemaphoreTimeout`] if a provider semaphore
    ///   cannot be acquired within the remaining time budget.
    /// - [`StageError::TemplateRender`] if the prompt template is invalid.
    /// - [`StageError::Provider`] if the LLM or embedding call fails.
    /// - [`StageError::Parse`] if the LLM response cannot be parsed.
    ///
    /// # Panics
    ///
    /// Panics if triage provider keys are not registered in the provider
    /// registry, if a semaphore is unexpectedly closed, or if the task
    /// has no batch index.
    async fn run_triage_one_shot(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
        stage_thread: &StageThread,
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

            let (system_pv, user_pv) = tokio::try_join!(
                self.load_prompt_version(STAGE_TRIAGE, ctx.job.triage_system_prompt_version_id()),
                self.load_prompt_version(STAGE_TRIAGE, ctx.job.triage_user_prompt_version_id()),
            )?;

            record_prompt_version_ids(
                ctx.job.triage_system_prompt_version_id(),
                ctx.job.triage_user_prompt_version_id(),
            );

            // Resolve the active profile once, so the candidate embedding,
            // the similar-item search, and tag resolution all target the same
            // active space, and the producing profile id reaches the commit's
            // flip-check.
            let active_profile = self.resolve_active_embedding(STAGE_TRIAGE).await?;
            let embedding_target = EmbeddingTarget::from(&active_profile);
            let attribution = stage_attribution(ctx.job, task, &stage_thread.thread);

            let embedding_response = self
                .embed_candidate(
                    ctx.candidate.content(),
                    &embedding_target,
                    deadline,
                    &attribution,
                )
                .await?;

            let search_results = self
                .search_similar_items(&embedding_response.vector, &active_profile, ctx.job)
                .await?;

            let similar_items: Vec<SimilarItemContext> = search_results
                .iter()
                .map(SimilarItemContext::from)
                .collect();

            // The model addresses similar items by position; the rendered
            // list and the search results it resolves against must agree
            // index-for-index. Length first, since zip would otherwise hide a
            // divergence where one list is a prefix of the other.
            debug_assert_eq!(
                similar_items.len(),
                search_results.len(),
                "similar_items and search_results differ in length",
            );
            for (i, (rendered, result)) in similar_items.iter().zip(&search_results).enumerate() {
                debug_assert_eq!(
                    rendered.item_id,
                    result.item.id(),
                    "similar_items[{i}] diverged from search_results — positional alignment broken",
                );
            }

            let fresh_slots: Vec<SimilarItemSlot> =
                search_results.iter().map(SimilarItemSlot::from).collect();

            let Some((mut classification, response, slots)) = self
                .classify_candidate(
                    &ctx,
                    task,
                    stage_thread,
                    system_pv.content(),
                    user_pv.content(),
                    &similar_items,
                    fresh_slots,
                    deadline,
                    &attribution,
                )
                .await?
            else {
                return Ok(None);
            };

            classification.reconcile();

            // Resolve the duplicate's context index to a concrete item once,
            // before tag resolution: an out-of-range index downgrades to Novel
            // here so the candidate still resolves its tags below and commits
            // as a novel item, rather than panicking at commit time.
            let resolved_outcome = resolve_triage_outcome(
                &classification.outcome,
                &slots,
                ctx.job.id(),
                ctx.batch_index,
            );

            let embedding_vector = embedding_response.vector;

            let resolved_tags = match &resolved_outcome {
                ResolvedTriageOutcome::Novel => Some(
                    tag_resolution::resolve_tags(
                        self.pool(),
                        ctx.candidate.suggested_tags(),
                        &ctx.tag_registry,
                        self.gateway(),
                        &embedding_target,
                        self.config().tag_similarity_threshold,
                        deadline,
                        &attribution,
                    )
                    .await?,
                ),
                ResolvedTriageOutcome::Duplicate { .. } => None,
            };

            let commit = self.build_triage_commit(
                &ctx,
                &resolved_outcome,
                &classification.similar_item_decisions,
                &slots,
                CandidateEmbedding {
                    vector: embedding_vector,
                    profile: &active_profile,
                },
                resolved_tags,
            );

            Ok(Some((commit, StageTerminal::Completion(Some(response)))))
        }
        .instrument(span)
        .await
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

impl Worker {
    /// Checks whether a triage result already exists for the given
    /// `(job_id, batch_index)` pair.
    pub(super) async fn check_triage_idempotency(
        &self,
        job_id: JobId,
        batch_index: u32,
    ) -> Result<bool, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| triage_sqlx_error("acquiring connection for idempotency check", e))?;

        let existing = PgTriageResultRepository
            .find_by_job_id_and_batch_index(&mut conn, job_id, batch_index)
            .await
            .map_err(|e| triage_db_error("checking triage idempotency", e))?;

        Ok(existing.is_some())
    }

    /// Loads the candidate at `batch_index` from the extraction result.
    pub(super) async fn load_triage_candidate(
        &self,
        job_id: JobId,
        batch_index: u32,
    ) -> Result<Candidate, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| triage_sqlx_error("acquiring connection for candidate", e))?;

        let extraction_result = PgExtractionResultRepository
            .find_by_job_id(&mut conn, job_id)
            .await
            .map_err(|e| triage_db_error("loading extraction result", e))?
            .ok_or_else(|| StageError::Database {
                stage: STAGE_TRIAGE.into(),
                context: format!("no extraction result for job {job_id}"),
                source: tribal_db::DbError::NotFound {
                    entity: "extraction_result",
                    id: job_id.to_string(),
                },
            })?;

        let candidates_json = extraction_result.candidates().clone();
        let candidates: Vec<Candidate> =
            serde_json::from_value(candidates_json.clone()).map_err(|_| {
                let raw: String = candidates_json
                    .to_string()
                    .chars()
                    .take(PARSE_PREVIEW_LENGTH)
                    .collect();
                StageError::Parse {
                    context: format!("deserialising candidates for job {job_id}"),
                    raw_response: Some(raw),
                }
            })?;

        candidates
            .get(batch_index as usize)
            .cloned()
            .ok_or_else(|| StageError::Database {
                stage: STAGE_TRIAGE.into(),
                context: format!(
                    "batch index {batch_index} out of bounds (candidates: {})",
                    candidates.len(),
                ),
                source: tribal_db::DbError::NotFound {
                    entity: "candidate",
                    id: format!("{job_id}[{batch_index}]"),
                },
            })
    }

    /// Generates an embedding for the candidate content.
    pub(super) async fn embed_candidate(
        &self,
        content: &str,
        target: &EmbeddingTarget,
        deadline: tokio::time::Instant,
        attribution: &UsageAttribution,
    ) -> Result<EmbeddingResponse, StageError> {
        let request = EmbeddingRequest {
            input: content.to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        };
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        self.gateway()
            .embed(
                target,
                request,
                PermitWait::Bounded { limit: remaining },
                attribution,
            )
            .await
            .map_err(|e| map_gateway_error("triage embedding call", e))
    }

    /// Runs semantic search against existing knowledge items using the
    /// candidate's embedding vector.
    pub(super) async fn search_similar_items(
        &self,
        embedding: &[f32],
        profile: &EmbeddingProfile,
        job: &Job,
    ) -> Result<Vec<SemanticSearchResult>, StageError> {
        let span = tracing::info_span!(
            "tribal.semantic_search",
            { span_attrs::SEARCH_LIMIT } = self.config().triage_search_limit,
            { span_attrs::SEARCH_RESULTS_COUNT } = tracing::field::Empty,
        );

        async {
            let mut conn =
                self.pool().acquire().await.map_err(|e| {
                    triage_sqlx_error("acquiring connection for semantic search", e)
                })?;

            let params = SemanticSearchParams::builder()
                .query_embedding(embedding.to_vec())
                .embedding_profile_id(profile.id())
                .dimensions(profile.dimensions())
                .project_id(Some(job.project_id()))
                .limit(self.config().triage_search_limit)
                .build();

            let response = PgKnowledgeItemRepository
                .semantic_search(&mut conn, &params)
                .await
                .map_err(|e| triage_db_error("semantic search", e))?;

            tracing::Span::current()
                .record(span_attrs::SEARCH_RESULTS_COUNT, response.results.len());

            Ok(response.results)
        }
        .instrument(span)
        .await
    }

    /// Assembles the triage prompt, calls the triage LLM, and parses
    /// the classification response.
    #[allow(clippy::too_many_arguments)]
    async fn classify_candidate(
        &self,
        ctx: &TriageContext<'_>,
        task: &Task,
        stage_thread: &StageThread,
        system_template: &str,
        user_template: &str,
        similar_items: &[SimilarItemContext],
        fresh_slots: Vec<SimilarItemSlot>,
        deadline: tokio::time::Instant,
        attribution: &UsageAttribution,
    ) -> Result<
        Option<(
            TriageClassification,
            CompletionResponse,
            Vec<SimilarItemSlot>,
        )>,
        StageError,
    > {
        let include_llm_content = self.include_llm_content();

        let request = assemble_triage_prompt(
            system_template,
            user_template,
            &ctx.candidate,
            similar_items,
            &ctx.tag_registry,
            &stage_thread.binding.definition().parameters,
        )?;

        if include_llm_content {
            tracing::debug!(
                system_prompt = %request.system.as_deref().unwrap_or(""),
                user_prompt = %request.messages.first().map_or("", |m| m.content()),
                "triage prompt assembled",
            );
        }

        let recorded_context = serde_json::to_value(&fresh_slots).map_err(|source| {
            map_runtime_error(
                STAGE_TRIAGE,
                "serialising the similar-item slots",
                tribal_agent_runtime::AgentRuntimeError::ContentSerialisation {
                    context: "serialising the similar-item slots".to_owned(),
                    source,
                },
            )
        })?;
        let Some(bracketed) = self
            .bracket_one_shot(
                STAGE_TRIAGE,
                stage_thread,
                ctx.job,
                task,
                request,
                Some(recorded_context),
            )
            .await?
        else {
            return Ok(None);
        };

        // The model's context indices address the list its conversation
        // rendered: on resume that is the recorded one, never this
        // attempt's re-derived search. A record from before slots were
        // recorded falls back to the fresh search, the old semantics.
        let slots = match bracketed.resolution_context {
            Some(context) => serde_json::from_value(context).map_err(|source| {
                map_runtime_error(
                    STAGE_TRIAGE,
                    "deserialising the recorded similar-item slots",
                    tribal_agent_runtime::AgentRuntimeError::ContentSerialisation {
                        context: "deserialising the recorded similar-item slots".to_owned(),
                        source,
                    },
                )
            })?,
            None => fresh_slots,
        };
        let request = bracketed.request;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let response = self
            .gateway()
            .complete(
                TaskType::Triage,
                request,
                PermitWait::Bounded { limit: remaining },
                attribution,
            )
            .await
            .map_err(|e| map_gateway_error("triage LLM call", e))?;

        if include_llm_content {
            tracing::debug!(
                response = %response.text,
                "triage response received",
            );
        }

        let classification = {
            let _parse_span = tracing::info_span!("tribal.triage.parse").entered();
            parse_triage_response(&response).inspect_err(|_| {
                if include_llm_content {
                    let preview: String =
                        response.text.chars().take(PARSE_PREVIEW_LENGTH).collect();
                    tracing::debug!(preview = %preview, "parse failure — raw LLM response");
                } else {
                    tracing::debug!(
                        response_length = response.text.len(),
                        "parse failure — response details redacted",
                    );
                }
            })?
        };

        Ok(Some((classification, response, slots)))
    }

    /// Builds the `StageCommit::Triage` variant from the resolved outcome,
    /// the per-similar-item decisions, the resolved tags, and the embedding
    /// vector.
    ///
    /// The `outcome` is already resolved: a `Duplicate` bears its concrete
    /// `KnowledgeItemId`, so no index resolution happens here.
    fn build_triage_commit(
        &self,
        ctx: &TriageContext<'_>,
        outcome: &ResolvedTriageOutcome,
        similar_item_decisions: &[SimilarItemClassification],
        slots: &[SimilarItemSlot],
        embedding: CandidateEmbedding<'_>,
        resolved_tags: Option<ResolvedTags>,
    ) -> StageCommit {
        let similar_item_decisions = build_similar_item_decisions(
            ctx.job.id(),
            ctx.job.principal_id(),
            ctx.batch_index,
            similar_item_decisions,
            slots,
        );

        let decision = match outcome {
            ResolvedTriageOutcome::Novel => {
                let tag_data =
                    resolved_tags.expect("resolved tags required for Novel classification");
                self.novel_decision(ctx.job, &ctx.candidate, embedding, tag_data)
            }
            ResolvedTriageOutcome::Duplicate { matched_item_id } => {
                duplicate_decision(ctx.job, *matched_item_id)
            }
        };

        StageCommit::Triage {
            project_id: ctx.job.project_id(),
            decision,
            similar_item_decisions,
        }
    }

    /// Builds the novel-item commit decision: the new knowledge item
    /// with its resolved tags and pre-embedded vector. Shared by both
    /// executors, so the committed shape cannot drift between them.
    pub(super) fn novel_decision(
        &self,
        job: &Job,
        candidate: &Candidate,
        embedding: CandidateEmbedding<'_>,
        tag_data: ResolvedTags,
    ) -> TriageCommitDecision {
        let mut all_tags = tag_data.resolved.clone();
        all_tags.extend(tag_data.new_tags.iter().map(|t| t.tag.clone()));

        let extraction_identity = self.gateway().completion_identity(TaskType::Extraction);
        let source_context = serde_json::json!({
            "provider": extraction_identity.name,
            "model": extraction_identity.model,
        });

        let knowledge_item = Box::new(
            NewKnowledgeItem::builder()
                .project_id(job.project_id())
                .principal_id(job.principal_id())
                .kind(candidate.kind())
                .content(candidate.content().to_owned())
                .tags(all_tags)
                .confidence(Confidence::Inferred)
                .source_context(source_context)
                .episode_id(job.correlation_id())
                .build(),
        );

        TriageCommitDecision::Novel {
            knowledge_item,
            embedding_vector: embedding.vector,
            embedding_model: embedding.profile.model().to_owned(),
            embedding_profile_id: embedding.profile.id(),
            suggested_references: candidate.suggested_references().to_vec(),
            new_tags: tag_data.new_tags,
            resolved_tags: tag_data.resolved,
        }
    }
}

/// Builds the duplicate commit decision: an observation against the
/// matched item. Shared by both executors.
pub(super) fn duplicate_decision(
    job: &Job,
    matched_item_id: KnowledgeItemId,
) -> TriageCommitDecision {
    let observation = NewItemObservation::builder()
        .knowledge_item_id(matched_item_id)
        .principal_id(job.principal_id())
        .source_type(SourceType::AgentMediated)
        .build();

    TriageCommitDecision::Duplicate { observation }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The triage outcome with its duplicate match resolved to a concrete
/// knowledge item.
///
/// A `Duplicate` here already bears its resolved [`KnowledgeItemId`], so the
/// id is consumed directly without re-resolving an index. A duplicate whose
/// index matched no search result is represented as `Novel` (see
/// [`resolve_triage_outcome`]).
enum ResolvedTriageOutcome {
    /// The candidate is novel — a new knowledge item will be created.
    Novel,
    /// The candidate duplicates an existing, already-resolved item.
    Duplicate { matched_item_id: KnowledgeItemId },
}

/// One similar-item slot as the model saw it: the positional source the
/// model's context indices resolve against. Recorded with the input
/// record, so a resumed attempt resolves the answer against what its
/// first attempt rendered — never against a re-derived search, whose
/// ranking can drift as sibling commits land between attempts.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
struct SimilarItemSlot {
    /// The knowledge item rendered at this position.
    item_id: KnowledgeItemId,
    /// The similarity the search scored it with.
    similarity: f64,
}

impl From<&SemanticSearchResult> for SimilarItemSlot {
    fn from(result: &SemanticSearchResult) -> Self {
        Self {
            item_id: result.item.id(),
            similarity: result.similarity,
        }
    }
}

/// Resolves a similar-item reference to its slot, returning `None` if
/// the index is out of range.
fn lookup_similar_item<'a>(
    reference: &TriageItemReference,
    slots: &'a [SimilarItemSlot],
) -> Option<&'a SimilarItemSlot> {
    let TriageItemReference::ContextIndex { context_index } = reference;
    slots.get(*context_index as usize)
}

/// Resolves a parsed [`TriageDecision`] into a [`ResolvedTriageOutcome`].
///
/// A `Duplicate` whose context index addresses a real search result is
/// resolved to that item's [`KnowledgeItemId`]. A `Duplicate` whose index is
/// out of range is downgraded to `Novel` with a warning — the candidate is
/// still ingested, just as a new item rather than an observation.
fn resolve_triage_outcome(
    outcome: &TriageDecision,
    slots: &[SimilarItemSlot],
    job_id: JobId,
    batch_index: u32,
) -> ResolvedTriageOutcome {
    match outcome {
        TriageDecision::Novel => ResolvedTriageOutcome::Novel,
        TriageDecision::Duplicate { matched_item } => {
            if let Some(slot) = lookup_similar_item(matched_item, slots) {
                ResolvedTriageOutcome::Duplicate {
                    matched_item_id: slot.item_id,
                }
            } else {
                tracing::warn!(
                    item = ?matched_item,
                    slot_count = slots.len(),
                    %job_id,
                    %batch_index,
                    "downgrading duplicate to novel — matched context index out of range",
                );
                ResolvedTriageOutcome::Novel
            }
        }
    }
}

/// Builds `NewTriageSimilarItemDecision` records by resolving each
/// classification's context index against the positionally-aligned
/// slots the model saw, taking the similarity score from the same slot.
///
/// An out-of-range index is dropped with a warning rather than failing the
/// stage. Each index addresses a position in both the rendered similar-items
/// list and the slots, an alignment guarded at the call site that builds
/// those lists.
fn build_similar_item_decisions(
    job_id: JobId,
    principal_id: PrincipalId,
    batch_index: u32,
    classifications: &[SimilarItemClassification],
    slots: &[SimilarItemSlot],
) -> Vec<NewTriageSimilarItemDecision> {
    classifications
        .iter()
        .filter_map(|c| {
            let Some(slot) = lookup_similar_item(&c.item, slots) else {
                tracing::warn!(
                    item = ?c.item,
                    slot_count = slots.len(),
                    %job_id,
                    %batch_index,
                    "dropping similar-item classification for index not in the rendered slots",
                );
                return None;
            };

            // Similarity scores persist as REAL (f32), so narrowing here
            // matches the storage precision and loses nothing.
            #[allow(clippy::cast_possible_truncation)]
            let similarity_score = slot.similarity as f32;

            Some(
                NewTriageSimilarItemDecision::builder()
                    .job_id(job_id)
                    .principal_id(Some(principal_id))
                    .batch_index(batch_index)
                    .matched_item_id(slot.item_id)
                    .similarity_score(similarity_score)
                    .suggested_relation(c.suggested_relation)
                    .justification_text(c.justification.clone())
                    .build(),
            )
        })
        .collect()
}

/// Builds a [`StageError::Database`] for the triage stage.
fn triage_db_error(context: &str, source: tribal_db::DbError) -> StageError {
    StageError::Database {
        stage: STAGE_TRIAGE.into(),
        context: context.into(),
        source,
    }
}

/// Wraps a raw [`sqlx::Error`] into a [`StageError::Database`] for the
/// triage stage.
fn triage_sqlx_error(context: &str, source: sqlx::Error) -> StageError {
    triage_db_error(
        context,
        tribal_db::DbError::QueryFailed {
            context: context.into(),
            source,
        },
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::RelationSuggestion;
    use tribal_test_utils::a_knowledge_item;

    use super::*;

    #[test]
    fn test_resolve_triage_outcome_downgrades_out_of_range_duplicate() {
        // A duplicate referencing an index with no backing search result
        // downgrades to Novel rather than failing the stage.
        let slots: Vec<SimilarItemSlot> = vec![];
        let outcome = TriageDecision::Duplicate {
            matched_item: TriageItemReference::ContextIndex { context_index: 0 },
        };
        let resolved = resolve_triage_outcome(&outcome, &slots, JobId::new(), 0);
        assert!(matches!(resolved, ResolvedTriageOutcome::Novel));
    }

    #[test]
    fn test_resolve_triage_outcome_passes_novel_through() {
        let slots: Vec<SimilarItemSlot> = vec![];
        let resolved = resolve_triage_outcome(&TriageDecision::Novel, &slots, JobId::new(), 0);
        assert!(matches!(resolved, ResolvedTriageOutcome::Novel));
    }

    #[test]
    fn test_resolve_triage_outcome_resolves_in_range_duplicate() {
        // A duplicate referencing an in-range, non-zero index resolves to that
        // entry's item id.
        let id_a = a_knowledge_item().build().id();
        let id_b = a_knowledge_item().build().id();
        let slots = vec![
            SimilarItemSlot {
                item_id: id_a,
                similarity: 0.9,
            },
            SimilarItemSlot {
                item_id: id_b,
                similarity: 0.5,
            },
        ];
        let outcome = TriageDecision::Duplicate {
            matched_item: TriageItemReference::ContextIndex { context_index: 1 },
        };
        let resolved = resolve_triage_outcome(&outcome, &slots, JobId::new(), 0);
        assert!(matches!(
            resolved,
            ResolvedTriageOutcome::Duplicate { matched_item_id } if matched_item_id == id_b
        ));
    }

    #[test]
    fn test_build_similar_item_decisions_resolves_by_index_and_drops_out_of_range() {
        // Indices deliberately diverge from classification order, and one is
        // out of range. This fails for any implementation that maps by
        // position rather than by the context index value.
        let id_a = a_knowledge_item().build().id();
        let id_b = a_knowledge_item().build().id();
        let id_c = a_knowledge_item().build().id();
        let slots = vec![
            SimilarItemSlot {
                item_id: id_a,
                similarity: 0.5,
            },
            SimilarItemSlot {
                item_id: id_b,
                similarity: 0.25,
            },
            SimilarItemSlot {
                item_id: id_c,
                similarity: 0.75,
            },
        ];

        let classifications = vec![
            SimilarItemClassification {
                item: TriageItemReference::ContextIndex { context_index: 2 },
                suggested_relation: RelationSuggestion::Supports,
                justification: "resolves to item_c".to_owned(),
            },
            SimilarItemClassification {
                item: TriageItemReference::ContextIndex { context_index: 99 },
                suggested_relation: RelationSuggestion::Contradicts,
                justification: "out of range — dropped".to_owned(),
            },
            SimilarItemClassification {
                item: TriageItemReference::ContextIndex { context_index: 0 },
                suggested_relation: RelationSuggestion::Unrelated,
                justification: "resolves to item_a".to_owned(),
            },
        ];

        let principal_id = PrincipalId::new();
        let rows =
            build_similar_item_decisions(JobId::new(), principal_id, 0, &classifications, &slots);

        // Out-of-range dropped; survivors keep classification order and each
        // resolves by index value to the right item and that entry's similarity.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].principal_id, Some(principal_id));
        assert_eq!(rows[0].matched_item_id, id_c);
        assert!((rows[0].similarity_score - 0.75).abs() < f32::EPSILON);
        assert_eq!(rows[1].matched_item_id, id_a);
        assert!((rows[1].similarity_score - 0.5).abs() < f32::EPSILON);
    }
}
