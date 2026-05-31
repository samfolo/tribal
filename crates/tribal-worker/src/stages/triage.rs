//! Triage stage: similarity search and LLM-based relevance scoring.

use std::{sync::Arc, time::Instant};

use tokio::sync::Semaphore;
use tracing::Instrument;
use tribal_db::{
    ExtractionResultRepository, KnowledgeItemRepository, NewItemObservation, NewKnowledgeItem,
    NewTriageSimilarItemDecision, PgExtractionResultRepository, PgKnowledgeItemRepository,
    PgTriageResultRepository, SemanticSearchParams, SemanticSearchResult, TriageResultRepository,
};
use tribal_domain::{
    Candidate, Confidence, EmbeddingPurpose, Job, JobId, KnowledgeItemId, SourceType,
    StageParameters, TagRegistryEntry, Task, span_attrs,
};
use tribal_inference::{
    EmbeddingRequest, EmbeddingResponse, InferenceProvider, ProviderKey, Usage,
};

use super::{
    StageCommit, StageOutput, TriageCommitDecision, load_active_embedding_profile,
    record_prompt_version_ids,
};
use crate::{
    common::{EXPECT_BATCH_INDEX, PARSE_PREVIEW_LENGTH},
    error::{SEMAPHORE_CLOSED, STAGE_TRIAGE, StageError},
    parsing::{
        SimilarItemClassification, TriageClassification, TriageDecision, TriageItemReference,
        parse_triage_response,
    },
    prompt::{SimilarItemContext, assemble_triage_prompt},
    tag_resolution::{self, ResolvedTags},
    worker::Worker,
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

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const EXPECT_TRIAGE_INFERENCE_KEY: &str = "triage inference key registered at startup";
const EXPECT_TRIAGE_EMBEDDING_KEY: &str = "triage embedding key registered at startup";

// ---------------------------------------------------------------------------
// Triage accessors
// ---------------------------------------------------------------------------

impl Worker {
    /// Returns a reference to the triage inference provider.
    pub(crate) fn triage_provider(&self) -> &Arc<dyn InferenceProvider> {
        &self.triage_provider
    }

    /// Returns the triage inference provider key.
    pub(crate) fn triage_inference_key(&self) -> &ProviderKey {
        &self.triage_inference_key
    }

    /// Returns the triage embedding provider key.
    pub(crate) fn triage_embedding_key(&self) -> &ProviderKey {
        &self.triage_embedding_key
    }

    /// Returns the triage inference semaphore from the provider registry.
    ///
    /// # Panics
    ///
    /// Panics if the triage inference key is not registered in the
    /// provider registry.
    pub(crate) fn triage_inference_semaphore(&self) -> &Arc<Semaphore> {
        self.provider_registry()
            .semaphore(self.triage_inference_key())
            .expect(EXPECT_TRIAGE_INFERENCE_KEY)
    }

    /// Returns the triage embedding semaphore from the provider registry.
    ///
    /// # Panics
    ///
    /// Panics if the triage embedding key is not registered in the
    /// provider registry.
    pub(crate) fn triage_embedding_semaphore(&self) -> &Arc<Semaphore> {
        self.provider_registry()
            .semaphore(self.triage_embedding_key())
            .expect(EXPECT_TRIAGE_EMBEDDING_KEY)
    }
}

// ---------------------------------------------------------------------------
// Stage implementation
// ---------------------------------------------------------------------------

impl Worker {
    /// Runs the triage stage for a single candidate.
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
    pub(crate) async fn run_triage(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
    ) -> Result<StageOutput, StageError> {
        let batch_index = task.batch_index().expect(EXPECT_BATCH_INDEX);

        let span = tracing::info_span!(
            "tribal.task.triage",
            { span_attrs::BATCH_INDEX } = batch_index,
            { span_attrs::TASK_ID } = %task.id(),
            { span_attrs::LLM_STAGE } = "triage",
            { span_attrs::RETRY_COUNT } = task.retry_count(),
            { span_attrs::LLM_SYSTEM_PROMPT_VERSION_ID } = tracing::field::Empty,
            { span_attrs::LLM_USER_PROMPT_VERSION_ID } = tracing::field::Empty,
        );

        async {
            if self.check_triage_idempotency(job.id(), batch_index).await? {
                return Ok(StageOutput {
                    commit: StageCommit::Triage {
                        project_id: job.project_id(),
                        decision: TriageCommitDecision::NoOp,
                        similar_item_decisions: vec![],
                    },
                    usages: vec![],
                });
            }

            let candidate = self.load_triage_candidate(job.id(), batch_index).await?;
            let tag_registry = self.load_tag_registry(STAGE_TRIAGE).await?;
            let ctx = TriageContext {
                job,
                candidate,
                batch_index,
                tag_registry,
            };

            let (system_pv, user_pv, fingerprint) = tokio::try_join!(
                self.load_prompt_version(STAGE_TRIAGE, ctx.job.triage_system_prompt_version_id()),
                self.load_prompt_version(STAGE_TRIAGE, ctx.job.triage_user_prompt_version_id()),
                self.load_system_fingerprint(STAGE_TRIAGE, ctx.job.system_fingerprint_hash()),
            )?;

            record_prompt_version_ids(
                ctx.job.triage_system_prompt_version_id(),
                ctx.job.triage_user_prompt_version_id(),
            );

            let embedding_response = self
                .embed_candidate(ctx.candidate.content(), deadline)
                .await?;

            let search_results = self
                .search_similar_items(&embedding_response.vector, ctx.job)
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

            let (mut classification, completion_response) = self
                .classify_candidate(
                    &ctx,
                    system_pv.content(),
                    user_pv.content(),
                    &similar_items,
                    &fingerprint.inference_parameters().triage,
                    deadline,
                )
                .await?;

            classification.reconcile();

            // Resolve the duplicate's context index to a concrete item once,
            // before tag resolution: an out-of-range index downgrades to Novel
            // here so the candidate still resolves its tags below and commits
            // as a novel item, rather than panicking at commit time.
            let resolved_outcome = resolve_triage_outcome(
                &classification.outcome,
                &search_results,
                ctx.job.id(),
                ctx.batch_index,
            );

            let embedding_usage = embedding_response.usage;
            let embedding_vector = embedding_response.vector;

            let (resolved_tags, tag_usages) = match &resolved_outcome {
                ResolvedTriageOutcome::Novel => {
                    let (resolved, usages) = tag_resolution::resolve_tags(
                        self.pool(),
                        ctx.candidate.suggested_tags(),
                        &ctx.tag_registry,
                        self.embedding_provider(),
                        self.triage_embedding_semaphore(),
                        &self.triage_embedding_key().to_string(),
                        self.config().tag_similarity_threshold,
                        deadline,
                        self.metrics(),
                    )
                    .await?;
                    (Some(resolved), usages)
                }
                ResolvedTriageOutcome::Duplicate { .. } => (None, vec![]),
            };

            let commit = self.build_triage_commit(
                &ctx,
                &resolved_outcome,
                &classification.similar_item_decisions,
                &search_results,
                embedding_vector,
                resolved_tags,
            );

            let mut usages = vec![
                Usage::Embedding {
                    usage: embedding_usage,
                    purpose: EmbeddingPurpose::Candidate,
                },
                Usage::Completion {
                    usage: completion_response.usage,
                },
            ];
            usages.extend(tag_usages);

            Ok(StageOutput { commit, usages })
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
    async fn check_triage_idempotency(
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
    async fn load_triage_candidate(
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
    async fn embed_candidate(
        &self,
        content: &str,
        deadline: tokio::time::Instant,
    ) -> Result<EmbeddingResponse, StageError> {
        let semaphore = self.triage_embedding_semaphore();
        let provider_key = self.triage_embedding_key().to_string();
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let semaphore_start = Instant::now();
        let _permit = tokio::time::timeout(remaining, Arc::clone(semaphore).acquire_owned())
            .await
            .map_err(|_| StageError::SemaphoreTimeout {
                provider_key: provider_key.clone(),
            })?
            .expect(SEMAPHORE_CLOSED);
        self.metrics()
            .record_semaphore_acquire(&provider_key, semaphore_start.elapsed());

        let request = EmbeddingRequest {
            input: content.to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        };

        let provider_start = Instant::now();
        let response = self
            .embedding_provider()
            .embed(request)
            .await
            .map_err(|e| StageError::Provider {
                context: "triage embedding call".into(),
                source: e,
            })?;
        let identity = self.embedding_provider().identity();
        self.metrics().record_provider_call(
            &identity.name,
            &identity.model,
            "triage_embedding",
            provider_start.elapsed(),
        );
        Ok(response)
    }

    /// Runs semantic search against existing knowledge items using the
    /// candidate's embedding vector.
    async fn search_similar_items(
        &self,
        embedding: &[f32],
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

            let profile = load_active_embedding_profile(&mut conn, STAGE_TRIAGE).await?;

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
    async fn classify_candidate(
        &self,
        ctx: &TriageContext<'_>,
        system_template: &str,
        user_template: &str,
        similar_items: &[SimilarItemContext],
        params: &StageParameters,
        deadline: tokio::time::Instant,
    ) -> Result<(TriageClassification, tribal_inference::CompletionResponse), StageError> {
        let include_llm_content = self.include_llm_content();

        let request = assemble_triage_prompt(
            system_template,
            user_template,
            &ctx.candidate,
            similar_items,
            &ctx.tag_registry,
            params,
        )?;

        if include_llm_content {
            tracing::debug!(
                system_prompt = %request.system.as_deref().unwrap_or(""),
                user_prompt = %request.messages.first().map_or("", |m| m.content.as_str()),
                "triage prompt assembled",
            );
        }

        let semaphore = self.triage_inference_semaphore();
        let provider_key = self.triage_inference_key().to_string();
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let semaphore_start = Instant::now();
        let _permit = tokio::time::timeout(remaining, Arc::clone(semaphore).acquire_owned())
            .await
            .map_err(|_| StageError::SemaphoreTimeout {
                provider_key: provider_key.clone(),
            })?
            .expect(SEMAPHORE_CLOSED);
        self.metrics()
            .record_semaphore_acquire(&provider_key, semaphore_start.elapsed());

        let provider_start = Instant::now();
        let response = self
            .triage_provider()
            .complete(request)
            .await
            .map_err(|e| StageError::Provider {
                context: "triage LLM call".into(),
                source: e,
            })?;
        let identity = self.triage_provider().identity();
        self.metrics().record_provider_call(
            &identity.name,
            &identity.model,
            "triage_inference",
            provider_start.elapsed(),
        );

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

        Ok((classification, response))
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
        search_results: &[SemanticSearchResult],
        embedding_vector: Vec<f32>,
        resolved_tags: Option<ResolvedTags>,
    ) -> StageCommit {
        let similar_item_decisions = build_similar_item_decisions(
            ctx.job.id(),
            ctx.batch_index,
            similar_item_decisions,
            search_results,
        );

        let decision = match outcome {
            ResolvedTriageOutcome::Novel => {
                let tag_data =
                    resolved_tags.expect("resolved tags required for Novel classification");

                let mut all_tags = tag_data.resolved.clone();
                all_tags.extend(tag_data.new_tags.iter().map(|t| t.tag.clone()));

                let extraction_identity = self.extraction_provider().identity();
                let source_context = serde_json::json!({
                    "provider": extraction_identity.name,
                    "model": extraction_identity.model,
                });

                let knowledge_item = Box::new(
                    NewKnowledgeItem::builder()
                        .project_id(ctx.job.project_id())
                        .principal_id(ctx.job.principal_id())
                        .kind(ctx.candidate.kind())
                        .content(ctx.candidate.content().to_owned())
                        .tags(all_tags)
                        .confidence(Confidence::Inferred)
                        .source_context(source_context)
                        .episode_id(ctx.job.correlation_id())
                        .build(),
                );

                let embedding_identity = self.embedding_provider().identity();

                TriageCommitDecision::Novel {
                    knowledge_item,
                    embedding_vector,
                    embedding_model: embedding_identity.model.clone(),
                    suggested_references: ctx.candidate.suggested_references().to_vec(),
                    new_tags: tag_data.new_tags,
                    resolved_tags: tag_data.resolved,
                }
            }
            ResolvedTriageOutcome::Duplicate { matched_item_id } => {
                let observation = NewItemObservation::builder()
                    .knowledge_item_id(*matched_item_id)
                    .principal_id(ctx.job.principal_id())
                    .source_type(SourceType::AgentMediated)
                    .build();

                TriageCommitDecision::Duplicate { observation }
            }
        };

        StageCommit::Triage {
            project_id: ctx.job.project_id(),
            decision,
            similar_item_decisions,
        }
    }
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

/// Resolves a similar-item reference to its entry in the search results,
/// returning `None` if the index is out of range.
fn lookup_similar_item<'a>(
    reference: &TriageItemReference,
    search_results: &'a [SemanticSearchResult],
) -> Option<&'a SemanticSearchResult> {
    let TriageItemReference::ContextIndex { context_index } = reference;
    search_results.get(*context_index as usize)
}

/// Resolves a parsed [`TriageDecision`] into a [`ResolvedTriageOutcome`].
///
/// A `Duplicate` whose context index addresses a real search result is
/// resolved to that item's [`KnowledgeItemId`]. A `Duplicate` whose index is
/// out of range is downgraded to `Novel` with a warning — the candidate is
/// still ingested, just as a new item rather than an observation.
fn resolve_triage_outcome(
    outcome: &TriageDecision,
    search_results: &[SemanticSearchResult],
    job_id: JobId,
    batch_index: u32,
) -> ResolvedTriageOutcome {
    match outcome {
        TriageDecision::Novel => ResolvedTriageOutcome::Novel,
        TriageDecision::Duplicate { matched_item } => {
            if let Some(result) = lookup_similar_item(matched_item, search_results) {
                ResolvedTriageOutcome::Duplicate {
                    matched_item_id: result.item.id(),
                }
            } else {
                tracing::warn!(
                    item = ?matched_item,
                    search_result_count = search_results.len(),
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
/// classification's context index against the positionally-aligned search
/// results, taking the similarity score from the same entry.
///
/// An out-of-range index is dropped with a warning rather than failing the
/// stage. Each index addresses a position in both the rendered similar-items
/// list and `search_results`, an alignment guarded at the call site that
/// builds those lists.
fn build_similar_item_decisions(
    job_id: JobId,
    batch_index: u32,
    classifications: &[SimilarItemClassification],
    search_results: &[SemanticSearchResult],
) -> Vec<NewTriageSimilarItemDecision> {
    classifications
        .iter()
        .filter_map(|c| {
            let Some(result) = lookup_similar_item(&c.item, search_results) else {
                tracing::warn!(
                    item = ?c.item,
                    search_result_count = search_results.len(),
                    %job_id,
                    %batch_index,
                    "dropping similar-item classification for index not in search results",
                );
                return None;
            };

            // Similarity scores persist as REAL (f32), so narrowing here
            // matches the storage precision and loses nothing.
            #[allow(clippy::cast_possible_truncation)]
            let similarity_score = result.similarity as f32;

            Some(
                NewTriageSimilarItemDecision::builder()
                    .job_id(job_id)
                    .batch_index(batch_index)
                    .matched_item_id(result.item.id())
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
        let search_results: Vec<SemanticSearchResult> = vec![];
        let outcome = TriageDecision::Duplicate {
            matched_item: TriageItemReference::ContextIndex { context_index: 0 },
        };
        let resolved = resolve_triage_outcome(&outcome, &search_results, JobId::new(), 0);
        assert!(matches!(resolved, ResolvedTriageOutcome::Novel));
    }

    #[test]
    fn test_resolve_triage_outcome_passes_novel_through() {
        let search_results: Vec<SemanticSearchResult> = vec![];
        let resolved =
            resolve_triage_outcome(&TriageDecision::Novel, &search_results, JobId::new(), 0);
        assert!(matches!(resolved, ResolvedTriageOutcome::Novel));
    }

    #[test]
    fn test_resolve_triage_outcome_resolves_in_range_duplicate() {
        // A duplicate referencing an in-range, non-zero index resolves to that
        // entry's item id.
        let item_a = a_knowledge_item().build();
        let item_b = a_knowledge_item().build();
        let id_b = item_b.id();
        let search_results = vec![
            SemanticSearchResult {
                item: item_a,
                similarity: 0.9,
            },
            SemanticSearchResult {
                item: item_b,
                similarity: 0.5,
            },
        ];
        let outcome = TriageDecision::Duplicate {
            matched_item: TriageItemReference::ContextIndex { context_index: 1 },
        };
        let resolved = resolve_triage_outcome(&outcome, &search_results, JobId::new(), 0);
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
        let item_a = a_knowledge_item().build();
        let item_b = a_knowledge_item().build();
        let item_c = a_knowledge_item().build();
        let id_a = item_a.id();
        let id_c = item_c.id();
        let search_results = vec![
            SemanticSearchResult {
                item: item_a,
                similarity: 0.5,
            },
            SemanticSearchResult {
                item: item_b,
                similarity: 0.25,
            },
            SemanticSearchResult {
                item: item_c,
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

        let rows = build_similar_item_decisions(JobId::new(), 0, &classifications, &search_results);

        // Out-of-range dropped; survivors keep classification order and each
        // resolves by index value to the right item and that entry's similarity.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].matched_item_id, id_c);
        assert!((rows[0].similarity_score - 0.75).abs() < f32::EPSILON);
        assert_eq!(rows[1].matched_item_id, id_a);
        assert!((rows[1].similarity_score - 0.5).abs() < f32::EPSILON);
    }
}
