//! Triage stage: similarity search and LLM-based relevance scoring.

use std::{collections::HashMap, sync::Arc, time::Instant};

use tokio::sync::Semaphore;
use tracing::Instrument;
use tribal_db::{
    ExtractionResultRepository, KnowledgeItemRepository, NewItemObservation, NewKnowledgeItem,
    NewTriageSimilarItemDecision, PgExtractionResultRepository, PgKnowledgeItemRepository,
    PgTriageResultRepository, SemanticSearchParams, SemanticSearchResult, TriageResultRepository,
};
use tribal_domain::{
    Candidate, Confidence, EmbeddingPurpose, Job, JobId, SourceType, TagRegistryEntry, Task,
    span_attrs,
};
use tribal_inference::{
    EmbeddingRequest, EmbeddingResponse, InferenceProvider, ProviderKey, Usage,
};

use super::{StageCommit, StageOutput, TriageCommitDecision, record_prompt_version_ids};
use crate::{
    common::{EXPECT_BATCH_INDEX, PARSE_PREVIEW_LENGTH},
    error::{SEMAPHORE_CLOSED, STAGE_TRIAGE, StageError},
    parsing::{
        SimilarItemClassification, TriageClassification, TriageDecision, parse_triage_response,
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

            let (system_pv, user_pv) = tokio::try_join!(
                self.load_prompt_version(STAGE_TRIAGE, ctx.job.triage_system_prompt_version_id()),
                self.load_prompt_version(STAGE_TRIAGE, ctx.job.triage_user_prompt_version_id()),
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

            let (classification, completion_response) = self
                .classify_candidate(
                    &ctx,
                    system_pv.content(),
                    user_pv.content(),
                    &similar_items,
                    deadline,
                )
                .await?;

            let embedding_usage = embedding_response.usage;
            let embedding_vector = embedding_response.vector;

            let (resolved_tags, tag_usages) = match &classification.outcome {
                TriageDecision::Novel => {
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
                TriageDecision::Duplicate { .. } => (None, vec![]),
            };

            let commit = self.build_triage_commit(
                &ctx,
                &classification,
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

            let params = SemanticSearchParams::builder()
                .query_embedding(embedding.to_vec())
                .embedding_model(self.embedding_provider().identity().model.clone())
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
        deadline: tokio::time::Instant,
    ) -> Result<(TriageClassification, tribal_inference::CompletionResponse), StageError> {
        let include_llm_content = self.include_llm_content();

        let request = assemble_triage_prompt(
            system_template,
            user_template,
            &ctx.candidate,
            similar_items,
            &ctx.tag_registry,
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

    /// Builds the `StageCommit::Triage` variant from the classification
    /// result, resolved tags, and embedding vector.
    fn build_triage_commit(
        &self,
        ctx: &TriageContext<'_>,
        classification: &TriageClassification,
        search_results: &[SemanticSearchResult],
        embedding_vector: Vec<f32>,
        resolved_tags: Option<ResolvedTags>,
    ) -> StageCommit {
        let similar_item_decisions = build_similar_item_decisions(
            ctx.job.id(),
            ctx.batch_index,
            &classification.similar_item_decisions,
            search_results,
        );

        let decision = match &classification.outcome {
            TriageDecision::Novel => {
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
            TriageDecision::Duplicate { matched_item_id } => {
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

/// Builds `NewTriageSimilarItemDecision` records by joining classifications
/// with search results to retrieve similarity scores.
fn build_similar_item_decisions(
    job_id: JobId,
    batch_index: u32,
    classifications: &[SimilarItemClassification],
    search_results: &[SemanticSearchResult],
) -> Vec<NewTriageSimilarItemDecision> {
    let similarity_by_id: HashMap<_, _> = search_results
        .iter()
        .map(|r| (r.item.id(), r.similarity))
        .collect();

    classifications
        .iter()
        .filter_map(|c| {
            let Some(&similarity) = similarity_by_id.get(&c.item_id) else {
                tracing::warn!(
                    matched_item_id = %c.item_id,
                    %job_id,
                    %batch_index,
                    "dropping similar-item classification for item not in search results",
                );
                return None;
            };

            #[allow(clippy::cast_possible_truncation)]
            let similarity_score = similarity as f32;

            Some(
                NewTriageSimilarItemDecision::builder()
                    .job_id(job_id)
                    .batch_index(batch_index)
                    .matched_item_id(c.item_id)
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
