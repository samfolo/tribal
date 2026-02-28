//! Triage stage: similarity search and LLM-based relevance scoring.

use std::{collections::HashMap, sync::Arc};

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
use tribal_inference::{EmbeddingRequest, EmbeddingResponse, Usage};

use super::{StageCommit, StageOutput, TriageCommitDecision};
use crate::{
    error::{SEMAPHORE_CLOSED, STAGE_TRIAGE, StageError},
    parsing::{
        SimilarItemClassification, TriageClassification, TriageDecision, parse_triage_response,
    },
    prompt::{SimilarItemContext, assemble_triage_prompt},
    tag_resolution::resolve_tags,
    worker::Worker,
};

// ---------------------------------------------------------------------------
// TriageContext
// ---------------------------------------------------------------------------

/// Context assembled before running the triage stage.
#[allow(dead_code)]
pub(crate) struct TriageContext {
    /// The parent job.
    pub job: Job,
    /// The claimed task.
    pub task: Task,
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
const EXPECT_BATCH_INDEX: &str = "triage tasks always have a batch index";
const PARSE_PREVIEW_LENGTH: usize = 200;

// ---------------------------------------------------------------------------
// Triage accessors
// ---------------------------------------------------------------------------

impl Worker {
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
        );

        async {
            if self.check_triage_idempotency(job.id(), batch_index).await? {
                return Ok(StageOutput {
                    commit: StageCommit::Triage {
                        job_id: job.id(),
                        project_id: job.project_id(),
                        batch_index,
                        decision: TriageCommitDecision::NoOp,
                        similar_item_decisions: vec![],
                    },
                    usages: vec![],
                });
            }

            let candidate = self.load_triage_candidate(job.id(), batch_index).await?;
            let tag_registry = self.load_tag_registry(STAGE_TRIAGE).await?;

            let embedding_response = self.embed_candidate(candidate.content(), deadline).await?;

            let search_results = self
                .search_similar_items(&embedding_response.vector, job)
                .await?;

            let similar_items: Vec<SimilarItemContext> = search_results
                .iter()
                .map(SimilarItemContext::from)
                .collect();

            let (classification, completion_response) = self
                .classify_candidate(job, &candidate, &similar_items, &tag_registry, deadline)
                .await?;

            let embedding_usage = embedding_response.usage;
            let embedding_vector = embedding_response.vector;

            let commit = self.build_triage_commit(
                job,
                batch_index,
                &candidate,
                &classification,
                &search_results,
                embedding_vector,
                &tag_registry,
            );

            Ok(StageOutput {
                commit,
                usages: vec![
                    Usage::Embedding(embedding_usage),
                    Usage::Completion(completion_response.usage),
                ],
            })
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

        let candidates: Vec<Candidate> =
            serde_json::from_value(extraction_result.candidates().clone()).map_err(|e| {
                StageError::Parse {
                    context: format!("deserialising candidates for job {job_id}"),
                    raw_response: Some(e.to_string()),
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
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let _permit = tokio::time::timeout(remaining, Arc::clone(semaphore).acquire_owned())
            .await
            .map_err(|_| StageError::SemaphoreTimeout {
                provider_key: format!("{:?}", self.triage_embedding_key()),
            })?
            .expect(SEMAPHORE_CLOSED);

        let request = EmbeddingRequest {
            input: content.to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        };

        self.embedding_provider()
            .embed(request)
            .await
            .map_err(|e| StageError::Provider {
                context: "triage embedding call".into(),
                source: e,
            })
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
        job: &Job,
        candidate: &Candidate,
        similar_items: &[SimilarItemContext],
        tag_registry: &[TagRegistryEntry],
        deadline: tokio::time::Instant,
    ) -> Result<(TriageClassification, tribal_inference::CompletionResponse), StageError> {
        let prompt_version = self
            .load_prompt_version(STAGE_TRIAGE, job.triage_prompt_version_id())
            .await?;

        let request = assemble_triage_prompt(
            prompt_version.content(),
            candidate,
            similar_items,
            tag_registry,
        )?;

        if self.config().include_llm_content {
            tracing::debug!(
                prompt = %request.system.as_deref().unwrap_or(""),
                "triage prompt assembled",
            );
        }

        let semaphore = self.triage_inference_semaphore();
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let _permit = tokio::time::timeout(remaining, Arc::clone(semaphore).acquire_owned())
            .await
            .map_err(|_| StageError::SemaphoreTimeout {
                provider_key: format!("{:?}", self.triage_inference_key()),
            })?
            .expect(SEMAPHORE_CLOSED);

        let response = self
            .triage_provider()
            .complete(request)
            .await
            .map_err(|e| StageError::Provider {
                context: "triage LLM call".into(),
                source: e,
            })?;

        if self.config().include_llm_content {
            tracing::debug!(
                response = %response.text,
                "triage response received",
            );
        }

        let classification = {
            let _parse_span = tracing::info_span!("tribal.triage.parse").entered();
            parse_triage_response(&response).inspect_err(|_| {
                let preview: String = response.text.chars().take(PARSE_PREVIEW_LENGTH).collect();
                tracing::warn!(
                    response_length = response.text.len(),
                    preview = %preview,
                    "triage response parse failure",
                );
            })?
        };

        Ok((classification, response))
    }

    /// Builds the `StageCommit::Triage` variant from the classification
    /// result, resolved tags, and embedding vector.
    #[allow(clippy::too_many_arguments)]
    fn build_triage_commit(
        &self,
        job: &Job,
        batch_index: u32,
        candidate: &Candidate,
        classification: &TriageClassification,
        search_results: &[SemanticSearchResult],
        embedding_vector: Vec<f32>,
        tag_registry: &[TagRegistryEntry],
    ) -> StageCommit {
        let similar_item_decisions = build_similar_item_decisions(
            job.id(),
            batch_index,
            &classification.similar_item_decisions,
            search_results,
        );

        let decision = match &classification.outcome {
            TriageDecision::Novel => {
                let (resolved_tags, new_tags) =
                    resolve_tags(candidate.suggested_tags(), tag_registry);

                let mut all_tags = resolved_tags;
                all_tags.extend(new_tags.iter().cloned());

                let extraction_identity = self.extraction_provider().identity();
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
                        .build(),
                );

                let embedding_identity = self.embedding_provider().identity();

                TriageCommitDecision::Novel {
                    knowledge_item,
                    embedding_vector,
                    embedding_model: embedding_identity.model.clone(),
                    suggested_references: candidate.suggested_references().to_vec(),
                    new_tags,
                }
            }
            TriageDecision::Duplicate { matched_item_id } => {
                let observation = NewItemObservation::builder()
                    .knowledge_item_id(*matched_item_id)
                    .principal_id(job.principal_id())
                    .source_type(SourceType::AgentMediated)
                    .build();

                TriageCommitDecision::Duplicate { observation }
            }
        };

        StageCommit::Triage {
            job_id: job.id(),
            project_id: job.project_id(),
            batch_index,
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
        .map(|c| {
            #[allow(clippy::cast_possible_truncation)]
            let similarity_score = similarity_by_id
                .get(&c.item_id)
                .copied()
                .map_or(0.0, |s| s as f32);

            NewTriageSimilarItemDecision::builder()
                .job_id(job_id)
                .batch_index(batch_index)
                .matched_item_id(c.item_id)
                .similarity_score(similarity_score)
                .suggested_relation(c.suggested_relation)
                .justification_text(c.justification.clone())
                .build()
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
