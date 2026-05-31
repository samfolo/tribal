//! Domain-effect commit handlers for each pipeline stage.

use std::collections::HashSet;

use chrono::Utc;
use tracing::Instrument;
use tribal_config::CredentialCatalogue;
use tribal_db::{
    AdvisoryLockRepository, EmbeddingRepository, ExtractionResultRepository,
    ItemObservationRepository, JobRepository, JobStatusTransition, KnowledgeItemRepository,
    NewEmbedding, NewExtractionResult, NewKnowledgeItemRelation, NewReference, NewTagEmbedding,
    NewTask, NewTriageResult, PgAdvisoryLockRepository, PgEmbeddingRepository,
    PgExtractionResultRepository, PgItemObservationRepository, PgJobRepository,
    PgKnowledgeItemRepository, PgReferenceRepository, PgReindexRunRepository, PgRelationRepository,
    PgTagEmbeddingRepository, PgTagRegistryRepository, PgTaskRepository, PgTriageResultRepository,
    PgTriageSimilarItemDecisionRepository, ReferenceRepository, ReindexRunRepository,
    RelationRepository, TagEmbeddingRepository, TagRegistryRepository, TaskRepository,
    TriageResultRepository, TriageSimilarItemDecisionRepository, advisory_locks,
};
use tribal_domain::{
    EmbeddingProfile, EmbeddingPurpose, Job, JobId, JobOutcome, JobState, JobStatus,
    KnowledgeItemId, ReferenceKind, RelationBatchId, Task, TriageOutcome, span_attrs,
};
use tribal_inference::{EmbeddingRequest, InferenceError, ProviderRegistry};

use super::Worker;
use crate::{
    common::{EXPECT_BATCH_INDEX, EXPECT_CLAIMED_AT},
    error::{STAGE_EXTRACTION, STAGE_RELATION, STAGE_TRIAGE, StageError},
    stages::{
        RelationCommitDecision, StageCommit, TriageCommitDecision, load_active_embedding_profile,
    },
    tag_resolution::NewTagWithEmbedding,
    worker::reindex::{EmbeddingProviderCache, build_target_provider},
};

// ---------------------------------------------------------------------------
// Worker impl
// ---------------------------------------------------------------------------

impl Worker {
    /// Commits domain effects produced by a successful stage.
    pub(crate) async fn commit_domain_effects(
        &self,
        task: &Task,
        job: &Job,
        commit: StageCommit,
    ) -> Result<(), StageError> {
        match commit {
            StageCommit::Extraction {
                extraction_result,
                triage_tasks,
                batch_size,
                original_count,
            } => {
                self.commit_extraction(
                    task,
                    job,
                    extraction_result,
                    triage_tasks,
                    batch_size,
                    original_count,
                )
                .await
            }
            StageCommit::Triage {
                project_id,
                decision,
                similar_item_decisions,
            } => {
                self.commit_triage(task, project_id, decision, similar_item_decisions)
                    .await
            }
            StageCommit::Relation { decision } => self.commit_relation(task, job, decision).await,
        }
    }

    /// Commits extraction stage effects within a single transaction:
    ///
    /// 1. Inserts the extraction result.
    /// 2. Creates triage tasks (skipped when `batch_size == 0`).
    /// 3. Updates the job's batch size and original count.
    /// 4. Transitions the job status to `Triaging` (or `Completed` /
    ///    `Empty` when zero candidates were extracted).
    /// 5. Marks the task as completed, guarded by claim token.
    async fn commit_extraction(
        &self,
        task: &Task,
        job: &Job,
        extraction_result: NewExtractionResult,
        triage_tasks: Vec<NewTask>,
        batch_size: u32,
        original_count: u32,
    ) -> Result<(), StageError> {
        let span = tracing::info_span!(
            "tribal.extraction.commit",
            { span_attrs::BATCH_SIZE } = tracing::field::Empty,
            { span_attrs::EXTRACTION_ORIGINAL_COUNT } = tracing::field::Empty,
        );

        async {
            tracing::Span::current().record(span_attrs::BATCH_SIZE, batch_size);
            tracing::Span::current().record(span_attrs::EXTRACTION_ORIGINAL_COUNT, original_count);

            let Some(claim_token) = task.claim_token() else {
                return Err(StageError::OwnershipLost);
            };

            let mut conn = self
                .pool()
                .acquire()
                .await
                .map_err(|e| stage_sqlx_error(STAGE_EXTRACTION, "acquiring connection", e))?;

            let mut txn = sqlx::Connection::begin(&mut *conn)
                .await
                .map_err(|e| stage_sqlx_error(STAGE_EXTRACTION, "beginning transaction", e))?;

            PgExtractionResultRepository
                .insert(&mut txn, &extraction_result)
                .await
                .map_err(|e| stage_db_error(STAGE_EXTRACTION, "inserting extraction result", e))?;

            let is_empty = batch_size == 0;

            if !is_empty {
                for new_task in &triage_tasks {
                    PgTaskRepository
                        .insert(&mut txn, new_task)
                        .await
                        .map_err(|e| stage_db_error(STAGE_EXTRACTION, "creating triage task", e))?;
                }
            }

            PgJobRepository
                .update_batch_size(&mut txn, task.job_id(), batch_size, original_count)
                .await
                .map_err(|e| stage_db_error(STAGE_EXTRACTION, "updating batch size", e))?;

            // Zero-candidate path: when extraction produces no candidates,
            // the job completes immediately with an Empty outcome — no
            // triage or relation stages are needed.
            let job_transition = if is_empty {
                JobStatusTransition::builder()
                    .status(JobStatus::Completed)
                    .outcome(Some(JobOutcome::Empty))
                    .completed_at(Some(Utc::now()))
                    .build()
            } else {
                JobStatusTransition::builder()
                    .status(JobStatus::Triaging)
                    .build()
            };

            PgJobRepository
                .update_status(&mut txn, task.job_id(), &job_transition)
                .await
                .map_err(|e| stage_db_error(STAGE_EXTRACTION, "transitioning job status", e))?;

            let rows = PgTaskRepository
                .complete(&mut txn, task.id(), claim_token)
                .await
                .map_err(|e| stage_db_error(STAGE_EXTRACTION, "completing task", e))?;

            if rows == 0 {
                return Err(StageError::OwnershipLost);
            }

            txn.commit()
                .await
                .map_err(|e| stage_sqlx_error(STAGE_EXTRACTION, "committing transaction", e))?;

            // chrono i64 milliseconds to f64 — precision loss negligible at this scale
            #[allow(clippy::cast_precision_loss)]
            let duration_ms = (Utc::now() - task.claimed_at().expect(EXPECT_CLAIMED_AT))
                .num_milliseconds() as f64;
            self.metrics()
                .record_task_completed(task.task_type().as_str(), duration_ms);

            if is_empty {
                // chrono i64 milliseconds to f64 — precision loss negligible at this scale
                #[allow(clippy::cast_precision_loss)]
                let job_duration_ms = (Utc::now() - job.created_at()).num_milliseconds() as f64;
                self.metrics()
                    .record_job_completed(JobOutcome::Empty.as_str(), Some(job_duration_ms));
            }

            // Notify watch subscribers of the post-extraction job state.
            let state = if is_empty {
                JobState::Completed
            } else {
                JobState::Triaging
            };
            self.notify_job_state(task.job_id(), state);

            tracing::info!(
                task_id = %task.id(),
                task_type = "extraction",
                job_id = %task.job_id(),
                "task.completed",
            );

            Ok(())
        }
        .instrument(span)
        .await
    }

    /// Commits triage stage effects within a single transaction.
    ///
    /// **`Novel`**: upserts new tags, inserts the knowledge item, embedding,
    /// and references, then records the triage result.
    ///
    /// **`Duplicate`**: inserts an observation against the matched item,
    /// then records the triage result.
    ///
    /// **`NoOp`**: completes the task without creating any domain entities.
    async fn commit_triage(
        &self,
        task: &Task,
        project_id: tribal_domain::ProjectId,
        decision: TriageCommitDecision,
        similar_item_decisions: Vec<tribal_db::NewTriageSimilarItemDecision>,
    ) -> Result<(), StageError> {
        let job_id = task.job_id();
        let batch_index = task.batch_index().expect(EXPECT_BATCH_INDEX);
        let span = tracing::info_span!(
            "tribal.triage.commit",
            { span_attrs::TRIAGE_OUTCOME } = tracing::field::Empty,
        );

        async {
            let Some(claim_token) = task.claim_token() else {
                return Err(StageError::OwnershipLost);
            };

            let mut conn = self
                .pool()
                .acquire()
                .await
                .map_err(|e| stage_sqlx_error(STAGE_TRIAGE, "acquiring connection", e))?;

            let mut txn = sqlx::Connection::begin(&mut *conn)
                .await
                .map_err(|e| stage_sqlx_error(STAGE_TRIAGE, "beginning transaction", e))?;

            let outcome = match decision {
                TriageCommitDecision::Novel {
                    knowledge_item,
                    embedding_vector,
                    embedding_model,
                    suggested_references,
                    new_tags,
                    resolved_tags,
                } => {
                    commit_novel(
                        &mut txn,
                        job_id,
                        project_id,
                        batch_index,
                        &knowledge_item,
                        embedding_vector,
                        embedding_model,
                        &suggested_references,
                        &new_tags,
                        &resolved_tags,
                        &ReembedDeps {
                            registry: self.provider_registry(),
                            cache: self.embedding_providers(),
                            credentials: self.credentials(),
                        },
                    )
                    .await?
                }
                TriageCommitDecision::Duplicate { observation } => {
                    commit_duplicate(&mut txn, job_id, batch_index, &observation).await?
                }
                TriageCommitDecision::NoOp => {
                    validate_triage_noop(&mut txn, job_id, batch_index).await?
                }
            };

            if !similar_item_decisions.is_empty() {
                PgTriageSimilarItemDecisionRepository
                    .batch_insert(&mut txn, &similar_item_decisions)
                    .await
                    .map_err(|e| {
                        stage_db_error(STAGE_TRIAGE, "inserting similar item decisions", e)
                    })?;
            }

            let rows = PgTaskRepository
                .complete(&mut txn, task.id(), claim_token)
                .await
                .map_err(|e| stage_db_error(STAGE_TRIAGE, "completing task", e))?;

            if rows == 0 {
                return Err(StageError::OwnershipLost);
            }

            let fan_in_fired = self
                .triage_fan_in(&mut txn, job_id, task.id())
                .await
                .map_err(|e| stage_db_error(STAGE_TRIAGE, "triage fan-in", e))?;

            txn.commit()
                .await
                .map_err(|e| stage_sqlx_error(STAGE_TRIAGE, "committing transaction", e))?;

            // chrono i64 milliseconds to f64 — precision loss negligible at this scale
            #[allow(clippy::cast_precision_loss)]
            let duration_ms = (Utc::now() - task.claimed_at().expect(EXPECT_CLAIMED_AT))
                .num_milliseconds() as f64;
            self.metrics()
                .record_task_completed(task.task_type().as_str(), duration_ms);

            if fan_in_fired {
                self.notify_job_state(job_id, JobState::Relating);
            }

            tracing::Span::current().record(span_attrs::TRIAGE_OUTCOME, outcome);

            tracing::info!(
                task_id = %task.id(),
                task_type = "triage",
                job_id = %task.job_id(),
                "task.completed",
            );

            Ok(())
        }
        .instrument(span)
        .await
    }

    /// Commits relation stage effects within a single transaction.
    ///
    /// **`Relate`**: seals the committed batch ID, batch-inserts relations,
    /// transitions the job to `Completed` with the computed outcome, and
    /// completes the task.
    ///
    /// **`NoOp`**: completes the task without creating any domain entities
    /// (previous attempt already committed).
    async fn commit_relation(
        &self,
        task: &Task,
        job: &Job,
        decision: RelationCommitDecision,
    ) -> Result<(), StageError> {
        let job_id = task.job_id();
        let span = tracing::info_span!(
            "tribal.relation.commit",
            { span_attrs::JOB_OUTCOME } = tracing::field::Empty,
            { span_attrs::RELATION_BATCH_ID } = tracing::field::Empty,
            { span_attrs::RELATIONS_COMMITTED } = tracing::field::Empty,
            { span_attrs::RELATIONS_SKIPPED } = tracing::field::Empty,
            { span_attrs::RELATIONS_VALIDATION_DROPPED } = tracing::field::Empty,
        );

        async {
            let Some(claim_token) = task.claim_token() else {
                return Err(StageError::OwnershipLost);
            };

            let mut conn = self
                .pool()
                .acquire()
                .await
                .map_err(|e| stage_sqlx_error(STAGE_RELATION, "acquiring connection", e))?;

            let mut txn = sqlx::Connection::begin(&mut *conn)
                .await
                .map_err(|e| stage_sqlx_error(STAGE_RELATION, "beginning transaction", e))?;

            let (is_terminal, relation_outcome) = match decision {
                RelationCommitDecision::Relate {
                    relations,
                    batch_id,
                    outcome,
                    skipped,
                } => {
                    let won_commit = self
                        .commit_relation_relate(
                            &mut txn,
                            task,
                            job_id,
                            relations,
                            batch_id,
                            outcome,
                            skipped,
                            claim_token,
                        )
                        .await?;
                    if won_commit {
                        (true, Some(outcome))
                    } else {
                        // Idempotency hit — task completed but this
                        // attempt did not seal the batch.
                        (false, None)
                    }
                }
                RelationCommitDecision::NoOp => {
                    let rows = PgTaskRepository
                        .complete(&mut txn, task.id(), claim_token)
                        .await
                        .map_err(|e| {
                            stage_db_error(STAGE_RELATION, "completing task (no-op)", e)
                        })?;

                    if rows == 0 {
                        return Err(StageError::OwnershipLost);
                    }
                    // NoOp does not record job metrics — the job was
                    // already completed by a prior commit attempt.
                    (false, None)
                }
            };

            txn.commit()
                .await
                .map_err(|e| stage_sqlx_error(STAGE_RELATION, "committing transaction", e))?;

            // chrono i64 milliseconds to f64 — precision loss negligible at this scale
            #[allow(clippy::cast_precision_loss)]
            let duration_ms = (Utc::now() - task.claimed_at().expect(EXPECT_CLAIMED_AT))
                .num_milliseconds() as f64;
            self.metrics()
                .record_task_completed(task.task_type().as_str(), duration_ms);

            if let Some(outcome) = relation_outcome {
                // chrono i64 milliseconds to f64 — precision loss negligible at this scale
                #[allow(clippy::cast_precision_loss)]
                let job_duration_ms = (Utc::now() - job.created_at()).num_milliseconds() as f64;
                self.metrics()
                    .record_job_completed(outcome.as_str(), Some(job_duration_ms));
            }

            if is_terminal {
                self.notify_job_state(job_id, JobState::Completed);
            }

            tracing::info!(
                task_id = %task.id(),
                task_type = "relation",
                job_id = %job_id,
                "task.completed",
            );

            Ok(())
        }
        .instrument(span)
        .await
    }

    /// Inner commit path for `RelationCommitDecision::Relate`.
    ///
    /// The `committed_batch_id` column is set conditionally (`WHERE
    /// committed_batch_id IS NULL`). If a concurrent or retried commit
    /// already wrote a batch, the conditional update returns zero rows
    /// and this method short-circuits to a task-only completion,
    /// preventing relation overwrites.
    /// Returns `true` if this attempt was the winning commit, `false`
    /// if the batch was already sealed by a prior attempt (idempotency
    /// hit — task completed but no job metrics should be recorded).
    #[allow(clippy::too_many_arguments)]
    async fn commit_relation_relate(
        &self,
        txn: &mut sqlx::PgConnection,
        task: &Task,
        job_id: JobId,
        relations: Vec<NewKnowledgeItemRelation>,
        batch_id: RelationBatchId,
        outcome: JobOutcome,
        skipped: usize,
        claim_token: uuid::Uuid,
    ) -> Result<bool, StageError> {
        // Attempt to claim the batch slot. If another commit already
        // wrote a batch_id, skip relation inserts and job transition.
        if PgJobRepository
            .set_committed_batch_id(txn, job_id, batch_id)
            .await
            .map_err(|e| stage_db_error(STAGE_RELATION, "setting committed batch ID", e))?
            .is_none()
        {
            tracing::warn!("committed_batch_id already set — completing task as idempotency hit");

            let rows = PgTaskRepository
                .complete(txn, task.id(), claim_token)
                .await
                .map_err(|e| stage_db_error(STAGE_RELATION, "completing task", e))?;

            if rows == 0 {
                return Err(StageError::OwnershipLost);
            }

            return Ok(false);
        }

        let relations = validate_relation_endpoints(txn, relations).await?;
        let relations_count = relations.len();

        if !relations.is_empty() {
            PgRelationRepository
                .batch_insert(txn, &relations)
                .await
                .map_err(|e| stage_db_error(STAGE_RELATION, "batch-inserting relations", e))?;
        }

        let transition = JobStatusTransition::builder()
            .status(JobStatus::Completed)
            .outcome(Some(outcome))
            .completed_at(Some(Utc::now()))
            .build();

        PgJobRepository
            .update_status(txn, job_id, &transition)
            .await
            .map_err(|e| stage_db_error(STAGE_RELATION, "transitioning job to completed", e))?;

        let rows = PgTaskRepository
            .complete(txn, task.id(), claim_token)
            .await
            .map_err(|e| stage_db_error(STAGE_RELATION, "completing task", e))?;

        if rows == 0 {
            return Err(StageError::OwnershipLost);
        }

        tracing::Span::current().record(span_attrs::JOB_OUTCOME, outcome.as_str());
        tracing::Span::current().record(
            span_attrs::RELATION_BATCH_ID,
            tracing::field::display(&batch_id),
        );
        tracing::Span::current().record(span_attrs::RELATIONS_COMMITTED, relations_count);
        tracing::Span::current().record(span_attrs::RELATIONS_SKIPPED, skipped);

        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// Triage fan-in
// ---------------------------------------------------------------------------

impl Worker {
    /// Checks whether all triage siblings are terminal and, if so,
    /// creates the relation task and transitions the job to `Relating`.
    ///
    /// Must be called within an active transaction.  The
    /// `current_task_id` is excluded from the sibling count as a
    /// defensive guard — its updated status is visible on the same
    /// connection but has not yet been committed to other transactions.
    ///
    /// Returns `true` if the fan-in fired (relation task creation was
    /// attempted), `false` if non-terminal siblings remain.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] on database errors.
    pub(super) async fn triage_fan_in(
        &self,
        txn: &mut sqlx::PgConnection,
        job_id: JobId,
        current_task_id: tribal_domain::TaskId,
    ) -> Result<bool, tribal_db::DbError> {
        let remaining = PgTaskRepository
            .count_siblings_by_status(
                txn,
                job_id,
                tribal_domain::TaskType::Triage,
                &[
                    tribal_domain::TaskStatus::Queued,
                    tribal_domain::TaskStatus::Claimed,
                ],
                current_task_id,
            )
            .await?;

        if remaining > 0 {
            return Ok(false);
        }

        let new_task = tribal_db::NewTask::builder()
            .job_id(job_id)
            .task_type(tribal_domain::TaskType::Relation)
            .build();

        let rows_affected = PgTaskRepository.upsert(txn, &new_task).await?;

        if rows_affected > 0 {
            tracing::info!(job_id = %job_id, "relation task created (triage fan-in)");
        } else {
            tracing::debug!(job_id = %job_id, "relation task already exists for job");
        }

        let transition = JobStatusTransition::builder()
            .status(JobStatus::Relating)
            .build();

        PgJobRepository
            .update_status(txn, job_id, &transition)
            .await?;

        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// Triage commit helpers
// ---------------------------------------------------------------------------

/// What the commit path needs to re-embed against a freshly activated profile:
/// the registry and built-provider cache to resolve the new active's provider,
/// and the catalogue for its credential.
struct ReembedDeps<'a> {
    registry: &'a ProviderRegistry,
    cache: &'a EmbeddingProviderCache,
    credentials: &'a CredentialCatalogue,
}

/// Re-embeds an item and its novel tags against `active` when a cutover flipped
/// the active profile between the pre-embed and the commit, so an old-space
/// vector is never written under the new active. Returns the new item vector
/// and the per-tag vectors in `new_tags` order.
async fn reembed_against_active(
    reembed: &ReembedDeps<'_>,
    active: &EmbeddingProfile,
    content: &str,
    new_tags: &[NewTagWithEmbedding],
) -> Result<(Vec<f32>, Vec<Vec<f32>>), StageError> {
    let (provider, semaphore) =
        build_target_provider(reembed.registry, reembed.cache, reembed.credentials, active)
            .map_err(|e| StageError::Provider {
                context: "resolving the flipped active profile's provider".to_owned(),
                source: InferenceError::ProviderUnavailable {
                    provider: active.provider_kind().to_string(),
                    reason: e.to_string(),
                },
            })?;
    let _permit = semaphore
        .acquire()
        .await
        .expect("embedding provider semaphore is never closed");

    let vector = provider
        .embed(EmbeddingRequest {
            input: content.to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        })
        .await
        .map_err(|e| StageError::Provider {
            context: "re-embedding the item against the flipped active".to_owned(),
            source: e,
        })?
        .vector;

    let mut tag_vectors = Vec::with_capacity(new_tags.len());
    for tag in new_tags {
        tag_vectors.push(
            provider
                .embed(EmbeddingRequest {
                    input: tag.tag.clone(),
                    purpose: EmbeddingPurpose::Tag,
                })
                .await
                .map_err(|e| StageError::Provider {
                    context: "re-embedding a tag against the flipped active".to_owned(),
                    source: e,
                })?
                .vector,
        );
    }
    Ok((vector, tag_vectors))
}

/// Inserts the knowledge item, embedding, references, and triage result
/// for a novel candidate.
#[allow(clippy::too_many_arguments)]
async fn commit_novel(
    txn: &mut sqlx::PgConnection,
    job_id: JobId,
    project_id: tribal_domain::ProjectId,
    batch_index: u32,
    knowledge_item: &tribal_db::NewKnowledgeItem,
    mut embedding_vector: Vec<f32>,
    mut embedding_model: String,
    suggested_references: &[tribal_domain::SuggestedReference],
    new_tags: &[NewTagWithEmbedding],
    resolved_tags: &[String],
    reembed: &ReembedDeps<'_>,
) -> Result<&'static str, StageError> {
    // While a reindex is live, hold the shared cutover lock for this commit so
    // the cutover's exclusive acquisition drains this in-flight write before it
    // runs the final set-difference and flips the active profile. Taken before
    // the item insert so a drained commit's item is visible to that final
    // sweep. When no reindex is live, the ingest path is unchanged: no lock,
    // one active-profile embedding.
    if PgReindexRunRepository
        .find_live(txn)
        .await
        .map_err(|e| stage_db_error(STAGE_TRIAGE, "checking for a live reindex", e))?
        .is_some()
    {
        PgAdvisoryLockRepository
            .acquire_shared_xact(txn, advisory_locks::CUTOVER)
            .await
            .map_err(|e| stage_db_error(STAGE_TRIAGE, "holding the shared cutover lock", e))?;
    }

    // Resolve the active profile inside the commit transaction's snapshot; both
    // the item and the novel-tag embeddings are written against it. Holding the
    // shared lock (when a reindex is live) keeps it stable for the rest of the
    // transaction, so this single read is the authoritative active.
    let active = load_active_embedding_profile(txn, STAGE_TRIAGE).await?;
    let profile_id = active.id();

    // If a cutover flipped the active between the pre-embed and now, the
    // pre-embedded vector is for the superseded geometry; re-embed the item and
    // its novel tags against the new active so an old-space vector is never
    // written under it.
    let mut tag_vectors: Vec<Vec<f32>> = new_tags.iter().map(|t| t.embedding.clone()).collect();
    let flipped = active.model() != embedding_model
        || u32::try_from(embedding_vector.len()).map_or(true, |len| len != active.dimensions());
    if flipped {
        let (item_vector, retagged) =
            reembed_against_active(reembed, &active, &knowledge_item.content, new_tags).await?;
        embedding_vector = item_vector;
        embedding_model = active.model().to_owned();
        tag_vectors = retagged;
    }

    // FK ordering: tag_registry inserts before tag_embeddings inserts.
    if !new_tags.is_empty() {
        let tag_names: Vec<String> = new_tags.iter().map(|t| t.tag.clone()).collect();
        PgTagRegistryRepository
            .batch_upsert(txn, &tag_names)
            .await
            .map_err(|e| stage_db_error(STAGE_TRIAGE, "upserting tags", e))?;

        let new_embeddings: Vec<NewTagEmbedding> = new_tags
            .iter()
            .zip(&tag_vectors)
            .map(|(t, vector)| {
                NewTagEmbedding::builder()
                    .tag(t.tag.clone())
                    .embedding_profile_id(profile_id)
                    .model(embedding_model.clone())
                    .embedding(vector.clone())
                    .build()
            })
            .collect();

        PgTagEmbeddingRepository
            .batch_upsert(txn, &new_embeddings)
            .await
            .map_err(|e| stage_db_error(STAGE_TRIAGE, "upserting tag embeddings", e))?;
    }

    let mut all_tags: Vec<String> = resolved_tags.to_vec();
    all_tags.extend(new_tags.iter().map(|t| t.tag.clone()));

    if !all_tags.is_empty() {
        PgTagRegistryRepository
            .increment_usage_count(txn, &all_tags)
            .await
            .map_err(|e| stage_db_error(STAGE_TRIAGE, "incrementing tag usage counts", e))?;
    }

    let item = PgKnowledgeItemRepository
        .insert(txn, knowledge_item)
        .await
        .map_err(|e| stage_db_error(STAGE_TRIAGE, "inserting knowledge item", e))?;

    let ki_id = item.id();

    let new_embedding = NewEmbedding::builder()
        .knowledge_item_id(ki_id)
        .embedding_profile_id(profile_id)
        .model(embedding_model)
        .embedding(embedding_vector)
        .build();

    PgEmbeddingRepository
        .insert(txn, &new_embedding)
        .await
        .map_err(|e| stage_db_error(STAGE_TRIAGE, "inserting embedding", e))?;

    for suggested_ref in suggested_references {
        let kind_str = suggested_ref.reference_type().trim().to_ascii_lowercase();
        let Ok(kind) = kind_str.parse::<ReferenceKind>() else {
            tracing::debug!(
                reference_type = %suggested_ref.reference_type(),
                "skipping unrecognised reference type",
            );
            continue;
        };

        let new_ref = NewReference::builder()
            .knowledge_item_id(ki_id)
            .kind(kind)
            .value(suggested_ref.value().to_owned())
            .description(suggested_ref.description().map(str::to_owned))
            .project_id(project_id)
            .build();

        PgReferenceRepository
            .insert(txn, &new_ref)
            .await
            .map_err(|e| stage_db_error(STAGE_TRIAGE, "inserting reference", e))?;
    }

    let triage_result = NewTriageResult::builder()
        .job_id(job_id)
        .batch_index(batch_index)
        .outcome(TriageOutcome::Created { item_id: ki_id })
        .build();

    PgTriageResultRepository
        .insert(txn, &triage_result)
        .await
        .map_err(|e| stage_db_error(STAGE_TRIAGE, "inserting triage result", e))?;

    Ok("created")
}

/// Inserts an observation and triage result for a duplicate candidate.
async fn commit_duplicate(
    txn: &mut sqlx::PgConnection,
    job_id: JobId,
    batch_index: u32,
    observation: &tribal_db::NewItemObservation,
) -> Result<&'static str, StageError> {
    let matched_item_id = observation.knowledge_item_id;

    let obs = PgItemObservationRepository
        .insert(txn, observation)
        .await
        .map_err(|e| stage_db_error(STAGE_TRIAGE, "inserting observation", e))?;

    let triage_result = NewTriageResult::builder()
        .job_id(job_id)
        .batch_index(batch_index)
        .outcome(TriageOutcome::Duplicate {
            observation_id: obs.id(),
            matched_item_id,
        })
        .build();

    PgTriageResultRepository
        .insert(txn, &triage_result)
        .await
        .map_err(|e| stage_db_error(STAGE_TRIAGE, "inserting triage result", e))?;

    Ok("duplicate")
}

/// Validates that a triage result already exists for a `NoOp` decision.
async fn validate_triage_noop(
    txn: &mut sqlx::PgConnection,
    job_id: JobId,
    batch_index: u32,
) -> Result<&'static str, StageError> {
    let existing = PgTriageResultRepository
        .find_by_job_id_and_batch_index(txn, job_id, batch_index)
        .await
        .map_err(|e| stage_db_error(STAGE_TRIAGE, "re-checking triage idempotency", e))?;

    if existing.is_none() {
        return Err(stage_db_error(
            STAGE_TRIAGE,
            "NoOp triage decision without existing triage result",
            tribal_db::DbError::NotFound {
                entity: "triage_result",
                id: format!("{job_id}[{batch_index}]"),
            },
        ));
    }

    Ok("no_op")
}

// ---------------------------------------------------------------------------
// Pre-insert validation
// ---------------------------------------------------------------------------

/// Validates that all relation endpoint IDs exist in `knowledge_items`.
///
/// Drops relations with missing endpoints rather than letting the FK
/// constraint fail the entire batch. This catches race conditions where
/// an item is deleted between the triage and relation stages.
async fn validate_relation_endpoints(
    conn: &mut sqlx::PgConnection,
    relations: Vec<NewKnowledgeItemRelation>,
) -> Result<Vec<NewKnowledgeItemRelation>, StageError> {
    if relations.is_empty() {
        tracing::Span::current().record(span_attrs::RELATIONS_VALIDATION_DROPPED, 0usize);
        return Ok(relations);
    }

    let all_ids: HashSet<KnowledgeItemId> = relations
        .iter()
        .flat_map(|r| [r.source_id, r.target_id])
        .collect();
    let id_vec: Vec<KnowledgeItemId> = all_ids.iter().copied().collect();

    let existing_ids: HashSet<KnowledgeItemId> = PgKnowledgeItemRepository
        .find_existing_ids(conn, &id_vec)
        .await
        .map_err(|e| stage_db_error(STAGE_RELATION, "validating relation endpoints", e))?
        .into_iter()
        .collect();

    let missing: Vec<KnowledgeItemId> = all_ids.difference(&existing_ids).copied().collect();
    if missing.is_empty() {
        tracing::Span::current().record(span_attrs::RELATIONS_VALIDATION_DROPPED, 0usize);
        return Ok(relations);
    }

    let before = relations.len();
    let valid: Vec<NewKnowledgeItemRelation> = relations
        .into_iter()
        .filter(|r| existing_ids.contains(&r.source_id) && existing_ids.contains(&r.target_id))
        .collect();
    let dropped = before - valid.len();

    tracing::warn!(
        dropped,
        ?missing,
        "dropping relations with non-existent endpoints",
    );
    tracing::Span::current().record(span_attrs::RELATIONS_VALIDATION_DROPPED, dropped);

    Ok(valid)
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

/// Builds a [`StageError::Database`] for any pipeline stage.
fn stage_db_error(stage: &str, context: &str, source: tribal_db::DbError) -> StageError {
    StageError::Database {
        stage: stage.into(),
        context: context.into(),
        source,
    }
}

/// Wraps a raw [`sqlx::Error`] into a [`StageError::Database`] for any
/// pipeline stage.
fn stage_sqlx_error(stage: &str, context: &str, source: sqlx::Error) -> StageError {
    stage_db_error(
        stage,
        context,
        tribal_db::DbError::QueryFailed {
            context: context.into(),
            source,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dashmap::DashMap;
    use tribal_config::CredentialCatalogue;
    use tribal_inference::{EmbeddingProvider, ProviderRegistry};
    use tribal_test_utils::{
        ExhaustBehaviour, MockEmbeddingProvider, an_embedding_profile, an_embedding_response,
    };

    use super::*;

    /// Deviation 1: when a cutover flips the active profile between an ingest's
    /// pre-embed and its commit, the commit re-embeds the item and its novel
    /// tags against the new active's provider (resolved from the cache the
    /// reindex driver populated) rather than writing the stale, old-space
    /// vectors under the new active.
    #[tokio::test]
    async fn test_reembed_against_active_uses_the_new_active_provider() {
        // The new active, its built provider already cached by the driver,
        // returns a recognisable vector for every input.
        let active = an_embedding_profile().build();
        let cache: EmbeddingProviderCache = Arc::new(DashMap::new());
        let mock: Arc<dyn EmbeddingProvider> = Arc::new(
            MockEmbeddingProvider::builder()
                .on_embed(an_embedding_response(vec![0.5_f32; 768]), None)
                .on_exhaust(ExhaustBehaviour::RepeatLast)
                .build(),
        );
        cache.insert(active.id(), mock);
        let registry = ProviderRegistry::new(vec![]).expect("registry");
        let credentials = CredentialCatalogue::default();
        let reembed = ReembedDeps {
            registry: &registry,
            cache: &cache,
            credentials: &credentials,
        };

        // The pre-embedded vectors (all 0.1) belong to the superseded geometry.
        let new_tags = [NewTagWithEmbedding {
            tag: "rust".to_owned(),
            embedding: vec![0.1_f32; 768],
        }];
        let (item_vector, tag_vectors) =
            reembed_against_active(&reembed, &active, "content", &new_tags)
                .await
                .expect("reembed");

        assert!(
            item_vector.iter().all(|&v| (v - 0.5).abs() < f32::EPSILON),
            "the item is re-embedded by the new active's provider, not left stale",
        );
        assert_eq!(tag_vectors.len(), 1);
        assert!(
            tag_vectors[0]
                .iter()
                .all(|&v| (v - 0.5).abs() < f32::EPSILON),
            "each novel tag is re-embedded against the new active too",
        );
    }
}
