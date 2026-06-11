//! Domain-effect commit handlers for each pipeline stage.

use std::collections::HashSet;

use chrono::Utc;
use tracing::Instrument;
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
    AgentThread, CompletionResponse, EmbeddingProfile, EmbeddingProfileId, EmbeddingPurpose, Job,
    JobId, JobOutcome, JobState, JobStatus, KnowledgeItemId, ReferenceKind, RelationBatchId, Task,
    TriageOutcome, span_attrs,
};
use tribal_inference::{
    EmbedGroupError, EmbeddingRequest, EmbeddingTarget, InferenceError, InferenceGateway,
    PermitWait, UsageAttribution,
};

use super::Worker;
use crate::{
    common::{EXPECT_BATCH_INDEX, EXPECT_CLAIMED_AT},
    error::{STAGE_EXTRACTION, STAGE_RELATION, STAGE_TRIAGE, StageError},
    stages::{
        RelationCommitDecision, StageCommit, StageRun, TriageCommitDecision, finish_thread,
        load_active_embedding_profile, stage_attribution,
    },
    tag_resolution::NewTagWithEmbedding,
    worker::coupling,
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
        run: StageRun,
    ) -> Result<(), StageError> {
        let StageRun {
            thread,
            commit,
            response,
        } = run;
        let response = response.as_ref();
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
                    &thread,
                    response,
                )
                .await
            }
            StageCommit::Triage {
                project_id,
                decision,
                similar_item_decisions,
            } => {
                self.commit_triage(
                    task,
                    job,
                    project_id,
                    decision,
                    similar_item_decisions,
                    &thread,
                    response,
                )
                .await
            }
            StageCommit::Relation { decision } => {
                self.commit_relation(task, job, decision, &thread, response)
                    .await
            }
        }
    }

    /// Commits extraction stage effects within a single transaction: the
    /// extraction result, the triage tasks it fanned out (none on an empty
    /// batch), the job's batch size and its status transition to triaging (or
    /// straight to completed with an empty outcome when no candidates were
    /// extracted), and the claim-guarded task completion.
    #[allow(clippy::too_many_arguments)]
    async fn commit_extraction(
        &self,
        task: &Task,
        job: &Job,
        extraction_result: NewExtractionResult,
        triage_tasks: Vec<NewTask>,
        batch_size: u32,
        original_count: u32,
        thread: &AgentThread,
        response: Option<&CompletionResponse>,
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
            // the job completes immediately with an Empty outcome; no
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
                .update_status_if_live(&mut txn, task.job_id(), &job_transition)
                .await
                .map_err(|e| stage_db_error(STAGE_EXTRACTION, "transitioning job status", e))?;

            let rows = PgTaskRepository
                .complete(&mut txn, task.id(), claim_token)
                .await
                .map_err(|e| stage_db_error(STAGE_EXTRACTION, "completing task", e))?;

            if rows == 0 {
                return Err(StageError::OwnershipLost);
            }

            finish_thread(&mut txn, STAGE_EXTRACTION, thread, response).await?;

            txn.commit()
                .await
                .map_err(|e| stage_sqlx_error(STAGE_EXTRACTION, "committing transaction", e))?;

            // chrono i64 milliseconds to f64; precision loss negligible at this scale
            #[allow(clippy::cast_precision_loss)]
            let duration_ms = (Utc::now() - task.claimed_at().expect(EXPECT_CLAIMED_AT))
                .num_milliseconds() as f64;
            self.metrics()
                .record_task_completed(task.task_type().as_str(), duration_ms);

            if is_empty {
                // chrono i64 milliseconds to f64; precision loss negligible at this scale
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
    #[allow(clippy::too_many_arguments)]
    async fn commit_triage(
        &self,
        task: &Task,
        job: &Job,
        project_id: tribal_domain::ProjectId,
        decision: TriageCommitDecision,
        similar_item_decisions: Vec<tribal_db::NewTriageSimilarItemDecision>,
        thread: &AgentThread,
        response: Option<&CompletionResponse>,
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

            let (outcome, fan_in_fired) = match decision {
                TriageCommitDecision::Novel {
                    knowledge_item,
                    embedding_vector,
                    embedding_model,
                    embedding_profile_id,
                    suggested_references,
                    new_tags,
                    resolved_tags,
                } => {
                    // Re-embed and retry under a bounded budget if a cutover
                    // flips the active profile between the pre-embed and the
                    // write, so a provider call never spans the commit
                    // transaction or holds the shared cutover lock.

                    let mut embedding = NovelEmbedding {
                        vector: embedding_vector,
                        model: embedding_model,
                        producing_profile_id: embedding_profile_id,
                        tag_vectors: new_tags.iter().map(|t| t.embedding.clone()).collect(),
                    };
                    let mut attempts = 0u32;
                    loop {
                        let mut txn = sqlx::Connection::begin(&mut *conn).await.map_err(|e| {
                            stage_sqlx_error(STAGE_TRIAGE, "beginning transaction", e)
                        })?;
                        match commit_novel(
                            &mut txn,
                            job_id,
                            project_id,
                            batch_index,
                            &knowledge_item,
                            &embedding,
                            &suggested_references,
                            &new_tags,
                            &resolved_tags,
                        )
                        .await?
                        {
                            CommitNovelOutcome::Committed(outcome) => {
                                let fan_in_fired = self
                                    .finalise_triage_commit(
                                        &mut txn,
                                        &similar_item_decisions,
                                        task,
                                        claim_token,
                                        job_id,
                                        thread,
                                        response,
                                    )
                                    .await?;
                                txn.commit().await.map_err(|e| {
                                    stage_sqlx_error(STAGE_TRIAGE, "committing transaction", e)
                                })?;
                                break (outcome, fan_in_fired);
                            }
                            CommitNovelOutcome::Reembed(active) => {
                                drop(txn);
                                attempts += 1;
                                if attempts > MAX_REEMBED_RETRIES {
                                    return Err(StageError::Provider {
                                        context: "re-embedding against the flipped active profile"
                                            .to_owned(),
                                        source: InferenceError::provider_unavailable(
                                            active.provider_kind().to_string(),
                                            "the active profile flipped repeatedly during one \
                                             commit",
                                        ),
                                    });
                                }
                                let reembedded = reembed_against_active(
                                    self.gateway(),
                                    &stage_attribution(job, task, thread),
                                    &active,
                                    &knowledge_item.content,
                                    &new_tags,
                                )
                                .await?;
                                embedding = NovelEmbedding {
                                    vector: reembedded.item,
                                    model: active.model().to_owned(),
                                    producing_profile_id: active.id(),
                                    tag_vectors: reembedded.tags,
                                };
                            }
                        }
                    }
                }
                TriageCommitDecision::Duplicate { observation } => {
                    let mut txn = sqlx::Connection::begin(&mut *conn)
                        .await
                        .map_err(|e| stage_sqlx_error(STAGE_TRIAGE, "beginning transaction", e))?;
                    let outcome =
                        commit_duplicate(&mut txn, job_id, batch_index, &observation).await?;
                    let fan_in_fired = self
                        .finalise_triage_commit(
                            &mut txn,
                            &similar_item_decisions,
                            task,
                            claim_token,
                            job_id,
                            thread,
                            response,
                        )
                        .await?;
                    txn.commit()
                        .await
                        .map_err(|e| stage_sqlx_error(STAGE_TRIAGE, "committing transaction", e))?;
                    (outcome, fan_in_fired)
                }
                TriageCommitDecision::NoOp => {
                    let mut txn = sqlx::Connection::begin(&mut *conn)
                        .await
                        .map_err(|e| stage_sqlx_error(STAGE_TRIAGE, "beginning transaction", e))?;
                    let outcome = validate_triage_noop(&mut txn, job_id, batch_index).await?;
                    let fan_in_fired = self
                        .finalise_triage_commit(
                            &mut txn,
                            &similar_item_decisions,
                            task,
                            claim_token,
                            job_id,
                            thread,
                            response,
                        )
                        .await?;
                    txn.commit()
                        .await
                        .map_err(|e| stage_sqlx_error(STAGE_TRIAGE, "committing transaction", e))?;
                    (outcome, fan_in_fired)
                }
            };

            self.record_triage_completion(task, outcome, fan_in_fired);

            Ok(())
        }
        .instrument(span)
        .await
    }

    /// Records the side effects of a completed triage commit: the duration
    /// metric, the relation-stage notification when the fan-in fired, and the
    /// completion span and log.
    fn record_triage_completion(&self, task: &Task, outcome: &'static str, fan_in_fired: bool) {
        // chrono i64 milliseconds to f64; precision loss is negligible at this scale
        #[allow(clippy::cast_precision_loss)]
        let duration_ms =
            (Utc::now() - task.claimed_at().expect(EXPECT_CLAIMED_AT)).num_milliseconds() as f64;
        self.metrics()
            .record_task_completed(task.task_type().as_str(), duration_ms);

        if fan_in_fired {
            self.notify_job_state(task.job_id(), JobState::Relating);
        }

        tracing::Span::current().record(span_attrs::TRIAGE_OUTCOME, outcome);

        tracing::info!(
            task_id = %task.id(),
            task_type = "triage",
            job_id = %task.job_id(),
            "task.completed",
        );
    }

    /// Finalises a triage commit in the caller's transaction: the similar-item
    /// decisions, the claim-guarded task completion, and the fan-in. Returns
    /// whether the fan-in fired the relation stage.
    #[allow(clippy::too_many_arguments)]
    async fn finalise_triage_commit(
        &self,
        txn: &mut sqlx::PgConnection,
        similar_item_decisions: &[tribal_db::NewTriageSimilarItemDecision],
        task: &Task,
        claim_token: uuid::Uuid,
        job_id: JobId,
        thread: &AgentThread,
        response: Option<&CompletionResponse>,
    ) -> Result<bool, StageError> {
        if !similar_item_decisions.is_empty() {
            PgTriageSimilarItemDecisionRepository
                .batch_insert(txn, similar_item_decisions)
                .await
                .map_err(|e| stage_db_error(STAGE_TRIAGE, "inserting similar item decisions", e))?;
        }

        let rows = PgTaskRepository
            .complete(txn, task.id(), claim_token)
            .await
            .map_err(|e| stage_db_error(STAGE_TRIAGE, "completing task", e))?;
        if rows == 0 {
            return Err(StageError::OwnershipLost);
        }

        finish_thread(txn, STAGE_TRIAGE, thread, response).await?;

        coupling::triage_fan_in(txn, job_id, task.id())
            .await
            .map_err(|e| stage_db_error(STAGE_TRIAGE, "triage fan-in", e))
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
        thread: &AgentThread,
        response: Option<&CompletionResponse>,
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
                            thread,
                            response,
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
                        // Idempotency hit; task completed but this
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
                    finish_thread(&mut txn, STAGE_RELATION, thread, response).await?;
                    // NoOp does not record job metrics; the job was
                    // already completed by a prior commit attempt.
                    (false, None)
                }
            };

            txn.commit()
                .await
                .map_err(|e| stage_sqlx_error(STAGE_RELATION, "committing transaction", e))?;

            // chrono i64 milliseconds to f64; precision loss negligible at this scale
            #[allow(clippy::cast_precision_loss)]
            let duration_ms = (Utc::now() - task.claimed_at().expect(EXPECT_CLAIMED_AT))
                .num_milliseconds() as f64;
            self.metrics()
                .record_task_completed(task.task_type().as_str(), duration_ms);

            if let Some(outcome) = relation_outcome {
                // chrono i64 milliseconds to f64; precision loss negligible at this scale
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
    /// hit; task completed but no job metrics should be recorded).
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    async fn commit_relation_relate(
        &self,
        thread: &AgentThread,
        response: Option<&CompletionResponse>,
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
            tracing::warn!("committed_batch_id already set; completing task as idempotency hit");

            let rows = PgTaskRepository
                .complete(txn, task.id(), claim_token)
                .await
                .map_err(|e| stage_db_error(STAGE_RELATION, "completing task", e))?;

            if rows == 0 {
                return Err(StageError::OwnershipLost);
            }
            finish_thread(txn, STAGE_RELATION, thread, response).await?;

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
            .update_status_if_live(txn, job_id, &transition)
            .await
            .map_err(|e| stage_db_error(STAGE_RELATION, "transitioning job to completed", e))?;

        let rows = PgTaskRepository
            .complete(txn, task.id(), claim_token)
            .await
            .map_err(|e| stage_db_error(STAGE_RELATION, "completing task", e))?;

        if rows == 0 {
            return Err(StageError::OwnershipLost);
        }
        finish_thread(txn, STAGE_RELATION, thread, response).await?;

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
// Triage commit helpers
// ---------------------------------------------------------------------------

/// The vectors a flipped-active re-embed produced: the item's vector and
/// the per-tag vectors in the input tags' order.
struct ReembeddedVectors {
    item: Vec<f32>,
    tags: Vec<Vec<f32>>,
}

/// Re-embeds an item and its novel tags against `active` when a cutover flipped
/// the active profile between the pre-embed and the commit, so an old-space
/// vector is never written under the new active.
///
/// The group embeds under one concurrency permit: the re-embed occupies one
/// slot, like the stage embedding it replaces.
async fn reembed_against_active(
    gateway: &InferenceGateway,
    attribution: &UsageAttribution,
    active: &EmbeddingProfile,
    content: &str,
    new_tags: &[NewTagWithEmbedding],
) -> Result<ReembeddedVectors, StageError> {
    let mut requests = Vec::with_capacity(new_tags.len() + 1);
    requests.push(EmbeddingRequest {
        input: content.to_owned(),
        purpose: EmbeddingPurpose::Candidate,
    });
    requests.extend(new_tags.iter().map(|tag| EmbeddingRequest {
        input: tag.tag.clone(),
        purpose: EmbeddingPurpose::Tag,
    }));

    let responses = gateway
        .embed_group(
            &EmbeddingTarget::from(active),
            requests,
            PermitWait::Unbounded,
            attribution,
        )
        .await
        .map_err(|e| match e {
            EmbedGroupError::Setup { source } => StageError::Provider {
                context: "resolving the flipped active profile's provider".to_owned(),
                source,
            },
            EmbedGroupError::Input { index: 0, source } => StageError::Provider {
                context: "re-embedding the item against the flipped active".to_owned(),
                source,
            },
            EmbedGroupError::Input { source, .. } => StageError::Provider {
                context: "re-embedding a tag against the flipped active".to_owned(),
                source,
            },
        })?;

    let mut vectors = responses.into_iter().map(|response| response.vector);
    let item = vectors
        .next()
        .expect("the group always contains the item's request");
    Ok(ReembeddedVectors {
        item,
        tags: vectors.collect(),
    })
}

/// The pre-embedded vectors for a novel item and its tags, against the active
/// profile read before the commit. Re-derived against the new active when a
/// cutover flips the active mid-commit.
struct NovelEmbedding {
    /// The item vector.
    vector: Vec<f32>,
    /// The model that produced these vectors.
    model: String,
    /// The profile the vectors were produced against; the commit flip-check
    /// compares it to the active read under the cutover lock.
    producing_profile_id: EmbeddingProfileId,
    /// The novel tags' vectors, in `new_tags` order.
    tag_vectors: Vec<Vec<f32>>,
}

/// The result of one `commit_novel` attempt.
enum CommitNovelOutcome {
    /// The item, embedding, tags, and references were written.
    Committed(&'static str),
    /// A cutover flipped the active profile since the pre-embed; the caller must
    /// re-embed against this new active and retry, holding no lock across the
    /// provider call.
    Reembed(EmbeddingProfile),
}

/// The bound on re-embed retries when a cutover repeatedly flips the active
/// profile during one commit. A flip is a once-per-reindex event, so a single
/// retry suffices in practice; the bound guards against pathological churn.
const MAX_REEMBED_RETRIES: u32 = 3;

/// Inserts the knowledge item, embedding, references, and triage result
/// for a novel candidate.
#[allow(clippy::too_many_arguments)]
async fn commit_novel(
    txn: &mut sqlx::PgConnection,
    job_id: JobId,
    project_id: tribal_domain::ProjectId,
    batch_index: u32,
    knowledge_item: &tribal_db::NewKnowledgeItem,
    embedding: &NovelEmbedding,
    suggested_references: &[tribal_domain::SuggestedReference],
    new_tags: &[NewTagWithEmbedding],
    resolved_tags: &[String],
) -> Result<CommitNovelOutcome, StageError> {
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
    // pre-embedded vector is for the superseded profile's geometry. Compare
    // profile identity, not (model, dimension): two profiles can share a model
    // and width yet hold different geometries (a different endpoint or
    // revision), so a name-and-width match is not an identity match. Signal the
    // caller to re-embed against the new active and retry, rather than embedding
    // here: a provider call must never hold this lock or the open transaction.
    if active.id() != embedding.producing_profile_id {
        return Ok(CommitNovelOutcome::Reembed(active));
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
            .zip(&embedding.tag_vectors)
            .map(|(t, vector)| {
                NewTagEmbedding::builder()
                    .tag(t.tag.clone())
                    .embedding_profile_id(profile_id)
                    .model(embedding.model.clone())
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
        .model(embedding.model.clone())
        .embedding(embedding.vector.clone())
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

    Ok(CommitNovelOutcome::Committed("created"))
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
    use std::{sync::Arc, time::Duration};

    use tribal_db::{EmbeddingRepository, PgEmbeddingRepository};
    use tribal_domain::{EmbeddingProfileId, PromptRole};
    use tribal_inference::{
        EmbeddingProvider, InjectedEmbedding, InjectedProviders, NoopLedgerSink, ProviderKey,
        ProviderLimits, ProviderRegistry, RequestClass,
    };
    use tribal_test_utils::{
        ExhaustBehaviour, MockEmbeddingProvider, MockInferenceProvider, Seed, a_candidate,
        a_new_knowledge_item, a_new_prompt_version, active_embedding_profile, an_embedding_profile,
        an_embedding_response, seed_triage_job, test_context,
    };

    use super::*;

    /// When a cutover flips the active profile between an ingest's pre-embed and
    /// its commit, the commit re-embeds the item and its novel tags against the
    /// new active's provider (resolved through the gateway's per-profile cache)
    /// rather than writing the stale, old-space vectors under the new active.
    #[tokio::test]
    async fn test_reembed_against_active_uses_the_new_active_provider() {
        // The new active, its built provider already cached, returns a
        // recognisable vector for every input.
        let active = an_embedding_profile().build();
        let mock: Arc<dyn EmbeddingProvider> = Arc::new(
            MockEmbeddingProvider::builder()
                .on_embed(an_embedding_response(vec![0.5_f32; 768]), None)
                .on_exhaust(ExhaustBehaviour::RepeatLast)
                .build(),
        );
        let inference_key =
            ProviderKey::new("mock", "http://localhost:9999", RequestClass::Inference)
                .expect("inference key");
        let endpoint = ProviderKey::new(
            active.provider_kind().to_string(),
            active.normalised_base_url(),
            RequestClass::Embedding,
        )
        .expect("endpoint key");
        let limits = ProviderLimits {
            max_in_flight: 1,
            request_timeout: Duration::from_secs(30),
        };
        let registry = ProviderRegistry::new(vec![
            (inference_key.clone(), limits.clone()),
            (endpoint, limits),
        ])
        .expect("registry");
        let gateway = InferenceGateway::with_providers(InjectedProviders::uniform(
            registry,
            Arc::new(MockInferenceProvider::builder().build()),
            inference_key,
            vec![InjectedEmbedding {
                profile_id: active.id(),
                provider: mock,
            }],
            Arc::new(NoopLedgerSink),
        ));

        // The pre-embedded vectors (all 0.1) belong to the superseded geometry.
        let new_tags = [NewTagWithEmbedding {
            tag: "rust".to_owned(),
            embedding: vec![0.1_f32; 768],
        }];
        let reembedded = reembed_against_active(
            &gateway,
            &UsageAttribution::default(),
            &active,
            "content",
            &new_tags,
        )
        .await
        .expect("reembed");

        assert!(
            reembedded
                .item
                .iter()
                .all(|&v| (v - 0.5).abs() < f32::EPSILON),
            "the item is re-embedded by the new active's provider, not left stale",
        );
        assert_eq!(reembedded.tags.len(), 1);
        assert!(
            reembedded.tags[0]
                .iter()
                .all(|&v| (v - 0.5).abs() < f32::EPSILON),
            "each novel tag is re-embedded against the new active too",
        );
    }

    /// When the active a cutover has since flipped is not the profile the
    /// pre-embed was produced against, `commit_novel` writes nothing and returns
    /// the new active for the caller to re-embed against, so a provider call
    /// never spans the open transaction or the cutover lock. The check is on
    /// profile identity, so it catches a flip even when the model and dimension
    /// are unchanged.
    #[tokio::test]
    async fn test_commit_novel_signals_reembed_when_the_active_flipped() {
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");

        let seed = Seed::new()
            .define_principal("op", "user:reembed-signal")
            .define_project("proj", "git@github.com:tribal/reembed-signal.git")
            .set_embedding_model("new-model", 768)
            .define_prompt_version("sys", a_new_prompt_version().build())
            .define_prompt_version(
                "usr",
                a_new_prompt_version()
                    .role(PromptRole::User)
                    .content_hash("c".repeat(64))
                    .build(),
            )
            .execute(&mut txn)
            .await;
        let principal = seed.principal_id("op");
        let project = seed.project_id("proj");
        let (job_id, _) = seed_triage_job(
            &mut txn,
            principal,
            project,
            seed.prompt_version_id("sys"),
            seed.prompt_version_id("usr"),
            &[a_candidate().build()],
        )
        .await;
        let new_item = a_new_knowledge_item()
            .project_id(project)
            .principal_id(principal)
            .build();

        let active = active_embedding_profile(&mut txn).await;

        // Two profiles can share a model and width yet be distinct geometries (a
        // different endpoint or revision). The pre-embed names a producing
        // profile the active is not, even though the model and dimension still
        // match, so identity, not (model, dimension), must drive the re-embed.
        let produced_elsewhere = NovelEmbedding {
            vector: vec![0.1_f32; 768],
            model: active.model().to_owned(),
            producing_profile_id: EmbeddingProfileId::new(),
            tag_vectors: vec![],
        };
        let outcome = commit_novel(
            &mut txn,
            job_id,
            project,
            0,
            &new_item,
            &produced_elsewhere,
            &[],
            &[],
            &[],
        )
        .await
        .expect("commit_novel");
        assert!(
            matches!(&outcome, CommitNovelOutcome::Reembed(reembed_target) if reembed_target.id() == active.id()),
            "a non-active producing profile re-embeds against the active",
        );

        // The signal precedes every insert, so this job recorded no triage
        // result. Scoping the check to the job keeps it free of items other
        // tests commit concurrently to the shared database.
        let result = PgTriageResultRepository
            .find_by_job_id_and_batch_index(&mut txn, job_id, 0)
            .await
            .expect("find triage result");
        assert!(
            result.is_none(),
            "a re-embed signal writes no triage result",
        );
    }

    /// `commit_novel` writes the item and its embedding against the active when
    /// the pre-embedded vector's model and dimension still match, returning the
    /// created outcome.
    #[tokio::test]
    async fn test_commit_novel_commits_against_the_matching_active() {
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");

        let seed = Seed::new()
            .define_principal("op", "user:commit-match")
            .define_project("proj", "git@github.com:tribal/commit-match.git")
            .set_embedding_model("new-model", 768)
            .define_prompt_version("sys", a_new_prompt_version().build())
            .define_prompt_version(
                "usr",
                a_new_prompt_version()
                    .role(PromptRole::User)
                    .content_hash("c".repeat(64))
                    .build(),
            )
            .execute(&mut txn)
            .await;
        let principal = seed.principal_id("op");
        let project = seed.project_id("proj");
        let active = active_embedding_profile(&mut txn).await;
        let (job_id, _) = seed_triage_job(
            &mut txn,
            principal,
            project,
            seed.prompt_version_id("sys"),
            seed.prompt_version_id("usr"),
            &[a_candidate().build()],
        )
        .await;

        let new_item = a_new_knowledge_item()
            .project_id(project)
            .principal_id(principal)
            .build();
        let embedding = NovelEmbedding {
            vector: vec![0.5_f32; 768],
            model: "new-model".to_owned(),
            producing_profile_id: active.id(),
            tag_vectors: vec![],
        };
        let outcome = commit_novel(
            &mut txn,
            job_id,
            project,
            0,
            &new_item,
            &embedding,
            &[],
            &[],
            &[],
        )
        .await
        .expect("commit_novel");
        assert!(
            matches!(outcome, CommitNovelOutcome::Committed("created")),
            "a matching embedding is committed",
        );

        // Resolve the committed item through this job's triage result rather
        // than a global item scan, so the assertion sees only this test's write
        // and not items other tests commit concurrently.
        let result = PgTriageResultRepository
            .find_by_job_id_and_batch_index(&mut txn, job_id, 0)
            .await
            .expect("find triage result")
            .expect("the novel commit records a triage result");
        let item_id = match result.outcome() {
            TriageOutcome::Created { item_id } => *item_id,
            other => panic!("a novel commit records a Created outcome, got {other:?}"),
        };
        let stored = PgEmbeddingRepository
            .find_by_knowledge_item_id(&mut txn, item_id, active.id())
            .await
            .expect("find embedding")
            .expect("the committed item is embedded under the active");
        assert_eq!(
            stored.model(),
            "new-model",
            "the stored lineage names the active model",
        );
        assert!(
            stored
                .embedding()
                .iter()
                .all(|&v| (v - 0.5).abs() < f32::EPSILON),
            "the stored vector is the committed one",
        );
    }
}
