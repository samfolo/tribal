//! Domain-effect commit handlers for each pipeline stage.

use chrono::Utc;
use opentelemetry::KeyValue;
use tracing::Instrument;
use tribal_telemetry::{LABEL_OUTCOME, LABEL_TASK_TYPE};
use tribal_common::clamp_to_u32;
use tribal_db::{
    EmbeddingRepository, ExtractionResultRepository, ItemObservationRepository, JobRepository,
    JobStatusTransition, KnowledgeItemRepository, NewEmbedding, NewExtractionResult,
    NewKnowledgeItemRelation, NewReference, NewTagEmbedding, NewTask, NewTriageResult,
    PgEmbeddingRepository, PgExtractionResultRepository, PgItemObservationRepository,
    PgJobRepository, PgKnowledgeItemRepository, PgReferenceRepository, PgRelationRepository,
    PgTagEmbeddingRepository, PgTagRegistryRepository, PgTaskRepository, PgTriageResultRepository,
    PgTriageSimilarItemDecisionRepository, ReferenceRepository, RelationRepository,
    TagEmbeddingRepository, TagRegistryRepository, TaskRepository, TriageResultRepository,
    TriageSimilarItemDecisionRepository,
};
use tribal_domain::{
    Job, JobId, JobOutcome, JobState, JobStatus, ReferenceKind, RelationBatchId, Task,
    TriageOutcome, span_attrs,
};

use super::Worker;
use crate::{
    common::{EXPECT_BATCH_INDEX, EXPECT_CLAIMED_AT},
    error::{STAGE_EXTRACTION, STAGE_RELATION, STAGE_TRIAGE, StageError},
    stages::{RelationCommitDecision, StageCommit, TriageCommitDecision},
    tag_resolution::NewTagWithEmbedding,
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
            StageCommit::Relation { decision } => {
                self.commit_relation(task, job, decision).await
            }
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

            let task_type_attr = KeyValue::new(LABEL_TASK_TYPE, task.task_type().as_str());
            self.metrics().tasks_completed.add(1, &[task_type_attr.clone()]);
            #[allow(clippy::cast_precision_loss)]
            let duration_ms = (Utc::now() - task.claimed_at().expect(EXPECT_CLAIMED_AT))
                .num_milliseconds() as f64;
            self.metrics().task_duration_ms.record(duration_ms, &[task_type_attr]);

            if is_empty {
                let outcome_attr = KeyValue::new(LABEL_OUTCOME, JobOutcome::Empty.as_str());
                self.metrics().jobs_completed.add(1, &[outcome_attr.clone()]);
                #[allow(clippy::cast_precision_loss)]
                let job_duration_ms = (Utc::now() - job.created_at())
                    .num_milliseconds() as f64;
                self.metrics().job_duration_ms.record(job_duration_ms, &[outcome_attr]);
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

            let task_type_attr = KeyValue::new(LABEL_TASK_TYPE, task.task_type().as_str());
            self.metrics().tasks_completed.add(1, &[task_type_attr.clone()]);
            #[allow(clippy::cast_precision_loss)]
            let duration_ms = (Utc::now() - task.claimed_at().expect(EXPECT_CLAIMED_AT))
                .num_milliseconds() as f64;
            self.metrics().task_duration_ms.record(duration_ms, &[task_type_attr]);

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
                    self.commit_relation_relate(
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
                    (true, Some(outcome))
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

            let task_type_attr = KeyValue::new(LABEL_TASK_TYPE, task.task_type().as_str());
            self.metrics().tasks_completed.add(1, &[task_type_attr.clone()]);
            #[allow(clippy::cast_precision_loss)]
            let duration_ms = (Utc::now() - task.claimed_at().expect(EXPECT_CLAIMED_AT))
                .num_milliseconds() as f64;
            self.metrics().task_duration_ms.record(duration_ms, &[task_type_attr]);

            if let Some(outcome) = relation_outcome {
                let outcome_attr = KeyValue::new(LABEL_OUTCOME, outcome.as_str());
                self.metrics().jobs_completed.add(1, &[outcome_attr.clone()]);
                #[allow(clippy::cast_precision_loss)]
                let job_duration_ms = (Utc::now() - job.created_at())
                    .num_milliseconds() as f64;
                self.metrics().job_duration_ms.record(job_duration_ms, &[outcome_attr]);
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
    ) -> Result<(), StageError> {
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

            return Ok(());
        }

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

        Ok(())
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

/// Inserts the knowledge item, embedding, references, and triage result
/// for a novel candidate.
#[allow(clippy::too_many_arguments)]
async fn commit_novel(
    txn: &mut sqlx::PgConnection,
    job_id: JobId,
    project_id: tribal_domain::ProjectId,
    batch_index: u32,
    knowledge_item: &tribal_db::NewKnowledgeItem,
    embedding_vector: Vec<f32>,
    embedding_model: String,
    suggested_references: &[tribal_domain::SuggestedReference],
    new_tags: &[NewTagWithEmbedding],
    resolved_tags: &[String],
) -> Result<&'static str, StageError> {
    // FK ordering: tag_registry inserts before tag_embeddings inserts.
    if !new_tags.is_empty() {
        let tag_names: Vec<String> = new_tags.iter().map(|t| t.tag.clone()).collect();
        PgTagRegistryRepository
            .batch_upsert(txn, &tag_names)
            .await
            .map_err(|e| stage_db_error(STAGE_TRIAGE, "upserting tags", e))?;

        let new_embeddings: Vec<NewTagEmbedding> = new_tags
            .iter()
            .map(|t| {
                NewTagEmbedding::builder()
                    .tag(t.tag.clone())
                    .model(embedding_model.clone())
                    .dimensions(t.dimensions)
                    .embedding(t.embedding.clone())
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
        .model(embedding_model)
        .dimensions(clamp_to_u32(embedding_vector.len()))
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
