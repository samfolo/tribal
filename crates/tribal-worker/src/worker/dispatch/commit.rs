//! Domain-effect commit handlers for each pipeline stage.

use chrono::Utc;
use tracing::Instrument;
use tribal_db::{
    EmbeddingRepository, ExtractionResultRepository, ItemObservationRepository, JobRepository,
    JobStatusTransition, KnowledgeItemRepository, NewEmbedding, NewExtractionResult, NewReference,
    NewTask, NewTriageResult, PgEmbeddingRepository, PgExtractionResultRepository,
    PgItemObservationRepository, PgJobRepository, PgKnowledgeItemRepository, PgReferenceRepository,
    PgTagRegistryRepository, PgTaskRepository, PgTriageResultRepository,
    PgTriageSimilarItemDecisionRepository, ReferenceRepository, TagRegistryRepository,
    TaskRepository, TriageResultRepository, TriageSimilarItemDecisionRepository,
};
use tribal_domain::{JobId, JobOutcome, JobStatus, ReferenceKind, Task, TriageOutcome, span_attrs};

use super::Worker;
use crate::{
    common::{EXPECT_BATCH_INDEX, clamp_to_u32},
    error::{STAGE_EXTRACTION, STAGE_TRIAGE, StageError},
    stages::{StageCommit, TriageCommitDecision},
};

// ---------------------------------------------------------------------------
// Worker impl
// ---------------------------------------------------------------------------

impl Worker {
    /// Commits domain effects produced by a successful stage.
    pub(crate) async fn commit_domain_effects(
        &self,
        task: &Task,
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

            // Notify watch subscribers of the job state change.
            self.notify_job_state(task.job_id());

            // Clean up watch channel entry for terminal job transitions.
            if is_empty {
                self.remove_job_state(task.job_id());
            }

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
                    )
                    .await?
                }
                TriageCommitDecision::Duplicate { observation } => {
                    commit_duplicate(&mut txn, job_id, batch_index, &observation).await?
                }
                TriageCommitDecision::NoOp => {
                    let existing = PgTriageResultRepository
                        .find_by_job_id_and_batch_index(&mut txn, job_id, batch_index)
                        .await
                        .map_err(|e| {
                            stage_db_error(STAGE_TRIAGE, "re-checking triage idempotency", e)
                        })?;
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
                    "no_op"
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

            txn.commit()
                .await
                .map_err(|e| stage_sqlx_error(STAGE_TRIAGE, "committing transaction", e))?;

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
    new_tags: &[String],
) -> Result<&'static str, StageError> {
    if !new_tags.is_empty() {
        PgTagRegistryRepository
            .batch_upsert(txn, new_tags)
            .await
            .map_err(|e| stage_db_error(STAGE_TRIAGE, "upserting tags", e))?;
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
