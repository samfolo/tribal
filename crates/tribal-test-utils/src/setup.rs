//! Test setup helpers for entities that lack a direct production
//! repository path.
//!
//! Functions in this module either delegate to an existing repository
//! (prompt versions) or use raw SQL for entities with no production
//! insertion path (embeddings, committed relations).

use sqlx::PgConnection;
use tribal_db::{
    ExtractionResultRepository, JobRepository, JobStatusTransition, NewPromptVersion,
    PgExtractionResultRepository, PgJobRepository, PgPromptVersionRepository, PgTaskRepository,
    PromptVersionRepository, TaskRepository,
};
use tribal_domain::{
    Candidate, JobId, JobStatus, KnowledgeItemId, PrincipalId, ProjectId, PromptVersionId,
    RelationBatchId, RelationKind, TaskId, TaskStatus, TaskType,
};

use crate::{a_new_extraction_result, a_new_job, a_new_task, candidates_json};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const EMBEDDING_DIMENSIONS_EXCEED_I32: &str = "embedding dimensions exceed i32";

// ---------------------------------------------------------------------------
// insert_prompt_version
// ---------------------------------------------------------------------------

/// Inserts a prompt version via the production repository upsert.
///
/// Callers use the existing `a_new_prompt_version()` factory to build
/// the input.  The upsert is idempotent — repeated calls with the same
/// `(stage, content_hash)` return the existing row.
///
/// # Panics
///
/// Panics if the database operation fails.
pub async fn insert_prompt_version(
    conn: &mut PgConnection,
    new: &NewPromptVersion,
) -> PromptVersionId {
    PgPromptVersionRepository
        .upsert(conn, new)
        .await
        .expect("setup: insert prompt version")
        .id()
}

// ---------------------------------------------------------------------------
// insert_embedding
// ---------------------------------------------------------------------------

/// Inserts a test embedding for a knowledge item.
///
/// No production repository exposes direct embedding insertion — this
/// uses raw SQL.
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn insert_embedding(
    conn: &mut PgConnection,
    knowledge_item_id: KnowledgeItemId,
    model: &str,
    vector: Vec<f32>,
) {
    let dimensions = i32::try_from(vector.len()).expect(EMBEDDING_DIMENSIONS_EXCEED_I32);
    let pgvec = pgvector::Vector::from(vector);
    sqlx::query(
        "INSERT INTO embeddings (knowledge_item_id, model, dimensions, embedding) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(knowledge_item_id.inner())
    .bind(model)
    .bind(dimensions)
    .bind(pgvec)
    .execute(&mut *conn)
    .await
    .expect("setup: insert embedding");
}

// ---------------------------------------------------------------------------
// insert_committed_relation
// ---------------------------------------------------------------------------

/// Inserts a committed knowledge item relation.
///
/// No production repository exposes direct relation insertion with a
/// pre-assigned batch — this uses raw SQL.
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn insert_committed_relation(
    conn: &mut PgConnection,
    batch_id: RelationBatchId,
    source_id: KnowledgeItemId,
    target_id: KnowledgeItemId,
    relation_type: RelationKind,
    principal_id: PrincipalId,
) {
    sqlx::query(
        "INSERT INTO knowledge_item_relations \
             (relation_batch_id, source_id, target_id, \
              relation_type, principal_id) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(batch_id.inner())
    .bind(source_id.inner())
    .bind(target_id.inner())
    .bind(relation_type.as_str())
    .bind(principal_id.inner())
    .execute(&mut *conn)
    .await
    .expect("setup: insert committed relation");
}

// ---------------------------------------------------------------------------
// seed_extraction_job
// ---------------------------------------------------------------------------

/// Inserts a job and its extraction task, returning both IDs.
///
/// The job uses the same prompt version for all three stages, which
/// is the common case in integration tests.
///
/// # Panics
///
/// Panics if either insert fails.
pub async fn seed_extraction_job(
    conn: &mut PgConnection,
    principal_id: PrincipalId,
    project_id: ProjectId,
    pv_id: PromptVersionId,
) -> (JobId, TaskId) {
    let job = PgJobRepository
        .insert(
            conn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_prompt_version_id(pv_id)
                .triage_prompt_version_id(pv_id)
                .relation_prompt_version_id(pv_id)
                .build(),
        )
        .await
        .expect("setup: insert job");
    let task = PgTaskRepository
        .insert(
            conn,
            &a_new_task()
                .job_id(job.id())
                .task_type(TaskType::Extraction)
                .build(),
        )
        .await
        .expect("setup: insert task");
    (job.id(), task.id())
}

// ---------------------------------------------------------------------------
// seed_triage_job
// ---------------------------------------------------------------------------

/// Inserts a job in `Triaging` status with a completed extraction task,
/// an extraction result containing the given candidates, and a queued
/// triage task at `batch_index = 0`.
///
/// Returns `(job_id, triage_task_id)`.
///
/// # Panics
///
/// Panics if any insert or status transition fails.
pub async fn seed_triage_job(
    conn: &mut PgConnection,
    principal_id: PrincipalId,
    project_id: ProjectId,
    pv_id: PromptVersionId,
    candidates: &[Candidate],
) -> (JobId, TaskId) {
    let batch_size = u32::try_from(candidates.len()).expect("candidate count fits u32");

    let job = PgJobRepository
        .insert(
            conn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_prompt_version_id(pv_id)
                .triage_prompt_version_id(pv_id)
                .relation_prompt_version_id(pv_id)
                .build(),
        )
        .await
        .expect("setup: insert job");

    let job_id = job.id();

    // Mark extraction as completed.
    PgTaskRepository
        .insert_for_test(
            conn,
            &a_new_task()
                .job_id(job_id)
                .task_type(TaskType::Extraction)
                .build(),
            TaskStatus::Completed,
        )
        .await
        .expect("setup: insert extraction task");

    // Persist the extraction result with candidate JSON.
    PgExtractionResultRepository
        .insert(
            conn,
            &a_new_extraction_result()
                .job_id(job_id)
                .candidates(candidates_json(candidates))
                .build(),
        )
        .await
        .expect("setup: insert extraction result");

    // Update batch size and transition to Triaging.
    PgJobRepository
        .update_batch_size(conn, job_id, batch_size, batch_size)
        .await
        .expect("setup: update batch size");

    let transition = JobStatusTransition::builder()
        .status(JobStatus::Triaging)
        .build();
    PgJobRepository
        .update_status(conn, job_id, &transition)
        .await
        .expect("setup: transition job to triaging");

    // Create the triage task.
    let triage_task = PgTaskRepository
        .insert(
            conn,
            &a_new_task()
                .job_id(job_id)
                .task_type(TaskType::Triage)
                .batch_index(Some(0))
                .build(),
        )
        .await
        .expect("setup: insert triage task");

    (job_id, triage_task.id())
}
