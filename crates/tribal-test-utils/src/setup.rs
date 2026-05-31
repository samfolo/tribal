//! Test setup helpers for entities that lack a direct production
//! repository path.
//!
//! Functions in this module either delegate to an existing repository
//! (prompt versions) or use raw SQL for entities with no production
//! insertion path (embeddings, committed relations).

use sqlx::PgConnection;
use tribal_db::{
    EmbeddingProfileRepository, EmbeddingRepository, ExtractionResultRepository, JobRepository,
    JobStatusTransition, KnowledgeItemRepository, NewEmbeddingProfile, NewPromptVersion,
    NewSystemFingerprint, PgEmbeddingProfileRepository, PgEmbeddingRepository,
    PgExtractionResultRepository, PgJobRepository, PgKnowledgeItemRepository,
    PgPromptVersionRepository, PgSystemFingerprintRepository, PgTaskRepository,
    PgTriageResultRepository, PromptVersionRepository, SystemFingerprintRepository, TaskRepository,
    TriageResultRepository,
};
use tribal_domain::{
    Candidate, EmbeddingProfile, EmbeddingProfileId, JobId, JobStatus, KnowledgeItemId,
    PrincipalId, ProjectId, PromptVersionId, ProviderKind, RelationBatchId, RelationHint,
    RelationKind, TaskId, TaskStatus, TaskType, TriageOutcome,
};

use crate::{
    a_new_extraction_result, a_new_job, a_new_knowledge_item, a_new_system_fingerprint, a_new_task,
    a_new_triage_result_created, candidates_json, relation_hints_json,
};

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
// upsert_system_fingerprint
// ---------------------------------------------------------------------------

/// Upserts a system fingerprint via the production repository.
///
/// Callers use the existing `a_new_system_fingerprint()` factory to build
/// the input.  The upsert is idempotent — repeated calls with the same
/// `content_hash` return the existing row.
///
/// # Panics
///
/// Panics if the database operation fails.
pub async fn upsert_system_fingerprint(
    conn: &mut PgConnection,
    new: &NewSystemFingerprint,
) -> String {
    PgSystemFingerprintRepository
        .upsert(conn, new)
        .await
        .expect("setup: upsert system fingerprint")
        .content_hash()
        .to_owned()
}

// ---------------------------------------------------------------------------
// insert_embedding
// ---------------------------------------------------------------------------

/// Ensures a `complete` genesis embedding profile exists and returns it.
///
/// Reuses the active profile when one is present, otherwise creates a profile
/// from the given model and dimension and completes it, mirroring first-boot
/// provisioning so embedding inserts have a profile to key against.
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn ensure_genesis_profile(
    conn: &mut PgConnection,
    model: &str,
    dimensions: u32,
) -> EmbeddingProfile {
    ensure_genesis_profile_with_endpoint(
        conn,
        ProviderKind::Ollama,
        model,
        dimensions,
        ProviderKind::DEFAULT_OLLAMA_BASE_URL,
    )
    .await
}

/// Ensures an active genesis profile with a specific provider and endpoint.
///
/// Like [`ensure_genesis_profile`] but parameterised over the provider kind and
/// already-normalised base URL, for harnesses whose live embedding identity is
/// not the Ollama default (so the seeded genesis matches what the server's
/// provider builder will construct from it).
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn ensure_genesis_profile_with_endpoint(
    conn: &mut PgConnection,
    provider_kind: ProviderKind,
    model: &str,
    dimensions: u32,
    normalised_base_url: &str,
) -> EmbeddingProfile {
    if let Some(profile) = PgEmbeddingProfileRepository
        .find_active(&mut *conn)
        .await
        .expect("setup: find active profile")
    {
        return profile;
    }

    let new_profile = NewEmbeddingProfile::builder()
        .provider_kind(provider_kind)
        .normalised_base_url(normalised_base_url.to_owned())
        .model(model.to_owned())
        .dimensions(dimensions)
        .fingerprint_hash(format!("setup:{model}:{dimensions}"))
        .build();
    let profile = PgEmbeddingProfileRepository
        .insert(&mut *conn, &new_profile)
        .await
        .expect("setup: insert genesis profile");
    PgEmbeddingProfileRepository
        .mark_complete(&mut *conn, profile.id())
        .await
        .expect("setup: complete genesis profile");
    PgEmbeddingProfileRepository
        .find_active(&mut *conn)
        .await
        .expect("setup: re-read active profile")
        .expect("setup: active profile present after completion")
}

/// Inserts a fresh `complete` embedding profile and returns its id.
///
/// Unlike [`ensure_genesis_profile`] this always creates a new profile, for
/// tests that need two distinct profiles to exercise profile isolation.
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn create_complete_profile(
    conn: &mut PgConnection,
    model: &str,
    dimensions: u32,
) -> EmbeddingProfileId {
    let new_profile = NewEmbeddingProfile::builder()
        .provider_kind(ProviderKind::Ollama)
        .normalised_base_url(ProviderKind::DEFAULT_OLLAMA_BASE_URL.to_owned())
        .model(model.to_owned())
        .dimensions(dimensions)
        .fingerprint_hash(format!("setup:{model}:{dimensions}"))
        .build();
    let profile = PgEmbeddingProfileRepository
        .insert(&mut *conn, &new_profile)
        .await
        .expect("setup: insert complete profile");
    PgEmbeddingProfileRepository
        .mark_complete(&mut *conn, profile.id())
        .await
        .expect("setup: complete profile");
    profile.id()
}

/// Finds a knowledge item's embedding under the active profile.
///
/// A drop-in for tests that previously looked up by model: it resolves the
/// active profile and queries by its id, preserving the `Result<Option<_>>`
/// shape of [`EmbeddingRepository::find_by_knowledge_item_id`].
///
/// # Errors
///
/// Returns the underlying [`tribal_db::DbError`] when the query fails.
///
/// # Panics
///
/// Panics if there is no active profile.
pub async fn find_active_embedding(
    conn: &mut PgConnection,
    knowledge_item_id: KnowledgeItemId,
) -> Result<Option<tribal_domain::Embedding>, tribal_db::DbError> {
    let profile_id = active_embedding_profile(conn).await.id();
    PgEmbeddingRepository
        .find_by_knowledge_item_id(conn, knowledge_item_id, profile_id)
        .await
}

/// Returns the active embedding profile, panicking if none exists.
///
/// # Panics
///
/// Panics if there is no active profile or the query fails.
pub async fn active_embedding_profile(conn: &mut PgConnection) -> EmbeddingProfile {
    PgEmbeddingProfileRepository
        .find_active(&mut *conn)
        .await
        .expect("setup: find active profile")
        .expect("setup: an active embedding profile must exist")
}

/// Inserts a test embedding for a knowledge item, ensuring a genesis profile
/// exists and keying the row to it.
///
/// No production repository exposes direct embedding insertion; this uses raw
/// SQL.
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
    let dimensions = u32::try_from(vector.len()).expect("embedding dimensions exceed u32");
    let profile_id = ensure_genesis_profile(&mut *conn, model, dimensions)
        .await
        .id();
    insert_embedding_for_profile(conn, knowledge_item_id, profile_id, model, vector).await;
}

/// Inserts a test embedding keyed to an explicit profile.
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn insert_embedding_for_profile(
    conn: &mut PgConnection,
    knowledge_item_id: KnowledgeItemId,
    embedding_profile_id: EmbeddingProfileId,
    model: &str,
    vector: Vec<f32>,
) {
    let pgvec = pgvector::HalfVector::from_f32_slice(&vector);
    sqlx::query(
        "INSERT INTO embeddings (knowledge_item_id, embedding_profile_id, model, embedding) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(knowledge_item_id.inner())
    .bind(embedding_profile_id.inner())
    .bind(model)
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
    system_pv_id: PromptVersionId,
    user_pv_id: PromptVersionId,
) -> (JobId, TaskId) {
    let fingerprint_hash = upsert_system_fingerprint(
        conn,
        &a_new_system_fingerprint()
            .extraction_system_prompt_version_id(system_pv_id)
            .extraction_user_prompt_version_id(user_pv_id)
            .triage_system_prompt_version_id(system_pv_id)
            .triage_user_prompt_version_id(user_pv_id)
            .relation_system_prompt_version_id(system_pv_id)
            .relation_user_prompt_version_id(user_pv_id)
            .build(),
    )
    .await;

    let job = PgJobRepository
        .insert(
            conn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_system_prompt_version_id(system_pv_id)
                .extraction_user_prompt_version_id(user_pv_id)
                .triage_system_prompt_version_id(system_pv_id)
                .triage_user_prompt_version_id(user_pv_id)
                .relation_system_prompt_version_id(system_pv_id)
                .relation_user_prompt_version_id(user_pv_id)
                .system_fingerprint_hash(fingerprint_hash)
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
    system_pv_id: PromptVersionId,
    user_pv_id: PromptVersionId,
    candidates: &[Candidate],
) -> (JobId, TaskId) {
    assert!(
        !candidates.is_empty(),
        "seed_triage_job requires at least one candidate",
    );
    // The triage commit path writes embeddings against the active profile, so a
    // genesis profile must exist. Idempotent: reuses one a `Seed` already made.
    ensure_genesis_profile(conn, "mock-model", 768).await;
    let batch_size = u32::try_from(candidates.len()).expect("candidate count fits u32");

    let fingerprint_hash = upsert_system_fingerprint(
        conn,
        &a_new_system_fingerprint()
            .extraction_system_prompt_version_id(system_pv_id)
            .extraction_user_prompt_version_id(user_pv_id)
            .triage_system_prompt_version_id(system_pv_id)
            .triage_user_prompt_version_id(user_pv_id)
            .relation_system_prompt_version_id(system_pv_id)
            .relation_user_prompt_version_id(user_pv_id)
            .build(),
    )
    .await;

    let job = PgJobRepository
        .insert(
            conn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_system_prompt_version_id(system_pv_id)
                .extraction_user_prompt_version_id(user_pv_id)
                .triage_system_prompt_version_id(system_pv_id)
                .triage_user_prompt_version_id(user_pv_id)
                .relation_system_prompt_version_id(system_pv_id)
                .relation_user_prompt_version_id(user_pv_id)
                .system_fingerprint_hash(fingerprint_hash)
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

// ---------------------------------------------------------------------------
// seed_multiple_triage_tasks
// ---------------------------------------------------------------------------

/// Inserts a job in `Triaging` status with a completed extraction task,
/// an extraction result containing the given candidates, and one queued
/// triage task per candidate (`batch_index = 0..candidates.len()`).
///
/// Returns `(job_id, triage_task_ids)` where the task IDs are ordered by
/// batch index.
///
/// # Panics
///
/// Panics if any insert or status transition fails, or if `candidates`
/// is empty.
pub async fn seed_multiple_triage_tasks(
    conn: &mut PgConnection,
    principal_id: PrincipalId,
    project_id: ProjectId,
    system_pv_id: PromptVersionId,
    user_pv_id: PromptVersionId,
    candidates: &[Candidate],
) -> (JobId, Vec<TaskId>) {
    assert!(
        !candidates.is_empty(),
        "seed_multiple_triage_tasks requires at least one candidate",
    );
    let batch_size = u32::try_from(candidates.len()).expect("candidate count fits u32");

    let fingerprint_hash = upsert_system_fingerprint(
        conn,
        &a_new_system_fingerprint()
            .extraction_system_prompt_version_id(system_pv_id)
            .extraction_user_prompt_version_id(user_pv_id)
            .triage_system_prompt_version_id(system_pv_id)
            .triage_user_prompt_version_id(user_pv_id)
            .relation_system_prompt_version_id(system_pv_id)
            .relation_user_prompt_version_id(user_pv_id)
            .build(),
    )
    .await;

    let job = PgJobRepository
        .insert(
            conn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_system_prompt_version_id(system_pv_id)
                .extraction_user_prompt_version_id(user_pv_id)
                .triage_system_prompt_version_id(system_pv_id)
                .triage_user_prompt_version_id(user_pv_id)
                .relation_system_prompt_version_id(system_pv_id)
                .relation_user_prompt_version_id(user_pv_id)
                .system_fingerprint_hash(fingerprint_hash)
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

    // Create one triage task per candidate.
    let mut task_ids = Vec::with_capacity(candidates.len());
    for i in 0..candidates.len() {
        let batch_index = u32::try_from(i).expect("batch index fits u32");
        let task = PgTaskRepository
            .insert(
                conn,
                &a_new_task()
                    .job_id(job_id)
                    .task_type(TaskType::Triage)
                    .batch_index(Some(batch_index))
                    .build(),
            )
            .await
            .expect("setup: insert triage task");
        task_ids.push(task.id());
    }

    (job_id, task_ids)
}

// ---------------------------------------------------------------------------
// seed_triage_created_outcomes
// ---------------------------------------------------------------------------

/// Creates a knowledge item, a completed triage task, and a `Created`
/// triage result for each batch index in `0..batch_size`.
///
/// Returns the knowledge item IDs in batch-index order.
///
/// # Panics
///
/// Panics if any insert fails.
async fn seed_triage_created_outcomes(
    conn: &mut PgConnection,
    job_id: JobId,
    principal_id: PrincipalId,
    project_id: ProjectId,
    batch_size: u32,
) -> Vec<KnowledgeItemId> {
    let mut ki_ids = Vec::with_capacity(batch_size as usize);

    for batch_index in 0..batch_size {
        let ki = PgKnowledgeItemRepository
            .insert(
                conn,
                &a_new_knowledge_item()
                    .project_id(project_id)
                    .principal_id(principal_id)
                    .build(),
            )
            .await
            .expect("setup: insert knowledge item");
        let ki_id = ki.id();
        ki_ids.push(ki_id);

        PgTaskRepository
            .insert_for_test(
                conn,
                &a_new_task()
                    .job_id(job_id)
                    .task_type(TaskType::Triage)
                    .batch_index(Some(batch_index))
                    .build(),
                TaskStatus::Completed,
            )
            .await
            .expect("setup: insert triage task");

        PgTriageResultRepository
            .insert(
                conn,
                &a_new_triage_result_created()
                    .job_id(job_id)
                    .batch_index(batch_index)
                    .outcome(TriageOutcome::Created { item_id: ki_id })
                    .build(),
            )
            .await
            .expect("setup: insert triage result");
    }

    ki_ids
}

// ---------------------------------------------------------------------------
// seed_relation_job
// ---------------------------------------------------------------------------

/// Inserts a job in `Relating` status with completed extraction and triage
/// tasks, an extraction result containing the given candidates and relation
/// hints, knowledge items for each candidate (all with `Created` triage
/// outcomes), and a queued relation task.
///
/// Returns `(job_id, relation_task_id, knowledge_item_ids)` where the
/// knowledge item IDs are ordered by batch index.
///
/// # Panics
///
/// Panics if any insert or status transition fails, or if `candidates`
/// is empty.
pub async fn seed_relation_job(
    conn: &mut PgConnection,
    principal_id: PrincipalId,
    project_id: ProjectId,
    system_pv_id: PromptVersionId,
    user_pv_id: PromptVersionId,
    candidates: &[Candidate],
    relation_hints: &[RelationHint],
) -> (JobId, TaskId, Vec<KnowledgeItemId>) {
    assert!(
        !candidates.is_empty(),
        "seed_relation_job requires at least one candidate",
    );
    let batch_size = u32::try_from(candidates.len()).expect("candidate count fits u32");

    let fingerprint_hash = upsert_system_fingerprint(
        conn,
        &a_new_system_fingerprint()
            .extraction_system_prompt_version_id(system_pv_id)
            .extraction_user_prompt_version_id(user_pv_id)
            .triage_system_prompt_version_id(system_pv_id)
            .triage_user_prompt_version_id(user_pv_id)
            .relation_system_prompt_version_id(system_pv_id)
            .relation_user_prompt_version_id(user_pv_id)
            .build(),
    )
    .await;

    let job = PgJobRepository
        .insert(
            conn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_system_prompt_version_id(system_pv_id)
                .extraction_user_prompt_version_id(user_pv_id)
                .triage_system_prompt_version_id(system_pv_id)
                .triage_user_prompt_version_id(user_pv_id)
                .relation_system_prompt_version_id(system_pv_id)
                .relation_user_prompt_version_id(user_pv_id)
                .system_fingerprint_hash(fingerprint_hash)
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

    // Persist the extraction result with candidate and relation hint JSON.
    PgExtractionResultRepository
        .insert(
            conn,
            &a_new_extraction_result()
                .job_id(job_id)
                .candidates(candidates_json(candidates))
                .relation_hints(relation_hints_json(relation_hints))
                .build(),
        )
        .await
        .expect("setup: insert extraction result");

    // Update batch size and transition to Triaging.
    PgJobRepository
        .update_batch_size(conn, job_id, batch_size, batch_size)
        .await
        .expect("setup: update batch size");

    let triaging = JobStatusTransition::builder()
        .status(JobStatus::Triaging)
        .build();
    PgJobRepository
        .update_status(conn, job_id, &triaging)
        .await
        .expect("setup: transition job to triaging");

    // Create knowledge items and triage results (Created) for each candidate.
    let ki_ids =
        seed_triage_created_outcomes(conn, job_id, principal_id, project_id, batch_size).await;

    // Transition to Relating.
    let relating = JobStatusTransition::builder()
        .status(JobStatus::Relating)
        .build();
    PgJobRepository
        .update_status(conn, job_id, &relating)
        .await
        .expect("setup: transition job to relating");

    // Create the queued relation task.
    let relation_task = PgTaskRepository
        .insert(
            conn,
            &a_new_task()
                .job_id(job_id)
                .task_type(TaskType::Relation)
                .build(),
        )
        .await
        .expect("setup: insert relation task");

    (job_id, relation_task.id(), ki_ids)
}
