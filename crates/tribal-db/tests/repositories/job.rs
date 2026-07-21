use chrono::{SubsecRound, Utc};
use tribal_db::{
    DbError, IngestInsertOutcome, IngestJobRepository, JobRepository, PgJobRepository,
    PgPrincipalRepository, PgProjectRepository, PgTaskRepository, PrincipalRepository,
    ProjectRepository, RecentIngestionCursor, RecentIngestionsQuery,
};
use tribal_domain::{
    EpisodeId, ExtractionCommitOutcome, GitRemote, InferenceIdentity, JobId, JobOutcome, JobStatus,
    PrincipalId, ProjectId, PromptVersionId, RelationBatchId, TaskStatus, TaskType,
};
use tribal_test_utils::{
    TestDb, a_job_status_transition, a_new_job, a_new_principal, a_new_project,
    a_new_prompt_version, a_new_system_fingerprint, a_new_task, insert_prompt_version,
    upsert_system_fingerprint,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal, project, `prompt_version`, and system fingerprint
/// row, returning the IDs needed to create a job.
///
/// A single `prompt_version` ID is reused for all six job FK columns.
async fn setup_job_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (PrincipalId, ProjectId, PromptVersionId, String) {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:job-test-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal");

    let project = PgProjectRepository
        .insert_git(
            txn,
            &a_new_project()
                .remote(GitRemote::from_parts(
                    "github.com",
                    &format!("test/job-{suffix}"),
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project");

    let pv_id = insert_prompt_version(txn, &a_new_prompt_version().build()).await;

    let fingerprint_hash =
        upsert_system_fingerprint(txn, &a_new_system_fingerprint().build()).await;

    (principal.id(), project.id(), pv_id, fingerprint_hash)
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_returns_populated_job() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "insert").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash)
        .source_context(serde_json::json!({"tool": "test"}))
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(job.id().to_string().starts_with("job_"));
    assert_eq!(job.project_id(), project_id);
    assert_eq!(job.principal_id(), principal_id);
    assert_eq!(job.status(), JobStatus::Queued);
    assert!(job.outcome().is_none());
    assert_eq!(*job.source_context(), serde_json::json!({"tool": "test"}));
    assert_eq!(job.raw_input(), "test raw input");
    assert_eq!(job.extraction_system_prompt_version_id(), pv_id);
    assert_eq!(job.extraction_user_prompt_version_id(), pv_id);
    assert_eq!(job.triage_system_prompt_version_id(), pv_id);
    assert_eq!(job.triage_user_prompt_version_id(), pv_id);
    assert_eq!(job.relation_system_prompt_version_id(), pv_id);
    assert_eq!(job.relation_user_prompt_version_id(), pv_id);
    assert!(job.batch_size().is_none());
    assert!(job.committed_batch_id().is_none());
    assert!(job.error_message().is_none());
    assert!(job.completed_at().is_none());
}

#[tokio::test]
async fn test_insert_with_optional_fields_round_trips() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "insert-optional").await;

    let actor = PgPrincipalRepository
        .insert(
            &mut txn,
            &a_new_principal()
                .principal_key("user:job-test-actor".to_owned())
                .build(),
        )
        .await
        .expect("insert actor");

    let correlation_id = EpisodeId::new();
    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .actor_id(Some(actor.id()))
        .correlation_id(Some(correlation_id))
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash)
        .raw_input("custom raw input for round-trip".to_owned())
        .trace_context(Some("00-abc-def-01".to_owned()))
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(job.actor_id(), Some(actor.id()));
    assert_eq!(job.correlation_id(), Some(correlation_id));
    assert_eq!(job.raw_input(), "custom raw input for round-trip");
    assert_eq!(job.trace_context(), Some("00-abc-def-01"));
}

// ---------------------------------------------------------------------------
// find_by_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_id_returns_job() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "find-by-id").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash)
        .build();

    let inserted = repo.insert(&mut txn, &new).await.expect("insert");
    let found = repo
        .find_by_id(&mut txn, inserted.id())
        .await
        .expect("find");

    assert_eq!(found.id(), inserted.id());
    assert_eq!(found.status(), JobStatus::Queued);
    assert_eq!(found.raw_input(), "test raw input");
}

#[tokio::test]
async fn test_find_by_id_not_found() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let result = repo.find_by_id(&mut txn, JobId::new()).await;

    assert!(matches!(
        result,
        Err(DbError::NotFound { entity: "job", .. })
    ));
}

// ---------------------------------------------------------------------------
// find_by_project_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_project_id_returns_jobs() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "find-by-proj").await;

    let base = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash);

    let first = repo
        .insert(&mut txn, &base.clone().build())
        .await
        .expect("insert first");
    let second = repo
        .insert(&mut txn, &base.build())
        .await
        .expect("insert second");

    let jobs = repo
        .find_by_project_id(&mut txn, project_id)
        .await
        .expect("find_by_project_id");

    assert_eq!(jobs.len(), 2);
    // Both jobs share the same transaction-scoped created_at, so id
    // (DESC) breaks the tie deterministically.
    let first_id = std::cmp::max(first.id(), second.id());
    let second_id = std::cmp::min(first.id(), second.id());
    assert_eq!(jobs[0].id(), first_id);
    assert_eq!(jobs[1].id(), second_id);
}

#[tokio::test]
async fn test_find_by_project_id_empty() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let jobs = repo
        .find_by_project_id(&mut txn, ProjectId::new())
        .await
        .expect("find_by_project_id");

    assert!(jobs.is_empty());
}

// ---------------------------------------------------------------------------
// update_status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_update_status_valid_transition() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "status-valid").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash)
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    let transition = a_job_status_transition()
        .status(JobStatus::Extracting)
        .build();
    let updated = repo
        .update_status_if_live(&mut txn, job.id(), &transition)
        .await
        .expect("update_status")
        .expect("a live job updates");

    assert_eq!(updated.status(), JobStatus::Extracting);
    assert!(updated.outcome().is_none());
    assert!(updated.updated_at() >= job.updated_at());
}

#[tokio::test]
async fn test_update_status_to_completed() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "status-completed").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash)
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    // Truncate to microseconds — Postgres timestamptz has microsecond precision.
    let completed_at = Utc::now().trunc_subsecs(6);
    let transition = a_job_status_transition()
        .status(JobStatus::Completed)
        .outcome(Some(JobOutcome::Success))
        .completed_at(Some(completed_at))
        .build();
    let updated = repo
        .update_status_if_live(&mut txn, job.id(), &transition)
        .await
        .expect("update_status")
        .expect("a live job updates");

    assert_eq!(updated.status(), JobStatus::Completed);
    assert_eq!(updated.outcome(), Some(JobOutcome::Success));
    assert_eq!(updated.completed_at(), Some(completed_at));
    assert!(updated.error_message().is_none());
}

#[tokio::test]
async fn test_update_status_to_failed() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "status-failed").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash)
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    // Truncate to microseconds — Postgres timestamptz has microsecond precision.
    let completed_at = Utc::now().trunc_subsecs(6);
    let transition = a_job_status_transition()
        .status(JobStatus::Failed)
        .outcome(Some(JobOutcome::Failure))
        .error_message(Some("extraction dead-lettered".to_owned()))
        .completed_at(Some(completed_at))
        .build();
    let updated = repo
        .update_status_if_live(&mut txn, job.id(), &transition)
        .await
        .expect("update_status")
        .expect("a live job updates");

    assert_eq!(updated.status(), JobStatus::Failed);
    assert_eq!(updated.outcome(), Some(JobOutcome::Failure));
    assert_eq!(updated.error_message(), Some("extraction dead-lettered"));
    assert_eq!(updated.completed_at(), Some(completed_at));
}

#[tokio::test]
async fn test_update_status_invalid_transition() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "status-invalid").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash)
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    // Completed without outcome — violates CHECK constraint.
    let transition = a_job_status_transition()
        .status(JobStatus::Completed)
        .build();
    let result = repo
        .update_status_if_live(&mut txn, job.id(), &transition)
        .await;

    assert!(
        matches!(result, Err(DbError::QueryFailed { .. })),
        "expected QueryFailed, got {result:?}"
    );
}

#[tokio::test]
async fn test_update_status_on_a_terminal_job_is_a_silent_no_op() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "status-terminal-noop").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash)
        .build();
    let job = repo.insert(&mut txn, &new).await.expect("insert");

    let complete = a_job_status_transition()
        .status(JobStatus::Completed)
        .outcome(Some(JobOutcome::Success))
        .completed_at(Some(Utc::now()))
        .build();
    repo.update_status_if_live(&mut txn, job.id(), &complete)
        .await
        .expect("complete")
        .expect("a live job updates");

    // The late terminal commit: zero rows is success, not an error, and
    // the transaction stays healthy for whatever else it carries.
    let late = a_job_status_transition()
        .status(JobStatus::Failed)
        .outcome(Some(JobOutcome::Failure))
        .error_message(Some("late failure".to_owned()))
        .completed_at(Some(Utc::now()))
        .build();
    let result = repo
        .update_status_if_live(&mut txn, job.id(), &late)
        .await
        .expect("the no-op commits");
    assert!(result.is_none(), "a terminal job is never transitioned");

    let unchanged = repo
        .find_by_id(&mut txn, job.id())
        .await
        .expect("the transaction stays usable after the no-op");
    assert_eq!(unchanged.status(), JobStatus::Completed);
}

#[tokio::test]
async fn test_update_status_not_found() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let transition = a_job_status_transition()
        .status(JobStatus::Extracting)
        .build();
    let result = repo
        .update_status_if_live(&mut txn, JobId::new(), &transition)
        .await;

    assert!(matches!(
        result,
        Err(DbError::NotFound { entity: "job", .. })
    ));
}

// ---------------------------------------------------------------------------
// update_batch_size
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_update_batch_size() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "batch-size").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash)
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    let updated = repo
        .update_batch_size(&mut txn, job.id(), 10, 15)
        .await
        .expect("update_batch_size");

    assert_eq!(updated.batch_size(), Some(10));
    assert_eq!(updated.extraction_original_count(), Some(15));
}

#[tokio::test]
async fn test_update_batch_size_not_found() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let result = repo.update_batch_size(&mut txn, JobId::new(), 10, 15).await;

    assert!(matches!(
        result,
        Err(DbError::NotFound { entity: "job", .. })
    ));
}

// ---------------------------------------------------------------------------
// set_committed_batch_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_set_committed_batch_id() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "committed-batch").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash)
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    let batch_id = RelationBatchId::new();
    let updated = repo
        .set_committed_batch_id(&mut txn, job.id(), batch_id)
        .await
        .expect("set_committed_batch_id")
        .expect("should return Some on first set");

    assert_eq!(updated.committed_batch_id(), Some(batch_id));
}

#[tokio::test]
async fn test_set_committed_batch_id_already_set_returns_none() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "committed-batch-idem").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash)
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    let first = RelationBatchId::new();
    let result = repo
        .set_committed_batch_id(&mut txn, job.id(), first)
        .await
        .expect("set_committed_batch_id");
    assert!(result.is_some(), "first set should succeed");

    let second = RelationBatchId::new();
    let result = repo
        .set_committed_batch_id(&mut txn, job.id(), second)
        .await
        .expect("set_committed_batch_id");
    assert!(result.is_none(), "second set should return None");
}

#[tokio::test]
async fn test_set_committed_batch_id_not_found() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let result = repo
        .set_committed_batch_id(&mut txn, JobId::new(), RelationBatchId::new())
        .await;

    assert!(matches!(
        result,
        Err(DbError::NotFound { entity: "job", .. })
    ));
}

// ---------------------------------------------------------------------------
// fail_stale_dead_lettered_jobs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fail_stale_dead_lettered_jobs_transitions_stuck_job() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "fail-dead-letter").await;

    let job = repo
        .insert(
            &mut txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_system_prompt_version_id(pv_id)
                .extraction_user_prompt_version_id(pv_id)
                .triage_system_prompt_version_id(pv_id)
                .triage_user_prompt_version_id(pv_id)
                .relation_system_prompt_version_id(pv_id)
                .relation_user_prompt_version_id(pv_id)
                .system_fingerprint_hash(fingerprint_hash)
                .build(),
        )
        .await
        .expect("insert job");

    // Transition job to extracting.
    let transition = a_job_status_transition()
        .status(JobStatus::Extracting)
        .build();
    repo.update_status_if_live(&mut txn, job.id(), &transition)
        .await
        .expect("update status");

    // Insert a dead-lettered extraction task.
    PgTaskRepository
        .insert_for_test(
            &mut txn,
            &a_new_task()
                .job_id(job.id())
                .task_type(TaskType::Extraction)
                .build(),
            TaskStatus::DeadLetter,
        )
        .await
        .expect("insert dead-lettered task");

    let failed_ids = repo
        .fail_stale_dead_lettered_jobs(&mut txn)
        .await
        .expect("fail_stale_dead_lettered_jobs");

    assert_eq!(failed_ids.len(), 1);
    assert_eq!(failed_ids[0], job.id());

    let found = repo
        .find_by_id(&mut txn, job.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.status(), JobStatus::Failed);
    assert_eq!(found.outcome(), Some(JobOutcome::Failure));
    assert_eq!(
        found.error_message(),
        Some("task dead-lettered during reclaim"),
    );
    assert!(found.completed_at().is_some());
}

#[tokio::test]
async fn test_fail_stale_dead_lettered_jobs_skips_triage() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "fail-skip-triage").await;

    let job = repo
        .insert(
            &mut txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_system_prompt_version_id(pv_id)
                .extraction_user_prompt_version_id(pv_id)
                .triage_system_prompt_version_id(pv_id)
                .triage_user_prompt_version_id(pv_id)
                .relation_system_prompt_version_id(pv_id)
                .relation_user_prompt_version_id(pv_id)
                .system_fingerprint_hash(fingerprint_hash)
                .build(),
        )
        .await
        .expect("insert job");

    let transition = a_job_status_transition()
        .status(JobStatus::Triaging)
        .build();
    repo.update_status_if_live(&mut txn, job.id(), &transition)
        .await
        .expect("update status");

    // Dead-lettered triage task — should NOT trigger job failure.
    PgTaskRepository
        .insert_for_test(
            &mut txn,
            &a_new_task()
                .job_id(job.id())
                .task_type(TaskType::Triage)
                .batch_index(Some(0))
                .build(),
            TaskStatus::DeadLetter,
        )
        .await
        .expect("insert dead-lettered triage task");

    let failed_ids = repo
        .fail_stale_dead_lettered_jobs(&mut txn)
        .await
        .expect("fail_stale_dead_lettered_jobs");

    assert!(failed_ids.is_empty());

    let found = repo
        .find_by_id(&mut txn, job.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.status(), JobStatus::Triaging);
}

#[tokio::test]
async fn test_fail_stale_dead_lettered_jobs_skips_already_failed() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "fail-already-failed").await;

    let job = repo
        .insert(
            &mut txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_system_prompt_version_id(pv_id)
                .extraction_user_prompt_version_id(pv_id)
                .triage_system_prompt_version_id(pv_id)
                .triage_user_prompt_version_id(pv_id)
                .relation_system_prompt_version_id(pv_id)
                .relation_user_prompt_version_id(pv_id)
                .system_fingerprint_hash(fingerprint_hash)
                .build(),
        )
        .await
        .expect("insert job");

    // Transition job to failed directly.
    let transition = a_job_status_transition()
        .status(JobStatus::Failed)
        .outcome(Some(JobOutcome::Failure))
        .error_message(Some("already failed".into()))
        .completed_at(Some(Utc::now()))
        .build();
    repo.update_status_if_live(&mut txn, job.id(), &transition)
        .await
        .expect("update status");

    PgTaskRepository
        .insert_for_test(
            &mut txn,
            &a_new_task()
                .job_id(job.id())
                .task_type(TaskType::Extraction)
                .build(),
            TaskStatus::DeadLetter,
        )
        .await
        .expect("insert dead-lettered task");

    let failed_ids = repo
        .fail_stale_dead_lettered_jobs(&mut txn)
        .await
        .expect("fail_stale_dead_lettered_jobs");

    assert!(failed_ids.is_empty());
}

#[tokio::test]
async fn test_find_stuck_triaging_skips_a_job_with_a_blocked_sibling() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id, fingerprint_hash) =
        setup_job_prerequisites(&mut txn, "stuck-blocked").await;

    let job = repo
        .insert(
            &mut txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_system_prompt_version_id(pv_id)
                .extraction_user_prompt_version_id(pv_id)
                .triage_system_prompt_version_id(pv_id)
                .triage_user_prompt_version_id(pv_id)
                .relation_system_prompt_version_id(pv_id)
                .relation_user_prompt_version_id(pv_id)
                .system_fingerprint_hash(fingerprint_hash)
                .build(),
        )
        .await
        .expect("insert job");
    let transition = a_job_status_transition()
        .status(JobStatus::Triaging)
        .build();
    repo.update_status_if_live(&mut txn, job.id(), &transition)
        .await
        .expect("update status");

    // One completed triage sibling and one blocked one: a blocked task
    // drives a suspended thread and counts as live, so the healing
    // authority must not converge this job.
    PgTaskRepository
        .insert_for_test(
            &mut txn,
            &a_new_task()
                .job_id(job.id())
                .task_type(TaskType::Triage)
                .batch_index(Some(0))
                .build(),
            TaskStatus::Completed,
        )
        .await
        .expect("insert completed sibling");
    let blocked = PgTaskRepository
        .insert_for_test(
            &mut txn,
            &a_new_task()
                .job_id(job.id())
                .task_type(TaskType::Triage)
                .batch_index(Some(1))
                .build(),
            TaskStatus::Blocked,
        )
        .await
        .expect("insert blocked sibling");

    let stuck = repo.find_stuck_triaging_jobs(&mut txn).await.expect("scan");
    assert!(
        stuck.is_empty(),
        "a blocked sibling holds the healing fan-in back",
    );

    // The blocked sibling reaching terminal releases the job to the scan.
    PgTaskRepository
        .set_status_for_test(&mut txn, blocked.id(), TaskStatus::Completed)
        .await
        .expect("complete the blocked sibling");
    let stuck = repo.find_stuck_triaging_jobs(&mut txn).await.expect("scan");
    assert_eq!(stuck, vec![job.id()]);
}

// ---------------------------------------------------------------------------
// set_extraction_identity
// ---------------------------------------------------------------------------

/// A V1 manual-capture context as the ingest writer stores it.
fn a_v1_source_context() -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "type": "manual_capture",
        "channel": "mcp_http",
    })
}

fn an_extraction_identity() -> InferenceIdentity {
    InferenceIdentity {
        provider: "anthropic".to_owned(),
        model: "claude-opus-4-6".to_owned(),
    }
}

#[tokio::test]
async fn test_a_first_extraction_identity_is_recorded_on_the_context() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (principal_id, project_id, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "extraction-identity-record").await;
    let job = repo
        .insert(
            &mut txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .source_context(a_v1_source_context())
                .extraction_system_prompt_version_id(pv_id)
                .extraction_user_prompt_version_id(pv_id)
                .triage_system_prompt_version_id(pv_id)
                .triage_user_prompt_version_id(pv_id)
                .relation_system_prompt_version_id(pv_id)
                .relation_user_prompt_version_id(pv_id)
                .system_fingerprint_hash(fp_hash)
                .build(),
        )
        .await
        .expect("insert job");

    let outcome = repo
        .set_extraction_identity(&mut txn, job.id(), &an_extraction_identity())
        .await
        .expect("first commit records");

    assert_eq!(outcome, ExtractionCommitOutcome::Recorded);
    let stored = repo.find_by_id(&mut txn, job.id()).await.expect("reload");
    assert_eq!(
        stored.source_context()["extraction"],
        serde_json::json!({ "provider": "anthropic", "model": "claude-opus-4-6" }),
    );
    assert_eq!(stored.source_context()["type"], "manual_capture");
}

#[tokio::test]
async fn test_an_identical_extraction_identity_recommit_is_a_no_op() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (principal_id, project_id, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "extraction-identity-noop").await;
    let job = repo
        .insert(
            &mut txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .source_context(a_v1_source_context())
                .extraction_system_prompt_version_id(pv_id)
                .extraction_user_prompt_version_id(pv_id)
                .triage_system_prompt_version_id(pv_id)
                .triage_user_prompt_version_id(pv_id)
                .relation_system_prompt_version_id(pv_id)
                .relation_user_prompt_version_id(pv_id)
                .system_fingerprint_hash(fp_hash)
                .build(),
        )
        .await
        .expect("insert job");
    repo.set_extraction_identity(&mut txn, job.id(), &an_extraction_identity())
        .await
        .expect("first commit records");

    let outcome = repo
        .set_extraction_identity(&mut txn, job.id(), &an_extraction_identity())
        .await
        .expect("identical recommit is accepted");

    assert_eq!(outcome, ExtractionCommitOutcome::AlreadyRecorded);
}

#[tokio::test]
async fn test_a_differing_extraction_identity_is_refused() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (principal_id, project_id, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "extraction-identity-conflict").await;
    let job = repo
        .insert(
            &mut txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .source_context(a_v1_source_context())
                .extraction_system_prompt_version_id(pv_id)
                .extraction_user_prompt_version_id(pv_id)
                .triage_system_prompt_version_id(pv_id)
                .triage_user_prompt_version_id(pv_id)
                .relation_system_prompt_version_id(pv_id)
                .relation_user_prompt_version_id(pv_id)
                .system_fingerprint_hash(fp_hash)
                .build(),
        )
        .await
        .expect("insert job");
    repo.set_extraction_identity(&mut txn, job.id(), &an_extraction_identity())
        .await
        .expect("first commit records");
    let other = InferenceIdentity {
        provider: "openai".to_owned(),
        model: "gpt-6".to_owned(),
    };

    let err = repo
        .set_extraction_identity(&mut txn, job.id(), &other)
        .await
        .unwrap_err();

    assert!(matches!(err, DbError::SourceContextRejected { .. }));
    let stored = repo.find_by_id(&mut txn, job.id()).await.expect("reload");
    assert_eq!(
        stored.source_context()["extraction"]["provider"],
        "anthropic"
    );
}

#[tokio::test]
async fn test_extraction_identity_on_a_missing_job_is_not_found() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    let err = PgJobRepository
        .set_extraction_identity(&mut txn, JobId::new(), &an_extraction_identity())
        .await
        .unwrap_err();

    assert!(matches!(err, DbError::NotFound { entity: "job", .. }));
}

#[tokio::test]
async fn test_extraction_identity_on_a_flat_context_is_unreadable() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (principal_id, project_id, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "extraction-identity-flat").await;
    let job = repo
        .insert(
            &mut txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .source_context(serde_json::json!({
                    "type": "AgentMediated",
                    "provider": "anthropic",
                    "model": "",
                }))
                .extraction_system_prompt_version_id(pv_id)
                .extraction_user_prompt_version_id(pv_id)
                .triage_system_prompt_version_id(pv_id)
                .triage_user_prompt_version_id(pv_id)
                .relation_system_prompt_version_id(pv_id)
                .relation_user_prompt_version_id(pv_id)
                .system_fingerprint_hash(fp_hash)
                .build(),
        )
        .await
        .expect("insert job");

    let err = repo
        .set_extraction_identity(&mut txn, job.id(), &an_extraction_identity())
        .await
        .unwrap_err();

    assert!(matches!(err, DbError::SourceContextUnreadable { .. }));
}

// ---------------------------------------------------------------------------
// source-context normalisation migration
// ---------------------------------------------------------------------------

/// The shipped migration, re-executed against rows this test writes in the
/// flat shape, so the transform is proven against the exact SQL production
/// ran. The statement is idempotent by its own version guard.
const NORMALISE_SOURCE_CONTEXT_SQL: &str =
    include_str!("../../migrations/20260718185340_normalise_job_source_context_to_v1.sql");

#[tokio::test]
async fn test_the_normalisation_migrates_flat_shapes_and_leaves_the_rest() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (principal_id, project_id, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "normalise-migration").await;
    let insert = |context: serde_json::Value| {
        a_new_job()
            .project_id(project_id)
            .principal_id(principal_id)
            .source_context(context)
            .extraction_system_prompt_version_id(pv_id)
            .extraction_user_prompt_version_id(pv_id)
            .triage_system_prompt_version_id(pv_id)
            .triage_user_prompt_version_id(pv_id)
            .relation_system_prompt_version_id(pv_id)
            .relation_user_prompt_version_id(pv_id)
            .system_fingerprint_hash(fp_hash.clone())
            .build()
    };
    let agent_flat = repo
        .insert(
            &mut txn,
            &insert(serde_json::json!({
                "type": "AgentMediated", "provider": "anthropic", "model": "",
            })),
        )
        .await
        .expect("insert agent-mediated flat job");
    let manual_flat = repo
        .insert(
            &mut txn,
            &insert(serde_json::json!({
                "type": "ManualCapture", "capture_method": "mcp",
            })),
        )
        .await
        .expect("insert manual-capture flat job");
    let unrecognised = repo
        .insert(&mut txn, &insert(serde_json::json!({ "shape": "unknown" })))
        .await
        .expect("insert unrecognised job");
    let already_v1 = repo
        .insert(&mut txn, &insert(a_v1_source_context()))
        .await
        .expect("insert v1 job");

    sqlx::raw_sql(NORMALISE_SOURCE_CONTEXT_SQL)
        .execute(&mut *txn)
        .await
        .expect("re-run the normalisation");

    let agent = repo
        .find_by_id(&mut txn, agent_flat.id())
        .await
        .expect("reload");
    assert_eq!(
        agent.source_context(),
        &serde_json::json!({
            "version": 1,
            "type": "agent_mediated",
            "claimed_actor": { "inference": { "provider": "anthropic" } },
        }),
        "the empty model is dropped, the provider becomes a claim, and no channel is invented",
    );

    let manual = repo
        .find_by_id(&mut txn, manual_flat.id())
        .await
        .expect("reload");
    assert_eq!(
        manual.source_context(),
        &serde_json::json!({ "version": 1, "type": "manual_capture" }),
    );

    let untouched = repo
        .find_by_id(&mut txn, unrecognised.id())
        .await
        .expect("reload");
    assert_eq!(
        untouched.source_context(),
        &serde_json::json!({ "shape": "unknown" })
    );

    let v1 = repo
        .find_by_id(&mut txn, already_v1.id())
        .await
        .expect("reload");
    assert_eq!(v1.source_context(), &a_v1_source_context());
}

// ---------------------------------------------------------------------------
// ingest idempotency arbitration
// ---------------------------------------------------------------------------

/// Builds the ingest-shaped `NewJob` the arbitration tests admit.
fn an_ingest(
    project_id: ProjectId,
    principal_id: PrincipalId,
    pv_id: PromptVersionId,
    fingerprint_hash: &str,
    key: Option<uuid::Uuid>,
    content: &str,
) -> tribal_db::NewJob {
    a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .raw_input(content.to_owned())
        .ingest_idempotency_key(key)
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .system_fingerprint_hash(fingerprint_hash.to_owned())
        .build()
}

#[tokio::test]
async fn test_a_keyless_ingest_creates_a_job_on_every_call() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (principal_id, project_id, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "idem-keyless").await;
    let new = an_ingest(project_id, principal_id, pv_id, &fp_hash, None, "note");

    let first = repo
        .insert_or_resolve_idempotency(&mut txn, &new)
        .await
        .expect("first keyless admit");
    let second = repo
        .insert_or_resolve_idempotency(&mut txn, &new)
        .await
        .expect("second keyless admit");

    let (IngestInsertOutcome::Inserted(a), IngestInsertOutcome::Inserted(b)) = (first, second)
    else {
        panic!("keyless admits must both insert");
    };
    assert_ne!(a.id(), b.id());
}

#[tokio::test]
async fn test_a_retried_key_converges_on_the_original_job() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (principal_id, project_id, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "idem-converge").await;
    let key = uuid::Uuid::new_v4();
    let new = an_ingest(project_id, principal_id, pv_id, &fp_hash, Some(key), "note");

    let first = repo
        .insert_or_resolve_idempotency(&mut txn, &new)
        .await
        .expect("first admit");
    let retry = repo
        .insert_or_resolve_idempotency(&mut txn, &new)
        .await
        .expect("retried admit");

    let IngestInsertOutcome::Inserted(original) = first else {
        panic!("an unclaimed key inserts");
    };
    let IngestInsertOutcome::Existing(resolved) = retry else {
        panic!("a retried key resolves to the committed job");
    };
    assert_eq!(resolved.id(), original.id());
    assert_eq!(original.ingest_idempotency_key(), Some(key));
}

#[tokio::test]
async fn test_a_reused_key_with_different_content_conflicts() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (principal_id, project_id, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "idem-conflict").await;
    let key = uuid::Uuid::new_v4();

    repo.insert_or_resolve_idempotency(
        &mut txn,
        &an_ingest(project_id, principal_id, pv_id, &fp_hash, Some(key), "note"),
    )
    .await
    .expect("first admit");
    let changed = repo
        .insert_or_resolve_idempotency(
            &mut txn,
            &an_ingest(
                project_id,
                principal_id,
                pv_id,
                &fp_hash,
                Some(key),
                "other",
            ),
        )
        .await
        .expect("changed-content admit");

    assert!(matches!(changed, IngestInsertOutcome::Conflict));
}

#[tokio::test]
async fn test_the_same_key_under_another_principal_is_independent() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (principal_a, project_a, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "idem-principal-a").await;
    let (principal_b, project_b, pv_b, fp_b) =
        setup_job_prerequisites(&mut txn, "idem-principal-b").await;
    let key = uuid::Uuid::new_v4();

    let a = repo
        .insert_or_resolve_idempotency(
            &mut txn,
            &an_ingest(project_a, principal_a, pv_id, &fp_hash, Some(key), "note"),
        )
        .await
        .expect("principal a admit");
    let b = repo
        .insert_or_resolve_idempotency(
            &mut txn,
            &an_ingest(project_b, principal_b, pv_b, &fp_b, Some(key), "note"),
        )
        .await
        .expect("principal b admit");

    let (IngestInsertOutcome::Inserted(job_a), IngestInsertOutcome::Inserted(job_b)) = (a, b)
    else {
        panic!("the same key under two principals inserts twice");
    };
    assert_ne!(job_a.id(), job_b.id());
}

#[tokio::test]
async fn test_concurrent_admits_under_one_key_converge() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let repo = PgJobRepository;
    let (principal_id, project_id, pv_id, fp_hash) = {
        let mut conn = pool.acquire().await.expect("acquire");
        setup_job_prerequisites(&mut conn, "idem-race").await
    };
    let key = uuid::Uuid::new_v4();
    let new = an_ingest(project_id, principal_id, pv_id, &fp_hash, Some(key), "note");

    let (left, right) = tokio::join!(
        async {
            let mut conn = pool.acquire().await.expect("acquire left");
            repo.insert_or_resolve_idempotency(&mut conn, &new).await
        },
        async {
            let mut conn = pool.acquire().await.expect("acquire right");
            repo.insert_or_resolve_idempotency(&mut conn, &new).await
        },
    );
    let left = left.expect("left admit");
    let right = right.expect("right admit");

    let ids: Vec<_> = [&left, &right]
        .iter()
        .map(|outcome| match outcome {
            IngestInsertOutcome::Inserted(job) | IngestInsertOutcome::Existing(job) => job.id(),
            IngestInsertOutcome::Conflict => panic!("identical racers never conflict"),
        })
        .collect();
    assert_eq!(ids[0], ids[1], "both racers converge on one job");
}

// ---------------------------------------------------------------------------
// principal-qualified find and recent listing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_a_foreign_job_reads_as_not_found() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (owner, project_id, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "principal-find").await;
    let (stranger, _, _, _) = setup_job_prerequisites(&mut txn, "principal-find-stranger").await;
    let job = repo
        .insert(
            &mut txn,
            &an_ingest(project_id, owner, pv_id, &fp_hash, None, "note"),
        )
        .await
        .expect("insert job");

    let read_back = repo
        .find_by_id_for_principal(&mut txn, job.id(), owner)
        .await
        .expect("owner reads the job");
    assert_eq!(read_back.id(), job.id());

    let foreign = repo
        .find_by_id_for_principal(&mut txn, job.id(), stranger)
        .await
        .unwrap_err();
    let missing = repo
        .find_by_id_for_principal(&mut txn, JobId::new(), owner)
        .await
        .unwrap_err();
    assert!(matches!(foreign, DbError::NotFound { entity: "job", .. }));
    assert!(matches!(missing, DbError::NotFound { entity: "job", .. }));
}

#[tokio::test]
async fn test_the_recent_listing_pages_by_cursor_with_bounded_previews() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (principal_id, project_id, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "recent-listing").await;
    let (other_principal, other_project, other_pv, other_fp) =
        setup_job_prerequisites(&mut txn, "recent-listing-other").await;

    let long_note = format!("padded   {}", "x".repeat(400));
    for content in ["first note", "second   note", &long_note] {
        repo.insert(
            &mut txn,
            &an_ingest(project_id, principal_id, pv_id, &fp_hash, None, content),
        )
        .await
        .expect("insert listed job");
    }
    repo.insert(
        &mut txn,
        &an_ingest(
            other_project,
            other_principal,
            other_pv,
            &other_fp,
            None,
            "foreign note",
        ),
    )
    .await
    .expect("insert foreign job");

    let first_page = repo
        .list_recent_for_principal(
            &mut txn,
            principal_id,
            &RecentIngestionsQuery {
                limit: 2,
                ..RecentIngestionsQuery::default()
            },
        )
        .await
        .expect("first page");
    assert_eq!(first_page.ingestions.len(), 2);
    let cursor = first_page.next_cursor.expect("a further page exists");
    assert!(
        first_page.ingestions[0].created_at >= first_page.ingestions[1].created_at,
        "newest first",
    );

    let second_page = repo
        .list_recent_for_principal(
            &mut txn,
            principal_id,
            &RecentIngestionsQuery {
                before: Some(cursor),
                limit: 2,
                ..RecentIngestionsQuery::default()
            },
        )
        .await
        .expect("second page");
    assert_eq!(
        second_page.ingestions.len(),
        1,
        "only the principal's rows list"
    );
    assert!(second_page.next_cursor.is_none());

    // Same-instant rows order by id, so position is not insertion order:
    // assert the padded row's preview wherever the pages placed it.
    let all_rows: Vec<_> = first_page
        .ingestions
        .iter()
        .chain(second_page.ingestions.iter())
        .collect();
    assert_eq!(all_rows.len(), 3);
    let padded = all_rows
        .iter()
        .find(|row| row.preview.starts_with("padded"))
        .expect("the padded row lists");
    assert!(
        padded.preview.starts_with("padded x"),
        "whitespace collapses: {}",
        padded.preview,
    );
    assert!(padded.preview.chars().count() <= 160, "preview is bounded");

    let decoded = RecentIngestionCursor::decode(&cursor.encode()).expect("cursor round-trips");
    assert_eq!(decoded, cursor);
}

#[tokio::test]
async fn test_the_recent_listing_filters_by_status() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgJobRepository;
    let (principal_id, project_id, pv_id, fp_hash) =
        setup_job_prerequisites(&mut txn, "recent-status-filter").await;
    let queued = repo
        .insert(
            &mut txn,
            &an_ingest(project_id, principal_id, pv_id, &fp_hash, None, "note"),
        )
        .await
        .expect("insert queued job");
    let completed = repo
        .insert(
            &mut txn,
            &an_ingest(project_id, principal_id, pv_id, &fp_hash, None, "done"),
        )
        .await
        .expect("insert completing job");
    repo.update_status_if_live(
        &mut txn,
        completed.id(),
        &a_job_status_transition()
            .status(JobStatus::Completed)
            .outcome(Some(JobOutcome::Success))
            .completed_at(Some(Utc::now()))
            .build(),
    )
    .await
    .expect("complete job");

    let page = repo
        .list_recent_for_principal(
            &mut txn,
            principal_id,
            &RecentIngestionsQuery {
                statuses: vec![JobStatus::Completed],
                limit: 10,
                ..RecentIngestionsQuery::default()
            },
        )
        .await
        .expect("filtered page");

    assert_eq!(page.ingestions.len(), 1);
    assert_eq!(page.ingestions[0].job_id, completed.id());
    assert_ne!(page.ingestions[0].job_id, queued.id());
}
