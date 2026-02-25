use chrono::{SubsecRound, Utc};
use tribal_db::{
    DbError, JobRepository, PgJobRepository, PgPrincipalRepository, PgProjectRepository,
    PrincipalRepository, ProjectRepository,
};
use tribal_domain::{
    EpisodeId, JobId, JobOutcome, JobStatus, PrincipalId, ProjectId, PromptVersionId,
    RelationBatchId,
};
use tribal_test_utils::{
    a_job_status_transition, a_new_job, a_new_principal, a_new_project, test_context,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal, project, and a prompt_version row, returning
/// the IDs needed to create a job.
///
/// A single prompt_version ID is reused for all three job FK columns.
async fn setup_job_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (PrincipalId, ProjectId, PromptVersionId) {
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
        .insert(
            txn,
            &a_new_project()
                .git_remote(format!("git@github.com:test/job-{suffix}.git"))
                .build(),
        )
        .await
        .expect("insert project");

    let content_hash = format!("{:064x}", uuid::Uuid::new_v4().as_u128());
    let prompt_version_id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO prompt_versions (stage, content_hash, content) \
         VALUES ('extraction', $1, 'test prompt') RETURNING id",
    )
    .bind(&content_hash)
    .fetch_one(&mut *txn)
    .await
    .expect("insert prompt_version");

    (
        principal.id(),
        project.id(),
        PromptVersionId::from(prompt_version_id),
    )
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_returns_populated_job() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) = setup_job_prerequisites(&mut txn, "insert").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_prompt_version_id(pv_id)
        .triage_prompt_version_id(pv_id)
        .relation_prompt_version_id(pv_id)
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
    assert_eq!(job.extraction_prompt_version_id(), pv_id);
    assert_eq!(job.triage_prompt_version_id(), pv_id);
    assert_eq!(job.relation_prompt_version_id(), pv_id);
    assert!(job.batch_size().is_none());
    assert!(job.committed_batch_id().is_none());
    assert!(job.error_message().is_none());
    assert!(job.completed_at().is_none());
}

#[tokio::test]
async fn test_insert_with_optional_fields_round_trips() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) =
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
        .extraction_prompt_version_id(pv_id)
        .triage_prompt_version_id(pv_id)
        .relation_prompt_version_id(pv_id)
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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) = setup_job_prerequisites(&mut txn, "find-by-id").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_prompt_version_id(pv_id)
        .triage_prompt_version_id(pv_id)
        .relation_prompt_version_id(pv_id)
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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) = setup_job_prerequisites(&mut txn, "find-by-proj").await;

    let base = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_prompt_version_id(pv_id)
        .triage_prompt_version_id(pv_id)
        .relation_prompt_version_id(pv_id);

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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) = setup_job_prerequisites(&mut txn, "status-valid").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_prompt_version_id(pv_id)
        .triage_prompt_version_id(pv_id)
        .relation_prompt_version_id(pv_id)
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    let transition = a_job_status_transition()
        .status(JobStatus::Extracting)
        .build();
    let updated = repo
        .update_status(&mut txn, job.id(), &transition)
        .await
        .expect("update_status");

    assert_eq!(updated.status(), JobStatus::Extracting);
    assert!(updated.outcome().is_none());
    assert!(updated.updated_at() >= job.updated_at());
}

#[tokio::test]
async fn test_update_status_to_completed() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) =
        setup_job_prerequisites(&mut txn, "status-completed").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_prompt_version_id(pv_id)
        .triage_prompt_version_id(pv_id)
        .relation_prompt_version_id(pv_id)
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
        .update_status(&mut txn, job.id(), &transition)
        .await
        .expect("update_status");

    assert_eq!(updated.status(), JobStatus::Completed);
    assert_eq!(updated.outcome(), Some(JobOutcome::Success));
    assert_eq!(updated.completed_at(), Some(completed_at));
    assert!(updated.error_message().is_none());
}

#[tokio::test]
async fn test_update_status_to_failed() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) =
        setup_job_prerequisites(&mut txn, "status-failed").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_prompt_version_id(pv_id)
        .triage_prompt_version_id(pv_id)
        .relation_prompt_version_id(pv_id)
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
        .update_status(&mut txn, job.id(), &transition)
        .await
        .expect("update_status");

    assert_eq!(updated.status(), JobStatus::Failed);
    assert_eq!(updated.outcome(), Some(JobOutcome::Failure));
    assert_eq!(updated.error_message(), Some("extraction dead-lettered"));
    assert_eq!(updated.completed_at(), Some(completed_at));
}

#[tokio::test]
async fn test_update_status_invalid_transition() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) =
        setup_job_prerequisites(&mut txn, "status-invalid").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_prompt_version_id(pv_id)
        .triage_prompt_version_id(pv_id)
        .relation_prompt_version_id(pv_id)
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    // Completed without outcome — violates CHECK constraint.
    let transition = a_job_status_transition()
        .status(JobStatus::Completed)
        .build();
    let result = repo.update_status(&mut txn, job.id(), &transition).await;

    assert!(
        matches!(result, Err(DbError::QueryFailed { .. })),
        "expected QueryFailed, got {result:?}"
    );
}

#[tokio::test]
async fn test_update_status_not_found() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let transition = a_job_status_transition()
        .status(JobStatus::Extracting)
        .build();
    let result = repo
        .update_status(&mut txn, JobId::new(), &transition)
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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) = setup_job_prerequisites(&mut txn, "batch-size").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_prompt_version_id(pv_id)
        .triage_prompt_version_id(pv_id)
        .relation_prompt_version_id(pv_id)
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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) =
        setup_job_prerequisites(&mut txn, "committed-batch").await;

    let new = a_new_job()
        .project_id(project_id)
        .principal_id(principal_id)
        .extraction_prompt_version_id(pv_id)
        .triage_prompt_version_id(pv_id)
        .relation_prompt_version_id(pv_id)
        .build();

    let job = repo.insert(&mut txn, &new).await.expect("insert");

    let batch_id = RelationBatchId::new();
    let updated = repo
        .set_committed_batch_id(&mut txn, job.id(), batch_id)
        .await
        .expect("set_committed_batch_id");

    assert_eq!(updated.committed_batch_id(), Some(batch_id));
}

#[tokio::test]
async fn test_set_committed_batch_id_not_found() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
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

/// Inserts a task row with the given status and task type for a job.
async fn insert_task_with_status(
    txn: &mut sqlx::PgConnection,
    job_id: JobId,
    task_type: &str,
    status: &str,
) {
    sqlx::query("INSERT INTO tasks (job_id, task_type, status) VALUES ($1, $2, $3)")
        .bind(job_id.inner())
        .bind(task_type)
        .bind(status)
        .execute(&mut *txn)
        .await
        .expect("insert task");
}

#[tokio::test]
async fn test_fail_stale_dead_lettered_jobs_transitions_stuck_job() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) =
        setup_job_prerequisites(&mut txn, "fail-dead-letter").await;

    let job = repo
        .insert(
            &mut txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_prompt_version_id(pv_id)
                .triage_prompt_version_id(pv_id)
                .relation_prompt_version_id(pv_id)
                .build(),
        )
        .await
        .expect("insert job");

    // Transition job to extracting.
    let transition = a_job_status_transition()
        .status(JobStatus::Extracting)
        .build();
    repo.update_status(&mut txn, job.id(), &transition)
        .await
        .expect("update status");

    // Insert a dead-lettered extraction task.
    insert_task_with_status(&mut txn, job.id(), "extraction", "dead_letter").await;

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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) =
        setup_job_prerequisites(&mut txn, "fail-skip-triage").await;

    let job = repo
        .insert(
            &mut txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_prompt_version_id(pv_id)
                .triage_prompt_version_id(pv_id)
                .relation_prompt_version_id(pv_id)
                .build(),
        )
        .await
        .expect("insert job");

    let transition = a_job_status_transition()
        .status(JobStatus::Triaging)
        .build();
    repo.update_status(&mut txn, job.id(), &transition)
        .await
        .expect("update status");

    // Dead-lettered triage task — should NOT trigger job failure.
    insert_task_with_status(&mut txn, job.id(), "triage", "dead_letter").await;

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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgJobRepository;

    let (principal_id, project_id, pv_id) =
        setup_job_prerequisites(&mut txn, "fail-already-failed").await;

    let job = repo
        .insert(
            &mut txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_prompt_version_id(pv_id)
                .triage_prompt_version_id(pv_id)
                .relation_prompt_version_id(pv_id)
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
    repo.update_status(&mut txn, job.id(), &transition)
        .await
        .expect("update status");

    insert_task_with_status(&mut txn, job.id(), "extraction", "dead_letter").await;

    let failed_ids = repo
        .fail_stale_dead_lettered_jobs(&mut txn)
        .await
        .expect("fail_stale_dead_lettered_jobs");

    assert!(failed_ids.is_empty());
}
