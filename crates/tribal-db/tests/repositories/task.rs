use chrono::SubsecRound;
use tribal_db::{
    DbError, JobRepository, PgJobRepository, PgPrincipalRepository, PgProjectRepository,
    PgTaskRepository, PrincipalRepository, ProjectRepository, TaskRepository,
};
use tribal_domain::{GitRemote, JobId, Task, TaskErrorKind, TaskId, TaskStatus, TaskType};
use tribal_test_utils::{
    TestDb, a_new_job, a_new_principal, a_new_project, a_new_prompt_version,
    a_new_system_fingerprint, a_new_task, backdate_task_heartbeat, count_tasks_by_status,
    insert_prompt_version, set_retry_count, shift_timestamp_by_id, upsert_system_fingerprint,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal, project, `prompt_version`, and a job, returning
/// the job ID for creating tasks.
async fn setup_task_prerequisites(txn: &mut sqlx::PgConnection, suffix: &str) -> JobId {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:task-test-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal");

    let project = PgProjectRepository
        .insert_git(
            txn,
            &a_new_project()
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    &format!("test/task-{suffix}"),
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project");

    let pv_id = insert_prompt_version(txn, &a_new_prompt_version().build()).await;

    let fingerprint_hash =
        upsert_system_fingerprint(txn, &a_new_system_fingerprint().build()).await;

    let job = PgJobRepository
        .insert(
            txn,
            &a_new_job()
                .project_id(project.id())
                .principal_id(principal.id())
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

    job.id()
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_extraction_task() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "insert-extraction").await;

    let new = a_new_task()
        .job_id(job_id)
        .task_type(TaskType::Extraction)
        .build();

    let task = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(task.id().to_string().starts_with("task_"));
    assert_eq!(task.job_id(), job_id);
    assert_eq!(task.task_type(), TaskType::Extraction);
    assert_eq!(task.status(), TaskStatus::Queued);
    assert!(task.batch_index().is_none());
    assert!(task.claim_token().is_none());
    assert!(task.claimed_by().is_none());
    assert!(task.claimed_at().is_none());
    assert!(task.heartbeat_at().is_none());
    assert_eq!(task.retry_count(), 0);
    assert!(task.error_kind().is_none());
    assert!(task.error_message().is_none());
}

#[tokio::test]
async fn test_insert_triage_task_with_batch_index() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "insert-triage").await;

    let new = a_new_task()
        .job_id(job_id)
        .task_type(TaskType::Triage)
        .batch_index(Some(0))
        .build();

    let task = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(task.task_type(), TaskType::Triage);
    assert_eq!(task.batch_index(), Some(0));
}

#[tokio::test]
async fn test_insert_duplicate_singleton_unique_violation() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "dup-singleton").await;

    let new = a_new_task()
        .job_id(job_id)
        .task_type(TaskType::Extraction)
        .build();

    repo.insert(&mut txn, &new).await.expect("first insert");

    let duplicate = a_new_task()
        .job_id(job_id)
        .task_type(TaskType::Extraction)
        .build();

    let result = repo.insert(&mut txn, &duplicate).await;
    assert!(
        matches!(result, Err(DbError::UniqueViolation { .. })),
        "expected UniqueViolation, got {result:?}"
    );
}

#[tokio::test]
async fn test_insert_duplicate_triage_unique_violation() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "dup-triage").await;

    let new = a_new_task()
        .job_id(job_id)
        .task_type(TaskType::Triage)
        .batch_index(Some(0))
        .build();

    repo.insert(&mut txn, &new).await.expect("first insert");

    let duplicate = a_new_task()
        .job_id(job_id)
        .task_type(TaskType::Triage)
        .batch_index(Some(0))
        .build();

    let result = repo.insert(&mut txn, &duplicate).await;
    assert!(
        matches!(result, Err(DbError::UniqueViolation { .. })),
        "expected UniqueViolation, got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// find_by_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_id_returns_task() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "find-by-id").await;

    let new = a_new_task().job_id(job_id).build();
    let inserted = repo.insert(&mut txn, &new).await.expect("insert");

    let found = repo
        .find_by_id(&mut txn, inserted.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.id(), inserted.id());
    assert_eq!(found.task_type(), TaskType::Extraction);
}

#[tokio::test]
async fn test_find_by_id_not_found() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let result = repo.find_by_id(&mut txn, TaskId::new()).await;

    assert!(matches!(
        result,
        Err(DbError::NotFound { entity: "task", .. })
    ));
}

// ---------------------------------------------------------------------------
// find_by_job_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_job_id() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "find-by-job").await;

    repo.insert(
        &mut txn,
        &a_new_task()
            .job_id(job_id)
            .task_type(TaskType::Extraction)
            .build(),
    )
    .await
    .expect("insert extraction");

    repo.insert(
        &mut txn,
        &a_new_task()
            .job_id(job_id)
            .task_type(TaskType::Triage)
            .batch_index(Some(0))
            .build(),
    )
    .await
    .expect("insert triage 0");

    repo.insert(
        &mut txn,
        &a_new_task()
            .job_id(job_id)
            .task_type(TaskType::Triage)
            .batch_index(Some(1))
            .build(),
    )
    .await
    .expect("insert triage 1");

    let tasks = repo
        .find_by_job_id(&mut txn, job_id)
        .await
        .expect("find_by_job_id");

    assert_eq!(tasks.len(), 3);
    // All tasks share a transaction-scoped created_at, so id (ASC)
    // breaks the tie. Verify monotonic id ordering.
    assert!(tasks[0].id() < tasks[1].id());
    assert!(tasks[1].id() < tasks[2].id());
    // All expected task types are present.
    let types: Vec<_> = tasks.iter().map(Task::task_type).collect();
    assert!(types.contains(&TaskType::Extraction));
    assert!(types.contains(&TaskType::Triage));
}

#[tokio::test]
async fn test_find_by_job_id_empty() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let tasks = repo
        .find_by_job_id(&mut txn, JobId::new())
        .await
        .expect("find_by_job_id");

    assert!(tasks.is_empty());
}

// ---------------------------------------------------------------------------
// claim
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_claim_returns_claimed_tasks() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "claim").await;

    repo.insert(
        &mut txn,
        &a_new_task()
            .job_id(job_id)
            .task_type(TaskType::Extraction)
            .build(),
    )
    .await
    .expect("insert extraction");

    repo.insert(
        &mut txn,
        &a_new_task()
            .job_id(job_id)
            .task_type(TaskType::Triage)
            .batch_index(Some(0))
            .build(),
    )
    .await
    .expect("insert triage");

    let claimed = repo.claim(&mut txn, 2, "worker-1").await.expect("claim");

    assert_eq!(claimed.len(), 2);
    for task in &claimed {
        assert_eq!(task.status(), TaskStatus::Claimed);
        assert!(task.claim_token().is_some());
        assert_eq!(task.claimed_by(), Some("worker-1"));
        assert!(task.claimed_at().is_some());
        assert!(task.heartbeat_at().is_some());
    }
}

#[tokio::test]
async fn test_claim_respects_limit() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "claim-limit").await;

    repo.insert(
        &mut txn,
        &a_new_task()
            .job_id(job_id)
            .task_type(TaskType::Extraction)
            .build(),
    )
    .await
    .expect("insert extraction");

    repo.insert(
        &mut txn,
        &a_new_task()
            .job_id(job_id)
            .task_type(TaskType::Triage)
            .batch_index(Some(0))
            .build(),
    )
    .await
    .expect("insert triage");

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");

    assert_eq!(claimed.len(), 1);
}

#[tokio::test]
async fn test_claim_skips_future_available_at() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "claim-future").await;

    let task = repo
        .insert(
            &mut txn,
            &a_new_task()
                .job_id(job_id)
                .task_type(TaskType::Extraction)
                .build(),
        )
        .await
        .expect("insert");

    // Push available_at into the future.
    shift_timestamp_by_id(
        &mut txn,
        "tasks",
        "available_at",
        *task.id().inner(),
        chrono::Duration::hours(1),
    )
    .await;

    let claimed = repo.claim(&mut txn, 10, "worker-1").await.expect("claim");

    assert!(claimed.is_empty());
}

#[tokio::test]
async fn test_claim_returns_empty_when_none_claimable() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let claimed = repo.claim(&mut txn, 10, "worker-1").await.expect("claim");

    assert!(claimed.is_empty());
}

// ---------------------------------------------------------------------------
// heartbeat
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_heartbeat_updates_timestamp() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "heartbeat").await;

    repo.insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");
    let task = &claimed[0];

    let original_heartbeat_at = task.heartbeat_at().expect("claimed task has heartbeat_at");

    let rows = repo
        .heartbeat(&mut txn, task.id(), task.claim_token().unwrap())
        .await
        .expect("heartbeat");

    assert_eq!(rows, 1);

    let found = repo
        .find_by_id(&mut txn, task.id())
        .await
        .expect("find_by_id");

    assert!(found.heartbeat_at().expect("heartbeat_at set") >= original_heartbeat_at);
}

#[tokio::test]
async fn test_heartbeat_wrong_token_returns_zero() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "heartbeat-wrong").await;

    repo.insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");
    let task = &claimed[0];

    let rows = repo
        .heartbeat(&mut txn, task.id(), uuid::Uuid::new_v4())
        .await
        .expect("heartbeat");

    assert_eq!(rows, 0);
}

// ---------------------------------------------------------------------------
// reset_retry_count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_reset_retry_count_is_claim_guarded() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "progress-reset").await;
    let inserted = repo
        .insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");
    tribal_test_utils::set_retry_count(&mut txn, inserted.id(), 3).await;

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");
    let task = &claimed[0];
    assert_eq!(task.retry_count(), 3);

    let refused = repo
        .reset_retry_count(&mut txn, task.id(), uuid::Uuid::new_v4())
        .await
        .expect("reset with wrong token");
    assert_eq!(refused, 0, "a stale token must not reset progress");

    let rows = repo
        .reset_retry_count(&mut txn, task.id(), task.claim_token().unwrap())
        .await
        .expect("reset");
    assert_eq!(rows, 1);

    let found = repo
        .find_by_id(&mut txn, task.id())
        .await
        .expect("find_by_id");
    assert_eq!(found.retry_count(), 0);
    assert_eq!(
        found.status(),
        TaskStatus::Claimed,
        "the reset touches progress accounting only",
    );
}

// ---------------------------------------------------------------------------
// complete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_complete_marks_completed() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "complete").await;

    repo.insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");
    let task = &claimed[0];

    let rows = repo
        .complete(&mut txn, task.id(), task.claim_token().unwrap())
        .await
        .expect("complete");

    assert_eq!(rows, 1);

    let found = repo
        .find_by_id(&mut txn, task.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.status(), TaskStatus::Completed);
    assert!(found.claim_token().is_none());
    // Audit trail retained.
    assert_eq!(found.claimed_by(), Some("worker-1"));
    assert!(found.claimed_at().is_some());
    assert!(found.heartbeat_at().is_some());
}

#[tokio::test]
async fn test_complete_wrong_token_returns_zero() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "complete-wrong").await;

    repo.insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");
    let task = &claimed[0];

    let rows = repo
        .complete(&mut txn, task.id(), uuid::Uuid::new_v4())
        .await
        .expect("complete");

    assert_eq!(rows, 0);
}

// ---------------------------------------------------------------------------
// fail
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fail_requeues_within_budget() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "fail-requeue").await;

    repo.insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");
    let task = &claimed[0];

    // Truncate to microseconds — Postgres timestamptz has microsecond precision.
    let backoff_at = (chrono::Utc::now() + chrono::Duration::seconds(4)).trunc_subsecs(6);
    let rows = repo
        .fail(
            &mut txn,
            task.id(),
            task.claim_token().unwrap(),
            3,
            backoff_at,
            TaskErrorKind::ProviderError,
            "provider returned 503",
        )
        .await
        .expect("fail");

    assert_eq!(rows, 1);

    let found = repo
        .find_by_id(&mut txn, task.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.status(), TaskStatus::Queued);
    assert_eq!(found.retry_count(), 1);
    assert!(found.claim_token().is_none());
    assert!(found.claimed_by().is_none());
    assert!(found.claimed_at().is_none());
    assert!(found.heartbeat_at().is_none());
    assert_eq!(found.error_kind(), Some(TaskErrorKind::ProviderError));
    assert_eq!(found.error_message(), Some("provider returned 503"));
    // available_at should be the caller-provided value.
    assert_eq!(found.available_at(), backoff_at);
}

#[tokio::test]
async fn test_fail_dead_letters_when_exceeding_max_retries() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "fail-dead-letter").await;

    let inserted = repo
        .insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");

    // Set retry_count to max_retries so next fail triggers dead-letter.
    set_retry_count(&mut txn, inserted.id(), 3).await;

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");
    let task = &claimed[0];

    let original_available_at = task.available_at();
    let backoff_at = chrono::Utc::now() + chrono::Duration::seconds(60);

    let rows = repo
        .fail(
            &mut txn,
            task.id(),
            task.claim_token().unwrap(),
            3, // max_retries = 3, retry_count will become 4 > 3
            backoff_at,
            TaskErrorKind::ProviderError,
            "final failure",
        )
        .await
        .expect("fail");

    assert_eq!(rows, 1);

    let found = repo
        .find_by_id(&mut txn, task.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.status(), TaskStatus::DeadLetter);
    assert_eq!(found.retry_count(), 4);
    assert!(found.claim_token().is_none());
    assert!(found.claimed_by().is_none());
    assert!(found.claimed_at().is_none());
    assert!(found.heartbeat_at().is_none());
    assert_eq!(found.error_kind(), Some(TaskErrorKind::ProviderError));
    assert_eq!(found.error_message(), Some("final failure"));
    // available_at preserved (not set to caller-provided backoff_at).
    assert_eq!(found.available_at(), original_available_at);
}

#[tokio::test]
async fn test_fail_wrong_token_returns_zero() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "fail-wrong-token").await;

    repo.insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");
    let task = &claimed[0];

    let rows = repo
        .fail(
            &mut txn,
            task.id(),
            uuid::Uuid::new_v4(),
            3,
            chrono::Utc::now(),
            TaskErrorKind::ProviderError,
            "should not apply",
        )
        .await
        .expect("fail");

    assert_eq!(rows, 0);
}

// ---------------------------------------------------------------------------
// reclaim_stale
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_reclaim_stale_heartbeat_expired() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "reclaim-heartbeat").await;

    repo.insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");
    let task = &claimed[0];

    backdate_task_heartbeat(&mut txn, task.id(), std::time::Duration::from_mins(2)).await;

    let result = repo
        .reclaim_stale(
            &mut txn,
            30,
            3,
            10,
            TaskErrorKind::HeartbeatExpired,
            "heartbeat lapsed",
            None,
        )
        .await
        .expect("reclaim_stale");

    assert_eq!(result.requeued, 1);
    assert_eq!(result.dead_lettered, 0);

    let found = repo
        .find_by_id(&mut txn, task.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.status(), TaskStatus::Queued);
    assert_eq!(found.retry_count(), 1);
    assert_eq!(found.error_kind(), Some(TaskErrorKind::HeartbeatExpired));
    assert_eq!(found.error_message(), Some("heartbeat lapsed"));
    assert!(found.claim_token().is_none());
    assert!(found.claimed_by().is_none());
    assert!(found.claimed_at().is_none());
    assert!(found.heartbeat_at().is_none());
    // Exponential backoff: power(2, 0) = 1 second. Both available_at and
    // updated_at use the same Postgres now(), so the difference is the
    // exact backoff duration with no wall-clock race.
    let backoff = found.available_at() - found.updated_at();
    assert_eq!(backoff, chrono::Duration::seconds(1));
}

#[tokio::test]
async fn test_reclaim_stale_startup_reclaim() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "reclaim-startup").await;

    repo.insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");
    let task = &claimed[0];

    backdate_task_heartbeat(&mut txn, task.id(), std::time::Duration::from_mins(2)).await;

    let result = repo
        .reclaim_stale(
            &mut txn,
            30,
            3,
            10,
            TaskErrorKind::StartupReclaim,
            "reclaimed during startup",
            Some(1),
        )
        .await
        .expect("reclaim_stale");

    assert_eq!(result.requeued, 1);
    assert_eq!(result.dead_lettered, 0);

    let found = repo
        .find_by_id(&mut txn, task.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.status(), TaskStatus::Queued);
    assert_eq!(found.retry_count(), 1);
    assert_eq!(found.error_kind(), Some(TaskErrorKind::StartupReclaim));
    assert_eq!(found.error_message(), Some("reclaimed during startup"));
    assert!(found.claim_token().is_none());
    assert!(found.claimed_by().is_none());
    assert!(found.claimed_at().is_none());
    assert!(found.heartbeat_at().is_none());
    // Startup reclaim: flat 1-second backoff. Both available_at and
    // updated_at use the same Postgres now(), so the difference is the
    // exact backoff duration with no wall-clock race.
    let backoff = found.available_at() - found.updated_at();
    assert_eq!(backoff, chrono::Duration::seconds(1));
}

#[tokio::test]
async fn test_reclaim_stale_dead_letters_exhausted_budget() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "reclaim-dead-letter").await;

    let inserted = repo
        .insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");

    // Set retry_count to max so reclaim triggers dead-letter.
    set_retry_count(&mut txn, inserted.id(), 3).await;

    let claimed = repo.claim(&mut txn, 1, "worker-1").await.expect("claim");
    let task = &claimed[0];

    // Push heartbeat_at into the past.
    backdate_task_heartbeat(&mut txn, task.id(), std::time::Duration::from_mins(2)).await;

    let pre_reclaim_available_at = repo
        .find_by_id(&mut txn, task.id())
        .await
        .expect("find_by_id")
        .available_at();

    let result = repo
        .reclaim_stale(
            &mut txn,
            30,
            3,
            10,
            TaskErrorKind::HeartbeatExpired,
            "heartbeat lapsed",
            None,
        )
        .await
        .expect("reclaim_stale");

    assert_eq!(result.requeued, 0);
    assert_eq!(result.dead_lettered, 1);

    let found = repo
        .find_by_id(&mut txn, task.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.status(), TaskStatus::DeadLetter);
    assert_eq!(found.retry_count(), 4);
    assert_eq!(found.error_kind(), Some(TaskErrorKind::HeartbeatExpired));
    assert_eq!(found.error_message(), Some("heartbeat lapsed"));
    assert!(found.claim_token().is_none());
    assert!(found.claimed_by().is_none());
    assert!(found.claimed_at().is_none());
    assert!(found.heartbeat_at().is_none());
    // Dead-letter preserves original available_at.
    assert_eq!(found.available_at(), pre_reclaim_available_at);
}

#[tokio::test]
async fn test_reclaim_stale_respects_limit() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    // Each task needs its own job due to the unique constraint on
    // (job_id, task_type).
    let mut task_ids = Vec::new();
    for i in 0..5 {
        let job_id = setup_task_prerequisites(&mut txn, &format!("reclaim-limit-{i}")).await;
        let task = repo
            .insert(&mut txn, &a_new_task().job_id(job_id).build())
            .await
            .expect("insert");
        task_ids.push(task.id());
    }

    // Claim all 5 and backdate their heartbeats.
    let claimed = repo.claim(&mut txn, 5, "worker-1").await.expect("claim");
    assert_eq!(claimed.len(), 5);

    for id in &task_ids {
        backdate_task_heartbeat(&mut txn, *id, std::time::Duration::from_mins(2)).await;
    }

    // Reclaim with limit = 3 — only 3 of 5 should be reclaimed.
    let result = repo
        .reclaim_stale(
            &mut txn,
            30,
            3,
            3,
            TaskErrorKind::HeartbeatExpired,
            "heartbeat lapsed",
            None,
        )
        .await
        .expect("reclaim_stale");

    assert_eq!(result.requeued, 3, "only 3 tasks should be reclaimed");
    assert_eq!(result.dead_lettered, 0);

    // Count remaining claimed tasks across all jobs.
    let still_claimed = count_tasks_by_status(&mut txn, "claimed").await;

    assert_eq!(still_claimed, 2, "2 tasks should still be claimed");

    // Reclaim again with a higher limit — should pick up the remaining 2.
    let result = repo
        .reclaim_stale(
            &mut txn,
            30,
            3,
            10,
            TaskErrorKind::HeartbeatExpired,
            "heartbeat lapsed",
            None,
        )
        .await
        .expect("reclaim_stale");

    assert_eq!(result.requeued, 2, "remaining 2 tasks should be reclaimed");
    assert_eq!(result.dead_lettered, 0);
}

// ---------------------------------------------------------------------------
// count_by_status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_count_by_status_empty_table() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let counts = repo
        .count_by_status(&mut txn)
        .await
        .expect("count_by_status");
    assert!(counts.is_empty(), "empty table should return empty vec");
}

#[tokio::test]
async fn test_count_by_status_groups_by_type_and_status() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgTaskRepository;

    let job_id = setup_task_prerequisites(&mut txn, "count-by-status").await;

    // Insert 2 extraction tasks (both start as queued).
    repo.insert(
        &mut txn,
        &a_new_task()
            .job_id(job_id)
            .task_type(TaskType::Extraction)
            .build(),
    )
    .await
    .expect("insert extraction 1");
    repo.insert(
        &mut txn,
        &a_new_task()
            .job_id(job_id)
            .task_type(TaskType::Relation)
            .build(),
    )
    .await
    .expect("insert relation");

    // Insert 2 triage tasks.
    repo.insert(
        &mut txn,
        &a_new_task()
            .job_id(job_id)
            .task_type(TaskType::Triage)
            .batch_index(Some(0))
            .build(),
    )
    .await
    .expect("insert triage 0");
    repo.insert(
        &mut txn,
        &a_new_task()
            .job_id(job_id)
            .task_type(TaskType::Triage)
            .batch_index(Some(1))
            .build(),
    )
    .await
    .expect("insert triage 1");

    // Claim 1 triage task to make it Claimed.
    let claimed = repo
        .claim(&mut txn, 1, "count-worker")
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);

    let counts = repo
        .count_by_status(&mut txn)
        .await
        .expect("count_by_status");

    assert!(
        !counts.is_empty(),
        "should have at least one row after seeding",
    );

    // Build a lookup for easy assertion.
    let find = |task_type: TaskType, status: TaskStatus| -> i64 {
        counts
            .iter()
            .find(|c| c.task_type == task_type && c.status == status)
            .map_or(0, |c| c.count)
    };

    // The claim picked the earliest available task. We seeded
    // extraction, relation, and 2 triage tasks — all start queued.
    // After claiming 1, we have 3 queued and 1 claimed.
    let total_queued = find(TaskType::Extraction, TaskStatus::Queued)
        + find(TaskType::Relation, TaskStatus::Queued)
        + find(TaskType::Triage, TaskStatus::Queued);
    let total_claimed = find(TaskType::Extraction, TaskStatus::Claimed)
        + find(TaskType::Relation, TaskStatus::Claimed)
        + find(TaskType::Triage, TaskStatus::Claimed);

    assert_eq!(total_queued, 3, "3 tasks should remain queued");
    assert_eq!(total_claimed, 1, "1 task should be claimed");
}

// ---------------------------------------------------------------------------
// Blocked status (the thread runtime's suspension state)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_block_clears_the_lease_and_makes_the_row_unclaimable() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let job_id = setup_task_prerequisites(&mut txn, "block").await;

    PgTaskRepository
        .insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");
    let claimed = PgTaskRepository
        .claim(&mut txn, 1, "worker-block")
        .await
        .expect("claim");
    let task = &claimed[0];
    let token = task.claim_token().expect("claimed rows carry tokens");

    let blocked = PgTaskRepository
        .block(&mut txn, task.id(), token)
        .await
        .expect("block");
    assert_eq!(blocked, 1);

    let read = PgTaskRepository
        .find_by_id(&mut txn, task.id())
        .await
        .expect("find");
    assert_eq!(read.status(), TaskStatus::Blocked);
    assert!(read.claim_token().is_none(), "blocked rows hold no lease");
    assert!(read.heartbeat_at().is_none());

    let reclaimed = PgTaskRepository
        .claim(&mut txn, 10, "worker-other")
        .await
        .expect("claim sweep");
    assert!(
        reclaimed.iter().all(|t| t.id() != task.id()),
        "the claim predicate never selects a blocked row",
    );
}

#[tokio::test]
async fn test_requeue_from_blocked_is_immediately_available() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let job_id = setup_task_prerequisites(&mut txn, "unblock").await;

    PgTaskRepository
        .insert(&mut txn, &a_new_task().job_id(job_id).build())
        .await
        .expect("insert");
    let claimed = PgTaskRepository
        .claim(&mut txn, 1, "worker-unblock")
        .await
        .expect("claim");
    let task = &claimed[0];
    let token = task.claim_token().expect("token");
    PgTaskRepository
        .block(&mut txn, task.id(), token)
        .await
        .expect("block");

    let requeued = PgTaskRepository
        .requeue_from_blocked(&mut txn, task.id())
        .await
        .expect("requeue");
    assert_eq!(requeued, 1);

    let reclaimed = PgTaskRepository
        .claim(&mut txn, 1, "worker-after")
        .await
        .expect("claim after requeue");
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].id(), task.id());
}

#[tokio::test]
async fn test_count_live_siblings_counts_blocked_as_live() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let job_id = setup_task_prerequisites(&mut txn, "live-count").await;

    let current = PgTaskRepository
        .insert(
            &mut txn,
            &a_new_task()
                .job_id(job_id)
                .task_type(TaskType::Triage)
                .batch_index(Some(0))
                .build(),
        )
        .await
        .expect("insert current");
    PgTaskRepository
        .insert(
            &mut txn,
            &a_new_task()
                .job_id(job_id)
                .task_type(TaskType::Triage)
                .batch_index(Some(1))
                .build(),
        )
        .await
        .expect("insert sibling");

    // Block the sibling: claim it, then block it.
    let claimed = PgTaskRepository
        .claim(&mut txn, 2, "worker-live")
        .await
        .expect("claim");
    let sibling = claimed
        .iter()
        .find(|t| t.id() != current.id())
        .expect("the sibling claims");
    PgTaskRepository
        .block(
            &mut txn,
            sibling.id(),
            sibling.claim_token().expect("token"),
        )
        .await
        .expect("block sibling");

    let live = PgTaskRepository
        .count_live_siblings(&mut txn, job_id, TaskType::Triage, current.id())
        .await
        .expect("count");
    assert_eq!(live, 1, "a blocked sibling is live: relation must wait");
}
