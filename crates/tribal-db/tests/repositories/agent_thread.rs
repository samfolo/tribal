use tribal_db::{
    AgentBindingVersionRepository, AgentDriverTaskRepository, AgentThreadRecordRepository,
    AgentThreadRepository, DbError, DrivingTaskRef, JobRepository, NewAgentBindingVersion,
    NewAgentDriverTask, NewAgentThread, NewAgentThreadRecord, PgAgentBindingVersionRepository,
    PgAgentDriverTaskRepository, PgAgentThreadRecordRepository, PgAgentThreadRepository,
    PgJobRepository, PgPrincipalRepository, PgProjectRepository, PgTaskRepository,
    PrincipalRepository, ProjectRepository, TaskRepository,
};
use tribal_domain::{
    AGENT_THREAD_FORMAT_VERSION, AgentDriverTaskKind, AgentDriverTaskState, AgentThread,
    AgentThreadRecordKind, AgentThreadRecordSeq, AgentThreadStatus, AgentThreadSuspension,
    AgentThreadTerminal, GitRemote, PrincipalId, TaskId, TaskType,
};
use tribal_test_utils::{
    a_new_job, a_new_principal, a_new_project, a_new_prompt_version, a_new_system_fingerprint,
    a_new_task, an_agent_definition, insert_prompt_version, test_context,
    upsert_system_fingerprint,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// Everything a thread row needs to exist: a principal, a stage task (with
/// its job chain), and a binding version.
struct ThreadPrerequisites {
    principal_id: PrincipalId,
    stage_task_id: TaskId,
    new_thread: NewAgentThread,
}

async fn setup_thread_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> ThreadPrerequisites {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:agent-thread-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal");

    let project = PgProjectRepository
        .insert(
            txn,
            &a_new_project()
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    &format!("test/agent-thread-{suffix}"),
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

    let task = PgTaskRepository
        .insert(txn, &a_new_task().job_id(job.id()).build())
        .await
        .expect("insert task");

    let binding = PgAgentBindingVersionRepository
        .record(
            txn,
            &NewAgentBindingVersion::builder()
                .hash(HASH_A.to_owned())
                .pipeline_stage(TaskType::Extraction)
                .definition(an_agent_definition().build())
                .build(),
        )
        .await
        .expect("record binding version");

    let new_thread = NewAgentThread::builder()
        .pipeline_stage(TaskType::Extraction)
        .binding_version_id(binding.id())
        .driving_task(DrivingTaskRef::Stage(task.id()))
        .principal_id(principal.id())
        .format_version(AGENT_THREAD_FORMAT_VERSION)
        .build();

    ThreadPrerequisites {
        principal_id: principal.id(),
        stage_task_id: task.id(),
        new_thread,
    }
}

async fn insert_thread(txn: &mut sqlx::PgConnection, suffix: &str) -> AgentThread {
    let prerequisites = setup_thread_prerequisites(txn, suffix).await;
    PgAgentThreadRepository
        .insert(txn, &prerequisites.new_thread)
        .await
        .expect("insert thread")
}

// ---------------------------------------------------------------------------
// Binding versions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_binding_version_record_is_idempotent_by_hash() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");

    let new = NewAgentBindingVersion::builder()
        .hash(HASH_A.to_owned())
        .pipeline_stage(TaskType::Triage)
        .definition(
            an_agent_definition()
                .pipeline_stage(TaskType::Triage)
                .build(),
        )
        .build();

    let first = PgAgentBindingVersionRepository
        .record(&mut txn, &new)
        .await
        .expect("first record");
    let second = PgAgentBindingVersionRepository
        .record(&mut txn, &new)
        .await
        .expect("second record");

    assert_eq!(first.id(), second.id());
    assert_eq!(first.hash(), HASH_A);

    let found = PgAgentBindingVersionRepository
        .find_by_id(&mut txn, first.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(found.definition().model, "llama3");
}

// ---------------------------------------------------------------------------
// Thread lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_thread_inserts_queued_with_stage_driver() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let prerequisites = setup_thread_prerequisites(&mut txn, "insert").await;

    let thread = PgAgentThreadRepository
        .insert(&mut txn, &prerequisites.new_thread)
        .await
        .expect("insert thread");

    assert_eq!(thread.status(), AgentThreadStatus::Queued);
    assert_eq!(thread.stage_task_id(), Some(prerequisites.stage_task_id));
    assert!(thread.driver_task_id().is_none());
    assert_eq!(thread.principal_id(), prerequisites.principal_id);
    assert_eq!(thread.format_version(), AGENT_THREAD_FORMAT_VERSION);
    assert_eq!(thread.recovery_attempts(), 0);
    assert!(thread.completed_at().is_none());

    let by_task = PgAgentThreadRepository
        .find_by_stage_task(&mut txn, prerequisites.stage_task_id)
        .await
        .expect("find by stage task")
        .expect("present");
    assert_eq!(by_task.id(), thread.id());
}

#[tokio::test]
async fn test_one_thread_per_stage_task_ever() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let prerequisites = setup_thread_prerequisites(&mut txn, "dup").await;

    PgAgentThreadRepository
        .insert(&mut txn, &prerequisites.new_thread)
        .await
        .expect("first thread");
    let err = PgAgentThreadRepository
        .insert(&mut txn, &prerequisites.new_thread)
        .await
        .expect_err("a second thread on the same stage task must be rejected");

    assert!(matches!(err, DbError::UniqueViolation { .. }));
}

#[tokio::test]
async fn test_status_cas_misses_return_zero_rows() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "cas").await;

    let moved = PgAgentThreadRepository
        .mark_running(&mut txn, thread.id(), AgentThreadStatus::Queued)
        .await
        .expect("mark running");
    assert_eq!(moved, 1);

    let repeat = PgAgentThreadRepository
        .mark_running(&mut txn, thread.id(), AgentThreadStatus::Queued)
        .await
        .expect("repeat mark running");
    assert_eq!(repeat, 0, "the from-status CAS must miss");
}

#[tokio::test]
async fn test_suspend_round_trips_the_typed_cause_and_resume_clears_it() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "suspend").await;
    PgAgentThreadRepository
        .mark_running(&mut txn, thread.id(), AgentThreadStatus::Queued)
        .await
        .expect("run");

    let cause = AgentThreadSuspension::DeferredToolResults {
        requesting_seq: AgentThreadRecordSeq::new(3),
        pending_tool_call_ids: vec!["call_a".to_owned()],
    };
    let suspended = PgAgentThreadRepository
        .suspend(&mut txn, thread.id(), &cause, None)
        .await
        .expect("suspend");
    assert_eq!(suspended, 1);

    let read = PgAgentThreadRepository
        .find_by_id(&mut txn, thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(read.status(), AgentThreadStatus::Suspended);
    assert_eq!(read.suspension(), Some(&cause));

    let resumed = PgAgentThreadRepository
        .mark_running(&mut txn, thread.id(), AgentThreadStatus::Suspended)
        .await
        .expect("resume");
    assert_eq!(resumed, 1);

    let read = PgAgentThreadRepository
        .find_by_id(&mut txn, thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(read.status(), AgentThreadStatus::Running);
    assert!(read.suspension().is_none(), "resume clears the payload");
}

#[tokio::test]
async fn test_suspend_refuses_to_commit_over_a_cancel_intent() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "cancel-window").await;
    PgAgentThreadRepository
        .mark_running(&mut txn, thread.id(), AgentThreadStatus::Queued)
        .await
        .expect("run");

    PgAgentThreadRepository
        .record_cancel_intent(&mut txn, thread.id(), "operator:test")
        .await
        .expect("record intent");

    let suspended = PgAgentThreadRepository
        .suspend(
            &mut txn,
            thread.id(),
            &AgentThreadSuspension::Timer,
            Some(chrono::Utc::now()),
        )
        .await
        .expect("suspend attempt");
    assert_eq!(
        suspended, 0,
        "a suspend over a durable cancel intent must refuse to commit",
    );
}

#[tokio::test]
async fn test_cancel_intent_is_idempotent_and_first_writer_wins() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "intent").await;

    PgAgentThreadRepository
        .record_cancel_intent(&mut txn, thread.id(), "operator:first")
        .await
        .expect("first intent");
    PgAgentThreadRepository
        .record_cancel_intent(&mut txn, thread.id(), "operator:second")
        .await
        .expect("second intent");

    let read = PgAgentThreadRepository
        .find_by_id(&mut txn, thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(read.cancel_requested_by(), Some("operator:first"));
    assert!(read.cancel_requested_at().is_some());
}

#[tokio::test]
async fn test_terminal_transition_stamps_completed_at_and_is_final() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "terminal").await;
    PgAgentThreadRepository
        .mark_running(&mut txn, thread.id(), AgentThreadStatus::Queued)
        .await
        .expect("run");

    let completed = PgAgentThreadRepository
        .complete(
            &mut txn,
            thread.id(),
            AgentThreadTerminal::Completed,
            AgentThreadStatus::Running,
        )
        .await
        .expect("complete");
    assert_eq!(completed, 1);

    let read = PgAgentThreadRepository
        .find_by_id(&mut txn, thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(read.status(), AgentThreadStatus::Completed);
    assert!(read.completed_at().is_some());

    let again = PgAgentThreadRepository
        .complete(
            &mut txn,
            thread.id(),
            AgentThreadTerminal::Failed,
            AgentThreadStatus::Running,
        )
        .await
        .expect("late terminal");
    assert_eq!(again, 0, "nothing transitions out of a terminal status");
}

#[tokio::test]
async fn test_recovery_attempts_accumulate() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "recovery").await;

    let first = PgAgentThreadRepository
        .increment_recovery_attempts(&mut txn, thread.id())
        .await
        .expect("first cycle");
    let second = PgAgentThreadRepository
        .increment_recovery_attempts(&mut txn, thread.id())
        .await
        .expect("second cycle");

    assert_eq!(first, 1);
    assert_eq!(second, 2);
}

// ---------------------------------------------------------------------------
// Records: the log and its fences
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_record_append_advances_seq_and_lists_in_order() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "log").await;

    let first_seq = PgAgentThreadRecordRepository
        .next_seq(&mut txn, thread.id())
        .await
        .expect("next seq");
    assert_eq!(first_seq, AgentThreadRecordSeq::FIRST);

    PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
                .seq(first_seq)
                .kind(AgentThreadRecordKind::Input)
                .content(serde_json::json!({"messages": []}))
                .build(),
        )
        .await
        .expect("append input");

    let next = PgAgentThreadRecordRepository
        .next_seq(&mut txn, thread.id())
        .await
        .expect("next seq after append");
    assert_eq!(next, first_seq.next());

    PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
                .seq(next)
                .kind(AgentThreadRecordKind::AssistantMessage)
                .content(serde_json::json!({"text": "done"}))
                .build(),
        )
        .await
        .expect("append assistant message");

    let records = PgAgentThreadRecordRepository
        .find_by_thread(&mut txn, thread.id())
        .await
        .expect("list");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].kind(), AgentThreadRecordKind::Input);
    assert_eq!(records[1].kind(), AgentThreadRecordKind::AssistantMessage);

    let last = PgAgentThreadRecordRepository
        .last_record(&mut txn, thread.id())
        .await
        .expect("last")
        .expect("present");
    assert_eq!(last.seq(), next);
}

#[tokio::test]
async fn test_duplicate_seq_is_a_unique_violation() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "dup-seq").await;

    let record = NewAgentThreadRecord::builder()
        .thread_id(thread.id())
        .seq(AgentThreadRecordSeq::FIRST)
        .kind(AgentThreadRecordKind::Input)
        .content(serde_json::json!({}))
        .build();

    PgAgentThreadRecordRepository
        .append(&mut txn, &record)
        .await
        .expect("first append");
    let err = PgAgentThreadRecordRepository
        .append(&mut txn, &record)
        .await
        .expect_err("the seq key must reject the conflict loser");

    assert!(matches!(err, DbError::UniqueViolation { .. }));
}

#[tokio::test]
async fn test_tool_result_without_fence_columns_is_rejected() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "fence-cols").await;

    let err = PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
                .seq(AgentThreadRecordSeq::FIRST)
                .kind(AgentThreadRecordKind::ToolResult)
                .content(serde_json::json!({}))
                .build(),
        )
        .await
        .expect_err("an executed result must name what it answers");

    assert!(matches!(err, DbError::QueryFailed { .. }));
}

#[tokio::test]
async fn test_executed_tool_results_are_exactly_once_per_call() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "fence").await;

    let result = |seq: i64| {
        NewAgentThreadRecord::builder()
            .thread_id(thread.id())
            .seq(AgentThreadRecordSeq::new(seq))
            .kind(AgentThreadRecordKind::ToolResult)
            .requesting_seq(Some(AgentThreadRecordSeq::FIRST))
            .tool_call_id(Some("call_a".to_owned()))
            .content(serde_json::json!({}))
            .build()
    };

    PgAgentThreadRecordRepository
        .append(&mut txn, &result(1))
        .await
        .expect("first result");
    let count = PgAgentThreadRecordRepository
        .count_tool_results(&mut txn, thread.id(), AgentThreadRecordSeq::FIRST)
        .await
        .expect("count");
    assert_eq!(count, 1);

    // The violation aborts the enclosing transaction (Postgres 25P02), so
    // it is the test's final act — and the reason a runtime conflict
    // loser retries its whole transaction rather than reconciling inside
    // the aborted one.
    let err = PgAgentThreadRecordRepository
        .append(&mut txn, &result(2))
        .await
        .expect_err("the fence must reject a second result for the same call");
    assert!(matches!(err, DbError::UniqueViolation { .. }));
}

#[tokio::test]
async fn test_observed_tool_events_never_claim_the_fence() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "observed").await;

    // Two observations of the same external call: both commit, because
    // observations are transcript data, not executed results.
    for seq in [0, 1] {
        PgAgentThreadRecordRepository
            .append(
                &mut txn,
                &NewAgentThreadRecord::builder()
                    .thread_id(thread.id())
                    .seq(AgentThreadRecordSeq::new(seq))
                    .kind(AgentThreadRecordKind::ObservedToolEvent)
                    .requesting_seq(Some(AgentThreadRecordSeq::FIRST))
                    .tool_call_id(Some("call_a".to_owned()))
                    .content(serde_json::json!({}))
                    .build(),
            )
            .await
            .expect("observed event commits");
    }
}

// ---------------------------------------------------------------------------
// Driver tasks: the lease protocol
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_driver_task_lease_round_trip() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "driver").await;

    let inserted = PgAgentDriverTaskRepository
        .insert(
            &mut txn,
            &NewAgentDriverTask::builder()
                .thread_id(thread.id())
                .kind(AgentDriverTaskKind::DeferredTool)
                .build(),
        )
        .await
        .expect("insert driver task");
    assert_eq!(inserted.state(), AgentDriverTaskState::Pending);

    let claimed = PgAgentDriverTaskRepository
        .claim(&mut txn, 5, "worker-test")
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    let token = claimed[0].claim_token().expect("claimed rows carry tokens");

    let wrong = PgAgentDriverTaskRepository
        .complete(&mut txn, inserted.id(), uuid::Uuid::new_v4())
        .await
        .expect("complete with wrong token");
    assert_eq!(wrong, 0, "a zombie's stale token must be rejected");

    let done = PgAgentDriverTaskRepository
        .complete(&mut txn, inserted.id(), token)
        .await
        .expect("complete");
    assert_eq!(done, 1);
}

#[tokio::test]
async fn test_driver_task_requeue_resets_the_lease() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "requeue").await;

    let inserted = PgAgentDriverTaskRepository
        .insert(
            &mut txn,
            &NewAgentDriverTask::builder()
                .thread_id(thread.id())
                .kind(AgentDriverTaskKind::Drive)
                .build(),
        )
        .await
        .expect("insert");
    let claimed = PgAgentDriverTaskRepository
        .claim(&mut txn, 1, "worker-test")
        .await
        .expect("claim");
    let token = claimed[0].claim_token().expect("token");

    let requeued = PgAgentDriverTaskRepository
        .requeue(
            &mut txn,
            inserted.id(),
            token,
            1,
            chrono::Utc::now(),
            "transient failure",
        )
        .await
        .expect("requeue");
    assert_eq!(requeued, 1);

    let read = PgAgentDriverTaskRepository
        .find_by_id(&mut txn, inserted.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(read.state(), AgentDriverTaskState::Pending);
    assert_eq!(read.attempt(), 1);
    assert!(read.claim_token().is_none());
    assert_eq!(read.last_error(), Some("transient failure"));
}

#[tokio::test]
async fn test_dispose_unclaimed_never_touches_a_claimed_row() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "dispose").await;

    let inserted = PgAgentDriverTaskRepository
        .insert(
            &mut txn,
            &NewAgentDriverTask::builder()
                .thread_id(thread.id())
                .kind(AgentDriverTaskKind::DeferredTool)
                .build(),
        )
        .await
        .expect("insert");

    PgAgentDriverTaskRepository
        .claim(&mut txn, 1, "worker-test")
        .await
        .expect("claim");
    let held = PgAgentDriverTaskRepository
        .dispose_unclaimed(&mut txn, inserted.id(), "cancelled")
        .await
        .expect("dispose attempt on claimed row");
    assert_eq!(held, 0, "the locked-unclaimed guard must skip a held row");

    let claimed = PgAgentDriverTaskRepository
        .find_by_id(&mut txn, inserted.id())
        .await
        .expect("find")
        .expect("present");
    let token = claimed.claim_token().expect("token");
    PgAgentDriverTaskRepository
        .requeue(
            &mut txn,
            inserted.id(),
            token,
            1,
            chrono::Utc::now(),
            "released",
        )
        .await
        .expect("release the lease");

    let disposed = PgAgentDriverTaskRepository
        .dispose_unclaimed(&mut txn, inserted.id(), "cancelled")
        .await
        .expect("dispose unclaimed");
    assert_eq!(disposed, 1);

    let read = PgAgentDriverTaskRepository
        .find_by_id(&mut txn, inserted.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(read.state(), AgentDriverTaskState::DeadLetter);
    assert!(read.completed_at().is_some());
}
