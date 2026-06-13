use tribal_db::{
    AgentBindingVersionRepository, AgentDriverTaskRepository, AgentThreadRecordRepository,
    AgentThreadRepository, DbError, DrivingTaskRef, JobRepository, NewAgentBindingVersion,
    NewAgentDriverTask, NewAgentThread, NewAgentThreadRecord, PgAgentBindingVersionRepository,
    PgAgentDriverTaskRepository, PgAgentThreadRecordRepository, PgAgentThreadRepository,
    PgJobRepository, PgPrincipalRepository, PgProjectRepository, PgTaskRepository,
    PgTokenUsageRepository, PrincipalRepository, ProjectRepository, TaskRepository,
    ThreadPruneCriteria, TokenUsageRepository,
};
use tribal_domain::{
    AGENT_THREAD_FORMAT_VERSION, AgentDriverTaskId, AgentDriverTaskKind, AgentDriverTaskState,
    AgentThread, AgentThreadRecordKind, AgentThreadRecordSeq, AgentThreadStatus,
    AgentThreadSuspension, AgentThreadTerminal, GitRemote, JobId, PrincipalId, TaskId, TaskType,
    TokenUsageStage,
};
use tribal_test_utils::{
    a_new_job, a_new_principal, a_new_project, a_new_prompt_version, a_new_system_fingerprint,
    a_new_task, a_new_token_usage, an_agent_definition, insert_prompt_version,
    shift_timestamp_by_id, test_context, upsert_system_fingerprint,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

/// Everything a thread row needs to exist: a principal, a stage task (with
/// its job chain), and a binding version.
struct ThreadPrerequisites {
    principal_id: PrincipalId,
    job_id: JobId,
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
        job_id: job.id(),
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
        .find_by_stage_task_id(&mut txn, prerequisites.stage_task_id)
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
async fn test_find_by_job_lists_only_the_jobs_stage_threads() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");

    // Job A drives two stage threads across two tasks.
    let prerequisites = setup_thread_prerequisites(&mut txn, "by-job-a").await;
    let thread_a1 = PgAgentThreadRepository
        .insert(&mut txn, &prerequisites.new_thread)
        .await
        .expect("insert thread a1");
    let second_task = PgTaskRepository
        .insert(
            &mut txn,
            &a_new_task()
                .job_id(prerequisites.job_id)
                .task_type(TaskType::Triage)
                .batch_index(Some(0))
                .build(),
        )
        .await
        .expect("insert second task");
    // The binding-stage FK requires a triage binding for a triage thread.
    let triage_binding = PgAgentBindingVersionRepository
        .record(
            &mut txn,
            &NewAgentBindingVersion::builder()
                .hash(HASH_B.to_owned())
                .pipeline_stage(TaskType::Triage)
                .definition(an_agent_definition().build())
                .build(),
        )
        .await
        .expect("record triage binding");
    let mut second_new = prerequisites.new_thread.clone();
    second_new.pipeline_stage = TaskType::Triage;
    second_new.binding_version_id = triage_binding.id();
    second_new.driving_task = DrivingTaskRef::Stage(second_task.id());
    let thread_a2 = PgAgentThreadRepository
        .insert(&mut txn, &second_new)
        .await
        .expect("insert thread a2");

    // A driver-driven child of A1 is not stage-task-bound, so it belongs
    // to its parent's lineage rather than the job's stage roster. The
    // deferred circular reference admits it before its driver row.
    let mut child_new = prerequisites.new_thread.clone();
    child_new.parent_thread_id = Some(thread_a1.id());
    child_new.driving_task = DrivingTaskRef::Driver(AgentDriverTaskId::from(uuid::Uuid::new_v4()));
    PgAgentThreadRepository
        .insert(&mut txn, &child_new)
        .await
        .expect("insert driver child");

    // Job B's thread must not bleed into job A's listing.
    let other = setup_thread_prerequisites(&mut txn, "by-job-b").await;
    PgAgentThreadRepository
        .insert(&mut txn, &other.new_thread)
        .await
        .expect("insert thread b1");

    let threads = PgAgentThreadRepository
        .find_by_job_id(&mut txn, prerequisites.job_id)
        .await
        .expect("find by job");

    let ids: Vec<_> = threads.iter().map(AgentThread::id).collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&thread_a1.id()));
    assert!(ids.contains(&thread_a2.id()));
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
        .find_by_thread_id(&mut txn, thread.id())
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
async fn test_record_pages_chain_through_the_log_without_gaps() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "page").await;

    for position in 0..5 {
        PgAgentThreadRecordRepository
            .append(
                &mut txn,
                &NewAgentThreadRecord::builder()
                    .thread_id(thread.id())
                    .seq(AgentThreadRecordSeq::new(position))
                    .kind(AgentThreadRecordKind::Input)
                    .content(serde_json::json!({"position": position}))
                    .build(),
            )
            .await
            .expect("append record");
    }

    let first = PgAgentThreadRecordRepository
        .find_by_thread_id_from(&mut txn, thread.id(), AgentThreadRecordSeq::FIRST, 2)
        .await
        .expect("first page");
    let seqs: Vec<i64> = first.iter().map(|r| r.seq().inner()).collect();
    assert_eq!(seqs, vec![0, 1]);

    // The next page starts at the previous page's last seq's successor;
    // chaining this way walks the log without gaps or repeats.
    let resume = first.last().expect("non-empty page").seq().next();
    let second = PgAgentThreadRecordRepository
        .find_by_thread_id_from(&mut txn, thread.id(), resume, 2)
        .await
        .expect("second page");
    let seqs: Vec<i64> = second.iter().map(|r| r.seq().inner()).collect();
    assert_eq!(seqs, vec![2, 3]);

    let resume = second.last().expect("non-empty page").seq().next();
    let tail = PgAgentThreadRecordRepository
        .find_by_thread_id_from(&mut txn, thread.id(), resume, 2)
        .await
        .expect("tail page");
    let seqs: Vec<i64> = tail.iter().map(|r| r.seq().inner()).collect();
    assert_eq!(seqs, vec![4]);

    let beyond = PgAgentThreadRecordRepository
        .find_by_thread_id_from(&mut txn, thread.id(), AgentThreadRecordSeq::new(5), 2)
        .await
        .expect("page beyond the log");
    assert!(beyond.is_empty());
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

#[tokio::test]
async fn test_driver_task_insert_honours_a_client_generated_id() {
    // The thread/driver pair writer names the driver task's id on the
    // thread row before the task exists, under the deferred FK, so the
    // insert must take the caller's id rather than minting its own.
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "client-id").await;

    let chosen = AgentDriverTaskId::from(uuid::Uuid::new_v4());
    let inserted = PgAgentDriverTaskRepository
        .insert(
            &mut txn,
            &NewAgentDriverTask::builder()
                .id(Some(chosen))
                .thread_id(thread.id())
                .kind(AgentDriverTaskKind::Drive)
                .build(),
        )
        .await
        .expect("insert with a client id");
    assert_eq!(inserted.id(), chosen, "the row takes the caller's id");

    // The absent-id path still mints a server id, so the launched callers
    // are unaffected.
    let minted = PgAgentDriverTaskRepository
        .insert(
            &mut txn,
            &NewAgentDriverTask::builder()
                .thread_id(thread.id())
                .kind(AgentDriverTaskKind::Drive)
                .build(),
        )
        .await
        .expect("insert without an id");
    assert_ne!(minted.id(), chosen);
}

#[tokio::test]
async fn test_lock_stale_selects_only_lapsed_claimed_rows() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "stale-driver").await;

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

    // A fresh heartbeat: the scan must leave it alone.
    let fresh = PgAgentDriverTaskRepository
        .lock_stale(&mut txn, 60)
        .await
        .expect("scan with a fresh beat");
    assert!(fresh.is_none(), "a live lease is never reclaimed");

    // Backdate the heartbeat past the timeout: now it is the scan's row,
    // returned with its claim token so the reclaimer's guarded
    // disposition targets exactly the lease it judged stale.
    shift_timestamp_by_id(
        &mut txn,
        "agent_driver_tasks",
        "heartbeat_at",
        inserted.id().inner().to_owned(),
        chrono::Duration::seconds(-120),
    )
    .await;
    let stale = PgAgentDriverTaskRepository
        .lock_stale(&mut txn, 60)
        .await
        .expect("scan with a stale beat")
        .expect("the lapsed row is selected");
    assert_eq!(stale.id(), inserted.id());
    assert_eq!(stale.claim_token(), Some(token));
}

// ---------------------------------------------------------------------------
// Sweep scan predicates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_timer_wake_scan_selects_only_due_intentless_stage_threads() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");

    // Due, intentless, stage-driven: the one row the predicate returns.
    let due = insert_thread(&mut txn, "wake-due").await;
    PgAgentThreadRepository
        .mark_running(&mut txn, due.id(), AgentThreadStatus::Queued)
        .await
        .expect("running");
    PgAgentThreadRepository
        .suspend(
            &mut txn,
            due.id(),
            &AgentThreadSuspension::Timer,
            Some(chrono::Utc::now() - chrono::Duration::seconds(5)),
        )
        .await
        .expect("suspend due");

    // Suspended with a future wake: not yet due.
    let future = insert_thread(&mut txn, "wake-future").await;
    PgAgentThreadRepository
        .mark_running(&mut txn, future.id(), AgentThreadStatus::Queued)
        .await
        .expect("running");
    PgAgentThreadRepository
        .suspend(
            &mut txn,
            future.id(),
            &AgentThreadSuspension::Timer,
            Some(chrono::Utc::now() + chrono::Duration::minutes(10)),
        )
        .await
        .expect("suspend future");

    // Due but carrying a cancel intent: the cancel predicate's row.
    let cancelled = insert_thread(&mut txn, "wake-intent").await;
    PgAgentThreadRepository
        .mark_running(&mut txn, cancelled.id(), AgentThreadStatus::Queued)
        .await
        .expect("running");
    PgAgentThreadRepository
        .suspend(
            &mut txn,
            cancelled.id(),
            &AgentThreadSuspension::Timer,
            Some(chrono::Utc::now() - chrono::Duration::seconds(5)),
        )
        .await
        .expect("suspend intent");
    PgAgentThreadRepository
        .record_cancel_intent(&mut txn, cancelled.id(), "operator:test")
        .await
        .expect("intent");

    // Due and intentless but driver-driven: waking it without its driver
    // half would strand it running, so the scan must leave it alone. The
    // deferred circular reference admits the thread before its driver row.
    let driver_prereq = setup_thread_prerequisites(&mut txn, "wake-driver").await;
    let driver_task_id = uuid::Uuid::new_v4();
    let mut driver_new = driver_prereq.new_thread;
    driver_new.driving_task = DrivingTaskRef::Driver(AgentDriverTaskId::from(driver_task_id));
    let driver_driven = PgAgentThreadRepository
        .insert(&mut txn, &driver_new)
        .await
        .expect("insert driver-driven thread");
    sqlx::query("INSERT INTO agent_driver_tasks (id, thread_id, kind) VALUES ($1, $2, 'drive')")
        .bind(driver_task_id)
        .bind(driver_driven.id().inner())
        .execute(&mut *txn)
        .await
        .expect("insert paired driver task");
    PgAgentThreadRepository
        .mark_running(&mut txn, driver_driven.id(), AgentThreadStatus::Queued)
        .await
        .expect("running");
    PgAgentThreadRepository
        .suspend(
            &mut txn,
            driver_driven.id(),
            &AgentThreadSuspension::Timer,
            Some(chrono::Utc::now() - chrono::Duration::seconds(5)),
        )
        .await
        .expect("suspend driver-driven");

    let woken = PgAgentThreadRepository
        .find_due_timer_wakes(&mut txn, 10)
        .await
        .expect("scan");

    assert_eq!(
        woken.len(),
        1,
        "only the due intentless stage-driven thread is woken"
    );
    assert_eq!(woken[0].id(), due.id());

    let intents = PgAgentThreadRepository
        .find_cancel_intents(&mut txn, 10)
        .await
        .expect("intent scan");
    assert_eq!(
        intents.len(),
        1,
        "the intent thread belongs to the cancel predicate"
    );
    assert_eq!(intents[0].id(), cancelled.id());
}

#[tokio::test]
async fn test_complete_from_suspended_clears_the_suspension_payload() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");

    let thread = insert_thread(&mut txn, "complete-suspended").await;
    PgAgentThreadRepository
        .mark_running(&mut txn, thread.id(), AgentThreadStatus::Queued)
        .await
        .expect("running");
    PgAgentThreadRepository
        .suspend(
            &mut txn,
            thread.id(),
            &AgentThreadSuspension::Timer,
            Some(chrono::Utc::now()),
        )
        .await
        .expect("suspend");

    let moved = PgAgentThreadRepository
        .complete(
            &mut txn,
            thread.id(),
            AgentThreadTerminal::Cancelled,
            AgentThreadStatus::Suspended,
        )
        .await
        .expect("complete");
    assert_eq!(moved, 1);

    let after = PgAgentThreadRepository
        .find_by_id(&mut txn, thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(after.status(), AgentThreadStatus::Cancelled);
    assert!(
        after.suspension().is_none() && after.wake_at().is_none(),
        "leaving suspended clears the payload so the paired CHECK can never trip",
    );
}

// ---------------------------------------------------------------------------
// Driver-task lease guards
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_driver_holds_claim_tracks_the_lease() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "driver-guard").await;

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
        .claim(&mut txn, 1, "guard-test")
        .await
        .expect("claim");
    let token = claimed[0].claim_token().expect("token");

    assert!(
        PgAgentDriverTaskRepository
            .holds_claim(&mut txn, inserted.id(), token)
            .await
            .expect("held")
    );
    assert!(
        !PgAgentDriverTaskRepository
            .holds_claim(&mut txn, inserted.id(), uuid::Uuid::new_v4())
            .await
            .expect("stale")
    );

    PgAgentDriverTaskRepository
        .complete(&mut txn, inserted.id(), token)
        .await
        .expect("complete");
    assert!(
        !PgAgentDriverTaskRepository
            .holds_claim(&mut txn, inserted.id(), token)
            .await
            .expect("terminal"),
        "a completed row releases its lease",
    );
}

#[tokio::test]
async fn test_driver_heartbeat_is_claim_guarded() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "driver-beat").await;

    PgAgentDriverTaskRepository
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
        .claim(&mut txn, 1, "beat-test")
        .await
        .expect("claim");
    let task = &claimed[0];
    let token = task.claim_token().expect("token");

    let beaten = PgAgentDriverTaskRepository
        .heartbeat(&mut txn, task.id(), token)
        .await
        .expect("beat");
    assert_eq!(beaten, 1);

    let stale = PgAgentDriverTaskRepository
        .heartbeat(&mut txn, task.id(), uuid::Uuid::new_v4())
        .await
        .expect("stale beat");
    assert_eq!(stale, 0, "a zombie's heartbeat must not land");
}

// ---------------------------------------------------------------------------
// Thread-scoped ledger reads (fixtures live here with the thread chain)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sum_thread_spend_counts_only_the_thread() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "spend-sum").await;

    for tokens in [100, 60] {
        PgTokenUsageRepository
            .insert(
                &mut txn,
                &a_new_token_usage()
                    .agent_thread_id(Some(thread.id()))
                    .tokens_input(tokens)
                    .tokens_output(0)
                    .build(),
            )
            .await
            .expect("insert thread row");
    }
    PgTokenUsageRepository
        .insert(&mut txn, &a_new_token_usage().tokens_input(999).build())
        .await
        .expect("insert unattributed row");

    let total = PgTokenUsageRepository
        .sum_thread_spend(&mut txn, thread.id())
        .await
        .expect("sum");
    assert_eq!(total, 160, "only the thread's rows count");
}

#[tokio::test]
async fn test_thread_link_picks_only_the_most_recent_unlinked_row() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let thread = insert_thread(&mut txn, "thread-link").await;

    let earlier = PgTokenUsageRepository
        .insert(
            &mut txn,
            &a_new_token_usage()
                .agent_thread_id(Some(thread.id()))
                .stage(TokenUsageStage::Triage)
                .build(),
        )
        .await
        .expect("insert earlier");
    shift_timestamp_by_id(
        &mut txn,
        "token_usage",
        "created_at",
        earlier.id().inner().to_owned(),
        chrono::Duration::seconds(-60),
    )
    .await;
    let later = PgTokenUsageRepository
        .insert(
            &mut txn,
            &a_new_token_usage()
                .agent_thread_id(Some(thread.id()))
                .stage(TokenUsageStage::Triage)
                .build(),
        )
        .await
        .expect("insert later");

    let record = PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
                .seq(AgentThreadRecordSeq::FIRST)
                .kind(AgentThreadRecordKind::AssistantMessage)
                .content(serde_json::json!({}))
                .build(),
        )
        .await
        .expect("append record");

    let linked = PgTokenUsageRepository
        .link_thread_completion_to_record(&mut txn, thread.id(), 0, record.id())
        .await
        .expect("link");
    assert_eq!(linked, 1, "exactly the most recent row links");

    let later_record: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT agent_thread_record_id FROM token_usage WHERE id = $1")
            .bind(later.id().inner())
            .fetch_one(&mut *txn)
            .await
            .expect("read later");
    let earlier_record: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT agent_thread_record_id FROM token_usage WHERE id = $1")
            .bind(earlier.id().inner())
            .fetch_one(&mut *txn)
            .await
            .expect("read earlier");
    assert_eq!(later_record, Some(record.id().inner().to_owned()));
    assert_eq!(
        earlier_record, None,
        "the earlier attempt's orphan stays thread-attributed",
    );
}

// ---------------------------------------------------------------------------
// Prune
// ---------------------------------------------------------------------------

/// Builds prune criteria collecting everything terminal as of now.
fn prune_now(cascade: bool) -> ThreadPruneCriteria {
    ThreadPruneCriteria {
        completed_before: chrono::Utc::now() + chrono::Duration::seconds(1),
        stage: None,
        cascade,
    }
}

#[tokio::test]
async fn test_prune_deletes_only_aged_terminal_threads() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");

    // A terminal thread with a record, and a live one.
    let terminal = insert_thread(&mut txn, "prune-terminal").await;
    PgAgentThreadRepository
        .mark_running(&mut txn, terminal.id(), AgentThreadStatus::Queued)
        .await
        .expect("running");
    PgAgentThreadRepository
        .complete(
            &mut txn,
            terminal.id(),
            AgentThreadTerminal::Completed,
            AgentThreadStatus::Running,
        )
        .await
        .expect("complete");
    PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(terminal.id())
                .seq(AgentThreadRecordSeq::FIRST)
                .kind(AgentThreadRecordKind::Input)
                .content(serde_json::json!({"kept": false}))
                .build(),
        )
        .await
        .expect("record");
    let live = insert_thread(&mut txn, "prune-live").await;

    // An age bound in the past collects nothing while the terminal
    // thread is present: the bound, not terminality, is what spares it.
    let too_old = ThreadPruneCriteria {
        completed_before: chrono::Utc::now() - chrono::Duration::days(30),
        stage: None,
        cascade: false,
    };
    let pruned = PgAgentThreadRepository
        .prune_threads(&mut txn, &too_old)
        .await
        .expect("prune");
    assert_eq!(pruned, 0, "nothing terminal predates the bound");
    assert!(
        PgAgentThreadRepository
            .find_by_id(&mut txn, terminal.id())
            .await
            .expect("find")
            .is_some(),
        "the bound spared the terminal thread",
    );

    let refused = PgAgentThreadRepository
        .count_refused_prune_roots(&mut txn, &prune_now(false))
        .await
        .expect("count");
    assert_eq!(refused, 0);
    let pruned = PgAgentThreadRepository
        .prune_threads(&mut txn, &prune_now(false))
        .await
        .expect("prune");
    assert_eq!(pruned, 1, "the terminal thread and only it");

    assert!(
        PgAgentThreadRepository
            .find_by_id(&mut txn, terminal.id())
            .await
            .expect("find")
            .is_none(),
        "the terminal thread is gone",
    );
    assert!(
        PgAgentThreadRecordRepository
            .find_by_thread_id(&mut txn, terminal.id())
            .await
            .expect("log")
            .is_empty(),
        "its records went with it",
    );
    assert!(
        PgAgentThreadRepository
            .find_by_id(&mut txn, live.id())
            .await
            .expect("find")
            .is_some(),
        "the live thread survives",
    );
}

#[tokio::test]
async fn test_prune_refuses_a_root_with_a_live_descendant_even_cascading() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");

    // A terminal parent with a live child.
    let parent = insert_thread(&mut txn, "prune-parent-live").await;
    PgAgentThreadRepository
        .mark_running(&mut txn, parent.id(), AgentThreadStatus::Queued)
        .await
        .expect("running");
    PgAgentThreadRepository
        .complete(
            &mut txn,
            parent.id(),
            AgentThreadTerminal::Failed,
            AgentThreadStatus::Running,
        )
        .await
        .expect("complete");
    let mut child_prereq = setup_thread_prerequisites(&mut txn, "prune-child-live").await;
    child_prereq.new_thread.parent_thread_id = Some(parent.id());
    let child = PgAgentThreadRepository
        .insert(&mut txn, &child_prereq.new_thread)
        .await
        .expect("insert child");

    for cascade in [false, true] {
        let refused = PgAgentThreadRepository
            .count_refused_prune_roots(&mut txn, &prune_now(cascade))
            .await
            .expect("count");
        assert_eq!(
            refused, 1,
            "the live descendant refuses (cascade {cascade})"
        );
        let pruned = PgAgentThreadRepository
            .prune_threads(&mut txn, &prune_now(cascade))
            .await
            .expect("prune");
        assert_eq!(pruned, 0, "nothing deletes (cascade {cascade})");
    }
    assert!(
        PgAgentThreadRepository
            .find_by_id(&mut txn, child.id())
            .await
            .expect("find")
            .is_some(),
    );
}

#[tokio::test]
async fn test_prune_cascade_collects_a_terminal_subtree() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");

    let parent = insert_thread(&mut txn, "prune-subtree-parent").await;
    PgAgentThreadRepository
        .mark_running(&mut txn, parent.id(), AgentThreadStatus::Queued)
        .await
        .expect("running");
    PgAgentThreadRepository
        .complete(
            &mut txn,
            parent.id(),
            AgentThreadTerminal::Completed,
            AgentThreadStatus::Running,
        )
        .await
        .expect("complete");
    let mut child_prereq = setup_thread_prerequisites(&mut txn, "prune-subtree-child").await;
    child_prereq.new_thread.parent_thread_id = Some(parent.id());
    let child = PgAgentThreadRepository
        .insert(&mut txn, &child_prereq.new_thread)
        .await
        .expect("insert child");
    PgAgentThreadRepository
        .mark_running(&mut txn, child.id(), AgentThreadStatus::Queued)
        .await
        .expect("running");
    PgAgentThreadRepository
        .complete(
            &mut txn,
            child.id(),
            AgentThreadTerminal::Completed,
            AgentThreadStatus::Running,
        )
        .await
        .expect("complete");

    // Without the cascade the descendant refuses the root.
    let refused = PgAgentThreadRepository
        .count_refused_prune_roots(&mut txn, &prune_now(false))
        .await
        .expect("count");
    assert_eq!(refused, 1, "any descendant refuses without the cascade");

    // With it, the whole terminal subtree collects in one pass.
    let pruned = PgAgentThreadRepository
        .prune_threads(&mut txn, &prune_now(true))
        .await
        .expect("prune");
    assert_eq!(pruned, 2, "parent and terminal child delete together");
}
