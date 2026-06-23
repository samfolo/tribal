//! Integration tests for the thread bracket around stage execution.
//!
//! The pipeline's observable surface is asserted unchanged by the
//! existing suites; these tests assert what the runtime adds underneath:
//! every executed stage task drives exactly one thread whose log carries
//! the rendered input and the assistant response, completed in the same
//! transaction as the task.

use tokio::sync::watch;
use tribal_agent_runtime::RenderedConversation;
use tribal_common::JobWatchEntry;
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, PgAgentThreadRecordRepository,
    PgAgentThreadRepository,
};
use tribal_domain::{AgentThreadRecordKind, AgentThreadStatus, JobState};

use super::{common::*, fixtures::extraction_response_json};

/// The extraction happy path leaves one completed thread driven by the
/// extraction task, with an input record carrying the conversation as
/// sent and an assistant record carrying the response and its usage.
#[tokio::test]
async fn test_stage_execution_commits_a_completed_thread_with_its_log() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "thread-log").await;

    let candidates = vec![a_candidate().content("threaded".to_owned()).build()];
    let response_json = extraction_response_json(&candidates, &[]);

    let (job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response(&response_json), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    let _ = poll_job_status(&pool, job_id, JobStatus::Triaging, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(&ctx).await;
    let thread = PgAgentThreadRepository
        .find_by_stage_task_id(&mut conn, task_id)
        .await
        .expect("find thread")
        .expect("the executed task drives a thread");

    assert_eq!(thread.status(), AgentThreadStatus::Completed);
    assert!(thread.completed_at().is_some());
    assert_eq!(thread.recovery_attempts(), 0);
    assert!(
        thread.execution_spend().is_some(),
        "the terminal commit projects the spend",
    );

    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread.id())
        .await
        .expect("read the log");
    assert_eq!(records.len(), 2, "a one-shot turn commits two records");
    assert_eq!(records[0].kind(), AgentThreadRecordKind::Input);
    assert_eq!(records[1].kind(), AgentThreadRecordKind::AssistantMessage);

    // The input record is the conversation as sent, re-readable in the
    // thread's serialisation format.
    let conversation: RenderedConversation =
        serde_json::from_value(records[0].content().clone()).expect("input content round-trips");
    assert_eq!(conversation.system_prompt_version_id, Some(system_pv_id));
    assert_eq!(conversation.user_prompt_version_id, Some(user_pv_id));
    assert!(
        !conversation.messages.is_empty(),
        "the rendered user message is stored verbatim",
    );

    assert!(
        records[1].usage().is_some(),
        "the assistant record carries the call's usage",
    );

    // The completion's ledger row is linked to the committed assistant
    // record in the same terminal transaction.
    let usage_rows = PgTokenUsageRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("ledger rows");
    let completion = usage_rows
        .iter()
        .find(|r| r.stage() == PipelineStage::Extraction)
        .expect("the extraction completion is ledgered");
    assert_eq!(
        completion.agent_thread_id(),
        Some(thread.id()),
        "completion spend attributes to the thread",
    );
    assert_eq!(
        completion.agent_thread_record_id(),
        Some(records[1].id()),
        "completion spend attributes to the committed assistant record",
    );
}

/// Drives suspend and resolve directly through the runtime against a live
/// job: the same task row blocks and re-queues, no extra task rows
/// appear, and the worker resumes the thread to completion — re-sending
/// the committed input rather than re-rendering.
#[tokio::test]
async fn test_suspend_and_resolve_preserve_job_shape_and_resume_completes() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "suspend-resolve").await;
    let candidates = vec![a_candidate().content("resumed".to_owned()).build()];
    let response_json = extraction_response_json(&candidates, &[]);

    let (job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    // Claim the task by hand and establish its thread through the runtime,
    // exactly as a worker would, then suspend on a timer that has already
    // elapsed so the availability sweep wakes it.
    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "suspend-test")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("find job");

    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &a_routed_definition(tribal_domain::TaskType::Extraction),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");

    let tasks_before = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("tasks before")
        .len();

    let outcome = tribal_agent_runtime::suspend_stage_thread(
        &mut conn,
        &stage_thread.thread,
        task.id(),
        task.claim_token().expect("token"),
        &tribal_domain::AgentThreadSuspension::Timer,
        Some(chrono::Utc::now() - chrono::Duration::seconds(1)),
    )
    .await
    .expect("suspend");
    assert!(matches!(
        outcome,
        tribal_agent_runtime::SuspendOutcome::Suspended
    ));

    let blocked = PgTaskRepository
        .find_by_id(&mut conn, task.id())
        .await
        .expect("find blocked");
    assert_eq!(blocked.status(), TaskStatus::Blocked);
    assert!(blocked.claim_token().is_none());

    // Suspension inserted no extra stage-task rows.
    let tasks_during = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("tasks during")
        .len();
    assert_eq!(tasks_during, tasks_before);
    drop(conn);

    // The live worker's availability sweep wakes the thread; the resumed
    // turn re-sends the committed input and completes the stage.
    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response(&response_json), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let deadline = tokio::time::Instant::now() + MULTI_CYCLE_SETTLE;
    loop {
        let mut probe = raw_conn(&ctx).await;
        let task_now = PgTaskRepository
            .find_by_id(&mut probe, task_id)
            .await
            .expect("probe task");
        if task_now.status() == TaskStatus::Completed {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let thread_now = PgAgentThreadRepository
                .find_by_stage_task_id(&mut probe, task_id)
                .await;
            token.cancel();
            let _ = handle.await;
            panic!("resume never completed; task: {task_now:?}; thread: {thread_now:?}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(&ctx).await;
    let thread = PgAgentThreadRepository
        .find_by_stage_task_id(&mut conn, task_id)
        .await
        .expect("find thread")
        .expect("present");
    assert_eq!(thread.status(), AgentThreadStatus::Completed);

    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread.id())
        .await
        .expect("log");
    let kinds: Vec<_> = records.iter().map(|r| r.kind()).collect();
    assert_eq!(
        kinds,
        [
            AgentThreadRecordKind::Suspension,
            AgentThreadRecordKind::Input,
            AgentThreadRecordKind::Input,
            AgentThreadRecordKind::AssistantMessage,
        ],
        "the log carries the whole story: suspend, wake, render, respond",
    );

    // The completed extraction legitimately fans out one triage task; the
    // suspension and resolution themselves added nothing (asserted above,
    // mid-suspension) and the extraction row is still the only one of its
    // type — the same row blocked, re-queued, and completed.
    let tasks_after = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("tasks after");
    let extraction_rows = tasks_after
        .iter()
        .filter(|t| t.task_type() == TaskType::Extraction)
        .count();
    assert_eq!(
        extraction_rows, 1,
        "suspension and resolution never insert or complete extra stage tasks",
    );
}

/// Suspend-versus-cancel converges in both orderings: an intent written
/// before the suspend refuses the suspend at its boundary, and an intent
/// written after it is honoured by the sweep's cancel fallback.
#[tokio::test]
async fn test_suspend_versus_cancel_converges_in_both_orderings() {
    let ctx = TestDb::new().await;

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "suspend-cancel").await;

    // Ordering one: intent first, suspend refused.
    let (job_a, _) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };
    let mut conn = raw_conn(&ctx).await;
    let task_a = PgTaskRepository
        .claim(&mut conn, 1, "ordering-one")
        .await
        .expect("claim")
        .first()
        .expect("claims")
        .clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_a)
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let thread_a = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task_a,
        task_a.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");

    PgAgentThreadRepository
        .record_cancel_intent(&mut conn, thread_a.thread.id(), "operator:first")
        .await
        .expect("intent");
    let outcome = tribal_agent_runtime::suspend_stage_thread(
        &mut conn,
        &thread_a.thread,
        task_a.id(),
        task_a.claim_token().expect("token"),
        &tribal_domain::AgentThreadSuspension::Timer,
        Some(chrono::Utc::now()),
    )
    .await
    .expect("suspend attempt");
    assert!(
        matches!(
            outcome,
            tribal_agent_runtime::SuspendOutcome::CancelIntervened
        ),
        "an intent written during the suspend window is honoured",
    );
    let still_claimed = PgTaskRepository
        .find_by_id(&mut conn, task_a.id())
        .await
        .expect("find");
    assert_eq!(
        still_claimed.status(),
        TaskStatus::Claimed,
        "the refused suspend rolled its task move back",
    );

    // Ordering two: suspend first, intent after; the cancel fallback
    // terminates the unclaimed suspended thread.
    let (job_b, task_b_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };
    let claimed = PgTaskRepository
        .claim(&mut conn, 2, "ordering-two")
        .await
        .expect("claim");
    let task_b = claimed
        .iter()
        .find(|t| t.id() == task_b_id)
        .expect("the second job's task claims")
        .clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_b)
        .await
        .expect("job");
    let thread_b = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task_b,
        task_b.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");

    tribal_agent_runtime::suspend_stage_thread(
        &mut conn,
        &thread_b.thread,
        task_b.id(),
        task_b.claim_token().expect("token"),
        &tribal_domain::AgentThreadSuspension::Timer,
        None,
    )
    .await
    .expect("suspend");
    PgAgentThreadRepository
        .record_cancel_intent(&mut conn, thread_b.thread.id(), "operator:second")
        .await
        .expect("intent");

    let thread_b_read = PgAgentThreadRepository
        .find_by_id(&mut conn, thread_b.thread.id())
        .await
        .expect("find")
        .expect("present");
    let cancelled_now = tribal_worker::coupling::cancel_thread(&mut conn, &thread_b_read)
        .await
        .expect("cancel fallback");
    assert!(matches!(
        cancelled_now,
        tribal_worker::coupling::CancelThreadOutcome::Cancelled {
            notification: Some(notice),
        } if notice.job_id == job_b && notice.state == tribal_domain::JobState::Failed
    ));

    let cancelled = PgAgentThreadRepository
        .find_by_id(&mut conn, thread_b.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(cancelled.status(), AgentThreadStatus::Cancelled);
    let task_after = PgTaskRepository
        .find_by_id(&mut conn, task_b.id())
        .await
        .expect("find");
    assert_eq!(
        task_after.status(),
        TaskStatus::DeadLetter,
        "a cancelled stage reads as a failed task on the launched surface",
    );

    // The cancellation coupled the job in the same transaction: an
    // extraction thread's cancel fails the job, exactly as a worker
    // dead-letter does.
    let job_after = PgJobRepository
        .find_by_id(&mut conn, job_b)
        .await
        .expect("find job");
    assert_eq!(job_after.status(), JobStatus::Failed);
}

/// Two concurrent resolutions cannot both commit without waking the
/// thread: the row lock serialises them, exactly one wakes it.
#[tokio::test]
async fn test_concurrent_resolutions_wake_the_thread_exactly_once() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "concurrent-resolve").await;
    let (job_id, _) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let mut conn = raw_conn(&ctx).await;
    let task = PgTaskRepository
        .claim(&mut conn, 1, "concurrent-resolve")
        .await
        .expect("claim")
        .first()
        .expect("claims")
        .clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    tribal_agent_runtime::suspend_stage_thread(
        &mut conn,
        &stage_thread.thread,
        task.id(),
        task.claim_token().expect("token"),
        &tribal_domain::AgentThreadSuspension::Timer,
        None,
    )
    .await
    .expect("suspend");
    drop(conn);

    let thread_id = stage_thread.thread.id();
    let resolve = |pool: sqlx::PgPool, label: &'static str| async move {
        let mut conn = pool.acquire().await.expect("acquire");
        tribal_agent_runtime::resolve_stage_thread(
            &mut conn,
            thread_id,
            &serde_json::json!({ "resolver": label }),
        )
        .await
        .expect("resolve")
    };

    let (first, second) = tokio::join!(
        resolve(pool.clone(), "left"),
        resolve(pool.clone(), "right"),
    );

    let woken = [first, second]
        .iter()
        .filter(|outcome| matches!(outcome, tribal_agent_runtime::ResolveOutcome::Woken))
        .count();
    assert_eq!(woken, 1, "exactly one resolution wakes the thread");

    let mut conn = raw_conn(&ctx).await;
    let task_after = PgTaskRepository
        .find_by_id(&mut conn, task.id())
        .await
        .expect("find");
    assert_eq!(task_after.status(), TaskStatus::Queued);

    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread_id)
        .await
        .expect("read the log");
    assert_eq!(
        records.len(),
        2,
        "the loser's arrival rolls back: one suspension, one resolution",
    );
}

/// A zombie worker's input commit fails on ownership, deterministically:
/// the input-record transaction carries the driving task's claim guard.
#[tokio::test]
async fn test_a_stale_lease_cannot_commit_an_input_record() {
    let ctx = TestDb::new().await;

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "zombie-input").await;
    let (job_id, _) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let mut conn = raw_conn(&ctx).await;
    let task = PgTaskRepository
        .claim(&mut conn, 1, "zombie-input")
        .await
        .expect("claim")
        .first()
        .expect("claims")
        .clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");

    let rendered = RenderedConversation {
        system: None,
        messages: vec![],
        system_prompt_version_id: Some(system_pv_id),
        user_prompt_version_id: Some(user_pv_id),
        resolution_context: None,
        response_schema: None,
    };
    let err = tribal_agent_runtime::begin_turn(
        &mut conn,
        &stage_thread.thread,
        tribal_agent_runtime::DrivingClaim::stage(task.id(), uuid::Uuid::new_v4()),
        None,
        rendered,
    )
    .await
    .expect_err("a stale token must be rejected");
    assert!(matches!(
        err,
        tribal_agent_runtime::AgentRuntimeError::LeaseLost { .. }
    ));

    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("log");
    assert!(records.is_empty(), "the rejected commit left no record");
}

/// A stale claimed thread-driving task within its retry budget is
/// re-queued by the thread-aware reclaim without touching its thread.
#[tokio::test]
async fn test_thread_aware_reclaim_requeues_without_touching_the_thread() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "thread-reclaim-requeue").await;
    let (_job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "crashed-worker")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, task.job_id())
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    let status_before = stage_thread.thread.status();
    backdate_task_heartbeat(&mut conn, task_id, STALE_HEARTBEAT_BACKDATE).await;

    let worker = build_test_worker(
        pool.clone(),
        CancellationToken::new(),
        test_config(),
        None,
        None,
    )
    .await;
    let stats = worker
        .run_thread_aware_reclaim(
            10,
            tribal_domain::TaskErrorKind::HeartbeatExpired,
            "heartbeat_expired",
            None,
        )
        .await
        .expect("reclaim");

    assert_eq!(stats.requeued, 1);
    assert_eq!(stats.recovery_cycles, 0);
    assert_eq!(stats.exhausted, 0);

    let task_after = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("task");
    assert_eq!(task_after.status(), TaskStatus::Queued);
    assert_eq!(task_after.retry_count(), 1);
    assert!(task_after.claim_token().is_none());

    let thread_after = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(thread_after.status(), status_before);
    assert_eq!(thread_after.recovery_attempts(), 0);
}

/// Reclaim exhaustion under the production cap dead-letters the thread
/// and its task and fails the job, in one commit — the task never
/// strands a running thread behind it.
#[tokio::test]
async fn test_reclaim_exhaustion_dead_letters_thread_and_task_and_fails_the_job() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let config = test_config();

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "thread-reclaim-exhaust").await;
    let (job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "crashed-worker")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    set_retry_count(&mut conn, task_id, config.task_max_retries).await;
    backdate_task_heartbeat(&mut conn, task_id, STALE_HEARTBEAT_BACKDATE).await;

    let (worker, job_state_txs) =
        build_test_worker_with_watch(pool.clone(), CancellationToken::new(), config, None, None)
            .await;
    let (watch_tx, keepalive_rx) = watch::channel(JobState::Queued);
    let mut watch_rx = watch_tx.subscribe();
    job_state_txs.insert(job_id, JobWatchEntry::new(watch_tx, keepalive_rx));

    let stats = worker
        .run_thread_aware_reclaim(
            10,
            tribal_domain::TaskErrorKind::HeartbeatExpired,
            "heartbeat_expired",
            None,
        )
        .await
        .expect("reclaim");

    assert_eq!(stats.exhausted, 1);
    assert_eq!(
        *watch_rx.borrow_and_update(),
        JobState::Failed,
        "the owed notification reaches watchers after the exhaustion commit",
    );
    assert_eq!(stats.requeued, 0);
    assert_eq!(stats.recovery_cycles, 0);

    let task_after = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("task");
    assert_eq!(task_after.status(), TaskStatus::DeadLetter);

    let thread_after = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(thread_after.status(), AgentThreadStatus::DeadLetter);
    assert!(thread_after.completed_at().is_some());

    let job_after = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    assert_eq!(
        job_after.status(),
        JobStatus::Failed,
        "an extraction exhaustion couples the job in the same commit",
    );
}

/// With recovery budget remaining, an exhausted retry budget opens a
/// fresh cycle: the task re-queues with its consecutive counter reset —
/// never transiting dead-letter — and the thread's cycle counter
/// advances.
#[tokio::test]
async fn test_reclaim_opens_a_recovery_cycle_with_reset_retry_counter() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let config = test_config();

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "thread-reclaim-cycle").await;
    let (_job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "crashed-worker")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, task.job_id())
        .await
        .expect("job");
    // A loop binding earns recovery cycles; one-shot would fail fast, so
    // the recovery path is only reachable under the loop executor.
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition()
            .executor(tribal_domain::StageExecutorKind::BuiltInLoop)
            .build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    let status_before = stage_thread.thread.status();
    set_retry_count(&mut conn, task_id, config.task_max_retries).await;
    backdate_task_heartbeat(&mut conn, task_id, STALE_HEARTBEAT_BACKDATE).await;

    let worker =
        build_test_worker(pool.clone(), CancellationToken::new(), config, None, None).await;
    let stats = worker
        .run_thread_aware_reclaim(
            10,
            tribal_domain::TaskErrorKind::HeartbeatExpired,
            "heartbeat_expired",
            None,
        )
        .await
        .expect("reclaim");

    assert_eq!(stats.recovery_cycles, 1);
    assert_eq!(stats.requeued, 0);
    assert_eq!(stats.exhausted, 0);

    let task_after = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("task");
    assert_eq!(
        task_after.status(),
        TaskStatus::Queued,
        "a recovery cycle never writes task dead-letter",
    );
    assert_eq!(
        task_after.retry_count(),
        0,
        "the consecutive counter resets at cycle start",
    );
    assert!(
        task_after.available_at() > chrono::Utc::now() + chrono::Duration::seconds(1),
        "the cycle's escalating backoff reaches the row, not just the pure function",
    );

    let thread_after = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(thread_after.recovery_attempts(), 1);
    assert_eq!(thread_after.status(), status_before);
}

/// A worker that claims a task whose thread carries a durable cancel
/// intent disposes of it without executing a turn: thread cancelled,
/// task dead-lettered, job coupled. The inference mock would answer a
/// turn successfully, so a completed thread here means the claim-time
/// rules were bypassed.
#[tokio::test]
async fn test_claim_time_disposal_cancels_an_intent_carrying_thread() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "claim-dispose-cancel").await;
    let candidates = vec![a_candidate().content("never sent".to_owned()).build()];
    let response_json = extraction_response_json(&candidates, &[]);

    let (job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    // Fabricate the crash-window state on the still-queued task: its
    // thread exists and carries a durable intent before any claim.
    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "fabricating-worker")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &a_routed_definition(tribal_domain::TaskType::Extraction),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    PgAgentThreadRepository
        .record_cancel_intent(&mut conn, stage_thread.thread.id(), "operator:test")
        .await
        .expect("intent");

    // Release the fabricating claim so the live worker can take the task.
    PgTaskRepository
        .block(
            &mut conn,
            task.id(),
            task.claim_token().expect("claim token"),
        )
        .await
        .expect("block");
    PgTaskRepository
        .requeue_from_blocked(&mut conn, task.id())
        .await
        .expect("requeue");

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response(&response_json), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let token = CancellationToken::new();
    // The sweep's cancel fallback could converge this state and send the
    // notification itself; disabling it pins the claim-time disposal as
    // the only sender.
    let config = WorkerConfig {
        reclaim_interval_ms: 120_000,
        ..test_config()
    };
    let (worker, job_state_txs) =
        build_test_worker_with_watch(pool.clone(), token.clone(), config, Some(inference), None)
            .await;
    let (watch_tx, keepalive_rx) = watch::channel(JobState::Queued);
    let mut watch_rx = watch_tx.subscribe();
    job_state_txs.insert(job_id, JobWatchEntry::new(watch_tx, keepalive_rx));
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    poll_task_status(&pool, task_id, TaskStatus::DeadLetter, POLL_SETTLE).await;
    let _ = poll_job_status(&pool, job_id, JobStatus::Failed, POLL_SETTLE).await;
    tokio::time::timeout(POLL_SETTLE, watch_rx.wait_for(|s| *s == JobState::Failed))
        .await
        .expect("the disposal's owed notification reaches watchers")
        .expect("watch channel stays open");
    token.cancel();
    let _ = handle.await;

    let thread_after = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(
        thread_after.status(),
        AgentThreadStatus::Cancelled,
        "the intent was honoured at the claim boundary, not after a turn",
    );

    let job_after = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    assert_eq!(job_after.status(), JobStatus::Failed);
}

/// A worker that claims a task whose thread is already terminal disposes
/// of the task and couples the job without executing a turn.
#[tokio::test]
async fn test_claim_time_disposal_dead_letters_a_task_with_a_terminal_thread() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "claim-dispose-terminal").await;
    let candidates = vec![a_candidate().content("never sent".to_owned()).build()];
    let response_json = extraction_response_json(&candidates, &[]);

    let (job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    // Fabricate the partial history: the thread reached a terminal
    // status while its task half stayed queued.
    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "fabricating-worker")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    let moved = PgAgentThreadRepository
        .complete(
            &mut conn,
            stage_thread.thread.id(),
            tribal_domain::AgentThreadTerminal::Failed,
            stage_thread.thread.status(),
        )
        .await
        .expect("complete");
    assert_eq!(moved, 1, "the fabricated terminal transition lands");

    // Release the fabricating claim so the live worker can take the task.
    PgTaskRepository
        .block(
            &mut conn,
            task.id(),
            task.claim_token().expect("claim token"),
        )
        .await
        .expect("block");
    PgTaskRepository
        .requeue_from_blocked(&mut conn, task.id())
        .await
        .expect("requeue");

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response(&response_json), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    poll_task_status(&pool, task_id, TaskStatus::DeadLetter, POLL_SETTLE).await;
    let _ = poll_job_status(&pool, job_id, JobStatus::Failed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let thread_after = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(
        thread_after.status(),
        AgentThreadStatus::Failed,
        "disposal leaves the terminal thread untouched",
    );
}

/// A worker that claims a task whose thread reads suspended re-blocks
/// the task and walks away — the defensive arm no committed history
/// reaches.
#[tokio::test]
async fn test_claim_time_disposal_reblocks_a_task_with_a_suspended_thread() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "claim-dispose-suspend").await;
    let candidates = vec![a_candidate().content("never sent".to_owned()).build()];
    let response_json = extraction_response_json(&candidates, &[]);

    let (_job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    // Fabricate the unmodelled state: the thread row alone moves to
    // suspended (no wake-at, no blocked task half).
    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "fabricating-worker")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, task.job_id())
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &a_routed_definition(tribal_domain::TaskType::Extraction),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    let moved = PgAgentThreadRepository
        .suspend(
            &mut conn,
            stage_thread.thread.id(),
            &tribal_domain::AgentThreadSuspension::Timer,
            None,
        )
        .await
        .expect("suspend the thread half");
    assert_eq!(moved, 1, "the fabricated suspension lands");

    // Release the fabricating claim so the live worker can take the task.
    PgTaskRepository
        .block(
            &mut conn,
            task.id(),
            task.claim_token().expect("claim token"),
        )
        .await
        .expect("block");
    PgTaskRepository
        .requeue_from_blocked(&mut conn, task.id())
        .await
        .expect("requeue");

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response(&response_json), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    poll_task_status(&pool, task_id, TaskStatus::Blocked, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let task_after = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("task");
    assert_eq!(task_after.status(), TaskStatus::Blocked);
    assert!(task_after.claim_token().is_none());

    let thread_after = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(thread_after.status(), AgentThreadStatus::Suspended);
}

/// The availability sweep's cancel fallback sends the owed notification:
/// a suspended thread with a pending intent and no live worker is
/// cancelled by the live worker's sweep, and the job-failure state
/// reaches watchers.
#[tokio::test]
async fn test_sweep_cancel_fallback_notifies_watchers() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "sweep-cancel-notify").await;
    let (job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    // A suspended thread with a durable intent and a blocked task: the
    // shape only the sweep's cancel fallback converges.
    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "suspending-worker")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    tribal_agent_runtime::suspend_stage_thread(
        &mut conn,
        &stage_thread.thread,
        task.id(),
        task.claim_token().expect("token"),
        &tribal_domain::AgentThreadSuspension::Timer,
        None,
    )
    .await
    .expect("suspend");
    PgAgentThreadRepository
        .record_cancel_intent(&mut conn, stage_thread.thread.id(), "operator:test")
        .await
        .expect("intent");

    let token = CancellationToken::new();
    let (worker, job_state_txs) =
        build_test_worker_with_watch(pool.clone(), token.clone(), test_config(), None, None).await;
    let (watch_tx, keepalive_rx) = watch::channel(JobState::Queued);
    let mut watch_rx = watch_tx.subscribe();
    job_state_txs.insert(job_id, JobWatchEntry::new(watch_tx, keepalive_rx));
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    tokio::time::timeout(POLL_SETTLE, watch_rx.wait_for(|s| *s == JobState::Failed))
        .await
        .expect("the sweep's owed notification reaches watchers")
        .expect("watch channel stays open");
    poll_task_status(&pool, task_id, TaskStatus::DeadLetter, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let thread_after = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(thread_after.status(), AgentThreadStatus::Cancelled);
}

/// The availability sweep's stuck-relating predicate converges a stranded
/// relation thread in one transaction: a relating job whose suspended
/// relation thread has no live resolver (no wake instant, no descendant) is
/// failed, its blocked task dead-lettered, its thread cancelled, and the
/// failure published. The trigger is seeded by hand, since a relation
/// verifier is deferred and this suspension does not arise naturally in this
/// release; the test exercises the convergence the gate exists to guarantee.
#[tokio::test]
async fn test_stuck_relating_sweep_fails_a_stranded_job_and_notifies() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "stuck-relating-sweep").await;
    let candidates = vec![a_candidate().content("a stranded claim".to_owned()).build()];
    let (job_id, task_id, _) = {
        let mut conn = raw_conn(&ctx).await;
        seed_relation_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
            &candidates,
            &[],
        )
        .await
    };

    // A relation thread suspended on deferred results with no wake instant
    // and no live descendant: the one suspension the terminal stage cannot
    // otherwise converge, and the only one the stuck-relating sweep selects.
    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "stranding-worker")
        .await
        .expect("claim");
    let task = claimed
        .into_iter()
        .find(|t| t.id() == task_id)
        .expect("the relation task claims");
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition()
            .pipeline_stage(tribal_domain::TaskType::Relation)
            .build(),
    )
    .await
    .expect("relation binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    tribal_agent_runtime::suspend_stage_thread(
        &mut conn,
        &stage_thread.thread,
        task.id(),
        task.claim_token().expect("token"),
        &tribal_domain::AgentThreadSuspension::DeferredToolResults {
            requesting_seq: tribal_domain::AgentThreadRecordSeq::FIRST,
            pending_tool_call_ids: vec!["call_submit".to_owned()],
        },
        None,
    )
    .await
    .expect("suspend");

    let token = CancellationToken::new();
    let (worker, job_state_txs) =
        build_test_worker_with_watch(pool.clone(), token.clone(), test_config(), None, None).await;
    let (watch_tx, keepalive_rx) = watch::channel(JobState::Queued);
    let mut watch_rx = watch_tx.subscribe();
    job_state_txs.insert(job_id, JobWatchEntry::new(watch_tx, keepalive_rx));
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    tokio::time::timeout(POLL_SETTLE, watch_rx.wait_for(|s| *s == JobState::Failed))
        .await
        .expect("the stuck-relating sweep's notification reaches watchers")
        .expect("watch channel stays open");
    poll_task_status(&pool, task_id, TaskStatus::DeadLetter, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    assert_eq!(job.status(), JobStatus::Failed);
    let thread_after = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(thread_after.status(), AgentThreadStatus::Cancelled);
}

/// The two reclaim scans partition the stale set: the legacy bulk scan
/// handles only rows with no thread, the thread-aware pass only rows
/// with one. Either predicate's removal makes a scan claim the other's
/// half.
#[tokio::test]
async fn test_reclaim_scans_partition_threaded_and_legacy_rows() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let config = test_config();

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "reclaim-partition").await;
    let mut conn = raw_conn(&ctx).await;
    let (job_threaded, task_threaded) = seed_extraction_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
    )
    .await;
    let (_job_legacy, task_legacy) = seed_extraction_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
    )
    .await;

    let claimed = PgTaskRepository
        .claim(&mut conn, 2, "crashed-worker")
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 2);
    let threaded = claimed
        .iter()
        .find(|t| t.id() == task_threaded)
        .expect("threaded task claims")
        .clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_threaded)
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &threaded,
        threaded.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    // The legacy row gets the deeper backdate so it heads the staleness
    // order: a thread-aware scan that lost its EXISTS clause would lock
    // it first, find no thread, and requeue nothing — failing the count
    // below rather than passing by ordering luck.
    backdate_task_heartbeat(&mut conn, task_threaded, STALE_HEARTBEAT_BACKDATE).await;
    backdate_task_heartbeat(&mut conn, task_legacy, STALE_HEARTBEAT_BACKDATE * 2).await;

    // The thread-aware pass first, against the full mixed population: it
    // must take only the thread-driving row, leaving the legacy row
    // claimed for the bulk scan.
    let worker = build_test_worker(
        pool.clone(),
        CancellationToken::new(),
        config.clone(),
        None,
        None,
    )
    .await;
    let stats = worker
        .run_thread_aware_reclaim(
            10,
            tribal_domain::TaskErrorKind::HeartbeatExpired,
            "heartbeat_expired",
            None,
        )
        .await
        .expect("thread-aware reclaim");
    assert_eq!(
        stats.requeued, 1,
        "the thread-aware pass takes only its half"
    );
    let legacy_after = PgTaskRepository
        .find_by_id(&mut conn, task_legacy)
        .await
        .expect("task");
    assert_eq!(
        legacy_after.status(),
        TaskStatus::Claimed,
        "the thread-aware pass never touches a legacy row",
    );
    let threaded_after = PgTaskRepository
        .find_by_id(&mut conn, task_threaded)
        .await
        .expect("task");
    assert_eq!(threaded_after.status(), TaskStatus::Queued);

    // The legacy bulk scan takes exactly the remaining half.
    let outcome = PgTaskRepository
        .reclaim_stale(
            &mut conn,
            10,
            config.task_max_retries,
            10,
            tribal_domain::TaskErrorKind::HeartbeatExpired,
            "heartbeat_expired",
            None,
        )
        .await
        .expect("legacy reclaim");
    assert_eq!(outcome.requeued, 1, "the legacy scan takes only its half");
    let legacy_after = PgTaskRepository
        .find_by_id(&mut conn, task_legacy)
        .await
        .expect("task");
    assert_eq!(legacy_after.status(), TaskStatus::Queued);
    let thread_after = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(thread_after.recovery_attempts(), 0);
}

/// A retried attempt adopts the committed input record rather than
/// appending a second one: the log ends with exactly one rendered
/// conversation and one assistant message after a transient provider
/// failure between the input commit and the response.
#[tokio::test]
async fn test_resume_adopts_the_committed_input_record() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "resume-adopt").await;
    let candidates = vec![a_candidate().content("resumed".to_owned()).build()];
    let response_json = extraction_response_json(&candidates, &[]);

    let (_job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    // Attempt one commits its input record, then the call fails
    // retryably; attempt two succeeds.
    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete_error(
                || tribal_inference::InferenceError::provider_unavailable("mock", "transient"),
                None,
            )
            .on_complete(a_completion_response(&response_json), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    poll_task_status(&pool, task_id, TaskStatus::Completed, MULTI_CYCLE_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(&ctx).await;
    let thread = PgAgentThreadRepository
        .find_by_stage_task_id(&mut conn, task_id)
        .await
        .expect("find thread")
        .expect("the executed task drives a thread");
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread.id())
        .await
        .expect("read the log");

    let inputs: Vec<_> = records
        .iter()
        .filter(|r| r.kind() == AgentThreadRecordKind::Input)
        .collect();
    assert_eq!(
        inputs.len(),
        1,
        "the resumed attempt adopts the committed input, never appending a second",
    );
    assert_eq!(records.len(), 2, "one input, one assistant message");
    assert_eq!(records[1].kind(), AgentThreadRecordKind::AssistantMessage);
}

/// An existing thread re-established under a different binding executes
/// and reports the binding its row records, not the supplied one.
#[tokio::test]
async fn test_resume_pairs_an_existing_thread_with_its_recorded_binding() {
    let ctx = TestDb::new().await;
    let _pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "recorded-binding").await;
    let (_job_id, _task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "first-config")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, task.job_id())
        .await
        .expect("job");

    let first = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("first binding");
    let created = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &first,
    )
    .await
    .expect("create");
    assert_eq!(created.binding.id(), first.id());

    // A configuration change between attempts: the re-establishment
    // supplies a different binding, but the thread keeps its pin.
    let second = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition()
            .model("changed-model".to_owned())
            .build(),
    )
    .await
    .expect("second binding");
    assert_ne!(first.id(), second.id());

    let resumed = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &second,
    )
    .await
    .expect("resume");

    assert_eq!(
        resumed.binding.id(),
        first.id(),
        "a resumed thread carries the binding its row records",
    );
    assert_eq!(
        resumed.thread.binding_version_id(),
        first.id(),
        "the row's pin is unchanged",
    );
}

/// Reclaim exhaustion dead-letters a thread that never reached running
/// (its queued row is the live status the locked CAS moves from).
#[tokio::test]
async fn test_reclaim_exhaustion_dead_letters_a_thread_that_never_started() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let config = test_config();

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "exhaust-queued").await;
    let (_job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    // A queued thread behind a claimed task: the crash window between
    // the thread insert and the running move.
    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "crashed-worker")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, task.job_id())
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    PgAgentThreadRepository
        .insert(
            &mut conn,
            &tribal_db::NewAgentThread::builder()
                .pipeline_stage(task.task_type())
                .binding_version_id(binding.id())
                .driving_task(tribal_db::DrivingTaskRef::Stage(task.id()))
                .principal_id(job.principal_id())
                .format_version(tribal_domain::AGENT_THREAD_FORMAT_VERSION)
                .build(),
        )
        .await
        .expect("insert the queued thread");
    set_retry_count(&mut conn, task_id, config.task_max_retries).await;
    backdate_task_heartbeat(&mut conn, task_id, STALE_HEARTBEAT_BACKDATE).await;

    let worker =
        build_test_worker(pool.clone(), CancellationToken::new(), config, None, None).await;
    let stats = worker
        .run_thread_aware_reclaim(
            10,
            tribal_domain::TaskErrorKind::HeartbeatExpired,
            "heartbeat_expired",
            None,
        )
        .await
        .expect("reclaim");
    assert_eq!(stats.exhausted, 1);

    let thread = PgAgentThreadRepository
        .find_by_stage_task_id(&mut conn, task_id)
        .await
        .expect("find")
        .expect("present");
    assert_eq!(thread.status(), AgentThreadStatus::DeadLetter);
    assert!(thread.completed_at().is_some());
}

/// The cancel fallback never disposes of a claimed stage task: a live
/// worker owns that boundary, so the whole transaction rolls back.
#[tokio::test]
async fn test_cancel_fallback_skips_a_claimed_stage_task() {
    let ctx = TestDb::new().await;
    let _pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "cancel-skip-claimed").await;
    let (_job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "live-worker")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, task.job_id())
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    PgAgentThreadRepository
        .record_cancel_intent(&mut conn, stage_thread.thread.id(), "operator:test")
        .await
        .expect("intent");

    let thread = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    let outcome = tribal_worker::coupling::cancel_thread(&mut conn, &thread)
        .await
        .expect("cancel attempt");

    assert!(matches!(
        outcome,
        tribal_worker::coupling::CancelThreadOutcome::Skipped
    ));
    let task_after = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("task");
    assert_eq!(task_after.status(), TaskStatus::Claimed);
    let thread_after = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(thread_after.status(), AgentThreadStatus::Running);
}

/// A one-shot terminal against a thread that left running (swept,
/// cancelled) refuses with a status CAS miss, so the enclosing commit
/// rolls back rather than completing a task whose thread is gone.
#[tokio::test]
async fn test_one_shot_terminal_refuses_a_non_running_thread() {
    let ctx = TestDb::new().await;
    let _pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "terminal-cas").await;
    let (_job_id, _task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "terminal-cas")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, task.job_id())
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");

    // The thread is cancelled out from under the worker's snapshot.
    let moved = PgAgentThreadRepository
        .complete(
            &mut conn,
            stage_thread.thread.id(),
            tribal_domain::AgentThreadTerminal::Cancelled,
            AgentThreadStatus::Running,
        )
        .await
        .expect("cancel");
    assert_eq!(moved, 1);

    let response = a_completion_response("late");
    let err =
        tribal_agent_runtime::commit_one_shot_terminal(&mut conn, &stage_thread.thread, &response)
            .await
            .expect_err("a terminal against a non-running thread must refuse");
    assert!(matches!(
        err,
        tribal_agent_runtime::AgentRuntimeError::StatusCasMissed { .. }
    ));
}

/// The claim guard at thread creation: a worker presenting a token the
/// lease does not hold cannot create a thread for the task, and the
/// refusal leaves no thread row behind.
#[tokio::test]
async fn test_ensure_stage_thread_refuses_a_stale_claim() {
    let ctx = TestDb::new().await;
    let _pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "stale-claim").await;
    let (_job_id, _task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "stale-claim")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, task.job_id())
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");

    let result = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        uuid::Uuid::new_v4(),
        &binding,
    )
    .await;
    let Err(err) = result else {
        panic!("a stale claim must not create a thread");
    };
    assert!(matches!(
        err,
        tribal_agent_runtime::AgentRuntimeError::LeaseLost { .. }
    ));

    assert!(
        PgAgentThreadRepository
            .find_by_stage_task_id(&mut conn, task.id())
            .await
            .expect("find")
            .is_none(),
        "the refused creation left no thread row",
    );
}

/// Inline retry exhaustion runs the same disposition as the reclaim
/// sweep: the task dead-letters, its thread dead-letters in the same
/// commit, and the job fails.
#[tokio::test]
async fn test_inline_failure_exhaustion_dead_letters_the_thread() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let config = test_config();

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "inline-exhaust").await;
    let (job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        let ids = seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await;
        // Extraction is one-shot, so the next failure exhausts the retry
        // budget straight into dead-letter: a one-shot has no committed
        // progress to recover to, so the recovery cap is zero for it.
        set_retry_count(&mut conn, ids.1, config.task_max_retries).await;
        ids
    };

    // Every call fails retryably; the None default stub errors.
    let token = CancellationToken::new();
    let worker = build_test_worker(pool.clone(), token.clone(), config, None, None).await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    poll_task_status(&pool, task_id, TaskStatus::DeadLetter, POLL_SETTLE).await;
    let _ = poll_job_status(&pool, job_id, JobStatus::Failed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(&ctx).await;
    let thread = PgAgentThreadRepository
        .find_by_stage_task_id(&mut conn, task_id)
        .await
        .expect("find")
        .expect("the executed task drives a thread");
    assert_eq!(
        thread.status(),
        AgentThreadStatus::DeadLetter,
        "the inline exhaustion maps the retryable class to a dead-lettered thread",
    );
    assert!(thread.completed_at().is_some());
}

/// An arrival at a terminal thread is recorded and discarded: the log
/// grows, nothing re-enqueues, and the durable arrival survives.
#[tokio::test]
async fn test_resolution_at_a_terminal_thread_is_recorded_and_discarded() {
    let ctx = TestDb::new().await;
    let _pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "resolve-terminal").await;
    let (_job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "resolve-terminal")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, task.job_id())
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    let moved = PgAgentThreadRepository
        .complete(
            &mut conn,
            stage_thread.thread.id(),
            tribal_domain::AgentThreadTerminal::Cancelled,
            AgentThreadStatus::Running,
        )
        .await
        .expect("cancel");
    assert_eq!(moved, 1);

    let before = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("log")
        .len();

    let outcome = tribal_agent_runtime::resolve_stage_thread(
        &mut conn,
        stage_thread.thread.id(),
        &serde_json::json!({ "cause": "late arrival" }),
    )
    .await
    .expect("resolve");

    assert!(matches!(
        outcome,
        tribal_agent_runtime::ResolveOutcome::RecordedAtTerminal
    ));
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("log");
    assert_eq!(
        records.len(),
        before + 1,
        "the durable arrival is recorded even though it re-enqueues nothing",
    );
    let task_after = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("task");
    assert_eq!(
        task_after.status(),
        TaskStatus::Claimed,
        "a terminal arrival never touches the task",
    );
}

/// A resolution against a running thread commits nothing: no record, no
/// task move.
#[tokio::test]
async fn test_resolution_at_a_running_thread_commits_nothing() {
    let ctx = TestDb::new().await;
    let _pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(&ctx, "resolve-running").await;
    let (_job_id, task_id) = {
        let mut conn = raw_conn(&ctx).await;
        seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await
    };

    let mut conn = raw_conn(&ctx).await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "resolve-running")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, task.job_id())
        .await
        .expect("job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");

    let outcome = tribal_agent_runtime::resolve_stage_thread(
        &mut conn,
        stage_thread.thread.id(),
        &serde_json::json!({ "cause": "premature" }),
    )
    .await
    .expect("resolve");

    assert!(matches!(
        outcome,
        tribal_agent_runtime::ResolveOutcome::NotSuspended
    ));
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("log");
    assert!(
        records.is_empty(),
        "the premature arrival's append rolls back with its transaction",
    );
    let task_after = PgTaskRepository
        .find_by_id(&mut conn, task_id)
        .await
        .expect("task");
    assert_eq!(task_after.status(), TaskStatus::Claimed);
}
