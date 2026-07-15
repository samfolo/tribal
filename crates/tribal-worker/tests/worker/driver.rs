//! Integration tests for the driver-family transitions, driven directly
//! through the runtime against a live job: the suspend-with-child pair
//! writer, the child terminal's hand-back, deferred death, and the
//! orphan window where a child resolves into a parent no longer waiting.

use tribal_agent_runtime::{
    ChildLaunch, ChildTerminalOutcome, ParentResolution, RecordedMessage, RenderedConversation,
    SuspendWithChildOutcome, commit_child_terminal, commit_deferred_death, resolve_binding,
    suspend_with_child, verdict_schema,
};
use tribal_db::{
    AgentDriverTaskRepository, AgentThreadRecordRepository, AgentThreadRepository,
    NewAgentThreadRecord, PgAgentDriverTaskRepository, PgAgentThreadRecordRepository,
    PgAgentThreadRepository,
};
use tribal_domain::{
    AGENT_THREAD_FORMAT_VERSION, AgentDefinition, AgentDriverTaskId, AgentDriverTaskState,
    AgentThread, AgentThreadRecordKind, AgentThreadRecordSeq, AgentThreadStatus,
    AgentThreadSuspension, AgentThreadTerminal,
};

use super::common::*;

/// The submit call id the parent's pending tool call carries.
const SUBMIT_CALL_ID: &str = "call_submit";

/// A suspended parent on a pending submit call, its launched child, and
/// the driver task driving that child.
struct SuspendedParent {
    parent: AgentThread,
    stage_task: tribal_domain::Task,
    child_thread_id: tribal_domain::AgentThreadId,
    driver_task_id: AgentDriverTaskId,
    requesting_seq: AgentThreadRecordSeq,
}

/// Seeds a running stage thread, gives it an assistant record bearing a
/// submit call, then suspends it with a child whose recorded route
/// matches the triage stage spec, as a freshly launched verifier's would.
async fn seed_suspended_parent(
    ctx: &TestDb,
    suffix: &str,
) -> (sqlx::PgConnection, SuspendedParent) {
    seed_suspended_parent_with_child(
        ctx,
        suffix,
        a_routed_definition(tribal_domain::TaskType::Triage),
    )
    .await
}

/// Like [`seed_suspended_parent`], with the child's recorded binding
/// definition supplied so a test can diverge its route from the stage
/// spec.
async fn seed_suspended_parent_with_child(
    ctx: &TestDb,
    suffix: &str,
    child_definition: AgentDefinition,
) -> (sqlx::PgConnection, SuspendedParent) {
    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, suffix).await;
    let mut conn = raw_conn(ctx).await;
    let (job_id, _task_id) = seed_extraction_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
    )
    .await;

    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "driver-test")
        .await
        .expect("claim");
    let task = claimed.first().expect("claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("find job");
    let binding = resolve_binding(&mut conn, &tribal_test_utils::an_agent_definition().build())
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

    // The parent's assistant record, bearing the submit call the child
    // answers. Its seq is the requesting_seq the hand-back fences on.
    let seq = PgAgentThreadRecordRepository
        .next_seq(&mut conn, stage_thread.thread.id())
        .await
        .expect("seq");
    PgAgentThreadRecordRepository
        .append(
            &mut conn,
            &NewAgentThreadRecord::builder()
                .thread_id(stage_thread.thread.id())
                .seq(seq)
                .kind(AgentThreadRecordKind::AssistantMessage)
                .content(serde_json::json!({
                    "text": "",
                    "tool_calls": [{"id": SUBMIT_CALL_ID, "name": "submit_result", "arguments": {}}],
                }))
                .build(),
        )
        .await
        .expect("assistant record");

    let child_stage = child_definition.pipeline_stage;
    let child_binding = resolve_binding(&mut conn, &child_definition)
        .await
        .expect("child binding");

    let outcome = suspend_with_child(
        &mut conn,
        &stage_thread.thread,
        task.id(),
        task.claim_token().expect("token"),
        seq,
        SUBMIT_CALL_ID,
        ChildLaunch {
            pipeline_stage: child_stage,
            binding_version_id: child_binding.id(),
            principal_id,
            job_id: None,
            format_version: AGENT_THREAD_FORMAT_VERSION,
        },
        &a_child_input(),
    )
    .await
    .expect("suspend with child");
    let SuspendWithChildOutcome::Launched(launched) = outcome else {
        panic!("the parent suspends and launches, got {outcome:?}");
    };

    (
        conn,
        SuspendedParent {
            parent: stage_thread.thread,
            stage_task: task,
            child_thread_id: launched.thread_id,
            driver_task_id: launched.driver_task_id,
            requesting_seq: seq,
        },
    )
}

/// Claims the child's driver task and marks the child running, exactly
/// as the driver loop would before executing it. Returns the driver
/// claim token.
async fn claim_and_run_child(conn: &mut sqlx::PgConnection, sp: &SuspendedParent) -> uuid::Uuid {
    let claimed = PgAgentDriverTaskRepository
        .claim(conn, 1, "driver-loop-test")
        .await
        .expect("claim driver task");
    assert_eq!(claimed.len(), 1, "the child's driver task is claimable");
    assert_eq!(claimed[0].id(), sp.driver_task_id);
    let token = claimed[0].claim_token().expect("token");
    PgAgentThreadRepository
        .mark_running(conn, sp.child_thread_id, AgentThreadStatus::Queued)
        .await
        .expect("mark child running");
    token
}

/// The child's opening, as a verifier's caller would render it.
fn a_child_input() -> RenderedConversation {
    RenderedConversation {
        system: Some("verify the submission".to_owned()),
        messages: vec![RecordedMessage {
            role: "user".to_owned(),
            content: "the submission to verify".to_owned(),
        }],
        system_prompt_version_id: None,
        user_prompt_version_id: None,
        resolution_context: None,
        response_schema: Some(verdict_schema()),
    }
}

fn a_parent_resolution(sp: &SuspendedParent) -> ParentResolution {
    ParentResolution {
        thread_id: sp.parent.id(),
        requesting_seq: sp.requesting_seq,
        tool_call_id: SUBMIT_CALL_ID.to_owned(),
    }
}

async fn child(conn: &mut sqlx::PgConnection, id: tribal_domain::AgentThreadId) -> AgentThread {
    PgAgentThreadRepository
        .find_by_id(conn, id)
        .await
        .expect("find child")
        .expect("present")
}

#[tokio::test]
async fn test_suspend_with_child_writes_the_pair_atomically() {
    let ctx = TestDb::new().await;
    let (mut conn, sp) = seed_suspended_parent(&ctx, "suspend-child").await;

    // The parent suspended on the pending call, its stage task blocked.
    let parent = PgAgentThreadRepository
        .find_by_id(&mut conn, sp.parent.id())
        .await
        .expect("find parent")
        .expect("present");
    assert_eq!(parent.status(), AgentThreadStatus::Suspended);
    assert!(matches!(
        parent.suspension(),
        Some(AgentThreadSuspension::DeferredToolResults { pending_tool_call_ids, requesting_seq })
            if pending_tool_call_ids == &[SUBMIT_CALL_ID.to_owned()]
                && *requesting_seq == sp.requesting_seq
    ));
    let stage_task = PgTaskRepository
        .find_by_id(&mut conn, sp.stage_task.id())
        .await
        .expect("find task");
    assert_eq!(stage_task.status(), TaskStatus::Blocked);

    // The child: queued, driver-driven, lineage to the parent.
    let child = child(&mut conn, sp.child_thread_id).await;
    assert_eq!(child.status(), AgentThreadStatus::Queued);
    assert_eq!(child.parent_thread_id(), Some(sp.parent.id()));
    assert_eq!(child.driver_task_id(), Some(sp.driver_task_id));
    assert!(child.stage_task_id().is_none());

    // The paired driver task: pending, immediately claimable.
    let driver = PgAgentDriverTaskRepository
        .find_by_id(&mut conn, sp.driver_task_id)
        .await
        .expect("find driver task")
        .expect("present");
    assert_eq!(driver.state(), AgentDriverTaskState::Pending);
    assert_eq!(driver.thread_id(), sp.child_thread_id);
}

#[tokio::test]
async fn test_child_terminal_hands_the_verdict_back_to_the_parent() {
    let ctx = TestDb::new().await;
    let (mut conn, sp) = seed_suspended_parent(&ctx, "hand-back").await;
    let token = claim_and_run_child(&mut conn, &sp).await;

    let response = a_completion_response(r#"{"accepted": true, "critique": null}"#);
    let child_thread = child(&mut conn, sp.child_thread_id).await;
    let outcome = commit_child_terminal(
        &mut conn,
        &child_thread,
        sp.driver_task_id,
        token,
        0,
        &response,
        &a_parent_resolution(&sp),
        noop_recorder().as_ref(),
    )
    .await
    .expect("child terminal");
    assert_eq!(outcome, ChildTerminalOutcome::HandedBack);

    // The child completed with its assistant record; the driver task
    // completed.
    let child = child(&mut conn, sp.child_thread_id).await;
    assert_eq!(child.status(), AgentThreadStatus::Completed);
    let child_records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, sp.child_thread_id)
        .await
        .expect("child log");
    assert!(
        child_records
            .iter()
            .any(|r| r.kind() == AgentThreadRecordKind::AssistantMessage),
        "the child's response committed",
    );
    let driver = PgAgentDriverTaskRepository
        .find_by_id(&mut conn, sp.driver_task_id)
        .await
        .expect("find driver")
        .expect("present");
    assert_eq!(driver.state(), AgentDriverTaskState::Completed);

    // The parent woke, its stage task re-queued, the verdict fenced as a
    // tool result against the submit call.
    let parent = PgAgentThreadRepository
        .find_by_id(&mut conn, sp.parent.id())
        .await
        .expect("find parent")
        .expect("present");
    assert_eq!(parent.status(), AgentThreadStatus::Running);
    let stage_task = PgTaskRepository
        .find_by_id(&mut conn, sp.stage_task.id())
        .await
        .expect("find task");
    assert_eq!(stage_task.status(), TaskStatus::Queued);
    let parent_records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, sp.parent.id())
        .await
        .expect("parent log");
    let result = parent_records
        .iter()
        .find(|r| r.kind() == AgentThreadRecordKind::ToolResult)
        .expect("the hand-back committed a tool result");
    assert_eq!(result.tool_call_id(), Some(SUBMIT_CALL_ID));
    assert_eq!(result.requesting_seq(), Some(sp.requesting_seq));
    assert_eq!(result.content()["is_error"], false);
    assert_eq!(
        result.content()["output"],
        r#"{"accepted": true, "critique": null}"#
    );
}

#[tokio::test]
async fn test_a_stale_driver_token_rolls_the_whole_terminal_back() {
    let ctx = TestDb::new().await;
    let (mut conn, sp) = seed_suspended_parent(&ctx, "stale-token").await;
    let _token = claim_and_run_child(&mut conn, &sp).await;

    // A reclaimed run presents a stale token: the driver-task completion
    // affects zero rows and the whole hand-back rolls back.
    let response = a_completion_response(r#"{"accepted": true}"#);
    let child_thread = child(&mut conn, sp.child_thread_id).await;
    let err = commit_child_terminal(
        &mut conn,
        &child_thread,
        sp.driver_task_id,
        uuid::Uuid::new_v4(),
        0,
        &response,
        &a_parent_resolution(&sp),
        noop_recorder().as_ref(),
    )
    .await
    .expect_err("a stale driver token is rejected");
    assert!(matches!(
        err,
        tribal_agent_runtime::AgentRuntimeError::LeaseLost { .. }
    ));

    // Nothing committed: the parent is still suspended, the child still
    // running, no tool result appeared.
    let parent = PgAgentThreadRepository
        .find_by_id(&mut conn, sp.parent.id())
        .await
        .expect("find parent")
        .expect("present");
    assert_eq!(parent.status(), AgentThreadStatus::Suspended);
    let parent_records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, sp.parent.id())
        .await
        .expect("parent log");
    assert!(
        parent_records
            .iter()
            .all(|r| r.kind() != AgentThreadRecordKind::ToolResult),
        "the rolled-back terminal left no hand-back",
    );
}

#[tokio::test]
async fn test_a_hand_back_rolls_back_when_the_parent_task_is_not_blocked() {
    let ctx = TestDb::new().await;
    let (mut conn, sp) = seed_suspended_parent(&ctx, "not-blocked").await;
    let token = claim_and_run_child(&mut conn, &sp).await;

    // The unmodelled pairing the hand-back guards against: a suspended
    // parent whose stage task is not blocked. Moving the task out of
    // blocked makes the hand-back's requeue find zero rows.
    PgTaskRepository
        .requeue_from_blocked(&mut conn, sp.stage_task.id())
        .await
        .expect("un-block the parent task");

    let response = a_completion_response(r#"{"accepted": true}"#);
    let child_thread = child(&mut conn, sp.child_thread_id).await;
    let err = commit_child_terminal(
        &mut conn,
        &child_thread,
        sp.driver_task_id,
        token,
        0,
        &response,
        &a_parent_resolution(&sp),
        noop_recorder().as_ref(),
    )
    .await
    .expect_err("a non-blocked parent task rolls the hand-back back");
    assert!(matches!(
        err,
        tribal_agent_runtime::AgentRuntimeError::DrivingTaskNotBlocked { .. }
    ));

    // The whole wake rolled back: the parent stays suspended, no hand-back
    // tool result appeared.
    let parent = PgAgentThreadRepository
        .find_by_id(&mut conn, sp.parent.id())
        .await
        .expect("find parent")
        .expect("present");
    assert_eq!(parent.status(), AgentThreadStatus::Suspended);
    let parent_records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, sp.parent.id())
        .await
        .expect("parent log");
    assert!(
        parent_records
            .iter()
            .all(|r| r.kind() != AgentThreadRecordKind::ToolResult),
        "the rolled-back hand-back left no tool result",
    );
}

#[tokio::test]
async fn test_child_terminal_discards_when_the_parent_is_no_longer_waiting() {
    let ctx = TestDb::new().await;
    let (mut conn, sp) = seed_suspended_parent(&ctx, "orphan").await;
    let token = claim_and_run_child(&mut conn, &sp).await;

    // The orphan window: the parent reaches a terminal (a cascade
    // cancel, say) while the child runs. The child's terminal must
    // complete the child and driver and discard the hand-back under the
    // parent row lock: never resurrecting the parent.
    let moved = PgAgentThreadRepository
        .complete(
            &mut conn,
            sp.parent.id(),
            AgentThreadTerminal::Cancelled,
            AgentThreadStatus::Suspended,
        )
        .await
        .expect("cancel the parent");
    assert_eq!(moved, 1);

    let response = a_completion_response(r#"{"accepted": true}"#);
    let child_thread = child(&mut conn, sp.child_thread_id).await;
    let outcome = commit_child_terminal(
        &mut conn,
        &child_thread,
        sp.driver_task_id,
        token,
        0,
        &response,
        &a_parent_resolution(&sp),
        noop_recorder().as_ref(),
    )
    .await
    .expect("child terminal against a terminal parent");
    assert_eq!(outcome, ChildTerminalOutcome::ParentNotWaiting);

    // The child and driver still completed; the parent stayed cancelled
    // with no hand-back record.
    let child = child(&mut conn, sp.child_thread_id).await;
    assert_eq!(child.status(), AgentThreadStatus::Completed);
    let driver = PgAgentDriverTaskRepository
        .find_by_id(&mut conn, sp.driver_task_id)
        .await
        .expect("find driver")
        .expect("present");
    assert_eq!(driver.state(), AgentDriverTaskState::Completed);
    let parent = PgAgentThreadRepository
        .find_by_id(&mut conn, sp.parent.id())
        .await
        .expect("find parent")
        .expect("present");
    assert_eq!(parent.status(), AgentThreadStatus::Cancelled);
    let parent_records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, sp.parent.id())
        .await
        .expect("parent log");
    assert!(
        parent_records
            .iter()
            .all(|r| r.kind() != AgentThreadRecordKind::ToolResult),
        "an orphaned hand-back commits no parent record",
    );
}

/// The live driver loop claims the child's `Drive` task, executes the
/// child one-shot against the mock, and hands the verdict back. The
/// parent wakes and its stage task re-queues, all without the main
/// claim loop running.
#[tokio::test]
async fn test_the_driver_loop_drives_a_child_and_hands_back() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let (conn, sp) = seed_suspended_parent(&ctx, "driver-loop").await;
    drop(conn);

    let provider = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response(r#"{"accepted": true, "critique": null}"#),
                None,
            )
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let inference: Arc<dyn InferenceProvider> = Arc::clone(&provider) as Arc<dyn InferenceProvider>;
    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
    )
    .await;

    // Only the driver loop runs: the parent's re-queued stage task then
    // sits idle, so the hand-back is observable without the main loop
    // re-running the parent.
    let driver = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run_driver_loop().await })
    };

    let driver_task_id = sp.driver_task_id;
    poll_until(
        "the child's driver task completes",
        POLL_INTERVAL,
        POLL_SETTLE,
        || {
            let pool = pool.clone();
            async move {
                let mut conn = pool.acquire().await.ok()?;
                let task = PgAgentDriverTaskRepository
                    .find_by_id(&mut conn, driver_task_id)
                    .await
                    .ok()??;
                (task.state() == AgentDriverTaskState::Completed).then_some(())
            }
        },
    )
    .await;
    token.cancel();
    let _ = driver.await;

    let mut conn = raw_conn(&ctx).await;
    let parent = PgAgentThreadRepository
        .find_by_id(&mut conn, sp.parent.id())
        .await
        .expect("find parent")
        .expect("present");
    assert_eq!(parent.status(), AgentThreadStatus::Running);
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, sp.parent.id())
        .await
        .expect("parent log");
    let result = records
        .iter()
        .find(|r| r.kind() == AgentThreadRecordKind::ToolResult)
        .expect("the loop handed a verdict back");
    assert_eq!(result.tool_call_id(), Some(SUBMIT_CALL_ID));
    assert_eq!(
        result.content()["output"],
        r#"{"accepted": true, "critique": null}"#,
        "the child's raw verdict is handed back as the tool-result output",
    );

    let child = child(&mut conn, sp.child_thread_id).await;
    assert_eq!(child.status(), AgentThreadStatus::Completed);

    // The driver adopted the child's committed conversation and re-sent it
    // verbatim: the verifier's rubric reached the model and the call was
    // constrained to the verdict schema. Without the adoption the child
    // would call the model with an empty placeholder and no schema.
    let requests = provider.completion_history();
    assert_eq!(requests.len(), 1, "the child ran one inference call");
    assert_eq!(
        requests[0].system.as_deref(),
        Some("verify the submission"),
        "the child's call carries the verifier rubric, not a placeholder",
    );
    assert!(
        matches!(
            requests[0].response_format,
            Some(tribal_inference::ResponseFormat::JsonSchema { .. })
        ),
        "the verdict schema constrains the child's structured output",
    );
}

/// A child whose recorded route diverges from its stage spec (a config
/// edit moved the route after the child was admitted) fails terminal
/// before any model call: the divergence crosses into the parent as an
/// error tool-result, and the gateway is never reached.
#[tokio::test]
async fn test_the_driver_loop_fails_a_route_diverged_child_before_the_call() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");
    // The default factory's route diverges from the triage stage spec the
    // test worker resolves, standing in for a post-admission config edit.
    let (conn, sp) = seed_suspended_parent_with_child(
        &ctx,
        "route-diverged",
        tribal_test_utils::an_agent_definition()
            .pipeline_stage(tribal_domain::TaskType::Triage)
            .build(),
    )
    .await;
    drop(conn);

    let provider = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response(r#"{"accepted": true, "critique": null}"#),
                None,
            )
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let inference: Arc<dyn InferenceProvider> = Arc::clone(&provider) as Arc<dyn InferenceProvider>;
    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
    )
    .await;

    let driver = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run_driver_loop().await })
    };

    let driver_task_id = sp.driver_task_id;
    poll_until(
        "the route-diverged child's driver task dead-letters",
        POLL_INTERVAL,
        POLL_SETTLE,
        || {
            let pool = pool.clone();
            async move {
                let mut conn = pool.acquire().await.ok()?;
                let task = PgAgentDriverTaskRepository
                    .find_by_id(&mut conn, driver_task_id)
                    .await
                    .ok()??;
                (task.state() == AgentDriverTaskState::DeadLetter).then_some(())
            }
        },
    )
    .await;
    token.cancel();
    let _ = driver.await;

    // The guard fired before the call, so the gateway was never reached.
    assert!(
        provider.completion_history().is_empty(),
        "the route-diverged child never calls the model",
    );

    let mut conn = raw_conn(&ctx).await;
    let child = child(&mut conn, sp.child_thread_id).await;
    assert_eq!(child.status(), AgentThreadStatus::DeadLetter);

    // The divergence crossed into the parent as an error tool-result, and
    // the parent woke to face it.
    let parent = PgAgentThreadRepository
        .find_by_id(&mut conn, sp.parent.id())
        .await
        .expect("find parent")
        .expect("present");
    assert_eq!(parent.status(), AgentThreadStatus::Running);
    let parent_records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, sp.parent.id())
        .await
        .expect("parent log");
    let result = parent_records
        .iter()
        .find(|r| r.kind() == AgentThreadRecordKind::ToolResult)
        .expect("the divergence committed an error tool result");
    assert_eq!(result.content()["is_error"], true);
    assert_eq!(result.tool_call_id(), Some(SUBMIT_CALL_ID));
}

/// A child carrying a cancellation intent (the §10.3 cascade, or an
/// operator) is refused before any model call: the driver hands the
/// cancellation back through deferred death rather than spend a paid turn.
#[tokio::test]
async fn test_the_driver_loop_refuses_a_cancelled_child_before_the_call() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");
    let (mut conn, sp) = seed_suspended_parent(&ctx, "cancelled-child").await;
    PgAgentThreadRepository
        .record_cancel_intent(&mut conn, sp.child_thread_id, "operator:test")
        .await
        .expect("record the child's cancel intent");
    drop(conn);

    let provider = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response(r#"{"accepted": true, "critique": null}"#),
                None,
            )
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let inference: Arc<dyn InferenceProvider> = Arc::clone(&provider) as Arc<dyn InferenceProvider>;
    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
    )
    .await;

    let driver = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run_driver_loop().await })
    };

    let driver_task_id = sp.driver_task_id;
    poll_until(
        "the cancelled child's driver task dead-letters",
        POLL_INTERVAL,
        POLL_SETTLE,
        || {
            let pool = pool.clone();
            async move {
                let mut conn = pool.acquire().await.ok()?;
                let task = PgAgentDriverTaskRepository
                    .find_by_id(&mut conn, driver_task_id)
                    .await
                    .ok()??;
                (task.state() == AgentDriverTaskState::DeadLetter).then_some(())
            }
        },
    )
    .await;
    token.cancel();
    let _ = driver.await;

    assert!(
        provider.completion_history().is_empty(),
        "a cancelled child never calls the model",
    );

    let mut conn = raw_conn(&ctx).await;
    let child = child(&mut conn, sp.child_thread_id).await;
    assert_eq!(child.status(), AgentThreadStatus::DeadLetter);

    // The cancellation crossed back to the still-suspended parent as an
    // error tool-result, so the parent is not stranded waiting on a child
    // that will never run.
    let parent = PgAgentThreadRepository
        .find_by_id(&mut conn, sp.parent.id())
        .await
        .expect("find parent")
        .expect("present");
    assert_eq!(parent.status(), AgentThreadStatus::Running);
    let parent_records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, sp.parent.id())
        .await
        .expect("parent log");
    let result = parent_records
        .iter()
        .find(|r| r.kind() == AgentThreadRecordKind::ToolResult)
        .expect("the cancellation committed an error tool result");
    assert_eq!(result.content()["is_error"], true);
}

/// Cancelling a parent with a queued verifier child cascades the intent to
/// the child and, on the cancel that follows, disposes the child's
/// unclaimed driver task so it never starts a paid call.
#[tokio::test]
async fn test_cancelling_a_parent_cascades_to_and_disposes_a_queued_child() {
    let ctx = TestDb::new().await;
    let (mut conn, sp) = seed_suspended_parent(&ctx, "cascade-queued-child").await;

    let outcome = tribal_worker::coupling::cancel_thread(&mut conn, &sp.parent)
        .await
        .expect("cancel the parent");
    assert!(matches!(
        outcome,
        tribal_worker::coupling::CancelThreadOutcome::Cancelled { .. }
    ));

    let parent = PgAgentThreadRepository
        .find_by_id(&mut conn, sp.parent.id())
        .await
        .expect("find parent")
        .expect("present");
    assert_eq!(parent.status(), AgentThreadStatus::Cancelled);

    // The post-commit cascade marked the queued child.
    let cascaded_child = child(&mut conn, sp.child_thread_id).await;
    assert!(
        cascaded_child.cancel_requested_at().is_some(),
        "the cancellation cascaded to the live child",
    );

    // The cancel that the fallback then runs disposes the child and its
    // still-unclaimed driver task in one transaction.
    let outcome = tribal_worker::coupling::cancel_thread(&mut conn, &cascaded_child)
        .await
        .expect("cancel the child");
    assert!(matches!(
        outcome,
        tribal_worker::coupling::CancelThreadOutcome::Cancelled { .. }
    ));
    let cancelled_child = child(&mut conn, sp.child_thread_id).await;
    assert_eq!(cancelled_child.status(), AgentThreadStatus::Cancelled);
    let driver_task = PgAgentDriverTaskRepository
        .find_by_id(&mut conn, sp.driver_task_id)
        .await
        .expect("find driver task")
        .expect("present");
    assert_eq!(
        driver_task.state(),
        AgentDriverTaskState::DeadLetter,
        "the child's unclaimed driver task is disposed with it",
    );
}

/// A `DeferredTool` driver task has no producer in this release; the
/// driver loop matches it exhaustively and dead-letters it loudly rather
/// than leaving it unexecuted.
#[tokio::test]
async fn test_the_driver_loop_dead_letters_a_deferred_tool_task() {
    let ctx = TestDb::new().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let driver_task_id = {
        let mut conn = raw_conn(&ctx).await;
        let (principal_id, project_id, system_pv_id, user_pv_id) =
            setup_prerequisites(&ctx, "deferred-tool").await;
        let (job_id, _) = seed_extraction_job(
            &mut conn,
            principal_id,
            project_id,
            system_pv_id,
            user_pv_id,
        )
        .await;
        let claimed = PgTaskRepository
            .claim(&mut conn, 1, "dt-seed")
            .await
            .expect("claim");
        let task = claimed.first().expect("claims").clone();
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
        PgAgentDriverTaskRepository
            .insert(
                &mut conn,
                &tribal_db::NewAgentDriverTask::builder()
                    .thread_id(stage_thread.thread.id())
                    .kind(tribal_domain::AgentDriverTaskKind::DeferredTool)
                    .build(),
            )
            .await
            .expect("enrol deferred-tool task")
            .id()
    };

    let token = CancellationToken::new();
    let worker = build_test_worker(pool.clone(), token.clone(), test_config(), None, None).await;
    let driver = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run_driver_loop().await })
    };

    let dead_lettered = poll_until(
        "the deferred-tool task dead-letters",
        POLL_INTERVAL,
        POLL_SETTLE,
        || {
            let pool = pool.clone();
            async move {
                let mut conn = pool.acquire().await.ok()?;
                let task = PgAgentDriverTaskRepository
                    .find_by_id(&mut conn, driver_task_id)
                    .await
                    .ok()??;
                (task.state() == AgentDriverTaskState::DeadLetter).then_some(task)
            }
        },
    )
    .await;
    token.cancel();
    let _ = driver.await;

    assert!(
        dead_lettered.last_error().is_some(),
        "the dead-letter records why",
    );
}

/// Prune refuses a terminal parent while its real `suspend_with_child`
/// child is live.
#[tokio::test]
async fn test_prune_refuses_a_terminal_parent_with_a_live_real_child() {
    let ctx = TestDb::new().await;
    let (mut conn, sp) = seed_suspended_parent(&ctx, "prune-real-pair").await;

    PgAgentThreadRepository
        .complete(
            &mut conn,
            sp.parent.id(),
            AgentThreadTerminal::Cancelled,
            AgentThreadStatus::Suspended,
        )
        .await
        .expect("cancel the parent");

    let criteria = tribal_db::ThreadPruneCriteria {
        completed_before: chrono::Utc::now() + chrono::Duration::seconds(1),
        stage: None,
        cascade: true,
        root_limit: u32::MAX,
    };
    let refused = PgAgentThreadRepository
        .count_refused_prune_roots(&mut conn, &criteria)
        .await
        .expect("count");
    assert_eq!(refused, 1, "the live child refuses the terminal parent");
    let pruned = PgAgentThreadRepository
        .prune_threads(&mut conn, &criteria)
        .await
        .expect("prune");
    assert_eq!(pruned, 0, "nothing deletes while the child is live");
}

#[tokio::test]
async fn test_deferred_death_crosses_the_failure_into_the_parent() {
    let ctx = TestDb::new().await;
    let (mut conn, sp) = seed_suspended_parent(&ctx, "deferred-death").await;
    let token = claim_and_run_child(&mut conn, &sp).await;

    let child_thread = child(&mut conn, sp.child_thread_id).await;
    let outcome = commit_deferred_death(
        &mut conn,
        &child_thread,
        sp.driver_task_id,
        token,
        &a_parent_resolution(&sp),
        "the verifier exhausted its retries",
        noop_recorder().as_ref(),
    )
    .await
    .expect("deferred death");
    assert_eq!(outcome, ChildTerminalOutcome::HandedBack);

    // The child and driver dead-lettered; the child never outlives its
    // driver row.
    let child = child(&mut conn, sp.child_thread_id).await;
    assert_eq!(child.status(), AgentThreadStatus::DeadLetter);
    let driver = PgAgentDriverTaskRepository
        .find_by_id(&mut conn, sp.driver_task_id)
        .await
        .expect("find driver")
        .expect("present");
    assert_eq!(driver.state(), AgentDriverTaskState::DeadLetter);

    // The permanent failure crossed into the parent's conversation as an
    // error tool-result, and the parent woke to face it.
    let parent = PgAgentThreadRepository
        .find_by_id(&mut conn, sp.parent.id())
        .await
        .expect("find parent")
        .expect("present");
    assert_eq!(parent.status(), AgentThreadStatus::Running);
    let parent_records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, sp.parent.id())
        .await
        .expect("parent log");
    let result = parent_records
        .iter()
        .find(|r| r.kind() == AgentThreadRecordKind::ToolResult)
        .expect("the death committed an error tool result");
    assert_eq!(result.content()["is_error"], true);
    assert_eq!(result.tool_call_id(), Some(SUBMIT_CALL_ID));
}
