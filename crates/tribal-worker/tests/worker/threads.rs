//! Integration tests for the thread bracket around stage execution.
//!
//! The pipeline's observable surface is asserted unchanged by the
//! existing suites; these tests assert what the runtime adds underneath:
//! every executed stage task drives exactly one thread whose log carries
//! the rendered input and the assistant response, completed in the same
//! transaction as the task.

use tribal_agent_runtime::RenderedConversation;
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, PgAgentThreadRecordRepository,
    PgAgentThreadRepository,
};
use tribal_domain::{AgentThreadRecordKind, AgentThreadStatus};

use super::{common::*, fixtures::extraction_response_json};

/// The extraction happy path leaves one completed thread driven by the
/// extraction task, with an input record carrying the conversation as
/// sent and an assistant record carrying the response and its usage.
#[tokio::test]
async fn test_stage_execution_commits_a_completed_thread_with_its_log() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "thread-log").await;

    let candidates = vec![a_candidate().content("threaded".to_owned()).build()];
    let response_json = extraction_response_json(&candidates, &[]);

    let (job_id, task_id) = {
        let mut conn = raw_conn(ctx).await;
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

    let mut conn = raw_conn(ctx).await;
    let thread = PgAgentThreadRepository
        .find_by_stage_task(&mut conn, task_id)
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
        .find_by_thread(&mut conn, thread.id())
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

    teardown(ctx).await;
}
