//! The managed-run commit surface: the metered-call commit over the seam's
//! guarded primitives, exercised against a real managed thread and its
//! run-claim fence.

use sqlx::Connection;
use tribal_agent_runtime::{AgentRuntimeError, DrivingClaim, commit_model_call};
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, DrivingTaskRef, NewAgentThread,
    PgAgentThreadRecordRepository, PgAgentThreadRepository,
};
use tribal_domain::{
    AGENT_THREAD_FORMAT_VERSION, AgentThread, AgentThreadRecordKind, AgentThreadStage, RunJobId,
};

use super::common::*;

/// Inserts a fresh managed thread, returning it with its run key and the
/// claim token it was adopted under.
async fn insert_managed_thread(
    conn: &mut sqlx::PgConnection,
) -> (AgentThread, RunJobId, uuid::Uuid) {
    let run_key = RunJobId::new();
    let token = uuid::Uuid::new_v4();
    let thread = PgAgentThreadRepository
        .insert(
            conn,
            &NewAgentThread::builder()
                .stage(AgentThreadStage::Managed)
                .driving_task(DrivingTaskRef::Managed(run_key))
                .run_claim_token(Some(token))
                .format_version(AGENT_THREAD_FORMAT_VERSION)
                .build(),
        )
        .await
        .expect("insert managed thread");
    (thread, run_key, token)
}

/// The metered call commits its assistant record under the managed claim,
/// and the stored output re-reads as the response text and usage.
#[tokio::test]
async fn test_commit_model_call_commits_the_metered_record_under_the_managed_claim() {
    let ctx = TestDb::new().await;
    let mut conn = raw_conn(&ctx).await;
    let (thread, run_key, token) = insert_managed_thread(&mut conn).await;

    let response = a_completion_response("the managed answer");
    let record = commit_model_call(
        &mut conn,
        &thread,
        &DrivingClaim::managed(run_key, token),
        &response,
    )
    .await
    .expect("commit the metered call");
    assert_eq!(record.kind(), AgentThreadRecordKind::AssistantMessage);

    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread.id())
        .await
        .expect("records");
    assert_eq!(
        records.len(),
        1,
        "the metered call commits exactly one record"
    );
    assert_eq!(records[0].content()["text"], "the managed answer");
    assert!(
        records[0].usage().is_some(),
        "the metered call records its usage",
    );
}

/// A stale claim token loses the managed lease: the guard refuses the
/// append and nothing is committed.
#[tokio::test]
async fn test_commit_model_call_is_refused_under_a_stale_managed_token() {
    let ctx = TestDb::new().await;
    let mut conn = raw_conn(&ctx).await;
    let (thread, run_key, _token) = insert_managed_thread(&mut conn).await;

    let response = a_completion_response("blocked");
    let err = commit_model_call(
        &mut conn,
        &thread,
        &DrivingClaim::managed(run_key, uuid::Uuid::new_v4()),
        &response,
    )
    .await
    .expect_err("a stale token loses the managed lease");
    assert!(matches!(err, AgentRuntimeError::LeaseLost { .. }));

    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread.id())
        .await
        .expect("records");
    assert!(records.is_empty(), "a refused call commits no record");
}

/// The extracted append helper carries no begin/commit of its own, so a
/// metered call's record is scoped to the caller's transaction: rolling the
/// enclosing transaction back discards it. This is the atomicity the
/// product turn relies on to keep its append and spend bundles one commit.
#[tokio::test]
async fn test_commit_model_call_participates_in_the_callers_transaction() {
    let ctx = TestDb::new().await;
    let mut conn = raw_conn(&ctx).await;
    let (thread, run_key, token) = insert_managed_thread(&mut conn).await;

    let mut outer = conn.begin().await.expect("outer transaction");
    let response = a_completion_response("rolled back");
    commit_model_call(
        &mut outer,
        &thread,
        &DrivingClaim::managed(run_key, token),
        &response,
    )
    .await
    .expect("commit within the outer transaction");
    outer
        .rollback()
        .await
        .expect("roll the outer transaction back");

    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread.id())
        .await
        .expect("records");
    assert!(
        records.is_empty(),
        "the rolled-back transaction discarded the metered record",
    );
}
