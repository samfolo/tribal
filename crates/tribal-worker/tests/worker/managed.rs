//! The managed-run thread primitives — ensure, metered call, terminal, and
//! suspend — exercised against a real managed thread and the run-claim fence
//! that serialises a reclaiming worker's adoption ahead of a stale worker's
//! commit.

use sqlx::Connection;
use tribal_agent_runtime::{
    AgentRuntimeError, DrivingClaim, ManagedRunDisposition, commit_managed_terminal,
    commit_model_call, ensure_managed_thread, suspend_managed_thread,
};
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, DrivingTaskRef, NewAgentThread,
    PgAgentThreadRecordRepository, PgAgentThreadRepository,
};
use tribal_domain::{
    AGENT_THREAD_FORMAT_VERSION, AgentThread, AgentThreadRecordKind, AgentThreadStage,
    AgentThreadStatus, AgentThreadSuspension, RunJobId,
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

// ---------------------------------------------------------------------------
// The ensure/terminal/suspend primitives over the fence
// ---------------------------------------------------------------------------

/// A managed run's opening input, seeded once so a resume re-derives from a
/// non-empty log.
fn an_opening_input() -> serde_json::Value {
    serde_json::json!({"probe": "opening"})
}

/// A fresh managed run creates its thread and commits the opening input; a
/// reclaiming worker adopts the same thread under a rotated token, and the
/// opening input is committed once, not per adoption.
#[tokio::test]
async fn test_ensure_managed_thread_creates_then_resumes_under_a_rotated_token() {
    let ctx = TestDb::new().await;
    let mut conn = raw_conn(&ctx).await;
    let run_key = RunJobId::new();
    let token_a = uuid::Uuid::new_v4();

    let created = ensure_managed_thread(&mut conn, run_key, token_a, an_opening_input())
        .await
        .expect("create the managed thread");
    assert_eq!(created.stage(), AgentThreadStage::Managed);

    let token_b = uuid::Uuid::new_v4();
    let resumed = ensure_managed_thread(&mut conn, run_key, token_b, an_opening_input())
        .await
        .expect("resume the managed thread");
    assert_eq!(resumed.id(), created.id(), "one thread per run key");

    assert!(
        PgAgentThreadRepository
            .holds_managed_claim(&mut conn, run_key, token_b)
            .await
            .expect("held"),
        "the rotated token holds the lease",
    );
    assert!(
        !PgAgentThreadRepository
            .holds_managed_claim(&mut conn, run_key, token_a)
            .await
            .expect("stale"),
        "the prior token lost the lease",
    );

    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, created.id())
        .await
        .expect("records");
    assert_eq!(
        records.len(),
        1,
        "the opening input is committed once, not per adoption",
    );
    assert_eq!(records[0].kind(), AgentThreadRecordKind::Input);
}

/// A rotated token loses the lease for both a metered append and a terminal:
/// the displaced worker's writes are refused and nothing commits.
#[tokio::test]
async fn test_a_stale_token_loses_the_lease_for_append_and_terminal() {
    let ctx = TestDb::new().await;
    let mut conn = raw_conn(&ctx).await;
    let run_key = RunJobId::new();
    let token_a = uuid::Uuid::new_v4();
    let thread = ensure_managed_thread(&mut conn, run_key, token_a, an_opening_input())
        .await
        .expect("create");

    let token_b = uuid::Uuid::new_v4();
    ensure_managed_thread(&mut conn, run_key, token_b, an_opening_input())
        .await
        .expect("adopt under a rotated token");

    let stale = DrivingClaim::managed(run_key, token_a);
    let append_err = commit_model_call(
        &mut conn,
        &thread,
        &stale,
        &a_completion_response("stale append"),
    )
    .await
    .expect_err("a stale append loses the lease");
    assert!(matches!(append_err, AgentRuntimeError::LeaseLost { .. }));
    let terminal_err =
        commit_managed_terminal(&mut conn, &thread, &stale, ManagedRunDisposition::Completed)
            .await
            .expect_err("a stale terminal loses the lease");
    assert!(matches!(terminal_err, AgentRuntimeError::LeaseLost { .. }));

    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread.id())
        .await
        .expect("records");
    assert_eq!(records.len(), 1, "no refused write committed a record");
    let read = PgAgentThreadRepository
        .find_by_id(&mut conn, thread.id())
        .await
        .expect("find")
        .expect("present");
    assert!(
        !read.status().is_terminal(),
        "the refused terminal did not land",
    );
}

/// A stale terminal converges: the run reaches its terminal under token A, a
/// reclaiming worker re-adopts and re-derives the already-terminal thread,
/// and its own terminal attempt finds the status CAS settled.
#[tokio::test]
async fn test_a_stale_terminal_converges_by_re_adopting_the_settled_run() {
    let ctx = TestDb::new().await;
    let mut conn = raw_conn(&ctx).await;
    let run_key = RunJobId::new();
    let token_a = uuid::Uuid::new_v4();
    let thread = ensure_managed_thread(&mut conn, run_key, token_a, an_opening_input())
        .await
        .expect("create");
    // The drive marks the thread running before committing its terminal.
    PgAgentThreadRepository
        .mark_running(&mut conn, thread.id(), AgentThreadStatus::Queued)
        .await
        .expect("run");

    commit_managed_terminal(
        &mut conn,
        &thread,
        &DrivingClaim::managed(run_key, token_a),
        ManagedRunDisposition::Completed,
    )
    .await
    .expect("terminal under token A");

    let token_b = uuid::Uuid::new_v4();
    let readopted = ensure_managed_thread(&mut conn, run_key, token_b, an_opening_input())
        .await
        .expect("re-adopt the settled run");
    assert_eq!(readopted.id(), thread.id());
    assert_eq!(
        readopted.status(),
        AgentThreadStatus::Completed,
        "the re-adopted run is already terminal",
    );

    let again = commit_managed_terminal(
        &mut conn,
        &thread,
        &DrivingClaim::managed(run_key, token_b),
        ManagedRunDisposition::Completed,
    )
    .await
    .expect_err("the settled run is not re-completed");
    assert!(matches!(again, AgentRuntimeError::StatusCasMissed { .. }));
}

/// The lease-holder parks the run under its signal cause; a displaced worker's
/// suspend is fenced and leaves the run running.
#[tokio::test]
async fn test_suspend_managed_thread_parks_under_its_signal_cause_and_fences_a_stale_token() {
    let ctx = TestDb::new().await;
    let mut conn = raw_conn(&ctx).await;
    let run_key = RunJobId::new();
    let token_a = uuid::Uuid::new_v4();
    let thread = ensure_managed_thread(&mut conn, run_key, token_a, an_opening_input())
        .await
        .expect("create");
    PgAgentThreadRepository
        .mark_running(&mut conn, thread.id(), AgentThreadStatus::Queued)
        .await
        .expect("run");

    let token_b = uuid::Uuid::new_v4();
    ensure_managed_thread(&mut conn, run_key, token_b, an_opening_input())
        .await
        .expect("adopt under a rotated token");

    let cause = AgentThreadSuspension::Signal {
        key: "run-signal:abc".to_owned(),
    };
    let wake_at = chrono::Utc::now() + chrono::Duration::minutes(5);

    let stale = suspend_managed_thread(
        &mut conn,
        &thread,
        &DrivingClaim::managed(run_key, token_a),
        &cause,
        Some(wake_at),
    )
    .await
    .expect_err("a stale suspend loses the lease");
    assert!(matches!(stale, AgentRuntimeError::LeaseLost { .. }));
    let running = PgAgentThreadRepository
        .find_by_id(&mut conn, thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(
        running.status(),
        AgentThreadStatus::Running,
        "the refused suspend left the run running",
    );

    suspend_managed_thread(
        &mut conn,
        &thread,
        &DrivingClaim::managed(run_key, token_b),
        &cause,
        Some(wake_at),
    )
    .await
    .expect("park the run under its lease");
    let read = PgAgentThreadRepository
        .find_by_id(&mut conn, thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(read.status(), AgentThreadStatus::Suspended);
    assert_eq!(
        read.suspension(),
        Some(&cause),
        "the signal cause round-trips",
    );
    assert!(
        read.wake_at().is_some(),
        "the durable wait carries its wake instant",
    );
}
