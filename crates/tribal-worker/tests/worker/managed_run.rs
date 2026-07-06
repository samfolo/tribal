//! The managed-run drive: the two-plane composition end to end, against a core
//! DB and a runtime DB with a mock provider and a stub gateway.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Duration;
use sqlx::PgPool;
use tokio::sync::Notify;
use tribal_agent_runtime::{
    AgentRuntimeError, DrivingClaim, ManagedRunDisposition, commit_managed_terminal,
    commit_model_call, ensure_managed_thread,
};
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, PgAgentThreadRecordRepository,
    PgAgentThreadRepository,
};
use tribal_domain::{AgentThreadRecordKind, AgentThreadStatus, CompletionResponse, RunJobId};
use tribal_inference::{
    CallContext, CompletionRequest, InferenceError, InferenceEventStream, InferenceProvider,
    Message, ProviderIdentity,
};
use tribal_runtime_db::{
    CapBehaviour, ClaimedJob, NewRunJob, PgRunJobRepository, PollBudget, RunJobRepository,
    RunJobState, TeardownError, mint_grant,
};
use tribal_test_utils::{
    ExhaustBehaviour, MockInferenceProvider, TestDb, a_completion_response, provision_runtime_db,
};
use tribal_wire::gateway::{GatewayError, HoldsReport, PositionKey};
use tribal_worker::{ManagedConfig, ManagedRunOutcome, ProbeSpec, drive_managed_run};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A metering gateway that resolves immediately — no live holds — and records
/// the position keys it acks, so a test can assert the settle-and-release.
#[derive(Clone, Default)]
struct StubGateway {
    acked: Arc<Mutex<Vec<String>>>,
}

impl StubGateway {
    fn acked(&self) -> Vec<String> {
        self.acked.lock().expect("acked lock").clone()
    }
}

#[async_trait]
impl tribal_runtime_db::MeteringGateway for StubGateway {
    async fn holds_report(&self, _run_key: &str) -> Result<HoldsReport, TeardownError> {
        Ok(HoldsReport { holds: vec![] })
    }

    async fn acknowledge(&self, position_key: &PositionKey) -> Result<(), TeardownError> {
        self.acked
            .lock()
            .expect("acked lock")
            .push(position_key.as_str().to_owned());
        Ok(())
    }
}

/// A provider whose call parks until the drive is torn down, signalling when
/// the call has begun — so a test can land a cancel strictly mid-call, past the
/// drive's up-front cancel check.
struct GatedProvider {
    began: Arc<Notify>,
    identity: ProviderIdentity,
}

impl GatedProvider {
    fn new(began: Arc<Notify>) -> Self {
        Self {
            began,
            identity: ProviderIdentity {
                name: "gated".to_owned(),
                model: "gated-model".to_owned(),
            },
        }
    }
}

#[async_trait]
impl InferenceProvider for GatedProvider {
    async fn complete(
        &self,
        _request: CompletionRequest,
    ) -> Result<CompletionResponse, InferenceError> {
        self.began.notify_one();
        // Park until the cancel tears the drive down and drops this future.
        std::future::pending().await
    }

    fn identity(&self) -> &ProviderIdentity {
        &self.identity
    }
}

/// A provider that records the position key each metered call presents and
/// serves a canned response, so a test can prove a crash-before-commit
/// re-presents the same key on resume.
struct PositionRecordingProvider {
    keys: Arc<Mutex<Vec<String>>>,
    inner: Arc<MockInferenceProvider>,
}

impl PositionRecordingProvider {
    fn new(keys: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            keys,
            inner: a_mock_provider(),
        }
    }
}

#[async_trait]
impl InferenceProvider for PositionRecordingProvider {
    async fn complete(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse, InferenceError> {
        self.inner.complete(request).await
    }

    async fn complete_stream(
        &self,
        request: CompletionRequest,
        context: &CallContext,
    ) -> Result<InferenceEventStream, InferenceError> {
        if let Some(key) = &context.position_key {
            self.keys
                .lock()
                .expect("keys lock")
                .push(key.as_str().to_owned());
        }
        self.inner.complete_stream(request, context).await
    }

    fn identity(&self) -> &ProviderIdentity {
        self.inner.identity()
    }
}

fn a_config() -> ManagedConfig {
    ManagedConfig {
        lease: Duration::seconds(60),
        heartbeat_interval: std::time::Duration::from_millis(200),
        call_max_tokens: 256,
        teardown_budget: PollBudget {
            interval: std::time::Duration::from_millis(1),
            max_reads: 3,
        },
        cap_give_up_after: Duration::hours(6),
        cap_backoff: Duration::seconds(30),
    }
}

fn a_probe_payload(calls: u32, wait_signal: Option<&str>) -> serde_json::Value {
    a_cap_probe_payload(calls, wait_signal, CapBehaviour::HardStop)
}

fn a_cap_probe_payload(
    calls: u32,
    wait_signal: Option<&str>,
    cap_behaviour: CapBehaviour,
) -> serde_json::Value {
    ProbeSpec {
        calls,
        wait_signal: wait_signal.map(ToOwned::to_owned),
        artifact_note: "the probe's mark".to_owned(),
        cap_behaviour,
        system: None,
    }
    .to_payload()
}

/// A provider that refuses its first `refusals` calls with the gateway error
/// `refusal`, then serves a success — the money seam a cap scenario drives on.
fn a_capped_provider(refusals: usize, refusal: GatewayError) -> Arc<MockInferenceProvider> {
    let mut builder = MockInferenceProvider::builder();
    for _ in 0..refusals {
        builder = builder.on_complete_error(
            move || InferenceError::GatewayRefused { error: refusal },
            None,
        );
    }
    Arc::new(
        builder
            .on_complete(a_completion_response("probe answer"), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    )
}

/// Fires a suspended managed run's wake: the resolver sets the thread's wake
/// instant due and the finder wakes the job back to queued.
async fn fire_wake(core_pool: &PgPool, runtime_pool: &PgPool, claimed: &ClaimedJob) {
    let mut core_conn = core_pool.acquire().await.expect("core connection");
    PgAgentThreadRepository
        .fast_forward_wake_for_test(&mut core_conn, claimed.id)
        .await
        .expect("fast-forward the wake instant due");
    let mut runtime_conn = runtime_pool.acquire().await.expect("runtime connection");
    assert!(
        PgRunJobRepository
            .wake(&mut runtime_conn, claimed.id)
            .await
            .expect("wake the job"),
        "the suspended job woke",
    );
}

/// The run keys the managed timer-wake sweep would re-find as due.
async fn due_managed_wakes(core_pool: &PgPool) -> Vec<RunJobId> {
    let mut conn = core_pool.acquire().await.expect("core connection");
    PgAgentThreadRepository
        .find_due_managed_wakes(&mut conn, 32)
        .await
        .expect("scan due managed wakes")
}

/// Reads the last cap wake cause the run's log recorded at a resolve.
async fn last_wake_cause(core_pool: &PgPool, claimed: &ClaimedJob) -> Option<String> {
    run_records(core_pool, claimed)
        .await
        .iter()
        .filter_map(|record| {
            record
                .content()
                .get("managed_wake")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .next_back()
}

fn a_mock_provider() -> Arc<MockInferenceProvider> {
    Arc::new(
        MockInferenceProvider::builder()
            .on_complete(a_completion_response("probe answer"), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    )
}

/// A completion request shaped like the probe's own metered call.
fn a_probe_completion_request() -> CompletionRequest {
    CompletionRequest {
        system: None,
        messages: vec![Message::User {
            content: "probe".to_owned(),
        }],
        tools: vec![],
        temperature: None,
        max_tokens: Some(256),
        response_format: None,
    }
}

/// Enqueues a probe for the shared tenant without claiming it.
async fn enqueue_probe(runtime_pool: &PgPool, payload: serde_json::Value) {
    let mut conn = runtime_pool.acquire().await.expect("runtime connection");
    PgRunJobRepository
        .enqueue(
            &mut conn,
            NewRunJob {
                account_id: "acct_managed".to_owned(),
                kind: "probe".to_owned(),
                payload,
                idempotency_key: format!("probe-{}", uuid::Uuid::new_v4().simple()),
                priority: 0,
            },
        )
        .await
        .expect("enqueue the probe");
}

/// Claims one probe under the given per-tenant cap, or `None` when the tenant is
/// saturated or the queue is empty.
async fn claim_at_cap(runtime_pool: &PgPool, cap: i32) -> Option<ClaimedJob> {
    let mut conn = runtime_pool.acquire().await.expect("runtime connection");
    PgRunJobRepository
        .claim(&mut conn, cap, Duration::seconds(60))
        .await
        .expect("claim")
}

/// Enqueues a probe and claims it, returning the claim the drive receives.
async fn enqueue_and_claim(runtime_pool: &PgPool, payload: serde_json::Value) -> ClaimedJob {
    enqueue_probe(runtime_pool, payload).await;
    claim_at_cap(runtime_pool, 4)
        .await
        .expect("a probe to claim")
}

async fn job_state(runtime_pool: &PgPool, claimed: &ClaimedJob) -> RunJobState {
    let mut conn = runtime_pool.acquire().await.expect("runtime connection");
    PgRunJobRepository
        .state_of(&mut conn, claimed.id)
        .await
        .expect("state read")
        .expect("the job exists")
}

/// Lists the run thread's committed records.
async fn run_records(
    core_pool: &PgPool,
    claimed: &ClaimedJob,
) -> Vec<tribal_domain::AgentThreadRecord> {
    let mut conn = core_pool.acquire().await.expect("core connection");
    let thread = PgAgentThreadRepository
        .find_by_run_key(&mut conn, claimed.id)
        .await
        .expect("find the run thread")
        .expect("the run thread exists");
    PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread.id())
        .await
        .expect("records")
}

/// Counts the run thread's committed records of one kind.
async fn record_count(
    core_pool: &PgPool,
    claimed: &ClaimedJob,
    kind: AgentThreadRecordKind,
) -> usize {
    run_records(core_pool, claimed)
        .await
        .into_iter()
        .filter(|record| record.kind() == kind)
        .count()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The happy path: a probe drives from claim to `done` — its calls and artifact
/// committed, the job settled, every position key acked.
#[tokio::test]
async fn test_a_probe_drives_from_claim_to_done() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let provider = a_mock_provider();
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(runtime.pool(), a_probe_payload(2, None)).await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("drive to done");

    assert_eq!(outcome, ManagedRunOutcome::Done);
    assert_eq!(job_state(runtime.pool(), &claimed).await, RunJobState::Done);
    assert_eq!(
        record_count(
            core.pool(),
            &claimed,
            AgentThreadRecordKind::AssistantMessage
        )
        .await,
        2,
        "both metered calls committed",
    );
    assert_eq!(
        record_count(core.pool(), &claimed, AgentThreadRecordKind::AppendArtifact).await,
        1,
        "the artifact committed",
    );
    assert_eq!(gateway.acked().len(), 2, "every position key acked");
}

/// Resume is re-derive: a run whose log already holds one call makes only the
/// remaining call, never repeating the committed one.
#[tokio::test]
async fn test_a_resumed_run_does_not_repeat_a_committed_call() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let provider = a_mock_provider();
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(runtime.pool(), a_probe_payload(2, None)).await;

    // Simulate a crash after the first of two calls: the thread and one
    // assistant record are durable under the claim's token.
    {
        let mut conn = core.pool().acquire().await.expect("core connection");
        let claim = DrivingClaim::managed(claimed.id, claimed.claim_token);
        let thread = ensure_managed_thread(
            &mut conn,
            claimed.id,
            claimed.claim_token,
            claimed.payload.clone(),
        )
        .await
        .expect("pre-seed the thread");
        commit_model_call(
            &mut conn,
            &thread,
            &claim,
            &a_completion_response("first call"),
        )
        .await
        .expect("pre-seed one call");
    }

    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("resume to done");

    assert_eq!(outcome, ManagedRunOutcome::Done);
    assert_eq!(
        record_count(
            core.pool(),
            &claimed,
            AgentThreadRecordKind::AssistantMessage
        )
        .await,
        2,
        "the resume made only the remaining call",
    );
    assert_eq!(
        provider.call_count(),
        1,
        "the provider was called once — the committed call was not repeated",
    );
}

/// A crash between a metered call's dispatch and its record commit re-presents
/// the identical position key on resume, so the gateway's charge-once anchor
/// bills it once. The re-derive counts committed records, so an uncommitted
/// dispatch never advances the position.
#[tokio::test]
async fn test_a_crash_before_commit_re_presents_the_same_position_key() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let keys = Arc::new(Mutex::new(Vec::new()));
    let provider = PositionRecordingProvider::new(keys.clone());
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(runtime.pool(), a_probe_payload(1, None)).await;
    let run_key = claimed.id;

    // Attempt one dispatches call 0 but crashes before committing its record:
    // the bracket presented `{run_key}:0` to the provider and took no custody.
    {
        let mut conn = core.pool().acquire().await.expect("core connection");
        let thread = ensure_managed_thread(
            &mut conn,
            run_key,
            claimed.claim_token,
            claimed.payload.clone(),
        )
        .await
        .expect("adopt the thread");
        PgAgentThreadRepository
            .mark_running(&mut conn, thread.id(), AgentThreadStatus::Queued)
            .await
            .expect("mark the thread running");
    }
    let context = CallContext {
        position_key: Some(PositionKey::new(format!("{run_key}:0"))),
        grant: Some(mint_grant(&claimed)),
    };
    let dispatched = provider
        .complete_stream(a_probe_completion_request(), &context)
        .await
        .expect("dispatch call 0");
    drop(dispatched); // The crash: no commit_model_call follows the dispatch.

    // The resume re-derives calls_done = 0 and re-presents the identical key.
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        &provider,
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("resume to done");
    assert_eq!(outcome, ManagedRunOutcome::Done);

    let expected = format!("{run_key}:0");
    assert_eq!(
        *keys.lock().expect("keys lock"),
        vec![expected.clone(), expected],
        "the resume re-presented the same position key the crash dispatched",
    );
    assert_eq!(
        record_count(
            core.pool(),
            &claimed,
            AgentThreadRecordKind::AssistantMessage,
        )
        .await,
        1,
        "exactly one assistant record results across the two attempts",
    );
}

/// A probe with a durable wait parks: the job leaves `running` as `suspended`
/// with its slot released; firing the signal wakes it, and a fresh claim drives
/// it to `done`.
#[tokio::test]
async fn test_a_durable_wait_suspends_then_resumes_under_a_fresh_claim() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let provider = a_mock_provider();
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(runtime.pool(), a_probe_payload(1, Some("go"))).await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("drive to the wait");
    assert_eq!(outcome, ManagedRunOutcome::Suspended);
    assert_eq!(
        job_state(runtime.pool(), &claimed).await,
        RunJobState::Suspended,
        "the job left running as suspended",
    );

    // Fire the signal: the resolver sets the thread's wake instant (making it
    // ready) and the finder wakes the job back to queued.
    fire_wake(core.pool(), runtime.pool(), &claimed).await;

    // A fresh claim resumes under a new token, adopts the ready thread, and
    // drives it to done.
    let resumed = enqueue_and_claim_existing(runtime.pool()).await;
    assert_ne!(
        resumed.claim_token, claimed.claim_token,
        "the resume runs under a fresh claim token",
    );
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &resumed,
        &a_config(),
    )
    .await
    .expect("resume to done");
    assert_eq!(outcome, ManagedRunOutcome::Done);
    assert_eq!(job_state(runtime.pool(), &resumed).await, RunJobState::Done);
    assert_eq!(
        record_count(core.pool(), &resumed, AgentThreadRecordKind::AppendArtifact).await,
        1,
        "the artifact committed after the wait",
    );
}

/// Claims the single woken job already on the plane (no new enqueue).
async fn enqueue_and_claim_existing(runtime_pool: &PgPool) -> ClaimedJob {
    let mut conn = runtime_pool.acquire().await.expect("runtime connection");
    PgRunJobRepository
        .claim(&mut conn, 4, Duration::seconds(60))
        .await
        .expect("claim")
        .expect("the woken job to claim")
}

/// An observed cancel tears the run down: the job settles to `cancelled` and
/// its outstanding position keys are acked.
#[tokio::test]
async fn test_an_observed_cancel_tears_the_run_down() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let provider = a_mock_provider();
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(runtime.pool(), a_probe_payload(2, None)).await;
    {
        let mut conn = runtime.pool().acquire().await.expect("runtime connection");
        assert!(
            PgRunJobRepository
                .request_cancel(&mut conn, claimed.id)
                .await
                .expect("request the cancel"),
        );
    }

    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("tear down on cancel");
    assert_eq!(outcome, ManagedRunOutcome::Cancelled);
    assert_eq!(
        job_state(runtime.pool(), &claimed).await,
        RunJobState::Cancelled,
    );
}

/// A cancel that lands during the final metered call tears the run down rather
/// than completing it: with no next loop iteration to observe the cancel, the
/// in-call and post-loop paths must honour it themselves.
#[tokio::test]
async fn test_a_cancel_during_the_final_call_tears_the_run_down() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let began = Arc::new(Notify::new());
    let provider = GatedProvider::new(began.clone());
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(runtime.pool(), a_probe_payload(1, None)).await;
    let run_key = claimed.id;

    // A brisk heartbeat, so the requested cancel is observed within one beat.
    let config = ManagedConfig {
        heartbeat_interval: std::time::Duration::from_millis(20),
        ..a_config()
    };

    let drive = drive_managed_run(
        core.pool(),
        runtime.pool(),
        &provider,
        &gateway,
        &claimed,
        &config,
    );
    // Once the call is in flight — strictly past the drive's up-front cancel
    // check — request the cancel the heartbeat fires the run's watch on.
    let inject = async {
        began.notified().await;
        let mut conn = runtime.pool().acquire().await.expect("runtime connection");
        PgRunJobRepository
            .request_cancel(&mut conn, run_key)
            .await
            .expect("request the cancel");
    };
    let (outcome, ()) = tokio::join!(drive, inject);
    let outcome = outcome.expect("tear down on the mid-call cancel");

    assert_eq!(
        outcome,
        ManagedRunOutcome::Cancelled,
        "a cancel during the final call tears down, never completes",
    );
    assert_eq!(
        job_state(runtime.pool(), &claimed).await,
        RunJobState::Cancelled,
        "the cancelled run's job settles cancelled",
    );
    assert_eq!(
        record_count(
            core.pool(),
            &claimed,
            AgentThreadRecordKind::AssistantMessage,
        )
        .await,
        0,
        "the cancelled call committed no assistant record",
    );
}

/// A stale terminal converges: a second drive of a run already `done` adopts
/// the terminal thread and settles the job under its own token, committing no
/// new work.
#[tokio::test]
async fn test_a_second_drive_of_a_done_run_converges() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let provider = a_mock_provider();
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(runtime.pool(), a_probe_payload(1, None)).await;
    drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("first drive to done");
    let calls_after_first = record_count(
        core.pool(),
        &claimed,
        AgentThreadRecordKind::AssistantMessage,
    )
    .await;

    // The job is already done, so a re-claim is impossible; a stale worker
    // re-driving the same claim adopts the terminal thread and converges.
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("second drive converges");

    assert!(
        matches!(
            outcome,
            ManagedRunOutcome::Done | ManagedRunOutcome::OwnershipLost
        ),
        "the second drive converges or yields, never corrupts",
    );
    assert_eq!(
        record_count(
            core.pool(),
            &claimed,
            AgentThreadRecordKind::AssistantMessage
        )
        .await,
        calls_after_first,
        "convergence committed no new call",
    );
}

/// A stale Failed terminal converges to a `cancelled` job, never a `done` one:
/// a rival adopting a thread already `Failed` settles the job on the thread's
/// own truth, not a hardcoded `done`.
#[tokio::test]
async fn test_a_stale_failed_terminal_converges_to_a_cancelled_job() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let provider = a_mock_provider();
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(runtime.pool(), a_probe_payload(1, None)).await;

    // A run that reached its thread's `Failed` terminal but crashed before its
    // teardown settled the job: the thread is `Failed`, the job still `running`.
    {
        let mut conn = core.pool().acquire().await.expect("core connection");
        let claim = DrivingClaim::managed(claimed.id, claimed.claim_token);
        let thread = ensure_managed_thread(
            &mut conn,
            claimed.id,
            claimed.claim_token,
            claimed.payload.clone(),
        )
        .await
        .expect("adopt the thread");
        PgAgentThreadRepository
            .mark_running(&mut conn, thread.id(), AgentThreadStatus::Queued)
            .await
            .expect("mark the thread running");
        commit_managed_terminal(&mut conn, &thread, &claim, ManagedRunDisposition::Failed)
            .await
            .expect("commit the Failed terminal");
    }

    // A rival re-driving the run adopts the `Failed` thread and converges.
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("converge the stale Failed terminal");
    assert_eq!(
        outcome,
        ManagedRunOutcome::Failed,
        "a Failed thread converges to Failed, never Done",
    );
    assert_eq!(
        job_state(runtime.pool(), &claimed).await,
        RunJobState::Cancelled,
        "the failed run's job settles cancelled, not done",
    );
}

/// A reclaim that finds an unsatisfied wait re-parks it rather than resuming —
/// the worker owns the resume, never the sweep. The job returns to `suspended`
/// and the thread takes no write.
#[tokio::test]
async fn test_a_reclaim_of_an_unsatisfied_wait_re_parks_it() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let provider = a_mock_provider();
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(runtime.pool(), a_probe_payload(1, Some("go"))).await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("drive to the wait");
    assert_eq!(outcome, ManagedRunOutcome::Suspended);
    let records_before = run_records(core.pool(), &claimed).await.len();

    // Wake the job to queued without firing the signal: the thread's wait is
    // still unsatisfied (`wake_at` null), so a reclaim must re-park it.
    {
        let mut conn = runtime.pool().acquire().await.expect("runtime connection");
        assert!(
            PgRunJobRepository
                .wake(&mut conn, claimed.id)
                .await
                .expect("wake the job"),
        );
    }

    let reclaimed = enqueue_and_claim_existing(runtime.pool()).await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &reclaimed,
        &a_config(),
    )
    .await
    .expect("re-park the unsatisfied wait");
    assert_eq!(
        outcome,
        ManagedRunOutcome::Suspended,
        "the unsatisfied wait re-parks, never resumes",
    );
    assert_eq!(
        job_state(runtime.pool(), &reclaimed).await,
        RunJobState::Suspended,
    );
    assert_eq!(
        run_records(core.pool(), &claimed).await.len(),
        records_before,
        "the re-park wrote nothing to the thread",
    );
}

/// The seam fence refuses a stale writer across the composed primitives: a
/// metered-call commit under a token a later claim rotated away is refused, so
/// a reclaimed run's predecessor can never append past its lease.
#[tokio::test]
async fn test_the_fence_refuses_a_commit_under_a_rotated_token() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;

    let claimed = enqueue_and_claim(runtime.pool(), a_probe_payload(1, None)).await;
    let token_a = claimed.claim_token;
    let mut conn = core.pool().acquire().await.expect("core connection");

    // The run's thread, adopted under token A.
    let thread = ensure_managed_thread(&mut conn, claimed.id, token_a, claimed.payload.clone())
        .await
        .expect("adopt under token A");

    // A reclaim rotates the fence to token B.
    let token_b = uuid::Uuid::new_v4();
    ensure_managed_thread(&mut conn, claimed.id, token_b, claimed.payload.clone())
        .await
        .expect("rotate the fence to token B");

    // The stale writer, still holding token A, is refused at the append.
    let stale = DrivingClaim::managed(claimed.id, token_a);
    let refused =
        commit_model_call(&mut conn, &thread, &stale, &a_completion_response("stale")).await;
    assert!(
        matches!(refused, Err(AgentRuntimeError::LeaseLost { .. })),
        "the append under the rotated-away token is refused, got {refused:?}",
    );
}

/// The tenant cap holds across the composed drive, not just the repository
/// seam: while one run holds a saturated tenant's only slot, a second stays
/// queued; driving the first out of `running` frees the slot and the second
/// claims.
#[tokio::test]
async fn test_the_tenant_cap_holds_across_the_composed_drive() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let provider = a_mock_provider();
    let gateway = StubGateway::default();

    // Two probes for one tenant; claim the first under a cap of one.
    enqueue_probe(runtime.pool(), a_probe_payload(1, None)).await;
    enqueue_probe(runtime.pool(), a_probe_payload(1, None)).await;
    let first = claim_at_cap(runtime.pool(), 1)
        .await
        .expect("the first probe claims");

    // While the first holds the tenant's only slot, the second stays queued.
    assert!(
        claim_at_cap(runtime.pool(), 1).await.is_none(),
        "a saturated tenant yields no second claim while the first runs",
    );

    // Driving the first to done leaves running and frees the slot.
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &first,
        &a_config(),
    )
    .await
    .expect("drive the first to done");
    assert_eq!(outcome, ManagedRunOutcome::Done);

    // Now the second is claimable — the cap released across the composed path.
    let second = claim_at_cap(runtime.pool(), 1)
        .await
        .expect("the second claims once the first leaves running");
    assert_ne!(
        second.id, first.id,
        "the second probe is a distinct run from the first",
    );
}

// ---------------------------------------------------------------------------
// Money dispositions
// ---------------------------------------------------------------------------

/// A hard-stopped run parks to its give-up deadline; the deadline's wake, credit
/// still short, fails it cleanly — the thread `Failed`, the job `cancelled`.
#[tokio::test]
async fn test_a_hard_stop_fails_when_credit_is_short_at_the_deadline() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    // The first call parks; the deadline wake's re-attempt refuses again.
    let provider = a_capped_provider(2, GatewayError::OverCap);
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(
        runtime.pool(),
        a_cap_probe_payload(1, None, CapBehaviour::HardStop),
    )
    .await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("park on the cap");
    assert_eq!(
        outcome,
        ManagedRunOutcome::Suspended,
        "the hard stop parks to its deadline",
    );
    assert_eq!(
        job_state(runtime.pool(), &claimed).await,
        RunJobState::Suspended,
    );

    fire_wake(core.pool(), runtime.pool(), &claimed).await;
    let resumed = enqueue_and_claim_existing(runtime.pool()).await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &resumed,
        &a_config(),
    )
    .await
    .expect("fail at the deadline");
    assert_eq!(
        outcome,
        ManagedRunOutcome::Failed,
        "credit short at the deadline fails the run",
    );
    assert_eq!(
        job_state(runtime.pool(), &resumed).await,
        RunJobState::Cancelled,
        "the failed run's job settles cancelled",
    );
    assert_eq!(
        last_wake_cause(core.pool(), &resumed).await.as_deref(),
        Some("give_up"),
        "the give-up cause survives the resolve",
    );
}

/// A run cancelled while cap-suspended does not strand the wake sweep: the cancel
/// teardown disarms the thread's give-up deadline, so once that instant elapses
/// the timer sweep no longer re-finds a thread whose job has already settled.
#[tokio::test]
async fn test_a_cancelled_cap_suspended_run_leaves_the_wake_sweep() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let provider = a_capped_provider(1, GatewayError::OverCap);
    let gateway = StubGateway::default();

    // Park the run on its cap give-up deadline.
    let claimed = enqueue_and_claim(
        runtime.pool(),
        a_cap_probe_payload(1, None, CapBehaviour::HardStop),
    )
    .await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("park on the cap");
    assert_eq!(outcome, ManagedRunOutcome::Suspended);

    // Advance the deadline: the sweep would now re-find the suspended thread —
    // the strand the cancel must clear.
    fire_wake(core.pool(), runtime.pool(), &claimed).await;
    assert!(
        due_managed_wakes(core.pool()).await.contains(&claimed.id),
        "the due cap-suspended run is swept before the cancel",
    );

    // Cancel it and re-drive: the up-front cancel path tears the run down.
    {
        let mut conn = runtime.pool().acquire().await.expect("runtime connection");
        PgRunJobRepository
            .request_cancel(&mut conn, claimed.id)
            .await
            .expect("request the cancel");
    }
    let resumed = enqueue_and_claim_existing(runtime.pool()).await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &resumed,
        &a_config(),
    )
    .await
    .expect("tear down on cancel");
    assert_eq!(outcome, ManagedRunOutcome::Cancelled);
    assert_eq!(
        job_state(runtime.pool(), &resumed).await,
        RunJobState::Cancelled,
    );

    assert!(
        !due_managed_wakes(core.pool()).await.contains(&claimed.id),
        "the cancelled run's thread no longer strands the wake sweep",
    );
}

/// A hard-stopped run whose credit is restored by its deadline wake completes.
#[tokio::test]
async fn test_a_hard_stop_completes_when_credit_returns_by_the_deadline() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    // One refusal parks it; the wake's re-attempt is served.
    let provider = a_capped_provider(1, GatewayError::OverCap);
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(
        runtime.pool(),
        a_cap_probe_payload(1, None, CapBehaviour::HardStop),
    )
    .await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("park on the cap");
    assert_eq!(outcome, ManagedRunOutcome::Suspended);

    fire_wake(core.pool(), runtime.pool(), &claimed).await;
    let resumed = enqueue_and_claim_existing(runtime.pool()).await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &resumed,
        &a_config(),
    )
    .await
    .expect("complete on the restored credit");
    assert_eq!(outcome, ManagedRunOutcome::Done);
    assert_eq!(job_state(runtime.pool(), &resumed).await, RunJobState::Done);
    assert_eq!(
        last_wake_cause(core.pool(), &resumed).await.as_deref(),
        Some("give_up"),
        "the hard-stop wake records the give-up cause even when it clears",
    );
}

/// A throttled run backs off and completes once credit returns by the backoff
/// wake.
#[tokio::test]
async fn test_a_throttle_queue_completes_through_a_backoff_wake() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let provider = a_capped_provider(1, GatewayError::OverCap);
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(
        runtime.pool(),
        a_cap_probe_payload(1, None, CapBehaviour::ThrottleQueue),
    )
    .await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("back off on the cap");
    assert_eq!(
        outcome,
        ManagedRunOutcome::Suspended,
        "the throttle backs off",
    );

    fire_wake(core.pool(), runtime.pool(), &claimed).await;
    let resumed = enqueue_and_claim_existing(runtime.pool()).await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &resumed,
        &a_config(),
    )
    .await
    .expect("complete after the backoff");
    assert_eq!(outcome, ManagedRunOutcome::Done);
    assert_eq!(job_state(runtime.pool(), &resumed).await, RunJobState::Done);
    assert_eq!(
        last_wake_cause(core.pool(), &resumed).await.as_deref(),
        Some("backoff"),
        "the backoff cause survives the resolve",
    );
}

/// A not-entitled refusal fails the run on first sight, whatever the cap
/// setting — no wait can clear it.
#[tokio::test]
async fn test_a_not_entitled_refusal_fails_on_first_sight() {
    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let provider = a_capped_provider(1, GatewayError::NotEntitled);
    let gateway = StubGateway::default();

    let claimed = enqueue_and_claim(
        runtime.pool(),
        a_cap_probe_payload(1, None, CapBehaviour::HardStop),
    )
    .await;
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        provider.as_ref(),
        &gateway,
        &claimed,
        &a_config(),
    )
    .await
    .expect("fail on the entitlement refusal");
    assert_eq!(
        outcome,
        ManagedRunOutcome::Failed,
        "a not-entitled refusal fails the run",
    );
    assert_eq!(
        job_state(runtime.pool(), &claimed).await,
        RunJobState::Cancelled,
    );
    assert_eq!(
        record_count(
            core.pool(),
            &claimed,
            AgentThreadRecordKind::AssistantMessage
        )
        .await,
        0,
        "the failed call committed no assistant record",
    );
}
