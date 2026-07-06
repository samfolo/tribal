//! The managed-runtime acceptance walk against a live metering-gateway stack.
//!
//! The stack is launched out of process by `just e2e`, which hands its
//! coordinates in through `CORTEX_E2E_*`: a gateway URL, a fleet bearer, the
//! seeded tenant account, and the ledger and response-store database URLs. The
//! walk enqueues a probe under that account, drives it through the worker's
//! managed loop against the real gateway (metered calls bracketed through the
//! Platform provider, a mock upstream behind the gateway), and asserts every
//! edge against state — the ledger's debit, the gateway's holds, the response
//! store's custody, the job and thread terminals — never logs.
//!
//! Absent the coordinates — the ordinary `just test` run has no stack — the walk
//! skips, so it runs only under the harness that stands its dependencies up.

use std::env;

use chrono::Duration;
use sqlx::PgPool;
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, PgAgentThreadRecordRepository,
    PgAgentThreadRepository,
};
use tribal_domain::{AgentThreadRecordKind, AgentThreadStatus};
use tribal_inference::{GatewayMeteringClient, PlatformInferenceProvider};
use tribal_runtime_db::{
    ClaimedJob, PgRunJobRepository, PollBudget, RunJobRepository, RunJobState, enqueue_job,
    mint_grant,
};
use tribal_test_utils::{TestDb, provision_runtime_db};
use tribal_wire::gateway::{GRANT_SET_HEADER, JobEnqueue, JobKind};
use tribal_worker::{
    ManagedConfig, ManagedRunOutcome, MeteringTransport, ProbeSpec, drive_managed_run,
};

/// A model the seeded rate card prices, so the metered call reserves and settles
/// rather than being refused as unpriceable.
const MODEL: &str = "claude-sonnet-4-5";

/// The stack coordinates the launcher exports.
struct Stack {
    gateway_url: String,
    bearer: String,
    account_id: String,
    ledger_database_url: String,
    response_store_database_url: String,
}

/// Reads the coordinates, or `None` when no stack is running — the signal to
/// skip, reported so a skipped walk is visible rather than silent.
fn stack() -> Option<Stack> {
    if env::var("CORTEX_E2E_READY").is_err() {
        eprintln!("skipping the managed-runtime walk: no e2e stack (run `just e2e`)");
        return None;
    }
    Some(Stack {
        gateway_url: var("CORTEX_E2E_GATEWAY_URL"),
        bearer: var("CORTEX_E2E_GATEWAY_BEARER"),
        account_id: var("CORTEX_E2E_ACCOUNT_ID"),
        ledger_database_url: var("CORTEX_E2E_LEDGER_DATABASE_URL"),
        response_store_database_url: var("CORTEX_E2E_RESPONSE_STORE_DATABASE_URL"),
    })
}

fn var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("the launcher exports {key}"))
}

fn a_config() -> ManagedConfig {
    ManagedConfig {
        lease: Duration::seconds(60),
        heartbeat_interval: std::time::Duration::from_millis(200),
        call_max_tokens: 256,
        teardown_budget: PollBudget {
            interval: std::time::Duration::from_millis(10),
            max_reads: 20,
        },
        cap_give_up_after: Duration::hours(6),
        cap_backoff: Duration::seconds(30),
    }
}

/// A mock-upstream directive reporting `output_tokens` of usage, so the gateway
/// meters a real charge. Crafted as JSON — the walk cannot link the platform
/// mock crate — and carried as the probe's system prompt, which the gateway
/// forwards to the mock verbatim.
fn steering_directive(output_tokens: u64) -> String {
    serde_json::json!({
        "chunks": ["ok"],
        "usage": [{ "dimension": "output_token", "tier": "none", "quantity": output_tokens }],
        "outcome": "complete",
        "delay_ms": 0,
    })
    .to_string()
}

/// The sum of the account's debit ledger entries — the money the metering path
/// has charged it, read directly from the ledger rows.
async fn debit_total(ledger: &PgPool, account_id: &str) -> i64 {
    let debits: Vec<i64> = sqlx::query_scalar(
        "SELECT amount_nanodollars FROM ledger_entries \
         WHERE account_id = $1 AND entry_type = 'debit'",
    )
    .bind(account_id)
    .fetch_all(ledger)
    .await
    .expect("the ledger debit read succeeds");
    debits.iter().sum()
}

/// The happy path against the real gateway: a probe drives from claim to `done`,
/// its one metered call bracketed through the gateway to a settled charge. Every
/// edge is asserted on state — the ledger booked a debit for the call, the
/// gateway's hold settled and is findable under the run's anchor, the response
/// store released its custody, and the run's own durable copy remains.
#[tokio::test]
async fn test_managed_run_drives_the_metered_bracket_to_a_charge() {
    let Some(stack) = stack() else { return };

    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let client = reqwest::Client::new();
    let ledger = PgPool::connect(&stack.ledger_database_url)
        .await
        .expect("connect to the ledger database");
    let store = PgPool::connect(&stack.response_store_database_url)
        .await
        .expect("connect to the response-store database");

    // The debited-total before the run, so the charge is asserted as the debit
    // delta this run books — isolated from the plan's lazy period-ration grant
    // and order-independent under a shared seeded account.
    let debit_before = debit_total(&ledger, &stack.account_id).await;

    // Enqueue a one-call probe under the seeded account, steering the mock to
    // report usage the gateway meters, then claim it as the drive would.
    let spec = ProbeSpec {
        calls: 1,
        wait_signal: None,
        artifact_note: "acceptance walk".to_owned(),
        cap_behaviour: tribal_runtime_db::CapBehaviour::HardStop,
        system: Some(steering_directive(40)),
    };
    let claimed = enqueue_and_claim(runtime.pool(), &stack.account_id, &spec).await;

    // Drive it through the managed loop against the real gateway: the metered
    // call brackets through the Platform provider, the teardown reconciles the
    // holds and acks the custody.
    let provider =
        PlatformInferenceProvider::new(client.clone(), &stack.gateway_url, MODEL, &stack.bearer);
    let transport = MeteringTransport::new(
        GatewayMeteringClient::new(client.clone(), &stack.gateway_url, &stack.bearer),
        mint_grant(&claimed),
    );
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        &provider,
        &transport,
        &claimed,
        &a_config(),
    )
    .await
    .expect("the run drives to a terminal");

    // The run reached its terminal on both planes.
    assert_eq!(outcome, ManagedRunOutcome::Done, "the run drove to done");
    assert_eq!(
        job_state(runtime.pool(), &claimed).await,
        RunJobState::Done,
        "the job settled to done",
    );

    // The run took its own durable custody of the completion.
    let mut core_conn = core.pool().acquire().await.expect("core connection");
    let thread = PgAgentThreadRepository
        .find_by_run_key(&mut core_conn, claimed.id)
        .await
        .expect("find the run thread")
        .expect("the run thread exists");
    assert_eq!(
        thread.status(),
        AgentThreadStatus::Completed,
        "the thread reached its terminal",
    );
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut core_conn, thread.id())
        .await
        .expect("the thread's records");
    let assistant_records = records
        .iter()
        .filter(|record| record.kind() == AgentThreadRecordKind::AssistantMessage)
        .count();
    assert_eq!(
        assistant_records, 1,
        "the run committed its own durable copy of the metered call",
    );

    // The metered call moved money: the ledger booked a debit for it.
    let debit_after = debit_total(&ledger, &stack.account_id).await;
    let charged = (debit_after - debit_before).abs();
    assert!(
        charged > 0,
        "the metered call booked a debit charge of {charged} nanodollars",
    );

    // The gateway's hold for the run settled and is findable under the run's
    // anchor — the run key the teardown reconciles by, the prefix of the call's
    // position key.
    let holds = holds_report(&client, &stack, &claimed).await;
    let holds = holds["holds"].as_array().expect("a holds array");
    assert_eq!(
        holds.len(),
        1,
        "the run's one call left one hold, findable under its run anchor",
    );
    assert_eq!(
        holds[0]["status"], "settled",
        "the hold settled — no live hold remains at the terminal",
    );

    // The gateway released its response custody once the run acked it, having
    // taken its own copy above.
    let position_key = format!("{}:0", claimed.id);
    let custody_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM response_row WHERE account_id = $1 AND position_key = $2",
    )
    .bind(&stack.account_id)
    .bind(&position_key)
    .fetch_one(&store)
    .await
    .expect("the response-store read succeeds");
    assert_eq!(
        custody_rows, 0,
        "the response store released the position's custody at the ack",
    );
}

/// A cancel tears a run down against the live gateway on ledger state. A probe
/// charges one metered call, then parks on its durable wait — a dispatched,
/// unsettled hold at the pause. Cancelled and resumed, its teardown settles that
/// hold to a charge, acks the custody, and settles the job `cancelled` — the
/// call billed exactly once across the two drives.
#[tokio::test]
async fn test_a_cancelled_run_settles_its_dispatched_hold_on_the_live_ledger() {
    let Some(stack) = stack() else { return };

    let core = TestDb::new().await;
    let runtime = provision_runtime_db().await;
    let client = reqwest::Client::new();
    let ledger = PgPool::connect(&stack.ledger_database_url)
        .await
        .expect("connect to the ledger database");
    let store = PgPool::connect(&stack.response_store_database_url)
        .await
        .expect("connect to the response-store database");

    let debit_before = debit_total(&ledger, &stack.account_id).await;

    // A one-call probe that parks on a durable wait after its metered call, so
    // the run holds a dispatched hold at the pause.
    let spec = ProbeSpec {
        calls: 1,
        wait_signal: Some("go".to_owned()),
        artifact_note: "cancel walk".to_owned(),
        cap_behaviour: tribal_runtime_db::CapBehaviour::HardStop,
        system: Some(steering_directive(40)),
    };
    let claimed = enqueue_and_claim(runtime.pool(), &stack.account_id, &spec).await;

    let provider =
        PlatformInferenceProvider::new(client.clone(), &stack.gateway_url, MODEL, &stack.bearer);
    let transport = MeteringTransport::new(
        GatewayMeteringClient::new(client.clone(), &stack.gateway_url, &stack.bearer),
        mint_grant(&claimed),
    );
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        &provider,
        &transport,
        &claimed,
        &a_config(),
    )
    .await
    .expect("drive to the wait");
    assert_eq!(
        outcome,
        ManagedRunOutcome::Suspended,
        "the probe parked on its durable wait",
    );

    // Cancel the parked run and wake its job so a fresh claim can resume it.
    {
        let mut conn = runtime.pool().acquire().await.expect("runtime connection");
        assert!(
            PgRunJobRepository
                .request_cancel(&mut conn, claimed.id)
                .await
                .expect("request the cancel"),
        );
        assert!(
            PgRunJobRepository
                .wake(&mut conn, claimed.id)
                .await
                .expect("wake the job"),
            "the suspended job woke",
        );
    }
    let resumed = {
        let mut conn = runtime.pool().acquire().await.expect("runtime connection");
        PgRunJobRepository
            .claim(&mut conn, 4, Duration::seconds(60))
            .await
            .expect("claim")
            .expect("the woken job claims")
    };
    let resumed_transport = MeteringTransport::new(
        GatewayMeteringClient::new(client.clone(), &stack.gateway_url, &stack.bearer),
        mint_grant(&resumed),
    );

    // The resume observes the cancel up front and tears down: the dispatched
    // hold settles, its debit books, the custody is acked, the job cancels.
    let outcome = drive_managed_run(
        core.pool(),
        runtime.pool(),
        &provider,
        &resumed_transport,
        &resumed,
        &a_config(),
    )
    .await
    .expect("tear down on the cancel");
    assert_eq!(
        outcome,
        ManagedRunOutcome::Cancelled,
        "the cancelled run tore down",
    );
    assert_eq!(
        job_state(runtime.pool(), &resumed).await,
        RunJobState::Cancelled,
        "the cancelled run's job settled cancelled",
    );

    // The one dispatched call settled to a charge — billed once across the two
    // drives, isolated from the plan's lazy period-ration grant as a delta.
    let charged = (debit_total(&ledger, &stack.account_id).await - debit_before).abs();
    assert!(
        charged > 0,
        "the dispatched call settled to a debit of {charged} nanodollars",
    );

    // The dispatched hold settled — no live hold remains — and the response
    // custody was released at the cancel ack.
    let holds = holds_report(&client, &stack, &resumed).await;
    let holds = holds["holds"].as_array().expect("a holds array");
    assert_eq!(
        holds.len(),
        1,
        "the run's one dispatched call left one hold",
    );
    assert_eq!(
        holds[0]["status"], "settled",
        "the dispatched hold settled at the cancel teardown",
    );

    let position_key = format!("{}:0", resumed.id);
    let custody_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM response_row WHERE account_id = $1 AND position_key = $2",
    )
    .bind(&stack.account_id)
    .bind(&position_key)
    .fetch_one(&store)
    .await
    .expect("the response-store read succeeds");
    assert_eq!(
        custody_rows, 0,
        "the response store released the position's custody at the cancel ack",
    );
}

/// Enqueues a probe under `account_id` and claims it, as the managed loop would.
async fn enqueue_and_claim(
    runtime_pool: &PgPool,
    account_id: &str,
    spec: &ProbeSpec,
) -> ClaimedJob {
    let mut conn = runtime_pool.acquire().await.expect("runtime connection");
    enqueue_job(
        &mut conn,
        account_id,
        JobEnqueue {
            kind: JobKind::Probe,
            payload: spec.to_payload(),
            idempotency_key: "acceptance-walk-metered".to_owned(),
            priority: 0,
        },
    )
    .await
    .expect("enqueue the probe");
    PgRunJobRepository
        .claim(&mut conn, 4, Duration::seconds(60))
        .await
        .expect("claim")
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

/// Reads the run's holds through the gateway, scoped to its grant.
async fn holds_report(
    client: &reqwest::Client,
    stack: &Stack,
    claimed: &ClaimedJob,
) -> serde_json::Value {
    let grant = serde_json::to_string(&mint_grant(claimed)).expect("the grant serialises");
    // The run key is a `job_<uuid>` — url-safe, so it needs no escaping.
    client
        .get(format!(
            "{}/v1/holds?run_key={}",
            stack.gateway_url, claimed.id
        ))
        .bearer_auth(&stack.bearer)
        .header(GRANT_SET_HEADER, grant)
        .send()
        .await
        .expect("the holds read reaches the gateway")
        .json()
        .await
        .expect("the holds report decodes")
}
