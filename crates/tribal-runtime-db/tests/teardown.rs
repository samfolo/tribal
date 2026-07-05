//! Cancel-teardown invariants against real Postgres: a run reaches its terminal
//! only once the holds report shows no unresolved money, the terminal commit is
//! claim-token fenced, and every position key is acked only behind that commit.

mod support;

use std::{collections::VecDeque, sync::Mutex, time::Duration as StdDuration};

use async_trait::async_trait;
use chrono::Duration;
use sqlx::PgConnection;
use tribal_runtime_db::{
    ClaimedJob, MeteringGateway, NewRunJob, PgRunJobRepository, PollBudget, PostRunningState,
    RunJobRepository, RunJobState, TeardownError, TeardownOutcome, TeardownTarget, cancel_teardown,
};
use tribal_wire::gateway::{HoldEntry, HoldStatus, HoldsReport, PositionKey};
use uuid::Uuid;

const RUN_JOB: PgRunJobRepository = PgRunJobRepository;
const DEFAULT_CAP: i32 = 4;

/// A scripted holds report and an ack recorder: each `holds_report` read pops the
/// next scripted report (the last repeats), and each `acknowledge` is recorded so
/// a test can assert the fenced ack fired — or did not.
struct FakeGateway {
    reports: Mutex<VecDeque<HoldsReport>>,
    acked: Mutex<Vec<String>>,
}

impl FakeGateway {
    fn scripted(reports: Vec<HoldsReport>) -> Self {
        Self {
            reports: Mutex::new(reports.into()),
            acked: Mutex::new(Vec::new()),
        }
    }

    fn acked(&self) -> Vec<String> {
        self.acked.lock().unwrap().clone()
    }
}

#[async_trait]
impl MeteringGateway for FakeGateway {
    async fn holds_report(&self, _run_key: &str) -> Result<HoldsReport, TeardownError> {
        let mut reports = self.reports.lock().unwrap();
        let report = if reports.len() > 1 {
            reports.pop_front().expect("a scripted report")
        } else {
            reports.front().expect("a scripted report").clone()
        };
        Ok(report)
    }

    async fn acknowledge(&self, position_key: &PositionKey) -> Result<(), TeardownError> {
        self.acked
            .lock()
            .unwrap()
            .push(position_key.as_str().to_owned());
        Ok(())
    }
}

fn report(entries: &[(&str, HoldStatus)]) -> HoldsReport {
    HoldsReport {
        holds: entries
            .iter()
            .map(|(key, status)| HoldEntry {
                position_key: PositionKey::new(*key),
                status: *status,
            })
            .collect(),
    }
}

fn budget() -> PollBudget {
    PollBudget {
        interval: StdDuration::ZERO,
        max_reads: 5,
    }
}

async fn enqueue_and_claim(conn: &mut PgConnection, tenant: &str, key: &str) -> ClaimedJob {
    RUN_JOB
        .enqueue(
            conn,
            NewRunJob {
                account_id: tenant.to_owned(),
                kind: "consolidate".to_owned(),
                payload: serde_json::json!({}),
                idempotency_key: key.to_owned(),
                priority: 0,
            },
        )
        .await
        .expect("enqueue");
    RUN_JOB
        .claim(conn, DEFAULT_CAP, Duration::seconds(60))
        .await
        .expect("claim")
        .expect("a job to claim")
}

fn target(claimed: &ClaimedJob, keys: &[&str]) -> TeardownTarget {
    TeardownTarget {
        id: claimed.id,
        claim_token: claimed.claim_token,
        run_key: "thread".to_owned(),
        position_keys: keys.iter().map(|key| PositionKey::new(*key)).collect(),
        terminal: PostRunningState::Cancelled,
    }
}

#[tokio::test]
async fn a_run_whose_holds_have_resolved_tears_down_and_acks_every_key() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.unwrap();
    let claimed = enqueue_and_claim(&mut conn, "account_a", "torn-down").await;

    let gateway = FakeGateway::scripted(vec![report(&[
        ("thread:1", HoldStatus::Settled),
        ("thread:2", HoldStatus::Released),
    ])]);
    let outcome = cancel_teardown(
        &mut conn,
        &gateway,
        &target(&claimed, &["thread:1", "thread:2"]),
        &budget(),
    )
    .await
    .expect("teardown");

    assert_eq!(outcome, TeardownOutcome::ToreDown);
    assert_eq!(
        RUN_JOB.state_of(&mut conn, claimed.id).await.unwrap(),
        Some(RunJobState::Cancelled),
        "the run committed to its terminal once its money was resolved",
    );
    assert_eq!(
        gateway.acked(),
        vec!["thread:1".to_owned(), "thread:2".to_owned()],
        "every outstanding key was acked once the run was terminal",
    );
}

#[tokio::test]
async fn a_live_hold_holds_the_run_short_of_terminal_and_acks_nothing() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.unwrap();
    let claimed = enqueue_and_claim(&mut conn, "account_a", "still-live").await;

    // A hold that never resolves through the whole poll budget.
    let gateway = FakeGateway::scripted(vec![report(&[("thread:1", HoldStatus::SettlePending)])]);
    let outcome = cancel_teardown(
        &mut conn,
        &gateway,
        &target(&claimed, &["thread:1"]),
        &budget(),
    )
    .await
    .expect("teardown");

    assert_eq!(outcome, TeardownOutcome::HoldsStillLive);
    assert_eq!(
        RUN_JOB.state_of(&mut conn, claimed.id).await.unwrap(),
        Some(RunJobState::Running),
        "a live hold keeps the run out of a terminal state",
    );
    assert!(
        gateway.acked().is_empty(),
        "no key is acked while the run is not terminal",
    );
}

#[tokio::test]
async fn teardown_waits_for_both_a_dispatched_and_an_undispatched_hold_to_resolve() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.unwrap();
    let claimed = enqueue_and_claim(&mut conn, "account_a", "split").await;

    // The split converging over reads: the dispatched hold settles, the
    // undispatched one releases, and only the read that finds both resolved lets
    // the run reach its terminal.
    let gateway = FakeGateway::scripted(vec![
        report(&[
            ("thread:1", HoldStatus::SettlePending),
            ("thread:2", HoldStatus::Active),
        ]),
        report(&[
            ("thread:1", HoldStatus::Settled),
            ("thread:2", HoldStatus::Active),
        ]),
        report(&[
            ("thread:1", HoldStatus::Settled),
            ("thread:2", HoldStatus::Released),
        ]),
    ]);
    let outcome = cancel_teardown(
        &mut conn,
        &gateway,
        &target(&claimed, &["thread:1", "thread:2"]),
        &budget(),
    )
    .await
    .expect("teardown");

    assert_eq!(outcome, TeardownOutcome::ToreDown);
    assert_eq!(
        RUN_JOB.state_of(&mut conn, claimed.id).await.unwrap(),
        Some(RunJobState::Cancelled),
    );
}

#[tokio::test]
async fn a_stale_claim_token_aborts_teardown_before_any_ack() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.unwrap();
    let claimed = enqueue_and_claim(&mut conn, "account_a", "reclaimed").await;

    // Holds are resolved, but a reclaim owns the job: the terminal commit's
    // claim-token fence misses, so nothing is torn down and nothing is acked.
    let gateway = FakeGateway::scripted(vec![report(&[("thread:1", HoldStatus::Settled)])]);
    let mut stale = target(&claimed, &["thread:1"]);
    stale.claim_token = Uuid::new_v4();
    let outcome = cancel_teardown(&mut conn, &gateway, &stale, &budget())
        .await
        .expect("teardown");

    assert_eq!(outcome, TeardownOutcome::OwnershipLost);
    assert_eq!(
        RUN_JOB.state_of(&mut conn, claimed.id).await.unwrap(),
        Some(RunJobState::Running),
        "a stale runner cannot move the job a live claim owns",
    );
    assert!(
        gateway.acked().is_empty(),
        "the ack is fenced behind the terminal commit, so a missed commit acks nothing",
    );
}
