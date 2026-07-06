//! The job plane's admission, fairness, and lifecycle invariants, against real
//! Postgres. Concurrency is exercised with real connections and a barrier so the
//! single-row admission compare-and-swap is put under genuine contention.

mod support;

use std::sync::Arc;

use chrono::Duration;
use sqlx::PgConnection;
use tokio::sync::Barrier;
use tribal_runtime_db::{
    NewRunJob, PgRunJobRepository, PgTenantSlotRepository, PostRunningState, RunJobId,
    RunJobRepository, RunJobState, TenantSlotRepository, WriteOutcome,
};

const RUN_JOB: PgRunJobRepository = PgRunJobRepository;
const TENANT_SLOT: PgTenantSlotRepository = PgTenantSlotRepository;
const DEFAULT_CAP: i32 = 4;

fn lease() -> Duration {
    Duration::seconds(60)
}

fn expired_lease() -> Duration {
    Duration::seconds(-5)
}

/// Enqueues a probe job for `tenant`, returning nothing — the claim reads it back.
async fn enqueue(conn: &mut PgConnection, tenant: &str, key: &str) {
    RUN_JOB
        .enqueue(
            &mut *conn,
            NewRunJob {
                account_id: tenant.to_owned(),
                kind: "probe".to_owned(),
                payload: serde_json::json!({ "key": key }),
                idempotency_key: key.to_owned(),
                priority: 0,
            },
        )
        .await
        .expect("enqueue");
}

async fn running_count(conn: &mut PgConnection, tenant: &str) -> i32 {
    TENANT_SLOT
        .get(&mut *conn, tenant)
        .await
        .expect("read slot")
        .map_or(0, |slot| slot.running)
}

// ---------------------------------------------------------------------------
// Enqueue and basic claim
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_enqueue_dedups_on_idempotency_key() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");
    let first = RUN_JOB
        .enqueue(
            &mut conn,
            NewRunJob {
                account_id: "account_a".to_owned(),
                kind: "cron".to_owned(),
                payload: serde_json::json!({}),
                idempotency_key: "same".to_owned(),
                priority: 0,
            },
        )
        .await
        .expect("first enqueue");
    let second = RUN_JOB
        .enqueue(
            &mut conn,
            NewRunJob {
                account_id: "account_a".to_owned(),
                kind: "cron".to_owned(),
                payload: serde_json::json!({}),
                idempotency_key: "same".to_owned(),
                priority: 0,
            },
        )
        .await
        .expect("second enqueue");
    assert!(matches!(
        first,
        tribal_runtime_db::EnqueueOutcome::Enqueued(_)
    ));
    assert!(matches!(
        second,
        tribal_runtime_db::EnqueueOutcome::Deduplicated(_)
    ));
    assert_eq!(first.id(), second.id(), "a re-enqueue returns the same job");
}

#[tokio::test]
async fn test_cancel_requested_reads_the_intent_the_writer_sets() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");
    let id = RUN_JOB
        .enqueue(
            &mut conn,
            NewRunJob {
                account_id: "account_cancel".to_owned(),
                kind: "probe".to_owned(),
                payload: serde_json::json!({}),
                idempotency_key: "cancel-read".to_owned(),
                priority: 0,
            },
        )
        .await
        .expect("enqueue")
        .id();

    assert!(
        !RUN_JOB
            .cancel_requested(&mut conn, id)
            .await
            .expect("read the fresh flag"),
        "a fresh job carries no cancel intent",
    );
    assert!(
        !RUN_JOB
            .cancel_requested(&mut conn, RunJobId::new())
            .await
            .expect("read a vanished job"),
        "a vanished job reads as not cancelled",
    );

    assert!(
        RUN_JOB
            .request_cancel(&mut conn, id)
            .await
            .expect("request the cancel"),
    );
    assert!(
        RUN_JOB
            .cancel_requested(&mut conn, id)
            .await
            .expect("read the set flag"),
        "the reader observes the intent the writer set",
    );
}

/// Enqueues, claims, and leaves a job `suspended` — optionally with a cancel
/// intent — returning its id.
async fn suspend_job(conn: &mut PgConnection, key: &str, cancel: bool) -> RunJobId {
    let id = RUN_JOB
        .enqueue(
            &mut *conn,
            NewRunJob {
                account_id: key.to_owned(),
                kind: "probe".to_owned(),
                payload: serde_json::json!({}),
                idempotency_key: key.to_owned(),
                priority: 0,
            },
        )
        .await
        .expect("enqueue")
        .id();
    let claimed = RUN_JOB
        .claim(&mut *conn, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("a job to claim");
    if cancel {
        RUN_JOB
            .request_cancel(&mut *conn, id)
            .await
            .expect("request cancel");
    }
    RUN_JOB
        .leave_running(
            &mut *conn,
            id,
            claimed.claim_token,
            PostRunningState::Suspended,
        )
        .await
        .expect("suspend the job");
    id
}

#[tokio::test]
async fn test_find_suspended_cancel_requested_selects_only_suspended_intents() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");

    // Suspended and cancelled: the run the sweep must wake to be torn down.
    let cancelled = suspend_job(&mut conn, "susp-cancel", true).await;
    // Suspended without an intent: left parked.
    suspend_job(&mut conn, "susp-plain", false).await;
    // Cancelled but still queued: not suspended, so the running-worker path owns
    // it and the sweep's `state = 'suspended'` filter excludes it.
    let queued = RUN_JOB
        .enqueue(
            &mut conn,
            NewRunJob {
                account_id: "queued-cancel".to_owned(),
                kind: "probe".to_owned(),
                payload: serde_json::json!({}),
                idempotency_key: "queued-cancel".to_owned(),
                priority: 0,
            },
        )
        .await
        .expect("enqueue")
        .id();
    assert!(
        RUN_JOB
            .request_cancel(&mut conn, queued)
            .await
            .expect("cancel the queued job"),
    );

    let found = RUN_JOB
        .find_suspended_cancel_requested(&mut conn, 10)
        .await
        .expect("scan");
    assert_eq!(
        found,
        vec![cancelled],
        "only the suspended cancelled run is returned",
    );
}

#[tokio::test]
async fn test_an_enqueued_job_is_claimable_with_no_schedule() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");
    enqueue(&mut conn, "account_a", "job-1").await;
    let claimed = RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("claim");
    assert!(
        claimed.is_some(),
        "an enqueued job is claimable with no schedule"
    );
    assert_eq!(running_count(&mut conn, "account_a").await, 1);
}

// ---------------------------------------------------------------------------
// Admission concurrency
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_concurrent_claimers_at_cap_minus_one_admit_exactly_one() {
    let db = support::provision().await;
    let pool = db.pool().clone();
    let tenant = "account_a";
    let mut setup = pool.acquire().await.expect("acquire a setup connection");

    // cap = 5, pre-claim 4 so the tenant sits at running = cap - 1, with plenty
    // of queued jobs left for the racers to contend over.
    TENANT_SLOT
        .apply_cap_sync(&mut setup, tenant, 5)
        .await
        .expect("cap sync");
    for i in 0..12 {
        enqueue(&mut setup, tenant, &format!("job-{i}")).await;
    }
    for _ in 0..4 {
        RUN_JOB
            .claim(&mut setup, DEFAULT_CAP, lease())
            .await
            .expect("pre-claim")
            .expect("pre-claim admitted");
    }
    assert_eq!(running_count(&mut setup, tenant).await, 4, "at cap - 1");

    // 16 racers contend for the one remaining slot. Each acquires its own
    // connection before the barrier, so the claims race rather than the acquires.
    let racers = 16;
    let barrier = Arc::new(Barrier::new(racers));
    let mut handles = Vec::new();
    for _ in 0..racers {
        let pool = pool.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            let mut conn = pool.acquire().await.expect("acquire a racing connection");
            barrier.wait().await;
            RUN_JOB
                .claim(&mut conn, DEFAULT_CAP, lease())
                .await
                .expect("racing claim")
                .is_some()
        }));
    }
    let mut admitted = 0;
    for handle in handles {
        if handle.await.expect("join") {
            admitted += 1;
        }
    }

    assert_eq!(admitted, 1, "exactly one racer admits at cap - 1");
    assert_eq!(
        running_count(&mut setup, tenant).await,
        5,
        "running reaches the cap, never past"
    );
}

#[tokio::test]
async fn test_a_saturated_tenant_is_skipped_for_another_tenants_claimable_job() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");

    // Tenant A: cap 1, one job claimed (saturated), a second job queued and older
    // than B's — age order would prefer A, but saturation must skip it.
    TENANT_SLOT
        .apply_cap_sync(&mut conn, "account_a", 1)
        .await
        .expect("cap a");
    enqueue(&mut conn, "account_a", "a-1").await;
    RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("claim a-1")
        .expect("a-1 admitted");
    enqueue(&mut conn, "account_a", "a-2").await;

    // Tenant B: cap 1, one queued job, enqueued after A's second — so A's queued
    // job is the globally oldest, yet A is saturated.
    TENANT_SLOT
        .apply_cap_sync(&mut conn, "account_b", 1)
        .await
        .expect("cap b");
    enqueue(&mut conn, "account_b", "b-1").await;

    let claimed = RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("a claimable job exists");
    assert_eq!(
        claimed.account_id, "account_b",
        "the saturated tenant is skipped"
    );
}

#[tokio::test]
async fn test_two_tenants_with_headroom_drain_by_age() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");

    // Both tenants have headroom; the globally-older queued job is claimed first
    // regardless of tenant — cross-tenant order is age only.
    enqueue(&mut conn, "account_a", "older").await;
    enqueue(&mut conn, "account_b", "newer").await;

    let first = RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("first")
        .expect("first job");
    assert_eq!(
        first.account_id, "account_a",
        "the older job's tenant claims first"
    );
    let second = RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("second")
        .expect("second job");
    assert_eq!(second.account_id, "account_b", "the newer job follows");
}

// ---------------------------------------------------------------------------
// Cap sync
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_a_cap_sync_updates_the_cap_and_a_missing_slot_admits_at_default() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");

    // No slot row exists: the first claim admits at the conservative default.
    enqueue(&mut conn, "account_a", "job-0").await;
    RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("admitted at default");
    let slot = TENANT_SLOT
        .get(&mut conn, "account_a")
        .await
        .expect("slot")
        .expect("slot exists");
    assert_eq!(
        slot.cap, DEFAULT_CAP,
        "a missing slot is created at the default cap"
    );
    assert_eq!(slot.running, 1);

    // A cap sync raises the ceiling without disturbing the running count.
    TENANT_SLOT
        .apply_cap_sync(&mut conn, "account_a", 9)
        .await
        .expect("cap sync");
    let raised = TENANT_SLOT
        .get(&mut conn, "account_a")
        .await
        .expect("slot")
        .expect("slot exists");
    assert_eq!(raised.cap, 9);
    assert_eq!(
        raised.running, 1,
        "the run count is untouched by a cap sync"
    );
}

// ---------------------------------------------------------------------------
// Lease loss and reclaim
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_a_lost_lease_write_is_refused_by_the_token_fence_while_a_new_runner_holds_it() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");

    // A runner claims with an expired lease and is reclaimed; a fresh runner then
    // re-claims the same job. The job is running again — but under a new token.
    enqueue(&mut conn, "account_a", "job-0").await;
    let stale_runner = RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, expired_lease())
        .await
        .expect("claim")
        .expect("admitted");
    let reclaimed = RUN_JOB
        .reclaim_expired(&mut conn, 10)
        .await
        .expect("reclaim");
    assert_eq!(
        reclaimed,
        vec![stale_runner.id],
        "the expired job is reclaimed"
    );

    let live_runner = RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("re-claim")
        .expect("re-admitted");
    assert_eq!(
        live_runner.id, stale_runner.id,
        "the same job is re-claimed"
    );
    assert_ne!(
        live_runner.claim_token, stale_runner.claim_token,
        "a fresh claim mints a fresh token"
    );

    // The stale runner's write hits a job that IS running — so only the token
    // fence, not the state check, can refuse it. It must, and nothing changes.
    let stale = RUN_JOB
        .leave_running(
            &mut conn,
            stale_runner.id,
            stale_runner.claim_token,
            PostRunningState::Done,
        )
        .await
        .expect("stale write");
    assert_eq!(
        stale,
        WriteOutcome::OwnershipLost,
        "the stale token is fenced out"
    );
    assert_eq!(
        RUN_JOB
            .state_of(&mut conn, stale_runner.id)
            .await
            .expect("state"),
        Some(RunJobState::Running),
        "the live runner's job is untouched by the stale write",
    );
    assert_eq!(
        running_count(&mut conn, "account_a").await,
        1,
        "no double release of the slot"
    );

    // The live runner, holding the current token, commits its transition.
    let live = RUN_JOB
        .leave_running(
            &mut conn,
            live_runner.id,
            live_runner.claim_token,
            PostRunningState::Done,
        )
        .await
        .expect("live write");
    assert_eq!(live, WriteOutcome::Applied, "the live token commits");
}

#[tokio::test]
async fn test_running_never_leaks_when_a_claimed_job_is_reclaimed() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");

    enqueue(&mut conn, "account_a", "job-0").await;
    RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, expired_lease())
        .await
        .expect("claim")
        .expect("admitted");
    assert_eq!(
        running_count(&mut conn, "account_a").await,
        1,
        "the claim took a slot"
    );

    // A crash between claim and terminal is reclaimed with the slot released in
    // the same transaction that moves the job out of running.
    RUN_JOB
        .reclaim_expired(&mut conn, 10)
        .await
        .expect("reclaim");
    assert_eq!(
        running_count(&mut conn, "account_a").await,
        0,
        "the slot is released on reclaim"
    );
}

#[tokio::test]
async fn test_leaving_running_releases_the_slot_under_the_token_fence() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");

    enqueue(&mut conn, "account_a", "job-0").await;
    let claimed = RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("admitted");
    assert_eq!(running_count(&mut conn, "account_a").await, 1);

    let outcome = RUN_JOB
        .leave_running(
            &mut conn,
            claimed.id,
            claimed.claim_token,
            PostRunningState::Settling,
        )
        .await
        .expect("leave running");
    assert_eq!(outcome, WriteOutcome::Applied);
    assert_eq!(
        RUN_JOB
            .state_of(&mut conn, claimed.id)
            .await
            .expect("state"),
        Some(RunJobState::Settling),
    );
    assert_eq!(
        running_count(&mut conn, "account_a").await,
        0,
        "leaving running frees the slot"
    );
}

#[tokio::test]
async fn test_waking_a_suspended_job_requeues_it_for_a_fresh_claim() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");

    enqueue(&mut conn, "account_a", "job-0").await;
    let claimed = RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("admitted");
    RUN_JOB
        .leave_running(
            &mut conn,
            claimed.id,
            claimed.claim_token,
            PostRunningState::Suspended,
        )
        .await
        .expect("suspend");
    // A suspended job holds no slot and is not claimable.
    assert_eq!(running_count(&mut conn, "account_a").await, 0);
    assert!(
        RUN_JOB
            .claim(&mut conn, DEFAULT_CAP, lease())
            .await
            .expect("claim")
            .is_none(),
        "a suspended job is not claimable until it is woken",
    );

    let woke = RUN_JOB.wake(&mut conn, claimed.id).await.expect("wake");
    assert!(woke, "the suspended job woke");
    assert_eq!(
        RUN_JOB
            .state_of(&mut conn, claimed.id)
            .await
            .expect("state"),
        Some(RunJobState::Queued),
    );
    // The woken job is claimable again, under a fresh lease and token.
    let reclaimed = RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("the woken job is claimable");
    assert_eq!(reclaimed.id, claimed.id);
    assert_ne!(
        reclaimed.claim_token, claimed.claim_token,
        "the resume runs under a fresh token",
    );

    // Waking anything not suspended is a no-op.
    assert!(
        !RUN_JOB.wake(&mut conn, claimed.id).await.expect("wake"),
        "waking a running job changes nothing",
    );
}

// ---------------------------------------------------------------------------
// Erase reachability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_an_account_purge_removes_every_row_for_that_account_only() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.expect("acquire a connection");

    // Two tenants with jobs and slots; a claim on one leaves a running row too.
    enqueue(&mut conn, "account_a", "a-1").await;
    enqueue(&mut conn, "account_a", "a-2").await;
    RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("admitted");
    enqueue(&mut conn, "account_b", "b-1").await;

    let removed = tribal_runtime_db::purge_account(&mut conn, "account_a")
        .await
        .expect("purge");
    assert_eq!(removed, 2, "both of the account's jobs are removed");

    // The purged account leaves no residue: no jobs, no slot.
    assert!(
        TENANT_SLOT
            .get(&mut conn, "account_a")
            .await
            .expect("slot")
            .is_none(),
        "the purged account's slot is gone",
    );

    // The other account is untouched.
    let survivor = RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("claim survivor")
        .expect("the other account's job survives");
    assert_eq!(survivor.account_id, "account_b");
}
