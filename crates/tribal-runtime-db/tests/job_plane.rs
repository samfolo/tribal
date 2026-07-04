//! The job plane's admission, fairness, and lifecycle invariants, against real
//! Postgres. Concurrency is exercised with real connections and a barrier so the
//! single-row admission compare-and-swap is put under genuine contention.

mod support;

use std::sync::Arc;

use chrono::Duration;
use sqlx::PgPool;
use tokio::sync::Barrier;
use tribal_runtime_db::{
    NewRunJob, PgRunJobRepository, PgTenantSlotRepository, PostRunningState, RunJobRepository,
    RunJobState, TenantSlotRepository, WriteOutcome,
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
async fn enqueue(pool: &PgPool, tenant: &str, key: &str) {
    RUN_JOB
        .enqueue(
            pool,
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

async fn running_count(pool: &PgPool, tenant: &str) -> i32 {
    TENANT_SLOT
        .get(pool, tenant)
        .await
        .expect("read slot")
        .map_or(0, |slot| slot.running)
}

// ---------------------------------------------------------------------------
// Enqueue and basic claim
// ---------------------------------------------------------------------------

#[tokio::test]
async fn enqueue_dedups_on_idempotency_key() {
    let db = support::provision().await;
    let first = RUN_JOB
        .enqueue(
            db.pool(),
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
            db.pool(),
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
async fn an_enqueued_job_is_claimable_with_no_schedule() {
    let db = support::provision().await;
    enqueue(db.pool(), "account_a", "job-1").await;
    let claimed = RUN_JOB
        .claim(db.pool(), DEFAULT_CAP, lease())
        .await
        .expect("claim");
    assert!(
        claimed.is_some(),
        "an enqueued job is claimable with no schedule"
    );
    assert_eq!(running_count(db.pool(), "account_a").await, 1);
}

// ---------------------------------------------------------------------------
// Admission concurrency
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_claimers_at_cap_minus_one_admit_exactly_one() {
    let db = support::provision().await;
    let pool = db.pool().clone();
    let tenant = "account_a";

    // cap = 5, pre-claim 4 so the tenant sits at running = cap - 1, with plenty
    // of queued jobs left for the racers to contend over.
    TENANT_SLOT
        .apply_cap_sync(&pool, tenant, 5)
        .await
        .expect("cap sync");
    for i in 0..12 {
        enqueue(&pool, tenant, &format!("job-{i}")).await;
    }
    for _ in 0..4 {
        RUN_JOB
            .claim(&pool, DEFAULT_CAP, lease())
            .await
            .expect("pre-claim")
            .expect("pre-claim admitted");
    }
    assert_eq!(running_count(&pool, tenant).await, 4, "at cap - 1");

    // 16 racers contend for the one remaining slot.
    let racers = 16;
    let barrier = Arc::new(Barrier::new(racers));
    let mut handles = Vec::new();
    for _ in 0..racers {
        let pool = pool.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            RUN_JOB
                .claim(&pool, DEFAULT_CAP, lease())
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
        running_count(&pool, tenant).await,
        5,
        "running reaches the cap, never past"
    );
}

#[tokio::test]
async fn a_saturated_tenant_is_skipped_for_another_tenants_claimable_job() {
    let db = support::provision().await;
    let pool = db.pool();

    // Tenant A: cap 1, one job claimed (saturated), a second job queued and older
    // than B's — age order would prefer A, but saturation must skip it.
    TENANT_SLOT
        .apply_cap_sync(pool, "account_a", 1)
        .await
        .expect("cap a");
    enqueue(pool, "account_a", "a-1").await;
    RUN_JOB
        .claim(pool, DEFAULT_CAP, lease())
        .await
        .expect("claim a-1")
        .expect("a-1 admitted");
    enqueue(pool, "account_a", "a-2").await;

    // Tenant B: cap 1, one queued job, enqueued after A's second — so A's queued
    // job is the globally oldest, yet A is saturated.
    TENANT_SLOT
        .apply_cap_sync(pool, "account_b", 1)
        .await
        .expect("cap b");
    enqueue(pool, "account_b", "b-1").await;

    let claimed = RUN_JOB
        .claim(pool, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("a claimable job exists");
    assert_eq!(
        claimed.account_id, "account_b",
        "the saturated tenant is skipped"
    );
}

#[tokio::test]
async fn two_tenants_with_headroom_drain_by_age() {
    let db = support::provision().await;
    let pool = db.pool();

    // Both tenants have headroom; the globally-older queued job is claimed first
    // regardless of tenant — cross-tenant order is age only.
    enqueue(pool, "account_a", "older").await;
    enqueue(pool, "account_b", "newer").await;

    let first = RUN_JOB
        .claim(pool, DEFAULT_CAP, lease())
        .await
        .expect("first")
        .expect("first job");
    assert_eq!(
        first.account_id, "account_a",
        "the older job's tenant claims first"
    );
    let second = RUN_JOB
        .claim(pool, DEFAULT_CAP, lease())
        .await
        .expect("second")
        .expect("second job");
    assert_eq!(second.account_id, "account_b", "the newer job follows");
}

// ---------------------------------------------------------------------------
// Cap sync
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_cap_sync_updates_the_cap_and_a_missing_slot_admits_at_default() {
    let db = support::provision().await;
    let pool = db.pool();

    // No slot row exists: the first claim admits at the conservative default.
    enqueue(pool, "account_a", "job-0").await;
    RUN_JOB
        .claim(pool, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("admitted at default");
    let slot = TENANT_SLOT
        .get(pool, "account_a")
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
        .apply_cap_sync(pool, "account_a", 9)
        .await
        .expect("cap sync");
    let raised = TENANT_SLOT
        .get(pool, "account_a")
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
async fn a_lost_lease_write_is_refused_by_the_token_fence_while_a_new_runner_holds_it() {
    let db = support::provision().await;
    let pool = db.pool();

    // A runner claims with an expired lease and is reclaimed; a fresh runner then
    // re-claims the same job. The job is running again — but under a new token.
    enqueue(pool, "account_a", "job-0").await;
    let stale_runner = RUN_JOB
        .claim(pool, DEFAULT_CAP, expired_lease())
        .await
        .expect("claim")
        .expect("admitted");
    let reclaimed = RUN_JOB.reclaim_expired(pool, 10).await.expect("reclaim");
    assert_eq!(
        reclaimed,
        vec![stale_runner.id],
        "the expired job is reclaimed"
    );

    let live_runner = RUN_JOB
        .claim(pool, DEFAULT_CAP, lease())
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
            pool,
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
            .state_of(pool, stale_runner.id)
            .await
            .expect("state"),
        Some(RunJobState::Running),
        "the live runner's job is untouched by the stale write",
    );
    assert_eq!(
        running_count(pool, "account_a").await,
        1,
        "no double release of the slot"
    );

    // The live runner, holding the current token, commits its transition.
    let live = RUN_JOB
        .leave_running(
            pool,
            live_runner.id,
            live_runner.claim_token,
            PostRunningState::Done,
        )
        .await
        .expect("live write");
    assert_eq!(live, WriteOutcome::Applied, "the live token commits");
}

#[tokio::test]
async fn running_never_leaks_when_a_claimed_job_is_reclaimed() {
    let db = support::provision().await;
    let pool = db.pool();

    enqueue(pool, "account_a", "job-0").await;
    RUN_JOB
        .claim(pool, DEFAULT_CAP, expired_lease())
        .await
        .expect("claim")
        .expect("admitted");
    assert_eq!(
        running_count(pool, "account_a").await,
        1,
        "the claim took a slot"
    );

    // A crash between claim and terminal is reclaimed with the slot released in
    // the same transaction that moves the job out of running.
    RUN_JOB.reclaim_expired(pool, 10).await.expect("reclaim");
    assert_eq!(
        running_count(pool, "account_a").await,
        0,
        "the slot is released on reclaim"
    );
}

#[tokio::test]
async fn leaving_running_releases_the_slot_under_the_token_fence() {
    let db = support::provision().await;
    let pool = db.pool();

    enqueue(pool, "account_a", "job-0").await;
    let claimed = RUN_JOB
        .claim(pool, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("admitted");
    assert_eq!(running_count(pool, "account_a").await, 1);

    let outcome = RUN_JOB
        .leave_running(
            pool,
            claimed.id,
            claimed.claim_token,
            PostRunningState::Settling,
        )
        .await
        .expect("leave running");
    assert_eq!(outcome, WriteOutcome::Applied);
    assert_eq!(
        RUN_JOB.state_of(pool, claimed.id).await.expect("state"),
        Some(RunJobState::Settling),
    );
    assert_eq!(
        running_count(pool, "account_a").await,
        0,
        "leaving running frees the slot"
    );
}

// ---------------------------------------------------------------------------
// Erase reachability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_account_purge_removes_every_row_for_that_account_only() {
    let db = support::provision().await;
    let pool = db.pool();

    // Two tenants with jobs and slots; a claim on one leaves a running row too.
    enqueue(pool, "account_a", "a-1").await;
    enqueue(pool, "account_a", "a-2").await;
    RUN_JOB
        .claim(pool, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("admitted");
    enqueue(pool, "account_b", "b-1").await;

    let removed = tribal_runtime_db::purge_account(pool, "account_a")
        .await
        .expect("purge");
    assert_eq!(removed, 2, "both of the account's jobs are removed");

    // The purged account leaves no residue: no jobs, no slot.
    assert!(
        TENANT_SLOT
            .get(pool, "account_a")
            .await
            .expect("slot")
            .is_none(),
        "the purged account's slot is gone",
    );

    // The other account is untouched.
    let survivor = RUN_JOB
        .claim(pool, DEFAULT_CAP, lease())
        .await
        .expect("claim survivor")
        .expect("the other account's job survives");
    assert_eq!(survivor.account_id, "account_b");
}
