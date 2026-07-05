//! The control crossings against real Postgres: an enqueued job crossing is
//! claimable under its derived account, a cap-sync push governs the next claim's
//! admission, and the grant minted at claim is scoped to the job's account.

mod support;

use chrono::Duration;
use sqlx::PgConnection;
use tribal_runtime_db::{PgRunJobRepository, RunJobRepository, enqueue_job, mint_grant, sync_cap};
use tribal_wire::gateway::{AccountReference, CapSync, JobEnqueue, JobKind};

const RUN_JOB: PgRunJobRepository = PgRunJobRepository;
const DEFAULT_CAP: i32 = 4;

fn lease() -> Duration {
    Duration::seconds(60)
}

fn job(kind: JobKind, key: &str) -> JobEnqueue {
    JobEnqueue {
        kind,
        payload: serde_json::json!({}),
        idempotency_key: key.to_owned(),
        priority: 0,
    }
}

#[tokio::test]
async fn test_an_enqueued_job_crossing_is_claimable_and_mints_an_account_scoped_grant() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.unwrap();

    let outcome = enqueue_job(&mut conn, "account_a", job(JobKind::Consolidate, "j1"))
        .await
        .expect("enqueue crossing");
    // The job is now queued and claimable under its derived account.
    let claimed = RUN_JOB
        .claim(&mut conn, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .expect("the enqueued job is claimable");
    assert_eq!(claimed.id, outcome.id());
    assert_eq!(claimed.account_id, "account_a");
    assert_eq!(claimed.kind, "consolidate");

    // The grant minted at claim scopes to the job's account, with no principal
    // and no pre-granted tools.
    let grant = mint_grant(&claimed);
    assert_eq!(grant.account.as_str(), "account_a");
    assert!(grant.principal.is_none());
    assert!(grant.tools.is_empty());
}

#[tokio::test]
async fn test_a_cap_sync_push_governs_the_next_claims_admission() {
    let db = support::provision().await;
    let mut conn = db.pool().acquire().await.unwrap();

    // Push a cap of 1, then queue two jobs for the tenant.
    sync_cap(
        &mut conn,
        &CapSync {
            account: AccountReference::new("account_a"),
            cap: 1,
        },
    )
    .await
    .expect("cap sync");
    enqueue_job(&mut conn, "account_a", job(JobKind::Probe, "j1"))
        .await
        .expect("enqueue");
    enqueue_job(&mut conn, "account_a", job(JobKind::Probe, "j2"))
        .await
        .expect("enqueue");

    // The synced cap of 1 admits exactly one — not the claim's default of 4.
    assert!(
        claims_something(&mut conn).await,
        "the first claim admits under the cap of 1",
    );
    assert!(
        !claims_something(&mut conn).await,
        "the synced cap of 1 saturates the tenant at one running job",
    );
}

async fn claims_something(conn: &mut PgConnection) -> bool {
    RUN_JOB
        .claim(conn, DEFAULT_CAP, lease())
        .await
        .expect("claim")
        .is_some()
}
