//! Direct tests of the job-coupling seam.
//!
//! The fan-in's blocked-sibling behaviour is the load-bearing rule: a
//! task driving a suspended thread is live, so relation must
//! never fire while one exists. Tested against the seam itself — the one
//! callee both the commit and failure paths share — with the full-worker
//! fan-in tests covering each call site's integration.

use tribal_db::{JobRepository, TaskRepository};
use tribal_worker::coupling;

use crate::common::{
    JobStatus, PgJobRepository, PgTaskRepository, TaskType, a_candidate, raw_conn,
    seed_multiple_triage_tasks, serial_lock, setup_prerequisites, teardown, test_context,
};

#[tokio::test]
async fn test_fan_in_never_fires_while_a_sibling_is_blocked() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "fan-in-blocked").await;
    let candidates = vec![
        a_candidate()
            .content("Blocked sibling one".to_owned())
            .build(),
        a_candidate()
            .content("Blocked sibling two".to_owned())
            .build(),
    ];

    let mut conn = raw_conn(ctx).await;
    let (job_id, task_ids) = seed_multiple_triage_tasks(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
        &candidates,
    )
    .await;

    // Claim both siblings; block one (a suspended thread's driving task),
    // complete the other.
    let claimed = PgTaskRepository
        .claim(&mut conn, 2, "worker-coupling")
        .await
        .expect("claim both");
    assert_eq!(claimed.len(), 2);
    let blocked = claimed
        .iter()
        .find(|t| t.id() == task_ids[0])
        .expect("first");
    let completed = claimed
        .iter()
        .find(|t| t.id() == task_ids[1])
        .expect("second");

    PgTaskRepository
        .block(
            &mut conn,
            blocked.id(),
            blocked.claim_token().expect("token"),
        )
        .await
        .expect("block");
    PgTaskRepository
        .complete(
            &mut conn,
            completed.id(),
            completed.claim_token().expect("token"),
        )
        .await
        .expect("complete");

    let fired = coupling::triage_fan_in(&mut conn, job_id, completed.id())
        .await
        .expect("fan-in check");
    assert!(
        !fired,
        "a blocked sibling is live: the fan-in must not fire"
    );

    let tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find tasks");
    assert!(
        tasks.iter().all(|t| t.task_type() != TaskType::Relation),
        "no relation task may exist while a triage thread is suspended",
    );

    // Resolution re-queues the blocked row; once it completes, the same
    // seam fires.
    PgTaskRepository
        .requeue_from_blocked(&mut conn, blocked.id())
        .await
        .expect("requeue");
    let reclaimed = PgTaskRepository
        .claim(&mut conn, 1, "worker-coupling")
        .await
        .expect("reclaim");
    assert_eq!(reclaimed[0].id(), blocked.id());
    PgTaskRepository
        .complete(
            &mut conn,
            reclaimed[0].id(),
            reclaimed[0].claim_token().expect("token"),
        )
        .await
        .expect("complete the resolved task");

    let fired = coupling::triage_fan_in(&mut conn, job_id, reclaimed[0].id())
        .await
        .expect("fan-in check after resolution");
    assert!(fired, "the last live sibling's completion fires the fan-in");

    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("find job");
    assert_eq!(job.status(), JobStatus::Relating);

    teardown(ctx).await;
}
