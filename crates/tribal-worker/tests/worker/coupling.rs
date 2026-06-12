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

/// A fan-in against a terminal job reports no transition: the relation
/// task may be upserted, but callers publish nothing.
#[tokio::test]
async fn test_fan_in_against_a_terminal_job_reports_no_transition() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "fan-in-terminal").await;
    let candidates = vec![a_candidate().content("lone candidate".to_owned()).build()];

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
    let task_id = task_ids[0];

    // The job reaches a terminal state before the late fan-in arrives.
    let failed = tribal_db::JobStatusTransition::builder()
        .status(JobStatus::Failed)
        .outcome(Some(tribal_domain::JobOutcome::Failure))
        .error_message(Some("raced to terminal".to_owned()))
        .completed_at(Some(chrono::Utc::now()))
        .build();
    PgJobRepository
        .update_status_if_live(&mut conn, job_id, &failed)
        .await
        .expect("fail the job");

    let fired = coupling::triage_fan_in(&mut conn, job_id, task_id)
        .await
        .expect("fan-in");

    assert!(
        !fired,
        "a terminal job's silent no-op must not read as a fired transition",
    );
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    assert_eq!(job.status(), JobStatus::Failed, "the terminal state holds");

    teardown(ctx).await;
}

/// A dead-letter coupling against a terminal job owes no notification.
#[tokio::test]
async fn test_couple_dead_lettered_against_a_terminal_job_owes_nothing() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "couple-terminal").await;

    let mut conn = raw_conn(ctx).await;
    let (job_id, _task_id) = tribal_test_utils::seed_extraction_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
    )
    .await;
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "couple-terminal")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();

    // The job completes before the late coupling arrives.
    let completed = tribal_db::JobStatusTransition::builder()
        .status(JobStatus::Completed)
        .outcome(Some(tribal_domain::JobOutcome::Success))
        .completed_at(Some(chrono::Utc::now()))
        .build();
    PgJobRepository
        .update_status_if_live(&mut conn, job_id, &completed)
        .await
        .expect("complete the job");

    let owed = coupling::couple_dead_lettered_task(&mut conn, &task, "late failure")
        .await
        .expect("couple");

    assert!(
        owed.is_none(),
        "a completed job must not yield a Failed notification",
    );
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("job");
    assert_eq!(
        job.status(),
        JobStatus::Completed,
        "the late coupling never resurrects or fails a completed job",
    );

    teardown(ctx).await;
}
