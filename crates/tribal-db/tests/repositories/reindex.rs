use tribal_db::{
    DbError, NewReindexQuarantine, NewReindexRun, NewReindexTask, NewTagEmbedding,
    PgPrincipalRepository, PgReindexQuarantineRepository, PgReindexRunRepository,
    PgReindexTaskRepository, PgTagEmbeddingRepository, PgTagRegistryRepository,
    PrincipalRepository, ReindexQuarantineRepository, ReindexRunRepository, ReindexTaskRepository,
    TagEmbeddingRepository, TagRegistryRepository,
};
use tribal_domain::{
    EmbeddingErrorClass, ReindexEntityKind, ReindexRunId, ReindexRunState, ReindexTaskState,
};
use tribal_test_utils::{TestDb, a_new_principal, ensure_genesis_profile};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal and genesis profile, then a queued reindex run, and
/// returns the run id.
async fn setup_run(txn: &mut sqlx::PgConnection, suffix: &str) -> ReindexRunId {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:reindex-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal");
    let profile = ensure_genesis_profile(txn, "test-model", 768).await;
    let run = PgReindexRunRepository
        .insert(
            txn,
            &NewReindexRun::builder()
                .target_profile_id(profile.id())
                .epoch(profile.epoch())
                .initiated_by_principal_id(principal.id())
                .build(),
        )
        .await
        .expect("insert run");
    run.id()
}

fn item_task(run: ReindexRunId, target_ref: &str) -> NewReindexTask {
    NewReindexTask::builder()
        .reindex_run_id(run)
        .kind(ReindexEntityKind::Item)
        .target_ref(target_ref.to_owned())
        .build()
}

// ---------------------------------------------------------------------------
// Run lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_run_inserts_queued_and_find_live() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    let run_id = setup_run(&mut txn, "live").await;

    let live = PgReindexRunRepository
        .find_live(&mut txn)
        .await
        .expect("find_live")
        .expect("a live run");
    assert_eq!(live.id(), run_id);
    assert_eq!(live.state(), ReindexRunState::Queued);
    assert!(live.completed_at().is_none());

    // Complete it: no longer live, and completed_at is stamped.
    assert!(
        PgReindexRunRepository
            .transition(
                &mut txn,
                run_id,
                ReindexRunState::Queued,
                ReindexRunState::Completed,
                None,
            )
            .await
            .expect("transition")
    );
    assert!(
        PgReindexRunRepository
            .find_live(&mut txn)
            .await
            .expect("find_live")
            .is_none()
    );
    let completed = PgReindexRunRepository
        .find_by_id(&mut txn, run_id)
        .await
        .expect("find")
        .expect("run");
    assert_eq!(completed.state(), ReindexRunState::Completed);
    assert!(completed.completed_at().is_some());
}

#[tokio::test]
async fn test_single_flight_rejects_second_live_run() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    let _first = setup_run(&mut txn, "sf").await;

    // A second live run against the same table is rejected by uq_reindex_run_live.
    let principal = PgPrincipalRepository
        .insert(
            &mut txn,
            &a_new_principal()
                .principal_key("user:second".to_owned())
                .build(),
        )
        .await
        .expect("insert principal");
    let profile = ensure_genesis_profile(&mut txn, "test-model", 768).await;
    let result = PgReindexRunRepository
        .insert(
            &mut txn,
            &NewReindexRun::builder()
                .target_profile_id(profile.id())
                .epoch(profile.epoch())
                .initiated_by_principal_id(principal.id())
                .build(),
        )
        .await;
    assert!(
        matches!(result, Err(DbError::QueryFailed { .. })),
        "a second live run must be rejected, got {result:?}",
    );
}

#[tokio::test]
async fn test_run_tallies_accumulate() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let run_id = setup_run(&mut txn, "tally").await;

    PgReindexRunRepository
        .set_enumerated(&mut txn, run_id, 100, 10)
        .await
        .expect("set_enumerated");
    PgReindexRunRepository
        .bump_embedded(&mut txn, run_id, 40, 4)
        .await
        .expect("bump");
    PgReindexRunRepository
        .bump_embedded(&mut txn, run_id, 60, 6)
        .await
        .expect("bump");

    let run = PgReindexRunRepository
        .find_by_id(&mut txn, run_id)
        .await
        .expect("find")
        .expect("run");
    assert_eq!(run.items_enumerated(), Some(100));
    assert_eq!(run.tags_enumerated(), Some(10));
    assert_eq!(run.items_embedded(), 100);
    assert_eq!(run.tags_embedded(), 10);
}

/// The cumulative embedded tallies are BIGINT, so a corpus reindexed across many
/// catch-up passes can sum past the INT ceiling without overflowing.
#[tokio::test]
async fn test_embedded_tallies_exceed_int_ceiling() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let run_id = setup_run(&mut txn, "bigint").await;

    // Two passes of 1.5B each sum to 3B, past the 2.147B INT ceiling.
    let pass: u32 = 1_500_000_000;
    PgReindexRunRepository
        .bump_embedded(&mut txn, run_id, pass, pass)
        .await
        .expect("bump");
    PgReindexRunRepository
        .bump_embedded(&mut txn, run_id, pass, pass)
        .await
        .expect("bump");

    let run = PgReindexRunRepository
        .find_by_id(&mut txn, run_id)
        .await
        .expect("find")
        .expect("run");
    let expected = i64::from(pass) * 2;
    assert!(
        expected > i64::from(i32::MAX),
        "test must exceed the INT ceiling"
    );
    assert_eq!(run.items_embedded(), expected);
    assert_eq!(run.tags_embedded(), expected);
}

// ---------------------------------------------------------------------------
// Task lease
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_task_upsert_is_idempotent() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let run_id = setup_run(&mut txn, "upsert").await;

    let first = PgReindexTaskRepository
        .upsert(&mut txn, &item_task(run_id, "range:0-100"))
        .await
        .expect("upsert");
    assert_eq!(first, 1, "first enrolment inserts");

    let second = PgReindexTaskRepository
        .upsert(&mut txn, &item_task(run_id, "range:0-100"))
        .await
        .expect("upsert");
    assert_eq!(second, 0, "re-enrolment is a no-op");
}

#[tokio::test]
async fn test_task_claim_and_complete() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let run_id = setup_run(&mut txn, "claim").await;

    PgReindexTaskRepository
        .upsert(&mut txn, &item_task(run_id, "item:abc"))
        .await
        .expect("upsert");

    let claimed = PgReindexTaskRepository
        .claim(&mut txn, run_id, 10, "worker-1")
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    let task = &claimed[0];
    assert_eq!(task.state(), ReindexTaskState::Claimed);
    assert_eq!(task.claimed_by(), Some("worker-1"));
    let token = task.claim_token().expect("claimed task has a token");

    // A second claim finds nothing more.
    assert!(
        PgReindexTaskRepository
            .claim(&mut txn, run_id, 10, "worker-2")
            .await
            .expect("claim")
            .is_empty()
    );

    let affected = PgReindexTaskRepository
        .complete(&mut txn, task.id(), token)
        .await
        .expect("complete");
    assert_eq!(affected, 1);

    let stored = PgReindexTaskRepository
        .find_by_id(&mut txn, task.id())
        .await
        .expect("find")
        .expect("task");
    assert_eq!(stored.state(), ReindexTaskState::Completed);
    assert!(stored.completed_at().is_some());
}

#[tokio::test]
async fn test_claim_is_scoped_to_its_run_and_skips_a_prior_runs_leftover() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    // An older run enrols a task, then aborts, leaving the task pending. Single-
    // flight requires the older run to be terminal before a newer one is live.
    let older = setup_run(&mut txn, "older").await;
    PgReindexTaskRepository
        .upsert(&mut txn, &item_task(older, "item:leftover"))
        .await
        .expect("enrol older task");
    assert!(
        PgReindexRunRepository
            .transition(
                &mut txn,
                older,
                ReindexRunState::Queued,
                ReindexRunState::Aborted,
                Some("aborted"),
            )
            .await
            .expect("abort older run"),
    );

    // A newer run with its own task.
    let newer = setup_run(&mut txn, "newer").await;
    PgReindexTaskRepository
        .upsert(&mut txn, &item_task(newer, "item:current"))
        .await
        .expect("enrol newer task");

    // Claiming with the newer run's id never returns the older run's leftover.
    let claimed = PgReindexTaskRepository
        .claim(&mut txn, newer, 10, "worker")
        .await
        .expect("claim");
    assert_eq!(
        claimed.len(),
        1,
        "only the newer run's own task is claimable"
    );
    assert_eq!(
        claimed[0].target_ref(),
        "item:current",
        "the claim is scoped to the newer run, not the prior run's leftover",
    );
    assert_eq!(claimed[0].reindex_run_id(), newer);
}

#[tokio::test]
async fn test_task_fail_requeues_then_dead_letters() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let run_id = setup_run(&mut txn, "fail").await;

    // max_attempts defaults to 8; the task dead-letters once attempt exceeds it.
    PgReindexTaskRepository
        .upsert(&mut txn, &item_task(run_id, "item:flaky"))
        .await
        .expect("upsert");

    // Postgres `now()` is frozen at transaction start within a test, so requeue
    // each failure to a clearly-past `available_at` to keep it claimable.
    let available_at = chrono::Utc::now() - chrono::Duration::hours(1);
    let mut state = ReindexTaskState::Pending;
    for round in 0..9 {
        let claimed = PgReindexTaskRepository
            .claim(&mut txn, run_id, 1, "worker")
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "round {round}: should be claimable");
        let task = &claimed[0];
        PgReindexTaskRepository
            .fail(
                &mut txn,
                task.id(),
                task.claim_token().unwrap(),
                available_at,
                EmbeddingErrorClass::Transient,
                "transient blip",
            )
            .await
            .expect("fail");
        state = PgReindexTaskRepository
            .find_by_id(&mut txn, task.id())
            .await
            .expect("find")
            .expect("task")
            .state();
    }

    assert_eq!(
        state,
        ReindexTaskState::DeadLetter,
        "the task dead-letters after exhausting max_attempts",
    );
    // Dead-lettered tasks are no longer claimable.
    assert!(
        PgReindexTaskRepository
            .claim(&mut txn, run_id, 1, "worker")
            .await
            .expect("claim")
            .is_empty()
    );
}

#[tokio::test]
async fn test_task_count_by_state() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let run_id = setup_run(&mut txn, "count").await;

    for i in 0..3 {
        PgReindexTaskRepository
            .upsert(&mut txn, &item_task(run_id, &format!("item:{i}")))
            .await
            .expect("upsert");
    }
    let claimed = PgReindexTaskRepository
        .claim(&mut txn, run_id, 1, "worker")
        .await
        .expect("claim");
    PgReindexTaskRepository
        .complete(&mut txn, claimed[0].id(), claimed[0].claim_token().unwrap())
        .await
        .expect("complete");

    let counts = PgReindexTaskRepository
        .count_by_state(&mut txn, run_id)
        .await
        .expect("count");
    let pending = counts
        .iter()
        .find(|c| c.state == ReindexTaskState::Pending)
        .map_or(0, |c| c.count);
    let completed = counts
        .iter()
        .find(|c| c.state == ReindexTaskState::Completed)
        .map_or(0, |c| c.count);
    assert_eq!(pending, 2);
    assert_eq!(completed, 1);
}

// ---------------------------------------------------------------------------
// Quarantine
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quarantine_record_is_idempotent_and_counts() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let principal = PgPrincipalRepository
        .insert(
            &mut txn,
            &a_new_principal()
                .principal_key("user:quar".to_owned())
                .build(),
        )
        .await
        .expect("insert principal");
    let profile = ensure_genesis_profile(&mut txn, "test-model", 768).await;
    let run = PgReindexRunRepository
        .insert(
            &mut txn,
            &NewReindexRun::builder()
                .target_profile_id(profile.id())
                .epoch(profile.epoch())
                .initiated_by_principal_id(principal.id())
                .build(),
        )
        .await
        .expect("insert run");

    let quarantine = |entity: &str| NewReindexQuarantine {
        reindex_run_id: run.id(),
        target_profile_id: profile.id(),
        kind: ReindexEntityKind::Item,
        entity_ref: entity.to_owned(),
        error_class: EmbeddingErrorClass::Permanent,
        error_message: Some("bad input".to_owned()),
    };

    assert!(
        PgReindexQuarantineRepository
            .record(&mut txn, &quarantine("item:bad"))
            .await
            .expect("record"),
        "first record inserts",
    );
    assert!(
        !PgReindexQuarantineRepository
            .record(&mut txn, &quarantine("item:bad"))
            .await
            .expect("record"),
        "re-recording the same entity is a no-op",
    );
    PgReindexQuarantineRepository
        .record(&mut txn, &quarantine("item:also-bad"))
        .await
        .expect("record");

    let count = PgReindexQuarantineRepository
        .count_for_profile(&mut txn, profile.id())
        .await
        .expect("count");
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_find_tags_missing_embeddings_excludes_quarantined_tags() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let principal = PgPrincipalRepository
        .insert(
            &mut txn,
            &a_new_principal()
                .principal_key("user:tag-quar".to_owned())
                .build(),
        )
        .await
        .expect("insert principal");
    let profile = ensure_genesis_profile(&mut txn, "test-model", 768).await;
    let run = PgReindexRunRepository
        .insert(
            &mut txn,
            &NewReindexRun::builder()
                .target_profile_id(profile.id())
                .epoch(profile.epoch())
                .initiated_by_principal_id(principal.id())
                .build(),
        )
        .await
        .expect("insert run");

    PgTagRegistryRepository
        .batch_upsert(
            &mut txn,
            &["alpha".to_owned(), "beta".to_owned(), "gamma".to_owned()],
        )
        .await
        .expect("seed tags");

    // beta is embedded; gamma is permanently quarantined; only alpha is still
    // genuinely missing. Without the quarantine exclusion, gamma would recur
    // every pass and wedge the cutover.
    PgTagEmbeddingRepository
        .batch_upsert(
            &mut txn,
            &[NewTagEmbedding::builder()
                .tag("beta".to_owned())
                .embedding_profile_id(profile.id())
                .model("test-model".to_owned())
                .embedding(vec![0.1_f32; 768])
                .build()],
        )
        .await
        .expect("embed beta");
    PgReindexQuarantineRepository
        .record(
            &mut txn,
            &NewReindexQuarantine {
                reindex_run_id: run.id(),
                target_profile_id: profile.id(),
                kind: ReindexEntityKind::Tag,
                entity_ref: "gamma".to_owned(),
                error_class: EmbeddingErrorClass::Permanent,
                error_message: Some("poison".to_owned()),
            },
        )
        .await
        .expect("quarantine gamma");

    let missing = PgTagEmbeddingRepository
        .find_tags_missing_embeddings(&mut txn, profile.id())
        .await
        .expect("find_tags_missing_embeddings");
    assert_eq!(
        missing,
        vec!["alpha"],
        "the embedded tag and the quarantined tag are both excluded",
    );
}

#[tokio::test]
async fn test_claim_admits_nothing_after_the_run_aborts() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let run_id = setup_run(&mut txn, "aborting").await;
    PgReindexTaskRepository
        .upsert(&mut txn, &item_task(run_id, "item:pending"))
        .await
        .expect("enrol task");

    let before = PgReindexTaskRepository
        .claim(&mut txn, run_id, 10, "worker")
        .await
        .expect("claim under a live run");
    assert_eq!(
        before.len(),
        1,
        "the task is claimable while the run is live"
    );
    let reopened = PgReindexTaskRepository
        .fail(
            &mut txn,
            before[0].id(),
            before[0].claim_token().expect("claimed token"),
            chrono::Utc::now() - chrono::Duration::seconds(1),
            EmbeddingErrorClass::Transient,
            "requeue for the abort half",
        )
        .await
        .expect("requeue the task");
    assert_eq!(reopened, 1);

    assert!(
        PgReindexRunRepository
            .transition(
                &mut txn,
                run_id,
                ReindexRunState::Queued,
                ReindexRunState::Aborted,
                Some("aborted"),
            )
            .await
            .expect("abort run"),
    );

    let after = PgReindexTaskRepository
        .claim(&mut txn, run_id, 10, "worker")
        .await
        .expect("claim under an aborted run");
    assert!(
        after.is_empty(),
        "an aborted run admits no further claims, its pending task notwithstanding"
    );
}

#[tokio::test]
async fn test_retry_wait_aggregates_deferred_pending_tasks() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let run_id = setup_run(&mut txn, "retry-wait").await;
    PgReindexTaskRepository
        .upsert(&mut txn, &item_task(run_id, "item:deferred"))
        .await
        .expect("enrol task");

    let quiet = PgReindexTaskRepository
        .retry_wait(&mut txn, run_id)
        .await
        .expect("aggregate with nothing deferred");
    assert!(
        quiet.is_none(),
        "an immediately claimable task is not a retry wait"
    );

    let claimed = PgReindexTaskRepository
        .claim(&mut txn, run_id, 10, "worker")
        .await
        .expect("claim");
    let resume_at = chrono::Utc::now() + chrono::Duration::seconds(90);
    PgReindexTaskRepository
        .fail(
            &mut txn,
            claimed[0].id(),
            claimed[0].claim_token().expect("claimed token"),
            resume_at,
            EmbeddingErrorClass::RateLimited,
            "provider asked us to wait",
        )
        .await
        .expect("requeue with backoff");

    let wait = PgReindexTaskRepository
        .retry_wait(&mut txn, run_id)
        .await
        .expect("aggregate the deferred task")
        .expect("one task is waiting");
    // Postgres stores microseconds; compare at that precision.
    assert_eq!(
        wait.timestamp_micros(),
        resume_at.timestamp_micros(),
        "the earliest resume instant is reported"
    );
}
