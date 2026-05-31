//! Reindex run creation and the backfill driver.
//!
//! A reindex is single-flight per `embeddings` table: at most one run is live
//! at a time. Creation inserts a `building` target profile and a `queued` run
//! atomically under the single-flight advisory lock, so two concurrent
//! commands cannot both open a run. The command triggers creation; the worker's
//! reindex loop then reconciles an orphan building profile on boot, promotes the
//! queued run to `running`, and enrols its backfill backlog (build, catch-up,
//! cutover follow). The set-difference is the completeness source of truth, so
//! enrolment is re-derivable and crash-safe.

use sqlx::PgConnection;
use tribal_db::{
    AdvisoryLockRepository, DbError, EmbeddingProfileRepository, EmbeddingRepository,
    NewEmbeddingProfile, NewReindexRun, NewReindexTask, PgAdvisoryLockRepository,
    PgEmbeddingProfileRepository, PgEmbeddingRepository, PgReindexRunRepository,
    PgReindexTaskRepository, PgTagEmbeddingRepository, ReindexRunRepository, ReindexTaskRepository,
    TagEmbeddingRepository, advisory_locks,
};
use tribal_domain::{
    DistanceMetric, EmbeddingProfileId, KnowledgeItemId, PrincipalId, ProviderKind,
    ReindexEntityKind, ReindexRun, ReindexRunId, ReindexRunState,
};

// ---------------------------------------------------------------------------
// ReindexTarget
// ---------------------------------------------------------------------------

/// The target embedding identity for a reindex, fully specified on the command.
///
/// `(provider_kind, normalised_base_url, model, dimensions, distance_metric)`
/// names the geometry; `revision_token`/`probe_digest` are the drift signals
/// the embedding service resolved through the target provider; and
/// `fingerprint_hash` is the stable handle over the identity tuple.
#[derive(Debug, Clone)]
pub struct ReindexTarget {
    /// The provider that will produce the new geometry.
    pub provider_kind: ProviderKind,
    /// The provider endpoint, normalised for catalogue and registry lookup.
    pub normalised_base_url: String,
    /// The embedding model name.
    pub model: String,
    /// The target vector dimension.
    pub dimensions: u32,
    /// The distance metric the new index is built for.
    pub distance_metric: DistanceMetric,
    /// A provider-exposed revision signal; empty when none.
    pub revision_token: String,
    /// The quantised probe digest, when one was resolved.
    pub probe_digest: Option<String>,
    /// Stable handle over the identity tuple.
    pub fingerprint_hash: String,
}

impl ReindexTarget {
    /// Projects the target into the `building`-profile insert shape.
    fn to_new_profile(&self) -> NewEmbeddingProfile {
        NewEmbeddingProfile::builder()
            .provider_kind(self.provider_kind)
            .normalised_base_url(self.normalised_base_url.clone())
            .model(self.model.clone())
            .dimensions(self.dimensions)
            .distance_metric(self.distance_metric)
            .revision_token(self.revision_token.clone())
            .probe_digest(self.probe_digest.clone())
            .fingerprint_hash(self.fingerprint_hash.clone())
            .build()
    }
}

// ---------------------------------------------------------------------------
// ReindexCreationOutcome
// ---------------------------------------------------------------------------

/// The outcome of attempting to create a reindex run.
#[derive(Debug)]
pub enum ReindexCreationOutcome {
    /// A `building` target profile and a `queued` run were created.
    Created(ReindexRun),
    /// A reindex is already live; single-flight returns it unchanged, having
    /// written nothing.
    AlreadyLive(ReindexRun),
    /// Another creation holds the single-flight lock; the caller should retry.
    LockContended,
}

// ---------------------------------------------------------------------------
// Creation
// ---------------------------------------------------------------------------

/// Creates a reindex run under the single-flight lock.
///
/// Must be called inside a transaction: the single-flight advisory lock is
/// xact-scoped, so it is held until the surrounding transaction commits the new
/// profile-plus-run (or rolls back). When the lock is contended or a run is
/// already live, no rows are written.
///
/// # Errors
///
/// Returns [`DbError`] if the lock acquisition, the live-run lookup, or either
/// insert fails. A second concurrent committed run is additionally rejected by
/// the `uq_reindex_run_live` constraint as a defence in depth.
pub async fn create_reindex_run(
    conn: &mut PgConnection,
    target: &ReindexTarget,
    initiated_by: PrincipalId,
) -> Result<ReindexCreationOutcome, DbError> {
    if !PgAdvisoryLockRepository
        .try_acquire_exclusive_xact(conn, advisory_locks::REINDEX_SINGLE_FLIGHT)
        .await?
    {
        return Ok(ReindexCreationOutcome::LockContended);
    }

    if let Some(run) = PgReindexRunRepository.find_live(conn).await? {
        return Ok(ReindexCreationOutcome::AlreadyLive(run));
    }

    let profile = PgEmbeddingProfileRepository
        .insert(conn, &target.to_new_profile())
        .await?;

    let run = PgReindexRunRepository
        .insert(
            conn,
            &NewReindexRun::builder()
                .target_profile_id(profile.id())
                .epoch(profile.epoch())
                .initiated_by_principal_id(initiated_by)
                .build(),
        )
        .await?;

    Ok(ReindexCreationOutcome::Created(run))
}

// ---------------------------------------------------------------------------
// Backfill enrolment
// ---------------------------------------------------------------------------

/// Number of item ids one `range:` backfill task covers. Enrolment pages the
/// item set-difference at this width, so the task table stays in the thousands
/// of rows for a large corpus while each task is roughly one `embed_many`
/// batch's worth of work.
const BACKFILL_RANGE_WIDTH: i64 = 256;

/// Reconciles a building profile orphaned by a crashed run.
///
/// A building profile with no live run can never reach cutover (the run that
/// would drive it is gone), so it is failed on boot and drops out of every
/// set-difference. Returns whether one was reconciled; a building profile a live
/// run still owns is left untouched.
pub async fn reconcile_orphan_building_profile(conn: &mut PgConnection) -> Result<bool, DbError> {
    let Some(building) = PgEmbeddingProfileRepository.find_building(conn).await? else {
        return Ok(false);
    };
    if PgReindexRunRepository.find_live(conn).await?.is_some() {
        return Ok(false);
    }
    PgEmbeddingProfileRepository
        .mark_failed(conn, building.id())
        .await
}

/// Enrols the backfill backlog as idempotent tasks: items missing the building
/// embedding as `range:<lo>..<hi>` batches over a stable id order, and tags
/// missing it as `tag:<text>` singletons. Returns the `(items, tags)` backlog
/// counts for the run's enumeration tally.
///
/// Re-enrolment is a no-op (`uq_reindex_task`), so a resumed run re-derives the
/// same tasks. The `..` range separator keeps the two uuid bounds unambiguous; a
/// uuid itself contains `-`.
pub async fn enrol_backfill(
    conn: &mut PgConnection,
    run_id: ReindexRunId,
    building_id: EmbeddingProfileId,
) -> Result<(u32, u32), DbError> {
    let mut items: u64 = 0;
    let mut after: Option<KnowledgeItemId> = None;
    loop {
        let ids = PgEmbeddingRepository
            .find_items_without_embedding(conn, building_id, after, BACKFILL_RANGE_WIDTH)
            .await?;
        let (Some(lo), Some(hi)) = (ids.first(), ids.last()) else {
            break;
        };
        PgReindexTaskRepository
            .upsert(
                conn,
                &NewReindexTask::builder()
                    .reindex_run_id(run_id)
                    .kind(ReindexEntityKind::Item)
                    .target_ref(format!("range:{}..{}", lo.inner(), hi.inner()))
                    .build(),
            )
            .await?;
        items += ids.len() as u64;
        after = Some(*hi);
    }

    let tags = PgTagEmbeddingRepository
        .find_tags_missing_embeddings(conn, building_id)
        .await?;
    for tag in &tags {
        PgReindexTaskRepository
            .upsert(
                conn,
                &NewReindexTask::builder()
                    .reindex_run_id(run_id)
                    .kind(ReindexEntityKind::Tag)
                    .target_ref(format!("tag:{tag}"))
                    .build(),
            )
            .await?;
    }

    Ok((
        u32::try_from(items).unwrap_or(u32::MAX),
        u32::try_from(tags.len()).unwrap_or(u32::MAX),
    ))
}

/// Advances the single live reindex run by one cycle: promotes a queued run to
/// running, then enrols its backfill backlog once (gated by the enumeration
/// tally). A no-op when no run is live.
///
/// Idempotent: a promote that loses the compare-and-set (the run left `queued`
/// under it, e.g. cancelled) yields the cycle, and enrolment runs only while the
/// tally is unset, so re-driving a running run does no redundant work.
pub async fn drive_reindex(conn: &mut PgConnection) -> Result<(), DbError> {
    let Some(run) = PgReindexRunRepository.find_live(conn).await? else {
        return Ok(());
    };

    if run.state() == ReindexRunState::Queued
        && !PgReindexRunRepository
            .transition(
                conn,
                run.id(),
                ReindexRunState::Queued,
                ReindexRunState::Running,
                None,
            )
            .await?
    {
        return Ok(());
    }

    if run.items_enumerated().is_none() {
        let (items, tags) = enrol_backfill(conn, run.id(), run.target_profile_id()).await?;
        PgReindexRunRepository
            .set_enumerated(conn, run.id(), items, tags)
            .await?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_db::{PgPrincipalRepository, PrincipalRepository};
    use tribal_domain::{KnowledgeKind, ReindexRunState};
    use tribal_test_utils::{
        Seed, a_new_embedding_profile, a_new_principal, item, serial_lock, test_context,
    };

    use super::*;

    fn a_target() -> ReindexTarget {
        ReindexTarget {
            provider_kind: ProviderKind::Ollama,
            normalised_base_url: "http://localhost:11500".to_owned(),
            model: "nomic-embed-text:v1.5".to_owned(),
            dimensions: 768,
            distance_metric: DistanceMetric::Cosine,
            revision_token: String::new(),
            probe_digest: None,
            fingerprint_hash: "reindex-test-fingerprint".to_owned(),
        }
    }

    async fn insert_principal(conn: &mut PgConnection, key: &str) -> PrincipalId {
        PgPrincipalRepository
            .insert(
                conn,
                &a_new_principal().principal_key(key.to_owned()).build(),
            )
            .await
            .expect("insert principal")
            .id()
    }

    #[tokio::test]
    async fn test_create_reindex_run_creates_building_profile_and_queued_run() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let principal = insert_principal(&mut txn, "user:reindex-create").await;

        let outcome = create_reindex_run(&mut txn, &a_target(), principal)
            .await
            .expect("create");
        let ReindexCreationOutcome::Created(run) = outcome else {
            panic!("expected Created, got {outcome:?}");
        };

        assert_eq!(run.state(), ReindexRunState::Queued);
        let building = PgEmbeddingProfileRepository
            .find_building(&mut txn)
            .await
            .expect("find_building")
            .expect("a building profile");
        assert_eq!(run.target_profile_id(), building.id());
        assert_eq!(building.dimensions(), 768);
    }

    #[tokio::test]
    async fn test_create_reindex_run_is_single_flight() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let principal = insert_principal(&mut txn, "user:reindex-sf").await;

        let first = create_reindex_run(&mut txn, &a_target(), principal)
            .await
            .expect("create");
        assert!(matches!(first, ReindexCreationOutcome::Created(_)));

        // A second creation while a run is already live returns it unchanged,
        // writing no second profile or run.
        let second = create_reindex_run(&mut txn, &a_target(), principal)
            .await
            .expect("create");
        assert!(matches!(second, ReindexCreationOutcome::AlreadyLive(_)));
    }

    #[tokio::test]
    async fn test_reconcile_fails_an_orphan_building_profile() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");

        // A building profile with no live run is an orphan from a crashed run.
        PgEmbeddingProfileRepository
            .insert(&mut txn, &a_new_embedding_profile().build())
            .await
            .expect("insert building profile");

        assert!(
            reconcile_orphan_building_profile(&mut txn)
                .await
                .expect("reconcile"),
            "an orphan building profile is reconciled",
        );
        assert!(
            PgEmbeddingProfileRepository
                .find_building(&mut txn)
                .await
                .expect("find_building")
                .is_none(),
            "the orphan is no longer building",
        );
    }

    #[tokio::test]
    async fn test_reconcile_leaves_a_building_profile_a_live_run_owns() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let principal = insert_principal(&mut txn, "user:reindex-reconcile-live").await;

        // Creation leaves a building profile under a live run; not an orphan.
        let outcome = create_reindex_run(&mut txn, &a_target(), principal)
            .await
            .expect("create");
        assert!(matches!(outcome, ReindexCreationOutcome::Created(_)));

        assert!(
            !reconcile_orphan_building_profile(&mut txn)
                .await
                .expect("reconcile"),
            "a building profile a live run owns is left untouched",
        );
        assert!(
            PgEmbeddingProfileRepository
                .find_building(&mut txn)
                .await
                .expect("find_building")
                .is_some(),
            "the building profile is still building",
        );
    }

    #[tokio::test]
    async fn test_drive_reindex_promotes_a_queued_run_and_enrols_the_backlog() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");

        // Seed a corpus of three items and a tag against the active profile; none
        // carry the building geometry, so all fall in the building set-difference.
        let seed = Seed::new()
            .define_principal("user", "user:reindex-drive")
            .define_project("proj", "git@github.com:test/reindex-drive.git")
            .set_embedding_model("mock-model", 768)
            .define_tag("rust")
            .as_principal("user")
            .for_project("proj", |store| {
                store.add_item("a", item(KnowledgeKind::Fact, "first"));
                store.add_item("b", item(KnowledgeKind::Fact, "second"));
                store.add_item("c", item(KnowledgeKind::Fact, "third"));
            })
            .execute(&mut txn)
            .await;
        let principal = seed.principal_id("user");

        let outcome = create_reindex_run(&mut txn, &a_target(), principal)
            .await
            .expect("create");
        let ReindexCreationOutcome::Created(run) = outcome else {
            panic!("expected Created, got {outcome:?}");
        };
        assert_eq!(run.state(), ReindexRunState::Queued);

        drive_reindex(&mut txn).await.expect("drive");

        // The run is running, its backlog enumerated against the building profile.
        let live = PgReindexRunRepository
            .find_live(&mut txn)
            .await
            .expect("find_live")
            .expect("a live run");
        assert_eq!(live.state(), ReindexRunState::Running);
        assert_eq!(live.items_enumerated(), Some(3));
        assert_eq!(live.tags_enumerated(), Some(1));

        // Three items fit one range task; the lone tag is a singleton.
        let total: i64 = PgReindexTaskRepository
            .count_by_state(&mut txn, run.id())
            .await
            .expect("count")
            .iter()
            .map(|c| c.count)
            .sum();
        assert_eq!(total, 2, "one range task plus one tag task");

        // Re-driving a running run re-derives the same tasks, enrolling nothing new.
        drive_reindex(&mut txn).await.expect("redrive");
        let total_again: i64 = PgReindexTaskRepository
            .count_by_state(&mut txn, run.id())
            .await
            .expect("count")
            .iter()
            .map(|c| c.count)
            .sum();
        assert_eq!(total_again, 2, "enrolment is idempotent");
    }
}
