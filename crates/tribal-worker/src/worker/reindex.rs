//! Reindex run creation.
//!
//! A reindex is single-flight per `embeddings` table: at most one run is live
//! at a time. Creation inserts a `building` target profile and a `queued` run
//! atomically under the single-flight advisory lock, so two concurrent
//! commands cannot both open a run. The command triggers creation; the worker
//! loop then promotes the queued run to `running` and drives it (backfill,
//! build, catch-up, cutover).

use sqlx::PgConnection;
use tribal_db::{
    AdvisoryLockRepository, DbError, EmbeddingProfileRepository, NewEmbeddingProfile,
    NewReindexRun, PgAdvisoryLockRepository, PgEmbeddingProfileRepository, PgReindexRunRepository,
    ReindexRunRepository, advisory_locks,
};
use tribal_domain::{DistanceMetric, PrincipalId, ProviderKind, ReindexRun};

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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_db::{PgPrincipalRepository, PrincipalRepository};
    use tribal_domain::ReindexRunState;
    use tribal_test_utils::{a_new_principal, serial_lock, test_context};

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
}
