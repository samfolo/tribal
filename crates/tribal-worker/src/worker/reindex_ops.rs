//! Operator-facing reindex services: create, cancel, and prune.
//!
//! These wrap the reindex primitives behind one connection-and-registry
//! interface so the MCP tool handlers and the `tribal reindex` CLI drive the
//! same logic. The create path builds and probes the target provider before it
//! opens any transaction, preserving the invariant that no provider call spans
//! the single-flight lock.

use sqlx::PgPool;
use tribal_db::{
    DbError, EmbeddingIndexRepository, EmbeddingProfileRepository, EmbeddingRepository,
    EmbeddingTable, PgEmbeddingIndexRepository, PgEmbeddingProfileRepository,
    PgEmbeddingRepository, PgReindexRunRepository, PgTagEmbeddingRepository, ReindexRunRepository,
    TagEmbeddingRepository,
};
use tribal_domain::{
    DistanceMetric, EmbeddingProfileId, EndpointUrlError, PrincipalId, ProviderKind, ReindexRunId,
    ReindexRunState, normalise_endpoint_url,
};
use tribal_inference::{
    DimensionResolutionError, EmbeddingTarget, InferenceError, InferenceFacade, resolve_dimensions,
};

use super::reindex::{ReindexCreationOutcome, create_reindex_run, resolve_reindex_target};

/// The error message stamped on a run aborted by an operator cancel.
const CANCEL_REASON: &str = "cancelled by operator";

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

/// A named reindex target as supplied by an operator.
pub struct ReindexRunRequest {
    /// Provider name; parsed against [`ProviderKind`].
    pub provider: String,
    /// Embedding model.
    pub model: String,
    /// Explicit dimension, or `None` to resolve the provider/model native one.
    pub dimensions: Option<u32>,
    /// Endpoint base URL, or `None` for the provider's canonical endpoint.
    pub base_url: Option<String>,
    /// When set, estimate only; no run is created.
    pub dry_run: bool,
}

/// How a create request resolved against the latest profile. The run-bearing
/// variants own their run id, so a created or resumed run always has one and an
/// outcome carrying a run id without a creating resolution is unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReindexResolution {
    /// A dry run; no run was created.
    Plan,
    /// A new building profile and queued run were created.
    Created {
        /// The created run.
        run_id: ReindexRunId,
    },
    /// A matching live run already existed and was resumed.
    AlreadyLive {
        /// The resumed run.
        run_id: ReindexRunId,
    },
    /// The target already matches the active profile; nothing to do.
    Unchanged,
    /// Another invocation held the single-flight lock.
    LockContended,
}

impl ReindexResolution {
    /// The stable wire/CLI label for this resolution.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Created { .. } => "created",
            Self::AlreadyLive { .. } => "already_live",
            Self::Unchanged => "unchanged",
            Self::LockContended => "lock_contended",
        }
    }

    /// The run this resolution created or resumed, when one applies.
    #[must_use]
    pub fn run_id(self) -> Option<ReindexRunId> {
        match self {
            Self::Created { run_id } | Self::AlreadyLive { run_id } => Some(run_id),
            Self::Plan | Self::Unchanged | Self::LockContended => None,
        }
    }
}

/// The resolved target and pre-flight estimate of a create request.
pub struct ReindexRunOutcome {
    /// How the request resolved, owning the run id when one applies.
    pub resolution: ReindexResolution,
    /// The resolved target provider.
    pub provider: ProviderKind,
    /// The resolved target model.
    pub model: String,
    /// The resolved target dimension.
    pub dimensions: u32,
    /// The normalised target endpoint.
    pub normalised_base_url: String,
    /// Items still missing an embedding under the target geometry.
    pub estimated_items: u64,
    /// Tags still missing an embedding under the target geometry.
    pub estimated_tags: u64,
}

/// Errors from [`reindex_run`].
#[derive(Debug, thiserror::Error)]
pub enum ReindexOpError {
    /// The provider string did not name a known embedding provider.
    #[error("unknown embedding provider: {0}")]
    UnknownProvider(String),
    /// The target endpoint URL is malformed.
    #[error(transparent)]
    Url(#[from] EndpointUrlError),
    /// The target dimension could not be resolved for the model.
    #[error(transparent)]
    Dimensions(#[from] DimensionResolutionError),
    /// The target provider could not be built (a missing credential, an
    /// unsupported provider kind).
    #[error("resolving the target provider: {0}")]
    Provider(InferenceError),
    /// The drift-signal probe against the target provider failed.
    #[error("probing the target provider: {0}")]
    Probe(InferenceError),
    /// A database error.
    #[error(transparent)]
    Db(#[from] DbError),
}

/// Resolves a named target, validates its credential, probes its drift signal,
/// and creates a reindex run (or estimates only, for a dry run).
///
/// The provider build and probe precede the transaction, since no provider call
/// may span the single-flight lock; the run is then created atomically.
///
/// # Errors
///
/// Returns [`ReindexOpError`] when the target cannot be parsed, resolved,
/// built, probed, or persisted.
pub async fn reindex_run(
    pool: &PgPool,
    facade: &InferenceFacade,
    request: &ReindexRunRequest,
    principal_id: PrincipalId,
) -> Result<ReindexRunOutcome, ReindexOpError> {
    let provider = request
        .provider
        .parse::<ProviderKind>()
        .map_err(|_| ReindexOpError::UnknownProvider(request.provider.clone()))?;
    let base_url = request
        .base_url
        .clone()
        .unwrap_or_else(|| provider.default_base_url().to_owned());
    let normalised_base_url = normalise_endpoint_url(&base_url)?;
    let dimensions = resolve_dimensions(provider, &request.model, request.dimensions)?;

    // Preparing the target validates the credential fail-closed without a
    // network call; the probe (drift signal) is deferred to the real run.
    let target = EmbeddingTarget {
        provider,
        model: request.model.clone(),
        dimensions,
        base_url: normalised_base_url.clone(),
        profile_id: None,
    };
    facade
        .prepare_embedding_target(&target)
        .map_err(ReindexOpError::Provider)?;

    let (resolution, estimate_profile) = if request.dry_run {
        let mut conn = acquire(pool, "resolving the reindex estimate target").await?;
        let profile = dry_run_estimate_profile(
            &mut conn,
            provider,
            &normalised_base_url,
            &request.model,
            dimensions,
        )
        .await?;
        (ReindexResolution::Plan, Some(profile))
    } else {
        let target = resolve_reindex_target(facade, &target, DistanceMetric::Cosine)
            .await
            .map_err(ReindexOpError::Probe)?;

        let mut tx = pool.begin().await.map_err(|source| {
            ReindexOpError::Db(DbError::QueryFailed {
                context: "beginning the reindex transaction".to_owned(),
                source,
            })
        })?;
        let outcome = create_reindex_run(&mut tx, &target, principal_id).await?;
        tx.commit().await.map_err(|source| {
            ReindexOpError::Db(DbError::QueryFailed {
                context: "committing the reindex".to_owned(),
                source,
            })
        })?;

        match outcome {
            ReindexCreationOutcome::Created(run) => (
                ReindexResolution::Created { run_id: run.id() },
                Some(run.target_profile_id()),
            ),
            ReindexCreationOutcome::AlreadyLive(run) => (
                ReindexResolution::AlreadyLive { run_id: run.id() },
                Some(run.target_profile_id()),
            ),
            ReindexCreationOutcome::Unchanged(_) => (ReindexResolution::Unchanged, None),
            ReindexCreationOutcome::LockContended => (ReindexResolution::LockContended, None),
        }
    };

    let (estimated_items, estimated_tags) = match estimate_profile {
        Some(profile) => {
            let mut conn = acquire(pool, "estimating the reindex corpus").await?;
            estimate_corpus(&mut conn, profile).await?
        }
        None => (0, 0),
    };

    Ok(ReindexRunOutcome {
        resolution,
        provider,
        model: request.model.clone(),
        dimensions,
        normalised_base_url,
        estimated_items,
        estimated_tags,
    })
}

async fn acquire(
    pool: &PgPool,
    context: &str,
) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, ReindexOpError> {
    pool.acquire().await.map_err(|source| {
        ReindexOpError::Db(DbError::QueryFailed {
            context: format!("acquiring a connection for {context}"),
            source,
        })
    })
}

/// Counts the items and tags still missing an embedding under `profile_id`, the
/// work the target geometry must still embed. A fresh, never-used id sees the
/// whole corpus; the active id sees only the un-embedded backlog, so an
/// unchanged target estimates as near-zero rather than a full re-embed.
async fn estimate_corpus(
    conn: &mut sqlx::PgConnection,
    profile_id: EmbeddingProfileId,
) -> Result<(u64, u64), ReindexOpError> {
    let items = PgEmbeddingRepository
        .count_items_without_embedding(conn, profile_id)
        .await?;
    let tags = PgTagEmbeddingRepository
        .find_tags_missing_embeddings(conn, profile_id)
        .await?
        .len();
    Ok((
        u64::try_from(items).unwrap_or(0),
        u64::try_from(tags).unwrap_or(u64::MAX),
    ))
}

/// The profile a network-free dry run estimates against. When the declared
/// identity already matches the active, the corpus is in this geometry and the
/// active's backlog is the honest estimate; otherwise a fresh geometry must
/// embed the whole corpus, which a never-used id counts.
async fn dry_run_estimate_profile(
    conn: &mut sqlx::PgConnection,
    provider: ProviderKind,
    normalised_base_url: &str,
    model: &str,
    dimensions: u32,
) -> Result<EmbeddingProfileId, ReindexOpError> {
    let active = PgEmbeddingProfileRepository.find_active(conn).await?;
    Ok(match active {
        Some(active)
            if active.matches_geometry(
                provider,
                normalised_base_url,
                model,
                dimensions,
                DistanceMetric::Cosine,
            ) =>
        {
            active.id()
        }
        _ => EmbeddingProfileId::new(),
    })
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

/// The outcome of a cancel request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReindexCancelOutcome {
    /// A live run was transitioned to aborted and its building profile failed.
    Cancelled(ReindexRunId),
    /// No live run existed, or it reached a terminal state before the cancel
    /// could claim it.
    NoLiveRun,
}

/// Aborts the live reindex run within a single transaction.
///
/// Fails the profile before aborting the run so this transaction takes the
/// `embedding_profiles` and `reindex_runs` row locks in the same order the
/// worker's cutover and `fail_run` use (profile, then run); the opposite order
/// let a cancel and an in-flight cutover deadlock on the two rows. Both writes
/// are compare-and-set on their from-state, so a cutover that completes the
/// profile and run first wins the race: both guards no-op and the flip stands.
///
/// # Errors
///
/// Returns [`DbError`] on a database error.
pub async fn reindex_cancel(
    conn: &mut sqlx::PgConnection,
) -> Result<ReindexCancelOutcome, DbError> {
    let Some(run) = PgReindexRunRepository.find_live(conn).await? else {
        return Ok(ReindexCancelOutcome::NoLiveRun);
    };

    // Profile first, then run: the CAS on `state = 'building'` no-ops when a
    // cutover has already flipped the profile to complete, so failing it
    // unconditionally here is safe and keeps the lock order consistent.
    PgEmbeddingProfileRepository
        .mark_failed(conn, run.target_profile_id())
        .await?;

    let aborted = PgReindexRunRepository
        .transition(
            conn,
            run.id(),
            run.state(),
            ReindexRunState::Aborted,
            Some(CANCEL_REASON),
        )
        .await?;
    if aborted {
        Ok(ReindexCancelOutcome::Cancelled(run.id()))
    } else {
        Ok(ReindexCancelOutcome::NoLiveRun)
    }
}

// ---------------------------------------------------------------------------
// Prune
// ---------------------------------------------------------------------------

/// The counts a prune reclaimed, plus the epochs whose partial indexes the
/// caller drops after the transaction commits.
pub struct ReindexPruneOutcome {
    /// Profiles transitioned to superseded by this prune.
    pub profiles_superseded: u64,
    /// Embedding rows deleted.
    pub embeddings_deleted: u64,
    /// Tag-embedding rows deleted.
    pub tag_embeddings_deleted: u64,
    /// Every superseded epoch whose partial index still exists, so it is dropped
    /// after the prune transaction commits. This spans the epochs this prune
    /// freshly superseded plus any earlier-superseded epoch whose prior drop
    /// failed and whose dead index lingers, so a later prune retries it.
    pub superseded_epochs: Vec<i64>,
}

/// Supersedes every prunable profile and deletes their embeddings within a
/// single transaction. Supersede precedes delete, so the delete's join sees the
/// freshly-superseded profiles; the active profile and its rows are untouched.
/// The drop-eligible epochs are then re-derived from the superseded profiles
/// whose index still exists, so a prior failed drop is retried rather than lost.
///
/// # Errors
///
/// Returns [`DbError`] on a database error.
pub async fn reindex_prune(conn: &mut sqlx::PgConnection) -> Result<ReindexPruneOutcome, DbError> {
    let newly_superseded = PgEmbeddingProfileRepository
        .supersede_prunable(conn)
        .await?;
    let embeddings_deleted = PgEmbeddingRepository.delete_superseded(conn).await?;
    let tag_embeddings_deleted = PgTagEmbeddingRepository.delete_superseded(conn).await?;
    // Drive index cleanup from every superseded epoch whose index still exists,
    // not just the freshly-transitioned set: a prior prune's failed drop leaves
    // a profile already superseded, so the supersede transition no longer
    // re-emits it, yet its dead index must still be reclaimed.
    let superseded_epochs = PgEmbeddingProfileRepository
        .prunable_index_epochs(conn)
        .await?;
    Ok(ReindexPruneOutcome {
        profiles_superseded: u64::try_from(newly_superseded.len()).unwrap_or(u64::MAX),
        embeddings_deleted,
        tag_embeddings_deleted,
        superseded_epochs,
    })
}

/// Drops the partial HNSW indexes of the superseded profiles, reclaiming their
/// catalogue storage. Best-effort and outside the prune transaction, since
/// `DROP INDEX CONCURRENTLY` cannot run in one; the supersede and row deletes
/// have already committed, so a failed drop only leaves a dead, empty index for
/// a later prune to retry.
pub async fn drop_superseded_indexes(conn: &mut sqlx::PgConnection, epochs: &[i64]) {
    for &epoch in epochs {
        for table in [EmbeddingTable::Embeddings, EmbeddingTable::TagEmbeddings] {
            if let Err(e) = PgEmbeddingIndexRepository
                .drop_partial_hnsw(conn, table, epoch)
                .await
            {
                tracing::warn!(
                    epoch,
                    table = table.as_str(),
                    error = %e,
                    "failed to drop a superseded profile's partial index; a later prune retries",
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_test_utils::{
        a_new_embedding_profile, serial_lock, test_context, truncate_all_tables,
    };

    use super::*;

    /// A failed index drop is retried by a later prune. The supersede transition
    /// only re-emits freshly-transitioned profiles, so once a profile is
    /// superseded its dead index would otherwise linger forever; prune instead
    /// drives cleanup from every superseded profile whose index still exists, so
    /// the second prune still targets the epoch left behind by the failed drop.
    /// Against a live database, since a real partial index must exist to enumerate.
    #[tokio::test]
    async fn test_prune_retries_a_superseded_profiles_lingering_index() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        // CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction, so use
        // a committed raw connection; truncate cleans the committed rows at the
        // end (every reindex worker test serialises on the same lock).
        let mut conn = ctx.raw_connection().await.expect("raw connection");
        let table = EmbeddingTable::Embeddings;

        // A lower-epoch complete profile (prunable) below a higher-epoch complete
        // one (the active, never superseded).
        let prunable = PgEmbeddingProfileRepository
            .insert(&mut conn, &a_new_embedding_profile().build())
            .await
            .expect("insert prunable");
        PgEmbeddingProfileRepository
            .mark_complete(&mut conn, prunable.id())
            .await
            .expect("complete prunable");
        let active = PgEmbeddingProfileRepository
            .insert(&mut conn, &a_new_embedding_profile().build())
            .await
            .expect("insert active");
        PgEmbeddingProfileRepository
            .mark_complete(&mut conn, active.id())
            .await
            .expect("complete active");
        let epoch = PgEmbeddingProfileRepository
            .find_by_id(&mut conn, prunable.id())
            .await
            .expect("find prunable")
            .expect("the prunable profile")
            .epoch();

        // Build the prunable profile's partial index, the one whose drop will be
        // simulated as failing.
        PgEmbeddingIndexRepository
            .ensure_partial_hnsw(&mut conn, table, epoch, 768, prunable.id())
            .await
            .expect("build the prunable profile's index");

        // The first prune supersedes the prunable profile and targets its epoch.
        let first = reindex_prune(&mut conn).await.expect("first prune");
        assert!(
            first.superseded_epochs.contains(&epoch),
            "the first prune targets the freshly-superseded profile's index",
        );

        // Simulate the drop having failed by leaving the index in place. The
        // second prune transitions nothing new, yet must still target the epoch
        // because its index lingers.
        let second = reindex_prune(&mut conn).await.expect("second prune");
        assert_eq!(
            second.profiles_superseded, 0,
            "the already-superseded profile is not transitioned again",
        );
        assert!(
            second.superseded_epochs.contains(&epoch),
            "the failed drop is retried: the lingering index keeps the epoch targeted",
        );

        // Once the index is actually dropped, a later prune no longer targets it.
        drop_superseded_indexes(&mut conn, &[epoch]).await;
        let third = reindex_prune(&mut conn).await.expect("third prune");
        assert!(
            !third.superseded_epochs.contains(&epoch),
            "a dropped index removes the epoch from the prunable set",
        );

        truncate_all_tables(&mut conn).await;
    }
}
