//! Operator-facing reindex services: create, cancel, and prune.
//!
//! These wrap the reindex primitives behind one connection-and-registry
//! interface so the MCP tool handlers and the `tribal reindex` CLI drive the
//! same logic. The create path builds and probes the target provider before it
//! opens any transaction, preserving the invariant that no provider call spans
//! the single-flight lock.

use sqlx::PgPool;
use tribal_config::CredentialCatalogue;
use tribal_db::{
    DbError, EmbeddingIndexRepository, EmbeddingProfileRepository, EmbeddingRepository,
    EmbeddingTable, PgEmbeddingIndexRepository, PgEmbeddingProfileRepository, PgEmbeddingRepository,
    PgReindexRunRepository, PgTagEmbeddingRepository, ReindexRunRepository, TagEmbeddingRepository,
};
use tribal_domain::{
    DistanceMetric, EmbeddingProfileId, EndpointUrlError, PrincipalId, ProviderKind, ReindexRunId,
    ReindexRunState, normalise_endpoint_url,
};
use tribal_inference::{DimensionResolutionError, InferenceError, ProviderRegistry, resolve_dimensions};

use super::reindex::{
    ReindexCreationOutcome, TargetProviderError, build_provider_for_identity,
    create_reindex_run, resolve_reindex_target,
};

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
    /// When set, estimate only — no run is created.
    pub dry_run: bool,
}

/// How a create request resolved against the latest profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReindexRunStatus {
    /// A dry run; no run was created.
    Plan,
    /// A new building profile and queued run were created.
    Created,
    /// A matching live run already existed and was resumed.
    AlreadyLive,
    /// The target already matches the active profile; nothing to do.
    Unchanged,
    /// Another invocation held the single-flight lock.
    LockContended,
}

impl ReindexRunStatus {
    /// The stable wire/CLI label for this status.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Created => "created",
            Self::AlreadyLive => "already_live",
            Self::Unchanged => "unchanged",
            Self::LockContended => "lock_contended",
        }
    }
}

/// The resolved target and pre-flight estimate of a create request.
pub struct ReindexRunOutcome {
    /// How the request resolved.
    pub status: ReindexRunStatus,
    /// The created or resumed run, when one applies.
    pub run_id: Option<ReindexRunId>,
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
    Provider(TargetProviderError),
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
    registry: &ProviderRegistry,
    credentials: &CredentialCatalogue,
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

    // Building the provider validates the credential fail-closed without a
    // network call; the probe (drift signal) is deferred to the real run.
    let (built, _semaphore) = build_provider_for_identity(
        registry,
        credentials,
        provider,
        &normalised_base_url,
        &request.model,
        dimensions,
    )
    .map_err(ReindexOpError::Provider)?;

    let (status, run_id, estimate_profile) = if request.dry_run {
        let mut conn = acquire(pool, "resolving the reindex estimate target").await?;
        let profile =
            dry_run_estimate_profile(&mut conn, provider, &normalised_base_url, &request.model, dimensions)
                .await?;
        (ReindexRunStatus::Plan, None, Some(profile))
    } else {
        let target = resolve_reindex_target(
            built.as_ref(),
            provider,
            normalised_base_url.clone(),
            request.model.clone(),
            dimensions,
            DistanceMetric::Cosine,
        )
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
                ReindexRunStatus::Created,
                Some(run.id()),
                Some(run.target_profile_id()),
            ),
            ReindexCreationOutcome::AlreadyLive(run) => (
                ReindexRunStatus::AlreadyLive,
                Some(run.id()),
                Some(run.target_profile_id()),
            ),
            ReindexCreationOutcome::Unchanged(_) => (ReindexRunStatus::Unchanged, None, None),
            ReindexCreationOutcome::LockContended => (ReindexRunStatus::LockContended, None, None),
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
        status,
        run_id,
        provider,
        model: request.model.clone(),
        dimensions,
        normalised_base_url,
        estimated_items,
        estimated_tags,
    })
}

async fn acquire(pool: &PgPool, context: &str) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, ReindexOpError> {
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
            if active.provider_kind() == provider
                && active.normalised_base_url() == normalised_base_url
                && active.model() == model
                && active.dimensions() == dimensions
                && active.distance_metric() == DistanceMetric::Cosine =>
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
/// The run transition is a compare-and-set on its current state, so a cutover
/// that completes the run between the read and the write wins the race: the
/// guard fails, nothing is cancelled, and the flip stands. The building profile
/// is only failed when the run transition succeeded.
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

    let aborted = PgReindexRunRepository
        .transition(
            conn,
            run.id(),
            run.state(),
            ReindexRunState::Aborted,
            Some(CANCEL_REASON),
        )
        .await?;
    if !aborted {
        return Ok(ReindexCancelOutcome::NoLiveRun);
    }

    PgEmbeddingProfileRepository
        .mark_failed(conn, run.target_profile_id())
        .await?;
    Ok(ReindexCancelOutcome::Cancelled(run.id()))
}

// ---------------------------------------------------------------------------
// Prune
// ---------------------------------------------------------------------------

/// The counts a prune reclaimed, plus the epochs whose partial indexes the
/// caller drops after the transaction commits.
pub struct ReindexPruneOutcome {
    /// Profiles transitioned to superseded.
    pub profiles_superseded: u64,
    /// Embedding rows deleted.
    pub embeddings_deleted: u64,
    /// Tag-embedding rows deleted.
    pub tag_embeddings_deleted: u64,
    /// The superseded profiles' epochs, whose partial indexes are dropped
    /// after the prune transaction commits.
    pub superseded_epochs: Vec<i64>,
}

/// Supersedes every prunable profile and deletes their embeddings within a
/// single transaction. Supersede precedes delete, so the delete's join sees the
/// freshly-superseded profiles; the active profile and its rows are untouched.
///
/// # Errors
///
/// Returns [`DbError`] on a database error.
pub async fn reindex_prune(
    conn: &mut sqlx::PgConnection,
) -> Result<ReindexPruneOutcome, DbError> {
    let superseded_epochs = PgEmbeddingProfileRepository.supersede_prunable(conn).await?;
    let embeddings_deleted = PgEmbeddingRepository.delete_superseded(conn).await?;
    let tag_embeddings_deleted = PgTagEmbeddingRepository.delete_superseded(conn).await?;
    Ok(ReindexPruneOutcome {
        profiles_superseded: u64::try_from(superseded_epochs.len()).unwrap_or(u64::MAX),
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
