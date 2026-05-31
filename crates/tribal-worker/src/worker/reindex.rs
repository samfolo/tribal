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

use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use sqlx::PgConnection;
use tokio::sync::Semaphore;
use tribal_config::{CredentialCatalogue, MissingApiKey};
use tribal_db::{
    AdvisoryLockRepository, DbError, EmbeddingProfileRepository, EmbeddingRepository,
    KnowledgeItemRepository, NewEmbedding, NewEmbeddingProfile, NewReindexQuarantine,
    NewReindexRun, NewReindexTask, NewTagEmbedding, PgAdvisoryLockRepository,
    PgEmbeddingProfileRepository, PgEmbeddingRepository, PgKnowledgeItemRepository,
    PgReindexQuarantineRepository, PgReindexRunRepository, PgReindexTaskRepository,
    PgTagEmbeddingRepository, ReindexQuarantineRepository, ReindexRunRepository,
    ReindexTaskRepository, TagEmbeddingRepository, advisory_locks,
};
use tribal_domain::{
    DistanceMetric, EmbeddingErrorClass, EmbeddingProfile, EmbeddingProfileId, EmbeddingPurpose,
    KnowledgeItemId, PrincipalId, ProviderKind, ReindexEntityKind, ReindexRun, ReindexRunId,
    ReindexRunState, ReindexTask, ReindexTaskId,
};
use tribal_inference::{
    BatchEmbeddingResult, EmbeddingProvider, EmbeddingRequest, InferenceError, ProviderKey,
    ProviderLimits, ProviderRegistry, ProviderRegistryError, RequestClass,
    UnsupportedEmbeddingProvider, classify_embedding_error, make_embedding_provider,
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

/// Promotes a queued run to running and enrols its backfill backlog once
/// (gated by the enumeration tally), returning the running run for the caller
/// to process. Yields `None` when no run is live, or when a promote loses the
/// compare-and-set (the run left `queued` under it, e.g. cancelled).
///
/// Idempotent: enrolment runs only while the tally is unset, so re-driving a
/// running run does no redundant work.
pub async fn drive_reindex(conn: &mut PgConnection) -> Result<Option<ReindexRun>, DbError> {
    let Some(run) = PgReindexRunRepository.find_live(conn).await? else {
        return Ok(None);
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
        return Ok(None);
    }

    if run.items_enumerated().is_none() {
        let (items, tags) = enrol_backfill(conn, run.id(), run.target_profile_id()).await?;
        PgReindexRunRepository
            .set_enumerated(conn, run.id(), items, tags)
            .await?;
    }

    Ok(Some(run))
}

// ---------------------------------------------------------------------------
// Target provider
// ---------------------------------------------------------------------------

/// Built embedding providers keyed by profile id, shared across the worker.
///
/// The `ProviderKey` registry keys clients and rate-limit semaphores by
/// endpoint, so it cannot distinguish two profiles on one endpoint that differ
/// only by model or dimension; this cache holds the model/dimension-specific
/// built provider per profile. The reindex driver populates it for the building
/// profile; the commit path reads it to re-embed against a freshly-activated
/// profile.
pub type EmbeddingProviderCache = Arc<DashMap<EmbeddingProfileId, Arc<dyn EmbeddingProvider>>>;

/// Limits applied to a reindex target endpoint registered for the first time.
///
/// A model-change reindex reuses the active endpoint's already-registered
/// client and semaphore, so this conservative default bounds only the rarer
/// case of a reindex to a brand-new endpoint.
const DEFAULT_REINDEX_PROVIDER_LIMITS: ProviderLimits = ProviderLimits {
    max_in_flight: 4,
    request_timeout: Duration::from_mins(2),
};

/// A target embedding provider could not be built.
#[derive(Debug, thiserror::Error)]
pub enum TargetProviderError {
    /// Keying or registering the endpoint failed.
    #[error("registering the target provider: {0}")]
    Registry(#[from] ProviderRegistryError),
    /// The registry holds no client or semaphore for the registered endpoint.
    #[error("the target endpoint resolved no client or semaphore")]
    EndpointUnresolved,
    /// A provider that requires an API key has none in the catalogue.
    #[error(transparent)]
    Credential(#[from] MissingApiKey),
    /// The provider kind has no embedding API.
    #[error(transparent)]
    Unsupported(#[from] UnsupportedEmbeddingProvider),
}

/// A reindex drive cycle failed before per-entity handling.
#[derive(Debug, thiserror::Error)]
pub enum ReindexError {
    /// A database operation failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// The target provider could not be built or resolved.
    #[error(transparent)]
    Provider(#[from] TargetProviderError),
}

/// Builds the embedding provider for a profile, caching it by profile id, and
/// resolves its endpoint's rate-limit semaphore.
///
/// Registers the endpoint in the registry if it is new — a no-op for an
/// endpoint the active providers already cover, so a model-change reindex
/// shares their client and rate-limit budget — resolves the credential
/// fail-closed, and constructs the provider. A second call for the same profile
/// returns the cached provider.
///
/// # Errors
///
/// Returns [`TargetProviderError`] if the endpoint cannot be keyed or
/// registered, no client or semaphore resolves, the credential is missing, or
/// the provider kind has no embedding API.
pub fn build_target_provider(
    registry: &ProviderRegistry,
    cache: &EmbeddingProviderCache,
    credentials: &CredentialCatalogue,
    profile: &EmbeddingProfile,
) -> Result<(Arc<dyn EmbeddingProvider>, Arc<Semaphore>), TargetProviderError> {
    let kind = profile.provider_kind();
    let url = profile.normalised_base_url();
    let key = ProviderKey::new(kind.to_string(), url, RequestClass::Embedding)?;
    registry.register_building(key.clone(), &DEFAULT_REINDEX_PROVIDER_LIMITS)?;

    let provider = if let Some(cached) = cache.get(&profile.id()).map(|p| p.value().clone()) {
        cached
    } else {
        let client = registry
            .resolve_client(&key)
            .ok_or(TargetProviderError::EndpointUnresolved)?;
        let api_key = credentials.resolve_api_key(kind, url)?;
        let provider = make_embedding_provider(
            kind,
            client,
            url,
            profile.model(),
            profile.dimensions(),
            api_key,
        )?;
        cache.insert(profile.id(), Arc::clone(&provider));
        provider
    };

    let semaphore = registry
        .resolve_semaphore(&key)
        .ok_or(TargetProviderError::EndpointUnresolved)?;
    Ok((provider, semaphore))
}

// ---------------------------------------------------------------------------
// Task processing
// ---------------------------------------------------------------------------

/// How many tasks the driver claims at once. One keeps the claim window to a
/// single in-flight batch, so a slow embed cannot leave sibling tasks claimed
/// but unworked past their heartbeat.
const REINDEX_TASK_CLAIM_LIMIT: u32 = 1;

/// Caps the retry backoff shift, so the delay tops out at `2^6` seconds.
const REINDEX_RETRY_BACKOFF_SHIFT_CAP: u32 = 6;

/// A parsed reindex task reference. A `range:` batch drains the next unembedded
/// items (the set-difference, not the literal range, is the source of truth); a
/// `tag:<text>` singleton embeds that one tag.
enum ReindexTaskRef<'a> {
    /// A backfill batch over the item set-difference.
    ItemBatch,
    /// A single tag.
    Tag(&'a str),
}

fn parse_task_ref(target_ref: &str) -> Option<ReindexTaskRef<'_>> {
    if target_ref.starts_with("range:") {
        Some(ReindexTaskRef::ItemBatch)
    } else {
        target_ref.strip_prefix("tag:").map(ReindexTaskRef::Tag)
    }
}

/// The time a failed task becomes claimable again: exponential backoff in the
/// task's attempt count, capped.
fn retry_at(attempt: u32) -> DateTime<Utc> {
    let secs = 1i64 << attempt.min(REINDEX_RETRY_BACKOFF_SHIFT_CAP);
    Utc::now() + chrono::TimeDelta::seconds(secs)
}

/// Embeds a batch while holding one endpoint rate-limit permit, so a reindex
/// shares the endpoint's budget with live ingest rather than monopolising it.
async fn embed_with_permit(
    provider: &dyn EmbeddingProvider,
    semaphore: &Semaphore,
    requests: Vec<EmbeddingRequest>,
) -> BatchEmbeddingResult {
    let _permit = semaphore
        .acquire()
        .await
        .expect("reindex provider semaphore is never closed");
    provider.embed_many(requests).await
}

/// The per-cycle context every task in one drive cycle shares: the run, its
/// building profile, and that profile's provider and rate-limit semaphore.
struct ReindexCtx<'a> {
    run: &'a ReindexRun,
    building: &'a EmbeddingProfile,
    provider: &'a dyn EmbeddingProvider,
    semaphore: &'a Semaphore,
}

/// Records an entity in the durable quarantine, returning whether it was newly
/// added (so the tally counts each entity once).
async fn quarantine(
    conn: &mut PgConnection,
    ctx: &ReindexCtx<'_>,
    kind: ReindexEntityKind,
    entity_ref: String,
    message: String,
) -> Result<bool, DbError> {
    PgReindexQuarantineRepository
        .record(
            conn,
            &NewReindexQuarantine {
                reindex_run_id: ctx.run.id(),
                target_profile_id: ctx.building.id(),
                kind,
                entity_ref,
                error_class: EmbeddingErrorClass::Permanent,
                error_message: Some(message),
            },
        )
        .await
}

/// Classifies one embed result for an item: a dimension mismatch or a
/// permanent-class error quarantines the item; a transient-class error is
/// surfaced so the whole batch retries.
enum ItemOutcome {
    Embedded(NewEmbedding),
    Quarantine(String),
    Transient(String),
}

fn classify_item_embedding(
    item_id: KnowledgeItemId,
    building: &EmbeddingProfile,
    result: Result<Vec<f32>, InferenceError>,
) -> ItemOutcome {
    match result {
        Ok(vector) if u32::try_from(vector.len()) == Ok(building.dimensions()) => {
            ItemOutcome::Embedded(
                NewEmbedding::builder()
                    .knowledge_item_id(item_id)
                    .embedding_profile_id(building.id())
                    .model(building.model().to_owned())
                    .embedding(vector)
                    .build(),
            )
        }
        Ok(vector) => ItemOutcome::Quarantine(format!(
            "expected {} dimensions, got {}",
            building.dimensions(),
            vector.len(),
        )),
        Err(e) => match classify_embedding_error(&e) {
            EmbeddingErrorClass::Permanent => ItemOutcome::Quarantine(e.to_string()),
            EmbeddingErrorClass::Transient
            | EmbeddingErrorClass::RateLimited
            | EmbeddingErrorClass::Overloaded => ItemOutcome::Transient(e.to_string()),
        },
    }
}

/// Embeds the next backfill batch into the building profile, writing successes,
/// quarantining permanent failures, and failing the task (for retry) if any
/// item was transient. Completes the task when the set-difference is empty.
async fn process_item_batch(
    conn: &mut PgConnection,
    ctx: &ReindexCtx<'_>,
    task_id: ReindexTaskId,
    attempt: u32,
    claim_token: uuid::Uuid,
) -> Result<(), DbError> {
    let ids = PgEmbeddingRepository
        .find_items_without_embedding(conn, ctx.building.id(), None, BACKFILL_RANGE_WIDTH)
        .await?;
    if ids.is_empty() {
        PgReindexTaskRepository
            .complete(conn, task_id, claim_token)
            .await?;
        return Ok(());
    }

    let items = PgKnowledgeItemRepository.find_by_ids(conn, &ids).await?;
    let requests = items
        .iter()
        .map(|item| EmbeddingRequest {
            input: item.content().to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        })
        .collect();
    let result = embed_with_permit(ctx.provider, ctx.semaphore, requests).await;

    let mut rows = Vec::new();
    let mut quarantined = 0u32;
    let mut transient = None;
    for (item, res) in items.iter().zip(result.items) {
        match classify_item_embedding(item.id(), ctx.building, res) {
            ItemOutcome::Embedded(row) => rows.push(row),
            ItemOutcome::Quarantine(message) => {
                if quarantine(
                    conn,
                    ctx,
                    ReindexEntityKind::Item,
                    item.id().inner().to_string(),
                    message,
                )
                .await?
                {
                    quarantined += 1;
                }
            }
            ItemOutcome::Transient(message) => {
                transient.get_or_insert(message);
            }
        }
    }

    let embedded = u32::try_from(
        PgEmbeddingRepository
            .batch_insert_skipping_existing(conn, &rows)
            .await?,
    )
    .unwrap_or(u32::MAX);
    if embedded > 0 {
        PgReindexRunRepository
            .bump_embedded(conn, ctx.run.id(), embedded, 0)
            .await?;
    }
    if quarantined > 0 {
        PgReindexRunRepository
            .bump_quarantined(conn, ctx.run.id(), quarantined, 0)
            .await?;
    }

    if let Some(message) = transient {
        PgReindexTaskRepository
            .fail(
                conn,
                task_id,
                claim_token,
                retry_at(attempt),
                EmbeddingErrorClass::Transient,
                &message,
            )
            .await?;
    } else {
        PgReindexTaskRepository
            .complete(conn, task_id, claim_token)
            .await?;
    }
    Ok(())
}

/// Embeds a single tag into the building profile, quarantining a permanent
/// failure and failing the task (for retry) on a transient one.
async fn process_tag(
    conn: &mut PgConnection,
    ctx: &ReindexCtx<'_>,
    tag: &str,
    task_id: ReindexTaskId,
    attempt: u32,
    claim_token: uuid::Uuid,
) -> Result<(), DbError> {
    let request = EmbeddingRequest {
        input: tag.to_owned(),
        purpose: EmbeddingPurpose::Tag,
    };
    let mut result = embed_with_permit(ctx.provider, ctx.semaphore, vec![request]).await;

    let outcome = match result.items.pop() {
        Some(Ok(vector)) if u32::try_from(vector.len()) == Ok(ctx.building.dimensions()) => {
            PgTagEmbeddingRepository
                .batch_upsert(
                    conn,
                    &[NewTagEmbedding::builder()
                        .tag(tag.to_owned())
                        .embedding_profile_id(ctx.building.id())
                        .model(ctx.building.model().to_owned())
                        .embedding(vector)
                        .build()],
                )
                .await?;
            PgReindexRunRepository
                .bump_embedded(conn, ctx.run.id(), 0, 1)
                .await?;
            None
        }
        Some(Ok(vector)) => Some(ItemOutcome::Quarantine(format!(
            "expected {} dimensions, got {}",
            ctx.building.dimensions(),
            vector.len(),
        ))),
        Some(Err(e)) => Some(match classify_embedding_error(&e) {
            EmbeddingErrorClass::Permanent => ItemOutcome::Quarantine(e.to_string()),
            EmbeddingErrorClass::Transient
            | EmbeddingErrorClass::RateLimited
            | EmbeddingErrorClass::Overloaded => ItemOutcome::Transient(e.to_string()),
        }),
        None => Some(ItemOutcome::Transient(
            "embedding returned no result".to_owned(),
        )),
    };

    match outcome {
        None | Some(ItemOutcome::Embedded(_)) => {
            PgReindexTaskRepository
                .complete(conn, task_id, claim_token)
                .await?;
        }
        Some(ItemOutcome::Quarantine(message)) => {
            if quarantine(conn, ctx, ReindexEntityKind::Tag, tag.to_owned(), message).await? {
                PgReindexRunRepository
                    .bump_quarantined(conn, ctx.run.id(), 0, 1)
                    .await?;
            }
            PgReindexTaskRepository
                .complete(conn, task_id, claim_token)
                .await?;
        }
        Some(ItemOutcome::Transient(message)) => {
            PgReindexTaskRepository
                .fail(
                    conn,
                    task_id,
                    claim_token,
                    retry_at(attempt),
                    EmbeddingErrorClass::Transient,
                    &message,
                )
                .await?;
        }
    }
    Ok(())
}

/// Dispatches one claimed task to item-batch or tag processing.
async fn process_one(
    conn: &mut PgConnection,
    ctx: &ReindexCtx<'_>,
    task: &ReindexTask,
) -> Result<(), DbError> {
    let Some(claim_token) = task.claim_token() else {
        return Ok(());
    };
    let attempt = task.attempt();
    match parse_task_ref(task.target_ref()) {
        Some(ReindexTaskRef::ItemBatch) => {
            process_item_batch(conn, ctx, task.id(), attempt, claim_token).await
        }
        Some(ReindexTaskRef::Tag(tag)) => {
            process_tag(conn, ctx, tag, task.id(), attempt, claim_token).await
        }
        None => {
            PgReindexTaskRepository
                .fail(
                    conn,
                    task.id(),
                    claim_token,
                    retry_at(attempt),
                    EmbeddingErrorClass::Permanent,
                    "unrecognised reindex task ref",
                )
                .await?;
            Ok(())
        }
    }
}

/// Drains the run's claimable tasks, embedding each batch or tag into the
/// building profile, until none remain claimable this cycle.
async fn process_tasks(
    conn: &mut PgConnection,
    ctx: &ReindexCtx<'_>,
    claimed_by: &str,
) -> Result<(), DbError> {
    loop {
        let tasks = PgReindexTaskRepository
            .claim(conn, REINDEX_TASK_CLAIM_LIMIT, claimed_by)
            .await?;
        if tasks.is_empty() {
            return Ok(());
        }
        for task in tasks {
            process_one(conn, ctx, &task).await?;
        }
    }
}

/// Drives one cycle of the single live reindex run: promotes and enrols
/// (`drive_reindex`), builds the target provider, and drains its claimable
/// tasks into the building profile. A no-op when no run is live.
///
/// # Errors
///
/// Returns [`ReindexError`] if the database, the target provider, or task
/// processing fails.
pub async fn drive_reindex_cycle(
    conn: &mut PgConnection,
    registry: &ProviderRegistry,
    cache: &EmbeddingProviderCache,
    credentials: &CredentialCatalogue,
    claimed_by: &str,
) -> Result<(), ReindexError> {
    let Some(run) = drive_reindex(conn).await? else {
        return Ok(());
    };
    let Some(building) = PgEmbeddingProfileRepository.find_building(conn).await? else {
        return Ok(());
    };
    let (provider, semaphore) = build_target_provider(registry, cache, credentials, &building)?;
    let ctx = ReindexCtx {
        run: &run,
        building: &building,
        provider: provider.as_ref(),
        semaphore: &semaphore,
    };
    process_tasks(conn, &ctx, claimed_by).await?;
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
        ExhaustBehaviour, MockEmbeddingProvider, Seed, a_new_embedding_profile, a_new_principal,
        an_embedding_profile, an_embedding_response, item, serial_lock, test_context,
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

    #[test]
    fn test_build_target_provider_caches_by_profile_id() {
        let registry = ProviderRegistry::new(vec![]).expect("registry");
        let cache: EmbeddingProviderCache = Arc::new(DashMap::new());
        let catalogue = CredentialCatalogue::default();
        // The factory default is a local Ollama endpoint, which needs no key.
        let profile = an_embedding_profile().build();

        let (provider, _semaphore) =
            build_target_provider(&registry, &cache, &catalogue, &profile).expect("build");
        assert!(
            cache.contains_key(&profile.id()),
            "the built provider is cached by profile id",
        );

        let (again, _semaphore) =
            build_target_provider(&registry, &cache, &catalogue, &profile).expect("build");
        assert!(
            Arc::ptr_eq(&provider, &again),
            "a second call returns the cached provider, not a rebuild",
        );
    }

    #[tokio::test]
    async fn test_process_tasks_embeds_the_backlog_into_the_building_profile() {
        let _guard = serial_lock().await;
        let ctx_db = test_context().await;
        let mut txn = ctx_db.begin_test().await.expect("begin_test");

        // A two-item corpus against the active profile; neither carries the
        // building geometry, so both fall in the building set-difference.
        let seed = Seed::new()
            .define_principal("user", "user:reindex-process")
            .define_project("proj", "git@github.com:test/reindex-process.git")
            .set_embedding_model("mock-model", 768)
            .as_principal("user")
            .for_project("proj", |store| {
                store.add_item("a", item(KnowledgeKind::Fact, "first"));
                store.add_item("b", item(KnowledgeKind::Fact, "second"));
            })
            .execute(&mut txn)
            .await;
        let principal = seed.principal_id("user");

        let ReindexCreationOutcome::Created(_) =
            create_reindex_run(&mut txn, &a_target(), principal)
                .await
                .expect("create")
        else {
            panic!("expected a created run");
        };
        let run = drive_reindex(&mut txn)
            .await
            .expect("drive")
            .expect("a running run");
        let building = PgEmbeddingProfileRepository
            .find_building(&mut txn)
            .await
            .expect("find_building")
            .expect("a building profile");

        // A mock provider returns a building-dimension vector for every item.
        let provider: Arc<dyn EmbeddingProvider> = Arc::new(
            MockEmbeddingProvider::builder()
                .on_embed(an_embedding_response(vec![0.1_f32; 768]), None)
                .on_exhaust(ExhaustBehaviour::RepeatLast)
                .build(),
        );
        let semaphore = Semaphore::new(4);
        let ctx = ReindexCtx {
            run: &run,
            building: &building,
            provider: provider.as_ref(),
            semaphore: &semaphore,
        };
        process_tasks(&mut txn, &ctx, "test-reindex-worker")
            .await
            .expect("process tasks");

        for label in ["a", "b"] {
            let id = seed.item_id(label);
            assert!(
                PgEmbeddingRepository
                    .find_by_knowledge_item_id(&mut txn, id, building.id())
                    .await
                    .expect("find embedding")
                    .is_some(),
                "item {label} is embedded into the building profile",
            );
        }
        assert_eq!(
            PgEmbeddingRepository
                .count_items_without_embedding(&mut txn, building.id())
                .await
                .expect("count"),
            0,
            "the building set-difference is drained",
        );
    }
}
