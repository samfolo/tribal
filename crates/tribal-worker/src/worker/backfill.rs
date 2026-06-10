//! Startup backfill processor for embedding-dependent data.
//!
//! [`BackfillProcessor`] runs idempotent catch-up operations during
//! worker startup — conceptually similar to a migration runner, but for
//! data that depends on external services (embedding providers) rather
//! than schema changes.
//!
//! Each backfill method is:
//!
//! - **Idempotent** — `ON CONFLICT DO NOTHING` ensures repeated runs are
//!   safe.
//! - **Best-effort** — individual failures are logged and skipped; the
//!   next startup retries any gaps.
//! - **Cancellation-aware** — checks the cancellation token between
//!   batches and yields promptly on shutdown.
//! - **Batched** — processes items in chunks with an inter-batch delay
//!   to avoid overwhelming the embedding provider.

use std::sync::Arc;

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tribal_common::clamp_to_u32;
use tribal_db::{
    EmbeddingProfileRepository, NewTagEmbedding, PgEmbeddingProfileRepository,
    PgTagEmbeddingRepository, TagEmbeddingRepository,
};
use tribal_domain::{EmbeddingProfile, EmbeddingProfileId, EmbeddingPurpose};
use tribal_inference::{
    EmbeddingRequest, EmbeddingTarget, InferenceFacade, PermitWait, UsageAttribution,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of items to embed in a single batch before yielding.
const BATCH_SIZE: usize = 50;

/// Delay between batches to avoid saturating the embedding provider.
const INTER_BATCH_DELAY: std::time::Duration = std::time::Duration::from_millis(100);

/// Timeout for a single embedding call (permit acquisition + provider
/// round-trip).  Generous to accommodate slow providers while preventing
/// indefinite blocking during startup.
const EMBED_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Outcome of a backfill operation, reported to the caller for logging
/// and observability.
pub(crate) struct BackfillOutcome {
    /// Number of items successfully processed.
    pub processed: u32,
    /// Number of items skipped due to errors.
    pub skipped: u32,
    /// Total number of items that needed backfilling.
    pub total: u32,
    /// Whether the operation was interrupted by cancellation.
    pub cancelled: bool,
}

impl BackfillOutcome {
    /// Returns `true` if there was nothing to backfill.
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }
}

/// Runs idempotent backfill operations during worker startup.
///
/// Constructed from the worker's shared dependencies and reused across
/// multiple backfill methods.
pub(crate) struct BackfillProcessor {
    pool: PgPool,
    facade: Arc<InferenceFacade>,
    cancellation_token: CancellationToken,
}

impl BackfillProcessor {
    /// Creates a new backfill processor.
    pub(crate) fn new(
        pool: PgPool,
        facade: Arc<InferenceFacade>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            pool,
            facade,
            cancellation_token,
        }
    }

    /// Embeds tag registry entries that lack embeddings for the active
    /// model.
    ///
    /// Tags are processed in batches of [`BATCH_SIZE`].  Individual
    /// embedding failures are logged at `warn` and skipped — the next
    /// startup retries any gaps.
    pub(crate) async fn tag_embeddings(&self) -> BackfillOutcome {
        let span = tracing::info_span!(
            "tribal.backfill.tag_embeddings",
            processed = tracing::field::Empty,
            skipped = tracing::field::Empty,
            total = tracing::field::Empty,
        );

        async {
            let profile = match self.fetch_active_profile().await {
                Ok(profile) => profile,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to resolve active embedding profile for backfill");
                    return BackfillOutcome {
                        processed: 0,
                        skipped: 0,
                        total: 0,
                        cancelled: false,
                    };
                }
            };

            let missing = match self.fetch_missing_tags(profile.id()).await {
                Ok(tags) => tags,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to query tags missing embeddings");
                    return BackfillOutcome {
                        processed: 0,
                        skipped: 0,
                        total: 0,
                        cancelled: false,
                    };
                }
            };

            if missing.is_empty() {
                return BackfillOutcome {
                    processed: 0,
                    skipped: 0,
                    total: 0,
                    cancelled: false,
                };
            }

            let target = EmbeddingTarget::from(&profile);

            let total = clamp_to_u32(missing.len());
            tracing::info!(count = total, "backfilling tag embeddings");

            let mut processed: u32 = 0;
            let mut skipped: u32 = 0;

            for (batch_idx, chunk) in missing.chunks(BATCH_SIZE).enumerate() {
                if self.cancellation_token.is_cancelled() {
                    tracing::info!("backfill interrupted by cancellation");
                    return BackfillOutcome {
                        processed,
                        skipped,
                        total,
                        cancelled: true,
                    };
                }

                if batch_idx > 0 {
                    tokio::time::sleep(INTER_BATCH_DELAY).await;
                }

                let (batch_ok, batch_skip) = self.embed_and_store_batch(chunk, &target).await;

                processed += batch_ok;
                skipped += batch_skip;
            }

            let span = tracing::Span::current();
            span.record("processed", processed);
            span.record("skipped", skipped);
            span.record("total", total);

            BackfillOutcome {
                processed,
                skipped,
                total,
                cancelled: false,
            }
        }
        .instrument(span)
        .await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

impl BackfillProcessor {
    /// Resolves the active embedding profile, the target the backfill embeds
    /// into.
    async fn fetch_active_profile(&self) -> Result<EmbeddingProfile, tribal_db::DbError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| tribal_db::DbError::QueryFailed {
                context: "acquiring connection for active profile query".into(),
                source: e,
            })?;

        PgEmbeddingProfileRepository
            .find_active(&mut conn)
            .await?
            .ok_or(tribal_db::DbError::NotFound {
                entity: "embedding_profile",
                id: "active".to_owned(),
            })
    }

    /// Fetches tags from the registry that have no embedding for the given
    /// profile.
    async fn fetch_missing_tags(
        &self,
        profile_id: EmbeddingProfileId,
    ) -> Result<Vec<String>, tribal_db::DbError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| tribal_db::DbError::QueryFailed {
                context: "acquiring connection for tag backfill query".into(),
                source: e,
            })?;

        PgTagEmbeddingRepository
            .find_tags_missing_embeddings(&mut conn, profile_id)
            .await
    }

    /// Embeds each tag in the batch, collects successes, then upserts
    /// them all in one go.  Returns `(successes, failures)`.
    async fn embed_and_store_batch(&self, tags: &[String], target: &EmbeddingTarget) -> (u32, u32) {
        let profile_id = target
            .profile_id
            .expect("a backfill target derives from the active profile");
        let mut embeddings = Vec::with_capacity(tags.len());
        let mut failures: u32 = 0;

        for tag in tags {
            if self.cancellation_token.is_cancelled() {
                break;
            }

            match self.embed_tag(tag, target).await {
                Ok(response) => {
                    embeddings.push(
                        NewTagEmbedding::builder()
                            .tag(tag.clone())
                            .embedding_profile_id(profile_id)
                            .model(target.model.clone())
                            .embedding(response.vector)
                            .build(),
                    );
                }
                Err(e) => {
                    tracing::warn!(tag = %tag, error = %e, "skipping tag embedding");
                    failures += 1;
                }
            }
        }

        if embeddings.is_empty() {
            return (0, failures);
        }

        let upsert_count = clamp_to_u32(embeddings.len());

        match self.store_batch(&embeddings).await {
            Ok(()) => (upsert_count, failures),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    count = upsert_count,
                    "batch upsert failed, skipping entire batch",
                );
                (0, failures + upsert_count)
            }
        }
    }

    /// Embeds a single tag through the façade.
    ///
    /// The entire operation (permit acquisition + provider call) is
    /// bounded by [`EMBED_TIMEOUT`] to prevent a hung provider from
    /// blocking startup indefinitely.
    async fn embed_tag(
        &self,
        tag: &str,
        target: &EmbeddingTarget,
    ) -> Result<tribal_inference::EmbeddingResponse, tribal_inference::InferenceError> {
        let request = EmbeddingRequest {
            input: tag.to_owned(),
            purpose: EmbeddingPurpose::Tag,
        };
        tokio::time::timeout(
            EMBED_TIMEOUT,
            self.facade.embed(
                target,
                request,
                PermitWait::Unbounded,
                &UsageAttribution::default(),
            ),
        )
        .await
        .unwrap_or_else(|_| {
            Err(tribal_inference::InferenceError::provider_unavailable(
                target.provider.to_string(),
                format!("backfill embed timed out after {EMBED_TIMEOUT:?}"),
            ))
        })
    }

    /// Upserts a batch of tag embeddings into the database.
    async fn store_batch(&self, embeddings: &[NewTagEmbedding]) -> Result<(), tribal_db::DbError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| tribal_db::DbError::QueryFailed {
                context: "acquiring connection for tag embedding backfill upsert".into(),
                source: e,
            })?;

        PgTagEmbeddingRepository
            .batch_upsert(&mut conn, embeddings)
            .await
    }
}
