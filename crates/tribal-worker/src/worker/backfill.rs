//! Startup backfill: embeds tag registry entries that lack embeddings.
//!
//! On worker startup, any tags in the registry without a corresponding
//! embedding row for the active model are embedded and stored.  This
//! ensures that the semantic tag resolution path has coverage for tags
//! that were created before the embedding-based resolution was deployed.
//!
//! The backfill is idempotent (`ON CONFLICT DO NOTHING`), best-effort
//! (individual failures are logged and skipped), and batched to avoid
//! overwhelming the embedding provider.

use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::Semaphore;
use tracing::Instrument;
use tribal_db::{
    NewTagEmbedding, PgTagEmbeddingRepository, TagEmbeddingRepository,
};
use tribal_domain::EmbeddingPurpose;
use tribal_inference::{EmbeddingProvider, EmbeddingRequest};

use crate::common::clamp_to_u32;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of tags to embed in a single batch before yielding.
const BACKFILL_BATCH_SIZE: usize = 50;

/// Delay between batches to avoid saturating the embedding provider.
const BACKFILL_INTER_BATCH_DELAY: std::time::Duration =
    std::time::Duration::from_millis(100);

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Embeds tag registry entries that lack embeddings for the active model.
///
/// Tags are processed in batches of [`BACKFILL_BATCH_SIZE`].  Individual
/// embedding failures are logged at `warn` and skipped — the next
/// startup will retry any missing tags.
///
/// Returns the number of tags successfully backfilled.
pub(crate) async fn backfill_tag_embeddings(
    pool: &PgPool,
    embedding_provider: &Arc<dyn EmbeddingProvider>,
    semaphore: &Arc<Semaphore>,
    model: &str,
) -> u32 {
    let span = tracing::info_span!(
        "tribal.backfill.tag_embeddings",
        backfilled = tracing::field::Empty,
        skipped = tracing::field::Empty,
        total = tracing::field::Empty,
    );

    async {
        let missing = match fetch_missing_tags(pool, model).await {
            Ok(tags) => tags,
            Err(e) => {
                tracing::warn!(error = %e, "failed to query tags missing embeddings");
                return 0;
            }
        };

        if missing.is_empty() {
            return 0;
        }

        let total = missing.len();
        tracing::info!(count = total, "backfilling tag embeddings");

        let mut backfilled: u32 = 0;
        let mut skipped: u32 = 0;

        for (batch_idx, chunk) in missing.chunks(BACKFILL_BATCH_SIZE).enumerate() {
            if batch_idx > 0 {
                tokio::time::sleep(BACKFILL_INTER_BATCH_DELAY).await;
            }

            let (batch_ok, batch_skip) = process_batch(
                pool,
                embedding_provider,
                semaphore,
                model,
                chunk,
            )
            .await;

            backfilled += batch_ok;
            skipped += batch_skip;
        }

        let span = tracing::Span::current();
        span.record("backfilled", backfilled);
        span.record("skipped", skipped);
        span.record("total", clamp_to_u32(total));

        if backfilled > 0 || skipped > 0 {
            tracing::info!(
                backfilled,
                skipped,
                "tag embedding backfill complete",
            );
        }

        backfilled
    }
    .instrument(span)
    .await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Fetches tags from the registry that have no embedding for the given model.
async fn fetch_missing_tags(
    pool: &PgPool,
    model: &str,
) -> Result<Vec<String>, tribal_db::DbError> {
    let mut conn = pool.acquire().await.map_err(|e| {
        tribal_db::DbError::QueryFailed {
            context: "acquiring connection for tag backfill query".into(),
            source: e,
        }
    })?;

    PgTagEmbeddingRepository
        .find_tags_missing_embeddings(&mut conn, model)
        .await
}

/// Processes a single batch: embeds each tag, collects successes, then
/// upserts them all in one go.  Returns `(successes, failures)`.
async fn process_batch(
    pool: &PgPool,
    embedding_provider: &Arc<dyn EmbeddingProvider>,
    semaphore: &Arc<Semaphore>,
    model: &str,
    tags: &[String],
) -> (u32, u32) {
    let mut embeddings = Vec::with_capacity(tags.len());
    let mut failures: u32 = 0;

    for tag in tags {
        match embed_tag_for_backfill(tag, embedding_provider, semaphore).await {
            Ok(response) => {
                let dimensions = response.vector.len();
                embeddings.push(NewTagEmbedding {
                    tag: tag.clone(),
                    model: model.to_owned(),
                    dimensions: clamp_to_u32(dimensions),
                    embedding: response.vector,
                });
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

    let upsert_count = embeddings.len();

    match upsert_batch(pool, &embeddings).await {
        Ok(()) => (clamp_to_u32(upsert_count), failures),
        Err(e) => {
            tracing::warn!(
                error = %e,
                count = upsert_count,
                "batch upsert failed, skipping entire batch",
            );
            (0, failures + clamp_to_u32(upsert_count))
        }
    }
}

/// Embeds a single tag via the embedding provider, acquiring the
/// semaphore first.
async fn embed_tag_for_backfill(
    tag: &str,
    embedding_provider: &Arc<dyn EmbeddingProvider>,
    semaphore: &Arc<Semaphore>,
) -> Result<tribal_inference::EmbeddingResponse, tribal_inference::InferenceError> {
    let _permit = Arc::clone(semaphore).acquire_owned().await.expect(
        crate::error::SEMAPHORE_CLOSED,
    );

    let request = EmbeddingRequest {
        input: tag.to_owned(),
        purpose: EmbeddingPurpose::Tag,
    };

    embedding_provider.embed(request).await
}

/// Upserts a batch of tag embeddings into the database.
async fn upsert_batch(
    pool: &PgPool,
    embeddings: &[NewTagEmbedding],
) -> Result<(), tribal_db::DbError> {
    let mut conn = pool.acquire().await.map_err(|e| {
        tribal_db::DbError::QueryFailed {
            context: "acquiring connection for tag embedding backfill upsert".into(),
            source: e,
        }
    })?;

    PgTagEmbeddingRepository
        .batch_upsert(&mut conn, embeddings)
        .await
}
