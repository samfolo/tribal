//! Tag resolution: matching suggested tags against the tag registry.
//!
//! Tags are resolved in three tiers — normalisation, exact match against the
//! registry, then embedding-based semantic match via pgvector.  Pre-computed
//! embedding vectors for new tags are returned alongside the resolution
//! results so the commit path can store them without a redundant embedding
//! call.

use std::{collections::HashSet, sync::Arc, time::Instant};

use opentelemetry::KeyValue;
use sqlx::PgPool;
use tokio::sync::Semaphore;
use tracing::Instrument;
use tribal_db::{PgTagEmbeddingRepository, TagEmbeddingRepository};
use tribal_domain::{EmbeddingPurpose, TagRegistryEntry, span_attrs};
use tribal_inference::{EmbeddingProvider, EmbeddingRequest, Usage};
use tribal_telemetry::{LABEL_MODEL, LABEL_PROVIDER, LABEL_PROVIDER_KEY, LABEL_STAGE, Metrics};

use crate::error::{SEMAPHORE_CLOSED, STAGE_TRIAGE, StageError};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// The result of resolving suggested tags against the registry.
pub(crate) struct ResolvedTags {
    /// Tags that matched existing registry entries (exact or semantic).
    pub resolved: Vec<String>,
    /// Unmatched tags with their pre-computed embedding vectors for storage.
    pub new_tags: Vec<NewTagWithEmbedding>,
}

/// A new tag paired with the embedding vector already computed during
/// resolution, avoiding a redundant embedding call in the commit path.
pub(crate) struct NewTagWithEmbedding {
    /// The normalised tag string.
    pub tag: String,
    /// The embedding vector.
    pub embedding: Vec<f32>,
    /// The number of dimensions in the embedding vector.
    pub dimensions: u32,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Normalises a raw tag: trims whitespace, lowercases, and collapses
/// internal whitespace to single spaces.
///
/// Returns `None` for empty or whitespace-only input.
pub(crate) fn normalise_tag(raw: &str) -> Option<String> {
    let normalised: String = raw
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();

    if normalised.is_empty() {
        None
    } else {
        Some(normalised)
    }
}

/// Resolves suggested tags against the existing tag registry using
/// exact matching and embedding-based semantic matching.
///
/// Returns the resolution result and any embedding usages produced.
/// No database writes are performed — the caller is responsible for
/// passing resolved tags through to the commit path for storage.
///
/// # Errors
///
/// Returns [`StageError::Provider`] on embedding failures and
/// [`StageError::Database`] on pgvector query failures.
///
/// # Panics
///
/// Panics if the embedding provider semaphore is unexpectedly closed.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_tags(
    pool: &PgPool,
    suggested_tags: &[String],
    registry: &[TagRegistryEntry],
    embedding_provider: &Arc<dyn EmbeddingProvider>,
    semaphore: &Arc<Semaphore>,
    provider_key: &str,
    threshold: f64,
    deadline: tokio::time::Instant,
    metrics: &Metrics,
) -> Result<(ResolvedTags, Vec<Usage>), StageError> {
    let span = tracing::info_span!(
        "tribal.tag_resolution",
        { span_attrs::TAG_RESOLUTION_RESOLVED } = tracing::field::Empty,
        { span_attrs::TAG_RESOLUTION_NEW } = tracing::field::Empty,
        { span_attrs::TAG_RESOLUTION_SEMANTIC_MATCHED } = tracing::field::Empty,
        { span_attrs::TAG_RESOLUTION_BEST_SIMILARITY } = tracing::field::Empty,
    );

    async {
        let tag_count = suggested_tags.len();
        let model = &embedding_provider.identity().model;
        let registry_tags: HashSet<&str> = registry.iter().map(TagRegistryEntry::tag).collect();

        let mut seen_resolved: HashSet<String> = HashSet::with_capacity(tag_count);
        let mut resolved = Vec::with_capacity(tag_count);
        let mut seen_unmatched: HashSet<String> = HashSet::with_capacity(tag_count);
        let mut unmatched = Vec::with_capacity(tag_count);
        let mut usages = Vec::with_capacity(tag_count);

        for raw in suggested_tags {
            let Some(normalised) = normalise_tag(raw) else {
                tracing::debug!(raw_tag = %raw, "skipping empty/whitespace-only tag");
                continue;
            };

            if registry_tags.contains(normalised.as_str()) {
                if seen_resolved.insert(normalised.clone()) {
                    resolved.push(normalised);
                }
            } else if seen_unmatched.insert(normalised.clone()) {
                unmatched.push(normalised);
            }
        }

        let mut new_tags = Vec::with_capacity(unmatched.len());
        let mut semantic_match_count: u32 = 0;
        let mut best_similarity: Option<f64> = None;

        for tag in &unmatched {
            let embedding_response =
                embed_tag(tag, embedding_provider, semaphore, deadline, provider_key, metrics)
                    .await?;

            usages.push(Usage::Embedding {
                usage: embedding_response.usage,
                purpose: EmbeddingPurpose::Tag,
            });

            let mut conn = pool.acquire().await.map_err(|e| StageError::Database {
                stage: STAGE_TRIAGE.into(),
                context: "acquiring connection for tag similarity search".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;

            let matches = PgTagEmbeddingRepository
                .similarity_search(&mut conn, &embedding_response.vector, model, threshold, 1)
                .await
                .map_err(|e| StageError::Database {
                    stage: STAGE_TRIAGE.into(),
                    context: "tag embedding similarity search".into(),
                    source: e,
                })?;

            if let Some(best) = matches.first() {
                let sim = best.similarity();
                best_similarity = Some(best_similarity.map_or(sim, |prev: f64| prev.max(sim)));
                semantic_match_count += 1;
                let canonical = best.tag().to_owned();
                if seen_resolved.insert(canonical.clone()) {
                    resolved.push(canonical);
                }
            } else {
                let dimensions = embedding_response.vector.len();
                new_tags.push(NewTagWithEmbedding {
                    tag: tag.clone(),
                    embedding: embedding_response.vector,
                    dimensions: u32::try_from(dimensions)
                        .expect("embedding dimensions exceeds u32::MAX"),
                });
            }
        }

        let span = tracing::Span::current();
        span.record(span_attrs::TAG_RESOLUTION_RESOLVED, resolved.len());
        span.record(span_attrs::TAG_RESOLUTION_NEW, new_tags.len());
        span.record(
            span_attrs::TAG_RESOLUTION_SEMANTIC_MATCHED,
            semantic_match_count,
        );
        if let Some(sim) = best_similarity {
            span.record(span_attrs::TAG_RESOLUTION_BEST_SIMILARITY, sim);
        }

        Ok((ResolvedTags { resolved, new_tags }, usages))
    }
    .instrument(span)
    .await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Embeds a single tag via the embedding provider, acquiring the
/// semaphore first.
async fn embed_tag(
    tag: &str,
    embedding_provider: &Arc<dyn EmbeddingProvider>,
    semaphore: &Arc<Semaphore>,
    deadline: tokio::time::Instant,
    provider_key: &str,
    metrics: &Metrics,
) -> Result<tribal_inference::EmbeddingResponse, StageError> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    let semaphore_start = Instant::now();
    let _permit = tokio::time::timeout(remaining, Arc::clone(semaphore).acquire_owned())
        .await
        .map_err(|_| StageError::SemaphoreTimeout {
            provider_key: provider_key.to_owned(),
        })?
        .expect(SEMAPHORE_CLOSED);
    metrics.semaphore_acquire_wait_ms.record(
        semaphore_start.elapsed().as_secs_f64() * 1000.0,
        &[KeyValue::new(LABEL_PROVIDER_KEY, "tag_embedding")],
    );

    let request = EmbeddingRequest {
        input: tag.to_owned(),
        purpose: EmbeddingPurpose::Tag,
    };

    let provider_start = Instant::now();
    let response = embedding_provider
        .embed(request)
        .await
        .map_err(|e| StageError::Provider {
            context: format!("tag embedding call for {tag:?}"),
            source: e,
        })?;
    let identity = embedding_provider.identity();
    metrics.provider_call_ms.record(
        provider_start.elapsed().as_secs_f64() * 1000.0,
        &[
            KeyValue::new(LABEL_PROVIDER, identity.name.clone()),
            KeyValue::new(LABEL_MODEL, identity.model.clone()),
            KeyValue::new(LABEL_STAGE, "tag_embedding"),
        ],
    );
    Ok(response)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalise_tag_lowercases() {
        assert_eq!(normalise_tag("RUST"), Some("rust".to_owned()));
    }

    #[test]
    fn test_normalise_tag_trims_whitespace() {
        assert_eq!(normalise_tag("  rust  "), Some("rust".to_owned()));
    }

    #[test]
    fn test_normalise_tag_collapses_internal_whitespace() {
        assert_eq!(
            normalise_tag("machine   learning"),
            Some("machine learning".to_owned()),
        );
    }

    #[test]
    fn test_normalise_tag_empty_returns_none() {
        assert_eq!(normalise_tag(""), None);
    }

    #[test]
    fn test_normalise_tag_whitespace_only_returns_none() {
        assert_eq!(normalise_tag("   "), None);
    }

    #[test]
    fn test_normalise_tag_tab_and_newline() {
        assert_eq!(
            normalise_tag("  data\t\n science  "),
            Some("data science".to_owned()),
        );
    }
}
