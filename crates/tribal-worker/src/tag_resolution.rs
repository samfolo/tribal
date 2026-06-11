//! Tag resolution: matching suggested tags against the tag registry.
//!
//! Tags are resolved in three tiers — normalisation, exact match against the
//! registry, then embedding-based semantic match via pgvector.  Pre-computed
//! embedding vectors for new tags are returned alongside the resolution
//! results so the commit path can store them without a redundant embedding
//! call.

use std::collections::HashSet;

use sqlx::PgPool;
use tracing::Instrument;
use tribal_db::{PgTagEmbeddingRepository, TagEmbeddingRepository};
use tribal_domain::{EmbeddingPurpose, TagRegistryEntry, span_attrs};
use tribal_inference::{
    EmbeddingRequest, EmbeddingTarget, InferenceGateway, PermitWait, UsageAttribution,
};

use crate::{
    error::{STAGE_TRIAGE, StageError},
    stages::{load_active_embedding_profile, map_gateway_error},
};

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
/// No database writes are performed — the caller is responsible for
/// passing resolved tags through to the commit path for storage.
///
/// # Errors
///
/// Returns [`StageError::Provider`] on embedding failures and
/// [`StageError::Database`] on pgvector query failures.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_tags(
    pool: &PgPool,
    suggested_tags: &[String],
    registry: &[TagRegistryEntry],
    gateway: &InferenceGateway,
    embedding_target: &EmbeddingTarget,
    threshold: f64,
    deadline: tokio::time::Instant,
    attribution: &UsageAttribution,
) -> Result<ResolvedTags, StageError> {
    let span = tracing::info_span!(
        "tribal.tag_resolution",
        { span_attrs::TAG_RESOLUTION_RESOLVED } = tracing::field::Empty,
        { span_attrs::TAG_RESOLUTION_NEW } = tracing::field::Empty,
        { span_attrs::TAG_RESOLUTION_SEMANTIC_MATCHED } = tracing::field::Empty,
        { span_attrs::TAG_RESOLUTION_BEST_SIMILARITY } = tracing::field::Empty,
    );

    async {
        let tag_count = suggested_tags.len();
        let registry_tags: HashSet<&str> = registry.iter().map(TagRegistryEntry::tag).collect();

        let mut seen_resolved: HashSet<String> = HashSet::with_capacity(tag_count);
        let mut resolved = Vec::with_capacity(tag_count);
        let mut seen_unmatched: HashSet<String> = HashSet::with_capacity(tag_count);
        let mut unmatched = Vec::with_capacity(tag_count);

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

        // Tag similarity matches against the active profile; resolved once, and
        // only when there are unmatched tags to search against.
        let active_profile = if unmatched.is_empty() {
            None
        } else {
            let mut conn = pool.acquire().await.map_err(|e| StageError::Database {
                stage: STAGE_TRIAGE.into(),
                context: "acquiring connection for active embedding profile".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
            Some(load_active_embedding_profile(&mut conn, STAGE_TRIAGE).await?)
        };

        if let Some(profile) = &active_profile {
            for tag in &unmatched {
                let embedding_response =
                    embed_tag(tag, gateway, embedding_target, deadline, attribution).await?;

                let mut conn = pool.acquire().await.map_err(|e| StageError::Database {
                    stage: STAGE_TRIAGE.into(),
                    context: "acquiring connection for tag similarity search".into(),
                    source: tribal_db::DbError::QueryFailed {
                        context: "pool acquire".into(),
                        source: e,
                    },
                })?;

                let matches = PgTagEmbeddingRepository
                    .similarity_search(
                        &mut conn,
                        &embedding_response.vector,
                        profile.id(),
                        profile.dimensions(),
                        threshold,
                        1,
                    )
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
                    new_tags.push(NewTagWithEmbedding {
                        tag: tag.clone(),
                        embedding: embedding_response.vector,
                    });
                }
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

        Ok(ResolvedTags { resolved, new_tags })
    }
    .instrument(span)
    .await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Embeds a single tag through the gateway.
async fn embed_tag(
    tag: &str,
    gateway: &InferenceGateway,
    embedding_target: &EmbeddingTarget,
    deadline: tokio::time::Instant,
    attribution: &UsageAttribution,
) -> Result<tribal_inference::EmbeddingResponse, StageError> {
    let request = EmbeddingRequest {
        input: tag.to_owned(),
        purpose: EmbeddingPurpose::Tag,
    };
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    gateway
        .embed(
            embedding_target,
            request,
            PermitWait::Bounded { limit: remaining },
            attribution,
        )
        .await
        .map_err(|e| map_gateway_error(&format!("tag embedding call for {tag:?}"), e))
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
