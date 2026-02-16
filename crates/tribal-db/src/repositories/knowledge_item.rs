//! Knowledge item repository: trait definition and Postgres implementation.
//!
//! Knowledge items are immutable and append-only.  The repository provides
//! insert, lookup, and semantic search operations.  Semantic search uses
//! pgvector's HNSW index with cosine distance and supports structured
//! filtering, superseded-item exclusion, and cursor-based pagination.

use std::fmt::Write;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{
    Confidence, EpisodeId, KnowledgeItem, KnowledgeItemId, KnowledgeKind, PrincipalId, ProjectId,
};
use typed_builder::TypedBuilder;

use super::common::cursor::{decode_cursor, encode_cursor};
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const UNKNOWN_KIND_IN_DB: &str = "unrecognised knowledge kind in database — schema mismatch";
const UNKNOWN_CONFIDENCE_IN_DB: &str = "unrecognised confidence in database — schema mismatch";

/// Initial candidate multiplier for the widening strategy.
const CANDIDATE_MULTIPLIER: i64 = 5;
/// Widened candidate multiplier (applied once when initial fetch under-fills).
const WIDENED_CANDIDATE_MULTIPLIER: i64 = 10;

// ---------------------------------------------------------------------------
// Row mapping helper
// ---------------------------------------------------------------------------

/// Builds a [`KnowledgeItem`] from raw row field values.
///
/// Knowledge items have 13 fields, so this helper avoids repeating the
/// builder chain across the four repository methods.  Panics if `kind`
/// or `confidence` contain unrecognised values, which indicates a schema
/// mismatch (the CHECK constraints should prevent this).
#[allow(clippy::too_many_arguments)]
fn build_knowledge_item(
    id: uuid::Uuid,
    project_id: uuid::Uuid,
    principal_id: uuid::Uuid,
    kind: &str,
    content: String,
    tags: Vec<String>,
    confidence: &str,
    claim_context: Option<serde_json::Value>,
    source_context: serde_json::Value,
    episode_id: Option<uuid::Uuid>,
    capture_commit: Option<String>,
    capture_branch: Option<String>,
    created_at: DateTime<Utc>,
) -> KnowledgeItem {
    KnowledgeItem::builder()
        .id(KnowledgeItemId::from(id))
        .project_id(ProjectId::from(project_id))
        .principal_id(PrincipalId::from(principal_id))
        .kind(kind.parse::<KnowledgeKind>().expect(UNKNOWN_KIND_IN_DB))
        .content(content)
        .tags(tags)
        .confidence(
            confidence
                .parse::<Confidence>()
                .expect(UNKNOWN_CONFIDENCE_IN_DB),
        )
        .claim_context(claim_context)
        .source_context(source_context)
        .episode_id(episode_id.map(EpisodeId::from))
        .capture_commit(capture_commit)
        .capture_branch(capture_branch)
        .created_at(created_at)
        .build()
}

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new knowledge item.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING *`.
#[derive(Debug, TypedBuilder)]
pub struct NewKnowledgeItem {
    /// The project this item belongs to.
    pub project_id: ProjectId,
    /// The identity (user or agent) that created this item.
    pub principal_id: PrincipalId,
    /// Classification of this knowledge.
    pub kind: KnowledgeKind,
    /// The knowledge content.
    pub content: String,
    /// Free-form tags for categorisation.
    #[builder(default)]
    pub tags: Vec<String>,
    /// Confidence level of this item.
    pub confidence: Confidence,
    /// Reserved for structured scope qualifiers.
    #[builder(default)]
    pub claim_context: Option<serde_json::Value>,
    /// Discriminated union per source type (opaque JSONB).
    pub source_context: serde_json::Value,
    /// Groups co-extracted items (correlation key only).
    #[builder(default)]
    pub episode_id: Option<EpisodeId>,
    /// Git commit SHA anchoring this item.
    #[builder(default)]
    pub capture_commit: Option<String>,
    /// Branch name (informational only).
    #[builder(default)]
    pub capture_branch: Option<String>,
}

// ---------------------------------------------------------------------------
// Semantic search types
// ---------------------------------------------------------------------------

/// Parameters for a semantic search query against the knowledge graph.
#[derive(Debug, TypedBuilder)]
pub struct SemanticSearchParams {
    /// The pre-computed query embedding vector.
    pub query_embedding: Vec<f32>,
    /// The embedding model name to search against.
    pub embedding_model: String,
    /// Optional project filter.
    #[builder(default)]
    pub project_id: Option<ProjectId>,
    /// Optional kind filter (any of these kinds).
    #[builder(default)]
    pub kinds: Option<Vec<KnowledgeKind>>,
    /// Optional tag filter (item must contain all listed tags).
    #[builder(default)]
    pub tags: Option<Vec<String>>,
    /// Optional time range lower bound (inclusive).
    #[builder(default)]
    pub time_range_from: Option<DateTime<Utc>>,
    /// Optional time range upper bound (inclusive).
    #[builder(default)]
    pub time_range_to: Option<DateTime<Utc>>,
    /// Whether to include items that have been superseded.
    #[builder(default)]
    pub include_superseded: bool,
    /// Maximum number of results to return.
    pub limit: u32,
    /// Cursor for pagination (hex-encoded similarity + item id).
    #[builder(default)]
    pub cursor: Option<String>,
}

/// A single result from a semantic search.
#[derive(Debug, Clone)]
pub struct SemanticSearchResult {
    /// The knowledge item.
    pub item: KnowledgeItem,
    /// Cosine similarity (1 − cosine distance), as computed by Postgres.
    pub similarity: f64,
}

/// The full response from a semantic search.
#[derive(Debug)]
pub struct SemanticSearchResponse {
    /// The matching results, ordered by descending similarity.
    pub results: Vec<SemanticSearchResult>,
    /// Cursor for the next page, if more results are available.
    pub next_cursor: Option<String>,
    /// Whether the result set is exact (`true`) or potentially incomplete
    /// due to the widening heuristic being exhausted (`false`).
    pub exact: bool,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for knowledge items.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait KnowledgeItemRepository {
    /// Inserts a new knowledge item and returns the fully populated domain type.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewKnowledgeItem,
    ) -> Result<KnowledgeItem, DbError>;

    /// Finds a knowledge item by its ID.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no item with the given ID exists.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: KnowledgeItemId,
    ) -> Result<KnowledgeItem, DbError>;

    /// Finds multiple knowledge items by their IDs.
    ///
    /// Returns the items that were found, in no guaranteed order.  Missing
    /// IDs are silently omitted from the result (not an error).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_ids(
        &self,
        conn: &mut PgConnection,
        ids: &[KnowledgeItemId],
    ) -> Result<Vec<KnowledgeItem>, DbError>;

    /// Performs a semantic search against the knowledge graph.
    ///
    /// Uses cosine similarity against the HNSW-indexed embedding table,
    /// with optional structured filters, superseded-item exclusion, and
    /// cursor-based pagination.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::InvalidCursor`] if the cursor is malformed.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn semantic_search(
        &self,
        conn: &mut PgConnection,
        params: &SemanticSearchParams,
    ) -> Result<SemanticSearchResponse, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`KnowledgeItemRepository`].
///
/// A zero-sized type with no internal state.  The caller provides
/// the database connection explicitly on each method call.
pub struct PgKnowledgeItemRepository;

#[async_trait]
impl KnowledgeItemRepository for PgKnowledgeItemRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewKnowledgeItem,
    ) -> Result<KnowledgeItem, DbError> {
        let kind_str = new.kind.as_str();
        let confidence_str = new.confidence.as_str();
        let episode_id = new.episode_id.map(|e| *e.inner());

        let r = sqlx::query!(
            r#"
            INSERT INTO knowledge_items
                (project_id, principal_id, kind, content, tags, confidence,
                 claim_context, source_context, episode_id, capture_commit,
                 capture_branch)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING *
            "#,
            new.project_id.inner(),
            new.principal_id.inner(),
            kind_str,
            new.content,
            &new.tags,
            confidence_str,
            new.claim_context,
            new.source_context,
            episode_id,
            new.capture_commit,
            new.capture_branch,
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "inserting knowledge item".to_owned(),
            source: e,
        })?;

        Ok(build_knowledge_item(
            r.id,
            r.project_id,
            r.principal_id,
            &r.kind,
            r.content,
            r.tags,
            &r.confidence,
            r.claim_context,
            r.source_context,
            r.episode_id,
            r.capture_commit,
            r.capture_branch,
            r.created_at,
        ))
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: KnowledgeItemId,
    ) -> Result<KnowledgeItem, DbError> {
        let r = sqlx::query!(r#"SELECT * FROM knowledge_items WHERE id = $1"#, id.inner(),)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding knowledge item by id {id}"),
                source: e,
            })?
            .ok_or_else(|| DbError::NotFound {
                entity: "knowledge_item",
                id: id.to_string(),
            })?;

        Ok(build_knowledge_item(
            r.id,
            r.project_id,
            r.principal_id,
            &r.kind,
            r.content,
            r.tags,
            &r.confidence,
            r.claim_context,
            r.source_context,
            r.episode_id,
            r.capture_commit,
            r.capture_branch,
            r.created_at,
        ))
    }

    async fn find_by_ids(
        &self,
        conn: &mut PgConnection,
        ids: &[KnowledgeItemId],
    ) -> Result<Vec<KnowledgeItem>, DbError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let raw_ids: Vec<uuid::Uuid> = ids.iter().map(|id| *id.inner()).collect();

        let rows = sqlx::query!(
            r#"SELECT * FROM knowledge_items WHERE id = ANY($1)"#,
            &raw_ids,
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("finding knowledge items by {} ids", ids.len()),
            source: e,
        })?;

        Ok(rows
            .into_iter()
            .map(|r| {
                build_knowledge_item(
                    r.id,
                    r.project_id,
                    r.principal_id,
                    &r.kind,
                    r.content,
                    r.tags,
                    &r.confidence,
                    r.claim_context,
                    r.source_context,
                    r.episode_id,
                    r.capture_commit,
                    r.capture_branch,
                    r.created_at,
                )
            })
            .collect())
    }

    async fn semantic_search(
        &self,
        conn: &mut PgConnection,
        params: &SemanticSearchParams,
    ) -> Result<SemanticSearchResponse, DbError> {
        let cursor_values = params.cursor.as_deref().map(decode_cursor).transpose()?;

        let query_vector = pgvector::Vector::from(params.query_embedding.clone());
        let limit = params.limit as usize;

        // First attempt: K = limit × 5.
        let k = i64::from(params.limit) * CANDIDATE_MULTIPLIER;
        let candidates = fetch_candidates(conn, params, &query_vector, cursor_values, k).await?;
        let filtered = apply_structured_filters(candidates, params);

        if filtered.len() >= limit {
            let results: Vec<SemanticSearchResult> = filtered.into_iter().take(limit).collect();
            let next_cursor = results
                .last()
                .map(|r| encode_cursor(r.similarity, *r.item.id().inner()));
            return Ok(SemanticSearchResponse {
                results,
                next_cursor,
                exact: true,
            });
        }

        // Widened attempt: K = limit × 10.
        let k_wide = i64::from(params.limit) * WIDENED_CANDIDATE_MULTIPLIER;
        let candidates_wide =
            fetch_candidates(conn, params, &query_vector, cursor_values, k_wide).await?;
        let filtered_wide = apply_structured_filters(candidates_wide, params);

        let enough = filtered_wide.len() >= limit;
        let results: Vec<SemanticSearchResult> = filtered_wide.into_iter().take(limit).collect();
        let next_cursor = if results.len() >= limit {
            results
                .last()
                .map(|r| encode_cursor(r.similarity, *r.item.id().inner()))
        } else {
            None
        };

        Ok(SemanticSearchResponse {
            results,
            next_cursor,
            exact: enough,
        })
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Fetches candidate rows from the database using cosine distance.
///
/// Applies the embedding model filter, optional superseded-item exclusion,
/// and cursor pagination in SQL.  Structured filters (project, kinds, tags,
/// time range) are applied afterwards in Rust to avoid interfering with the
/// HNSW vector index scan.
async fn fetch_candidates(
    conn: &mut PgConnection,
    params: &SemanticSearchParams,
    query_vector: &pgvector::Vector,
    cursor_values: Option<(f64, uuid::Uuid)>,
    k: i64,
) -> Result<Vec<SemanticSearchResult>, DbError> {
    let mut sql = String::from(
        r"
        SELECT
            ki.id, ki.project_id, ki.principal_id, ki.kind, ki.content,
            ki.tags, ki.confidence, ki.claim_context, ki.source_context,
            ki.episode_id, ki.capture_commit, ki.capture_branch, ki.created_at,
            1.0 - (e.embedding <=> $1::vector) AS similarity
        FROM knowledge_items ki
        INNER JOIN embeddings e ON e.knowledge_item_id = ki.id
        WHERE e.model = $2
        ",
    );

    let mut param_idx: u32 = 3;

    // Superseded-item exclusion (only committed supersedes relations count).
    if !params.include_superseded {
        sql.push_str(
            r"
            AND NOT EXISTS (
                SELECT 1 FROM knowledge_item_relations kir
                INNER JOIN jobs j ON j.committed_batch_id = kir.relation_batch_id
                WHERE kir.target_id = ki.id
                AND kir.relation_type = 'supersedes'
            )
            ",
        );
    }

    // Cursor-based pagination.
    if cursor_values.is_some() {
        let id_idx = param_idx + 1;
        write!(
            sql,
            r"
            AND (
                1.0 - (e.embedding <=> $1::vector) < ${param_idx}
                OR (
                    1.0 - (e.embedding <=> $1::vector) = ${param_idx}
                    AND ki.id > ${id_idx}
                )
            )
            ",
        )
        .expect("writing to String is infallible");
        param_idx += 2;
    }

    write!(
        sql,
        r"
        ORDER BY similarity DESC, ki.id ASC
        LIMIT ${param_idx}
        "
    )
    .expect("writing to String is infallible");

    // Bind parameters.
    let mut query = sqlx::query(&sql)
        .bind(query_vector)
        .bind(&params.embedding_model);

    if let Some((cursor_sim, cursor_id)) = cursor_values {
        query = query.bind(cursor_sim).bind(cursor_id);
    }

    query = query.bind(k);

    let rows = query
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "semantic search candidate fetch".to_owned(),
            source: e,
        })?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let similarity: f64 = row.get("similarity");
            let kind: String = row.get("kind");
            let confidence: String = row.get("confidence");
            let item = build_knowledge_item(
                row.get("id"),
                row.get("project_id"),
                row.get("principal_id"),
                &kind,
                row.get("content"),
                row.get("tags"),
                &confidence,
                row.get("claim_context"),
                row.get("source_context"),
                row.get("episode_id"),
                row.get("capture_commit"),
                row.get("capture_branch"),
                row.get("created_at"),
            );

            SemanticSearchResult { item, similarity }
        })
        .collect())
}

/// Applies structured filters in Rust after the HNSW candidate fetch.
///
/// This avoids interfering with the vector index scan, which works best
/// with minimal WHERE predicates.  Superseded-item exclusion is handled
/// in SQL (not here) because it requires a NOT EXISTS subquery.
fn apply_structured_filters(
    candidates: Vec<SemanticSearchResult>,
    params: &SemanticSearchParams,
) -> Vec<SemanticSearchResult> {
    candidates
        .into_iter()
        .filter(|r| {
            if let Some(ref pid) = params.project_id
                && r.item.project_id() != *pid
            {
                return false;
            }

            if let Some(ref kinds) = params.kinds
                && !kinds.contains(&r.item.kind())
            {
                return false;
            }

            if let Some(ref tags) = params.tags {
                for tag in tags {
                    if !r.item.tags().contains(tag) {
                        return false;
                    }
                }
            }

            if let Some(from) = params.time_range_from
                && r.item.created_at() < from
            {
                return false;
            }

            if let Some(to) = params.time_range_to
                && r.item.created_at() > to
            {
                return false;
            }

            true
        })
        .collect()
}
