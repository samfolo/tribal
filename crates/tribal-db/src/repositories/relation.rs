//! Relation repository: trait definition and Postgres implementation.
//!
//! Knowledge item relations are append-only and batch-committed.  The
//! repository provides batch insert, directional lookups for committed
//! batches, and recursive CTE neighbourhood traversal with depth limiting.

use std::collections::HashSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{
    Confidence, Direction, EpisodeId, KnowledgeItem, KnowledgeItemId, KnowledgeKind, PrincipalId,
    ProjectId, RelationBatchId, RelationId, RelationKind,
};
use typed_builder::TypedBuilder;

use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const UNKNOWN_RELATION_KIND_IN_DB: &str =
    "unrecognised relation kind in database — schema mismatch";
const UNKNOWN_KNOWLEDGE_KIND_IN_DB: &str =
    "unrecognised knowledge kind in database — schema mismatch";
const UNKNOWN_CONFIDENCE_IN_DB: &str = "unrecognised confidence in database — schema mismatch";
const MAX_DEPTH_EXCEEDS_I32: &str = "max_depth exceeds i32 range";
const NEGATIVE_DEPTH_IN_CTE: &str = "negative depth in CTE — logic error";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new knowledge item relation.
///
/// Used by [`RelationRepository::batch_insert`] which inserts multiple
/// relations sharing a `relation_batch_id`.  Server-generated values
/// (`id`, `created_at`) are produced by Postgres.
#[derive(Debug, TypedBuilder)]
pub struct NewKnowledgeItemRelation {
    /// Groups relations from one relation task attempt.
    pub relation_batch_id: RelationBatchId,
    /// The item asserting the relationship (typically newer).
    pub source_id: KnowledgeItemId,
    /// The item being referenced.
    pub target_id: KnowledgeItemId,
    /// The type of relationship.
    pub relation_type: RelationKind,
    /// The principal who created this relation.
    pub principal_id: PrincipalId,
}

// ---------------------------------------------------------------------------
// Traversal output types
// ---------------------------------------------------------------------------

/// A single node discovered during graph traversal.
///
/// Pairs a joined [`KnowledgeItem`] with the relation metadata and BFS
/// depth at which it was discovered.  Analogous to how
/// [`SemanticSearchResult`](super::SemanticSearchResult) pairs an item
/// with a similarity score.
#[derive(Debug)]
pub struct TraversalNode {
    /// The knowledge item at this node in the graph.
    pub item: KnowledgeItem,
    /// The type of relationship connecting this item to the traversal path.
    pub relation_type: RelationKind,
    /// The source item in the relation (the asserting item).
    pub source_id: KnowledgeItemId,
    /// The target item in the relation (the referenced item).
    pub target_id: KnowledgeItemId,
    /// When the relation was created.
    pub relation_created_at: DateTime<Utc>,
    /// BFS depth from the anchor (1 = direct neighbour).
    pub depth: u32,
}

/// The result of a graph traversal from an anchor item.
#[derive(Debug)]
pub struct TraversalResponse {
    /// Discovered nodes ordered by ascending depth (BFS order).
    pub nodes: Vec<TraversalNode>,
    /// `true` if all reachable nodes within `max_depth` were returned;
    /// `false` if `limit` was reached before exhausting the frontier.
    pub exact: bool,
}

// ---------------------------------------------------------------------------
// Row mapping helpers
// ---------------------------------------------------------------------------

/// Builds a [`KnowledgeItemRelation`] from raw row field values.
///
/// Panics if `relation_type` contains an unrecognised value, which
/// indicates a schema mismatch (the CHECK constraint should prevent this).
fn build_relation(
    id: uuid::Uuid,
    relation_batch_id: uuid::Uuid,
    source_id: uuid::Uuid,
    target_id: uuid::Uuid,
    relation_type: &str,
    principal_id: uuid::Uuid,
    created_at: DateTime<Utc>,
) -> tribal_domain::KnowledgeItemRelation {
    tribal_domain::KnowledgeItemRelation::builder()
        .id(RelationId::from(id))
        .relation_batch_id(RelationBatchId::from(relation_batch_id))
        .source_id(KnowledgeItemId::from(source_id))
        .target_id(KnowledgeItemId::from(target_id))
        .relation_type(
            relation_type
                .parse::<RelationKind>()
                .expect(UNKNOWN_RELATION_KIND_IN_DB),
        )
        .principal_id(PrincipalId::from(principal_id))
        .created_at(created_at)
        .build()
}

/// Builds a [`TraversalNode`] from a raw CTE result row.
///
/// The CTE joins `knowledge_items` and `knowledge_item_relations` so
/// the row contains both item fields and relation metadata.
#[allow(clippy::too_many_arguments)]
fn build_traversal_node(
    // Knowledge item fields
    item_id: uuid::Uuid,
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
    item_created_at: DateTime<Utc>,
    // Relation metadata
    relation_type: &str,
    source_id: uuid::Uuid,
    target_id: uuid::Uuid,
    relation_created_at: DateTime<Utc>,
    depth: i32,
) -> TraversalNode {
    let item = KnowledgeItem::builder()
        .id(KnowledgeItemId::from(item_id))
        .project_id(ProjectId::from(project_id))
        .principal_id(PrincipalId::from(principal_id))
        .kind(kind.parse::<KnowledgeKind>().expect(UNKNOWN_KNOWLEDGE_KIND_IN_DB))
        .content(content)
        .tags(tags)
        .confidence(confidence.parse::<Confidence>().expect(UNKNOWN_CONFIDENCE_IN_DB))
        .claim_context(claim_context)
        .source_context(source_context)
        .episode_id(episode_id.map(EpisodeId::from))
        .capture_commit(capture_commit)
        .capture_branch(capture_branch)
        .created_at(item_created_at)
        .build();

    TraversalNode {
        item,
        relation_type: relation_type
            .parse::<RelationKind>()
            .expect(UNKNOWN_RELATION_KIND_IN_DB),
        source_id: KnowledgeItemId::from(source_id),
        target_id: KnowledgeItemId::from(target_id),
        relation_created_at,
        depth: u32::try_from(depth).expect(NEGATIVE_DEPTH_IN_CTE),
    }
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for knowledge item relations.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.  Relations are append-only;
/// there are no update or delete operations.
#[async_trait]
pub trait RelationRepository {
    /// Inserts a batch of relations and returns the fully populated domain types.
    ///
    /// All relations in the batch should share the same `relation_batch_id`.
    /// Uses a single `UNNEST`-based INSERT for efficiency.
    ///
    /// Returns an empty vec without issuing a query if `batch` is empty.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn batch_insert(
        &self,
        conn: &mut PgConnection,
        batch: &[NewKnowledgeItemRelation],
    ) -> Result<Vec<tribal_domain::KnowledgeItemRelation>, DbError>;

    /// Finds all committed inbound relations for a knowledge item.
    ///
    /// Inbound means `target_id = anchor_id`.  Only relations belonging
    /// to a committed batch (joined via `jobs.committed_batch_id`) are
    /// returned.  Results are ordered by `created_at DESC`.
    ///
    /// Optionally filters by one or more relation types.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_inbound(
        &self,
        conn: &mut PgConnection,
        anchor_id: KnowledgeItemId,
        relation_types: Option<&[RelationKind]>,
    ) -> Result<Vec<tribal_domain::KnowledgeItemRelation>, DbError>;

    /// Finds all committed outbound relations for a knowledge item.
    ///
    /// Outbound means `source_id = anchor_id`.  Only relations belonging
    /// to a committed batch (joined via `jobs.committed_batch_id`) are
    /// returned.  Results are ordered by `created_at DESC`.
    ///
    /// Optionally filters by one or more relation types.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_outbound(
        &self,
        conn: &mut PgConnection,
        anchor_id: KnowledgeItemId,
        relation_types: Option<&[RelationKind]>,
    ) -> Result<Vec<tribal_domain::KnowledgeItemRelation>, DbError>;

    /// Traverses the relation graph from an anchor item using BFS.
    ///
    /// `direction` controls which edges to follow.  `max_depth` limits
    /// recursion depth.  `limit` caps the total number of nodes returned.
    /// `relation_types` optionally restricts which edge types to follow
    /// in both the base case and recursive case.
    ///
    /// For [`Direction::Both`], inbound and outbound CTEs are run
    /// separately and merged in Rust with a visited set.
    ///
    /// Returns results in BFS order (depth ascending) so the limit
    /// naturally prioritises closer relations.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn traverse(
        &self,
        conn: &mut PgConnection,
        anchor_id: KnowledgeItemId,
        direction: Direction,
        max_depth: u32,
        limit: u32,
        relation_types: Option<&[RelationKind]>,
    ) -> Result<TraversalResponse, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`RelationRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgRelationRepository;

/// Direction for the CTE helper — distinct from the domain [`Direction`]
/// which includes `Both`.
enum CteDirection {
    Inbound,
    Outbound,
}

/// Converts an optional slice of relation kinds into an optional vec of
/// their string representations for SQL binding.
fn relation_type_strings(relation_types: Option<&[RelationKind]>) -> Option<Vec<String>> {
    relation_types.map(|rts| rts.iter().map(|rt| rt.as_str().to_owned()).collect())
}

/// Finds committed relations anchored on a given column.
///
/// `anchor_col` must be either `"r.target_id"` (inbound) or
/// `"r.source_id"` (outbound).  The query joins against
/// `jobs.committed_batch_id` and optionally filters by relation type.
async fn find_committed_relations(
    conn: &mut PgConnection,
    anchor_col: &str,
    direction_label: &str,
    anchor_id: KnowledgeItemId,
    relation_types: Option<&[RelationKind]>,
) -> Result<Vec<tribal_domain::KnowledgeItemRelation>, DbError> {
    let type_strings = relation_type_strings(relation_types);

    let sql = format!(
        "SELECT r.id, r.relation_batch_id, r.source_id, r.target_id, \
                r.relation_type, r.principal_id, r.created_at \
         FROM knowledge_item_relations r \
         INNER JOIN jobs j ON j.committed_batch_id = r.relation_batch_id \
         WHERE {anchor_col} = $1 \
           AND ($2::text[] IS NULL OR r.relation_type = ANY($2)) \
         ORDER BY r.created_at DESC"
    );

    let rows = sqlx::query(&sql)
        .bind(anchor_id.inner())
        .bind(&type_strings)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("finding {direction_label} relations for {anchor_id}"),
            source: e,
        })?;

    Ok(rows.into_iter().map(map_relation_row).collect())
}

/// Maps a raw `sqlx::Row` from a relation query into a
/// [`KnowledgeItemRelation`].
fn map_relation_row(r: sqlx::postgres::PgRow) -> tribal_domain::KnowledgeItemRelation {
    build_relation(
        r.get("id"),
        r.get("relation_batch_id"),
        r.get("source_id"),
        r.get("target_id"),
        r.get::<String, _>("relation_type").as_str(),
        r.get("principal_id"),
        r.get("created_at"),
    )
}

/// Maps a raw `sqlx::Row` from a traversal CTE into a [`TraversalNode`].
fn map_traversal_row(r: sqlx::postgres::PgRow) -> TraversalNode {
    build_traversal_node(
        r.get("item_id"),
        r.get("project_id"),
        r.get("item_principal_id"),
        r.get::<String, _>("kind").as_str(),
        r.get("content"),
        r.get("tags"),
        r.get::<String, _>("confidence").as_str(),
        r.get("claim_context"),
        r.get("source_context"),
        r.get("episode_id"),
        r.get("capture_commit"),
        r.get("capture_branch"),
        r.get("item_created_at"),
        r.get::<String, _>("relation_type").as_str(),
        r.get("source_id"),
        r.get("target_id"),
        r.get("relation_created_at"),
        r.get("depth"),
    )
}

/// Runs a single-direction recursive CTE traversal.
///
/// Returns the discovered nodes and whether the result set is exact
/// (all reachable nodes returned within `max_depth`).
async fn run_directional_cte(
    conn: &mut PgConnection,
    anchor_id: KnowledgeItemId,
    direction: CteDirection,
    max_depth: u32,
    limit: u32,
    type_strings: &Option<Vec<String>>,
) -> Result<(Vec<TraversalNode>, bool), DbError> {
    // Inbound: anchor sits at target_id, discovered items are source_id.
    // Outbound: anchor sits at source_id, discovered items are target_id.
    let (base_anchor_col, base_item_join_col, rec_join_col, rec_item_join_col) = match direction {
        CteDirection::Inbound => ("r.target_id", "r.source_id", "r.target_id", "r.source_id"),
        CteDirection::Outbound => ("r.source_id", "r.target_id", "r.source_id", "r.target_id"),
    };

    let direction_label = match direction {
        CteDirection::Inbound => "inbound",
        CteDirection::Outbound => "outbound",
    };

    let sql = format!(
        "WITH RECURSIVE traversal AS ( \
             SELECT ki.id AS item_id, ki.project_id, \
                    ki.principal_id AS item_principal_id, ki.kind, \
                    ki.content, ki.tags, ki.confidence, ki.claim_context, \
                    ki.source_context, ki.episode_id, ki.capture_commit, \
                    ki.capture_branch, ki.created_at AS item_created_at, \
                    r.relation_type, r.source_id, r.target_id, \
                    r.created_at AS relation_created_at, \
                    1 AS depth \
             FROM knowledge_item_relations r \
             INNER JOIN jobs j ON j.committed_batch_id = r.relation_batch_id \
             INNER JOIN knowledge_items ki ON ki.id = {base_item_join_col} \
             WHERE {base_anchor_col} = $1 \
               AND ($3::text[] IS NULL OR r.relation_type = ANY($3)) \
             UNION ALL \
             SELECT ki.id, ki.project_id, \
                    ki.principal_id, ki.kind, \
                    ki.content, ki.tags, ki.confidence, ki.claim_context, \
                    ki.source_context, ki.episode_id, ki.capture_commit, \
                    ki.capture_branch, ki.created_at, \
                    r.relation_type, r.source_id, r.target_id, \
                    r.created_at, t.depth + 1 \
             FROM knowledge_item_relations r \
             INNER JOIN jobs j ON j.committed_batch_id = r.relation_batch_id \
             INNER JOIN traversal t ON {rec_join_col} = t.item_id \
             INNER JOIN knowledge_items ki ON ki.id = {rec_item_join_col} \
             WHERE t.depth < $2 \
               AND ki.id NOT IN (SELECT item_id FROM traversal) \
               AND ($3::text[] IS NULL OR r.relation_type = ANY($3)) \
         ) \
         SELECT * FROM traversal \
         ORDER BY depth ASC \
         LIMIT $4"
    );

    let max_depth_i32 = i32::try_from(max_depth).expect(MAX_DEPTH_EXCEEDS_I32);
    let fetch_limit = i64::from(limit) + 1;

    let rows = sqlx::query(&sql)
        .bind(anchor_id.inner())
        .bind(max_depth_i32)
        .bind(type_strings)
        .bind(fetch_limit)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("{direction_label} traversal from {anchor_id}"),
            source: e,
        })?;

    let fetched = rows.len();
    let nodes: Vec<TraversalNode> = rows
        .into_iter()
        .take(limit as usize)
        .map(map_traversal_row)
        .collect();
    let exact = fetched <= limit as usize;

    Ok((nodes, exact))
}

#[async_trait]
impl RelationRepository for PgRelationRepository {
    async fn batch_insert(
        &self,
        conn: &mut PgConnection,
        batch: &[NewKnowledgeItemRelation],
    ) -> Result<Vec<tribal_domain::KnowledgeItemRelation>, DbError> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let batch_ids: Vec<uuid::Uuid> =
            batch.iter().map(|r| *r.relation_batch_id.inner()).collect();
        let source_ids: Vec<uuid::Uuid> =
            batch.iter().map(|r| *r.source_id.inner()).collect();
        let target_ids: Vec<uuid::Uuid> =
            batch.iter().map(|r| *r.target_id.inner()).collect();
        let types: Vec<String> = batch
            .iter()
            .map(|r| r.relation_type.as_str().to_owned())
            .collect();
        let principal_ids: Vec<uuid::Uuid> =
            batch.iter().map(|r| *r.principal_id.inner()).collect();

        let rows = sqlx::query(
            "INSERT INTO knowledge_item_relations \
                 (relation_batch_id, source_id, target_id, relation_type, principal_id) \
             SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::uuid[], $4::text[], $5::uuid[]) \
             RETURNING id, relation_batch_id, source_id, target_id, \
                       relation_type, principal_id, created_at",
        )
        .bind(&batch_ids)
        .bind(&source_ids)
        .bind(&target_ids)
        .bind(&types)
        .bind(&principal_ids)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("batch inserting {} relations", batch.len()),
            source: e,
        })?;

        Ok(rows.into_iter().map(map_relation_row).collect())
    }

    async fn find_inbound(
        &self,
        conn: &mut PgConnection,
        anchor_id: KnowledgeItemId,
        relation_types: Option<&[RelationKind]>,
    ) -> Result<Vec<tribal_domain::KnowledgeItemRelation>, DbError> {
        find_committed_relations(conn, "r.target_id", "inbound", anchor_id, relation_types).await
    }

    async fn find_outbound(
        &self,
        conn: &mut PgConnection,
        anchor_id: KnowledgeItemId,
        relation_types: Option<&[RelationKind]>,
    ) -> Result<Vec<tribal_domain::KnowledgeItemRelation>, DbError> {
        find_committed_relations(conn, "r.source_id", "outbound", anchor_id, relation_types).await
    }

    async fn traverse(
        &self,
        conn: &mut PgConnection,
        anchor_id: KnowledgeItemId,
        direction: Direction,
        max_depth: u32,
        limit: u32,
        relation_types: Option<&[RelationKind]>,
    ) -> Result<TraversalResponse, DbError> {
        let type_strings = relation_type_strings(relation_types);

        match direction {
            Direction::Inbound | Direction::Outbound => {
                let cte_dir = match direction {
                    Direction::Inbound => CteDirection::Inbound,
                    Direction::Outbound => CteDirection::Outbound,
                    Direction::Both => unreachable!(),
                };
                let (nodes, exact) = run_directional_cte(
                    conn,
                    anchor_id,
                    cte_dir,
                    max_depth,
                    limit,
                    &type_strings,
                )
                .await?;
                Ok(TraversalResponse { nodes, exact })
            }
            Direction::Both => {
                let (inbound_nodes, inbound_exact) = run_directional_cte(
                    conn,
                    anchor_id,
                    CteDirection::Inbound,
                    max_depth,
                    limit,
                    &type_strings,
                )
                .await?;
                let (outbound_nodes, outbound_exact) = run_directional_cte(
                    conn,
                    anchor_id,
                    CteDirection::Outbound,
                    max_depth,
                    limit,
                    &type_strings,
                )
                .await?;

                // Merge with deduplication by (source_id, target_id, relation_type).
                let mut seen = HashSet::new();
                let mut merged: Vec<TraversalNode> = Vec::new();

                let mut all_nodes: Vec<TraversalNode> =
                    inbound_nodes.into_iter().chain(outbound_nodes).collect();
                all_nodes.sort_by_key(|n| n.depth);

                for node in all_nodes {
                    let key = (
                        *node.source_id.inner(),
                        *node.target_id.inner(),
                        node.relation_type.as_str(),
                    );
                    if seen.insert(key) {
                        merged.push(node);
                    }
                }

                let total = merged.len();
                merged.truncate(limit as usize);
                let exact =
                    inbound_exact && outbound_exact && total <= limit as usize;

                Ok(TraversalResponse {
                    nodes: merged,
                    exact,
                })
            }
        }
    }
}
