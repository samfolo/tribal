//! Handler for `tribal_explore` — graph exploration from an anchor item.

use std::{collections::HashMap, str::FromStr};

use chrono::{DateTime, Utc};
use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::{PgConnection, PgPool};
use strum::IntoEnumIterator;
use tribal_db::{DbError, TraversalDirection};
use tribal_domain::{
    Direction, KnowledgeItemId, McpErrorCode, PrincipalId, Reference, RelationKind, Standing,
};

use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    mapping::{
        McpExplorationResult, McpExploreRequest, McpExploreResponse, McpKnowledgeItem,
        McpReference, McpRelationDirection, McpStanding,
    },
    server_handler::{ConnectionRepositories, POOL_NAME, TribalServerHandler},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_DIRECTION: Direction = Direction::Inbound;
const DEFAULT_DEPTH: u32 = 1;
const MAX_DEPTH: u32 = 3;
const DEFAULT_LIMIT: u32 = 20;
const MAX_LIMIT: u32 = 100;
const MAX_TRACE_ID_LEN: usize = 64;

// ---------------------------------------------------------------------------
// Service types
// ---------------------------------------------------------------------------

/// Resolved domain parameters for the explore operation.
struct ExploreParams {
    anchor_id: KnowledgeItemId,
    direction: Direction,
    relation_types: Option<Vec<RelationKind>>,
    depth: u32,
    limit: u32,
    include_standing: bool,
    include_references: bool,
}

/// A single item in the explore result set.
#[cfg_attr(test, derive(Debug))]
struct ExploreResultItem {
    item: tribal_domain::KnowledgeItem,
    relation_type: RelationKind,
    traversal_direction: TraversalDirection,
    depth: u32,
    relation_created_at: DateTime<Utc>,
    principal_key: String,
    standing: Option<Standing>,
    references: Option<Vec<Reference>>,
}

/// Domain-level result from [`execute_explore`].
#[cfg_attr(test, derive(Debug))]
struct ExploreResult {
    anchor: tribal_domain::KnowledgeItem,
    anchor_standing: Standing,
    anchor_principal_key: String,
    related_items: Vec<ExploreResultItem>,
    exact: bool,
}

/// Service-boundary error for [`execute_explore`].
#[derive(Debug, thiserror::Error)]
enum ExploreError {
    #[error(transparent)]
    Db(#[from] DbError),
}

impl IntoMcpError for ExploreError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

impl TribalServerHandler {
    /// Handles the `tribal_explore` tool call.
    pub(crate) async fn handle_explore(
        &self,
        params: serde_json::Value,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        Self::apply_explore(&self.pool, &self.repositories, params).await
    }

    /// Core logic for `tribal_explore`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    ///
    /// Parses the request, validates parameters, then delegates to
    /// [`execute_explore`] for all domain logic. Domain errors are
    /// returned as error `CallToolResult` values via `IntoMcpError` /
    /// `IntoCallToolResult`. Only protocol-level errors (malformed JSON)
    /// return `Err(McpError)`.
    async fn apply_explore(
        pool: &PgPool,
        repositories: &ConnectionRepositories,
        params: serde_json::Value,
    ) -> Result<CallToolResult, McpError> {
        let request: McpExploreRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;

        // -- Validate item_id prefix -----------------------------------------

        let anchor_id = match KnowledgeItemId::from_str(&request.item_id) {
            Ok(id) => id,
            Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
        };

        // -- Parse and validate direction ------------------------------------

        let direction: Direction = match request.direction.as_deref() {
            None => DEFAULT_DIRECTION,
            Some(s) => {
                let Ok(d) =
                    serde_json::from_value::<Direction>(serde_json::Value::String(s.to_owned()))
                else {
                    let valid: Vec<&str> = Direction::iter().map(Into::<&str>::into).collect();
                    return Ok(McpToolError {
                        code: McpErrorCode::InvalidArgument,
                        message: format!("invalid direction '{s}': must be one of {valid:?}"),
                        details: serde_json::json!({}),
                    }
                    .into_call_tool_result());
                };
                d
            }
        };

        // -- Parse and validate relation_types -------------------------------

        let relation_types = match request.relation_types {
            Some(types) if !types.is_empty() => {
                let mut parsed = Vec::with_capacity(types.len());
                for t in &types {
                    let Ok(rk) = RelationKind::from_str(t) else {
                        let valid: Vec<&str> = RelationKind::iter().map(|rk| rk.as_str()).collect();
                        return Ok(McpToolError {
                            code: McpErrorCode::InvalidArgument,
                            message: format!(
                                "invalid relation type '{t}': must be one of {valid:?}"
                            ),
                            details: serde_json::json!({}),
                        }
                        .into_call_tool_result());
                    };
                    parsed.push(rk);
                }
                Some(parsed)
            }
            _ => None,
        };

        // -- Apply defaults and validate depth/limit -------------------------

        let depth = request.depth.unwrap_or(DEFAULT_DEPTH);
        if !(1..=MAX_DEPTH).contains(&depth) {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: format!("depth must be between 1 and {MAX_DEPTH}, got {depth}"),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        }

        let limit = request.limit.unwrap_or(DEFAULT_LIMIT);
        if !(1..=MAX_LIMIT).contains(&limit) {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: format!("limit must be between 1 and {MAX_LIMIT}, got {limit}"),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        }

        // -- Validate session_trace_id ---------------------------------------

        if let Some(ref trace_id) = request.session_trace_id
            && (trace_id.is_empty() || trace_id.len() > MAX_TRACE_ID_LEN)
        {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: format!(
                    "session_trace_id must be a non-empty string of at most \
                     {MAX_TRACE_ID_LEN} characters"
                ),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        }

        // -- Acquire connection and execute ----------------------------------

        let mut conn = match pool.acquire().await {
            Ok(conn) => conn,
            Err(sqlx::Error::PoolTimedOut) => {
                let db_err = DbError::PoolExhausted {
                    pool_name: POOL_NAME,
                };
                return Ok(db_err.into_mcp_error().into_call_tool_result());
            }
            Err(other) => {
                let db_err = DbError::QueryFailed {
                    context: "acquiring connection from pool".into(),
                    source: other,
                };
                return Ok(db_err.into_mcp_error().into_call_tool_result());
            }
        };

        let explore_params = ExploreParams {
            anchor_id,
            direction,
            relation_types,
            depth,
            limit,
            include_standing: request.include_standing.unwrap_or(false),
            include_references: request.include_references.unwrap_or(false),
        };

        let result = match execute_explore(&mut conn, repositories, explore_params).await {
            Ok(r) => r,
            Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
        };

        // -- Build response --------------------------------------------------

        let trace_id = request
            .session_trace_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());

        let related_items: Vec<McpExplorationResult> = result
            .related_items
            .iter()
            .map(|r| McpExplorationResult {
                item: McpKnowledgeItem::from_item_with_principal_key(&r.item, &r.principal_key),
                relation_type: r.relation_type,
                relation_direction: McpRelationDirection::from(r.traversal_direction),
                depth: r.depth,
                relation_created_at: r.relation_created_at,
                standing: r.standing.as_ref().map(McpStanding::from),
                references: r
                    .references
                    .as_ref()
                    .map(|refs| refs.iter().map(McpReference::from).collect()),
            })
            .collect();

        let response = McpExploreResponse {
            anchor: McpKnowledgeItem::from_item_with_principal_key(
                &result.anchor,
                &result.anchor_principal_key,
            ),
            anchor_standing: McpStanding::from(&result.anchor_standing),
            related_items,
            trace_id,
            exact: result.exact,
        };

        Ok(response.into_call_tool_result())
    }
}

// ---------------------------------------------------------------------------
// Service function
// ---------------------------------------------------------------------------

/// Executes the explore operation against the knowledge graph.
///
/// This is the single application boundary called by the handler adapter.
/// All inputs and outputs are domain types — no MCP types cross this
/// boundary.
async fn execute_explore(
    conn: &mut PgConnection,
    repositories: &ConnectionRepositories,
    params: ExploreParams,
) -> Result<ExploreResult, ExploreError> {
    // -- Look up anchor item (required response field) -----------------------

    let anchor = repositories
        .knowledge_item
        .find_by_id(conn, params.anchor_id)
        .await?;

    // -- Compute anchor standing (always required) ---------------------------

    let anchor_standings = repositories
        .standing
        .compute(conn, &[params.anchor_id])
        .await?;
    // StandingRepository::compute guarantees one Standing per input ID
    // (zero-valued for items with no observations). The input slice is
    // always exactly one element, so this is never empty.
    let anchor_standing = anchor_standings
        .into_iter()
        .next()
        .expect("compute returns one Standing per input ID");

    // -- Traverse the graph --------------------------------------------------

    let traversal = repositories
        .relation
        .traverse(
            conn,
            params.anchor_id,
            params.direction,
            params.depth,
            params.limit,
            params.relation_types.as_deref(),
        )
        .await?;

    if traversal.nodes.is_empty() {
        let anchor_principal = repositories
            .principal
            .find_by_id(conn, anchor.principal_id())
            .await?;

        return Ok(ExploreResult {
            anchor,
            anchor_standing,
            anchor_principal_key: anchor_principal.principal_key().to_owned(),
            related_items: Vec::new(),
            exact: traversal.exact,
        });
    }

    // -- Resolve principal keys ----------------------------------------------

    let unique_principal_ids: Vec<PrincipalId> = {
        let mut ids: Vec<PrincipalId> = std::iter::once(anchor.principal_id())
            .chain(traversal.nodes.iter().map(|n| n.item.principal_id()))
            .collect();
        ids.sort();
        ids.dedup();
        ids
    };

    let principals = repositories
        .principal
        .find_by_ids(conn, &unique_principal_ids)
        .await?;
    let principal_map: HashMap<PrincipalId, String> = principals
        .into_iter()
        .map(|p| (p.id(), p.principal_key().to_owned()))
        .collect();

    let anchor_principal_key = principal_map
        .get(&anchor.principal_id())
        .cloned()
        .unwrap_or_else(|| anchor.principal_id().to_string());

    // -- Optional enrichment -------------------------------------------------

    let ki_ids: Vec<KnowledgeItemId> = traversal.nodes.iter().map(|n| n.item.id()).collect();

    let standings_map: Option<HashMap<KnowledgeItemId, Standing>> = if params.include_standing {
        let computed = repositories.standing.compute(conn, &ki_ids).await?;
        Some(ki_ids.iter().copied().zip(computed).collect())
    } else {
        None
    };

    let references_map: Option<HashMap<KnowledgeItemId, Vec<Reference>>> =
        if params.include_references {
            let all_refs = repositories
                .reference
                .find_by_knowledge_item_ids(conn, &ki_ids)
                .await?;
            let mut map: HashMap<KnowledgeItemId, Vec<Reference>> = HashMap::new();
            for r in all_refs {
                map.entry(r.knowledge_item_id()).or_default().push(r);
            }
            Some(map)
        } else {
            None
        };

    // -- Build result items --------------------------------------------------

    let related_items: Vec<ExploreResultItem> = traversal
        .nodes
        .into_iter()
        .map(|node| {
            let principal_key = principal_map
                .get(&node.item.principal_id())
                .cloned()
                .unwrap_or_else(|| node.item.principal_id().to_string());

            let standing = standings_map
                .as_ref()
                .and_then(|m| m.get(&node.item.id()).cloned());
            let references = references_map
                .as_ref()
                .map(|m| m.get(&node.item.id()).cloned().unwrap_or_default());

            ExploreResultItem {
                relation_type: node.relation_type,
                traversal_direction: node.traversal_direction,
                depth: node.depth,
                relation_created_at: node.relation_created_at,
                item: node.item,
                principal_key,
                standing,
                references,
            }
        })
        .collect();

    Ok(ExploreResult {
        anchor,
        anchor_standing,
        anchor_principal_key,
        related_items,
        exact: traversal.exact,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rmcp::model::ErrorCode;
    use tribal_db::{TraversalNode, TraversalResponse};
    use tribal_domain::{ProjectId, ReferenceKind};
    use tribal_test_utils::{
        MockKnowledgeItemRepository, MockPrincipalRepository, MockReferenceRepository,
        MockRelationRepository, MockStandingRepository, a_knowledge_item, a_principal, a_reference,
        a_standing, lazy_pool, test_context,
    };

    use super::*;
    use crate::test_utils::test_repositories;

    // -- Constants ---------------------------------------------------------

    const STRUCTURED_CONTENT: &str = "structured_content must be present";
    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";

    // -- Helpers -----------------------------------------------------------

    fn test_anchor(ki_id: KnowledgeItemId, prin_id: PrincipalId) -> tribal_domain::KnowledgeItem {
        a_knowledge_item().id(ki_id).principal_id(prin_id).build()
    }

    fn test_principal(id: PrincipalId, key: &str) -> tribal_domain::Principal {
        a_principal().id(id).principal_key(key.to_owned()).build()
    }

    fn test_traversal_node(
        item: &tribal_domain::KnowledgeItem,
        relation_type: RelationKind,
        direction: TraversalDirection,
        peer_id: KnowledgeItemId,
        depth: u32,
    ) -> TraversalNode {
        let (source_id, target_id) = match direction {
            TraversalDirection::Inbound => (item.id(), peer_id),
            TraversalDirection::Outbound => (peer_id, item.id()),
        };
        TraversalNode {
            item: item.clone(),
            relation_type,
            source_id,
            target_id,
            relation_created_at: Utc::now(),
            depth,
            traversal_direction: direction,
        }
    }

    fn default_params(anchor_id: KnowledgeItemId) -> ExploreParams {
        ExploreParams {
            anchor_id,
            direction: Direction::Inbound,
            relation_types: None,
            depth: DEFAULT_DEPTH,
            limit: DEFAULT_LIMIT,
            include_standing: false,
            include_references: false,
        }
    }

    fn repos_with_anchor_and_traversal(
        anchor: tribal_domain::KnowledgeItem,
        anchor_standing: Standing,
        traversal: TraversalResponse,
        principals: Vec<tribal_domain::Principal>,
    ) -> ConnectionRepositories {
        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_id(anchor, None)
            .build();

        let standing_mock = MockStandingRepository::builder()
            .on_compute(vec![anchor_standing], None)
            .build();

        let relation_mock = MockRelationRepository::builder()
            .on_traverse(traversal, None)
            .build();

        let prin_mock = MockPrincipalRepository::builder().on_find_by_ids(principals, None);

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.standing = Arc::new(standing_mock);
        repos.relation = Arc::new(relation_mock);
        repos.principal = Arc::new(prin_mock.build());
        repos
    }

    async fn call_execute(
        repos: &ConnectionRepositories,
        params: ExploreParams,
    ) -> Result<ExploreResult, ExploreError> {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");
        execute_explore(&mut tx, repos, params).await
    }

    // -- Service: happy path -----------------------------------------------

    #[tokio::test]
    async fn test_anchor_found_no_relations() {
        let anchor_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, prin_id);
        let standing = a_standing().build();
        let traversal = TraversalResponse {
            nodes: vec![],
            exact: true,
        };

        let repos = repos_with_anchor_and_traversal(
            anchor.clone(),
            standing.clone(),
            traversal,
            vec![test_principal(prin_id, "user:test")],
        );

        let result = call_execute(&repos, default_params(anchor_id))
            .await
            .unwrap();

        assert_eq!(result.anchor.id(), anchor_id);
        assert_eq!(
            result.anchor_standing.supporting_count(),
            standing.supporting_count(),
        );
        assert!(result.related_items.is_empty());
        assert!(result.exact);
        assert_eq!(result.anchor_principal_key, "user:test");
    }

    #[tokio::test]
    async fn test_anchor_not_found() {
        let anchor_id = KnowledgeItemId::new();

        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_id_error(
                tribal_test_utils::a_not_found("knowledge_item", anchor_id.to_string()),
                None,
            )
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);

        let result = call_execute(&repos, default_params(anchor_id)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_single_inbound_relation() {
        let anchor_id = KnowledgeItemId::new();
        let anchor_prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, anchor_prin_id);
        let standing = a_standing().build();

        let related_prin_id = PrincipalId::new();
        let related = a_knowledge_item().principal_id(related_prin_id).build();
        let node = test_traversal_node(
            &related,
            RelationKind::Supports,
            TraversalDirection::Inbound,
            anchor_id,
            1,
        );
        let traversal = TraversalResponse {
            nodes: vec![node],
            exact: true,
        };

        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_id(anchor, None)
            .build();
        let standing_mock = MockStandingRepository::builder()
            .on_compute(vec![standing], None)
            .build();
        let relation_mock = MockRelationRepository::builder()
            .on_traverse(traversal, None)
            .build();
        let prin_mock = MockPrincipalRepository::builder()
            .on_find_by_ids(
                vec![
                    test_principal(anchor_prin_id, "user:anchor"),
                    test_principal(related_prin_id, "user:related"),
                ],
                None,
            )
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.standing = Arc::new(standing_mock);
        repos.relation = Arc::new(relation_mock);
        repos.principal = Arc::new(prin_mock);

        let result = call_execute(&repos, default_params(anchor_id))
            .await
            .unwrap();

        assert_eq!(result.related_items.len(), 1);
        assert_eq!(
            result.related_items[0].relation_type,
            RelationKind::Supports
        );
        assert_eq!(
            result.related_items[0].traversal_direction,
            TraversalDirection::Inbound,
        );
        assert_eq!(result.related_items[0].depth, 1);
        assert_eq!(result.related_items[0].principal_key, "user:related");
    }

    #[tokio::test]
    async fn test_principal_key_resolution() {
        let anchor_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, prin_id);
        let standing = a_standing().build();

        let related = a_knowledge_item().principal_id(prin_id).build();
        let node = test_traversal_node(
            &related,
            RelationKind::Contradicts,
            TraversalDirection::Inbound,
            anchor_id,
            1,
        );
        let traversal = TraversalResponse {
            nodes: vec![node],
            exact: true,
        };

        let repos = repos_with_anchor_and_traversal(
            anchor,
            standing,
            traversal,
            vec![test_principal(prin_id, "user:shared")],
        );

        let result = call_execute(&repos, default_params(anchor_id))
            .await
            .unwrap();

        assert_eq!(result.anchor_principal_key, "user:shared");
        assert_eq!(result.related_items[0].principal_key, "user:shared");
    }

    #[tokio::test]
    async fn test_shared_principal_deduplicates_lookups() {
        let anchor_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, prin_id);
        let standing = a_standing().build();

        let related = a_knowledge_item().principal_id(prin_id).build();
        let node = test_traversal_node(
            &related,
            RelationKind::Supports,
            TraversalDirection::Inbound,
            anchor_id,
            1,
        );
        let traversal = TraversalResponse {
            nodes: vec![node],
            exact: true,
        };

        let principal = test_principal(prin_id, "user:shared");
        let prin_mock = MockPrincipalRepository::builder()
            .when_find_by_ids(move |ids| ids.len() == 1 && ids[0] == prin_id)
            .respond_with(vec![principal], None)
            .build();

        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_id(anchor, None)
            .build();
        let standing_mock = MockStandingRepository::builder()
            .on_compute(vec![standing], None)
            .build();
        let relation_mock = MockRelationRepository::builder()
            .on_traverse(traversal, None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.standing = Arc::new(standing_mock);
        repos.relation = Arc::new(relation_mock);
        repos.principal = Arc::new(prin_mock);

        let result = call_execute(&repos, default_params(anchor_id))
            .await
            .unwrap();

        assert_eq!(result.anchor_principal_key, "user:shared");
        assert_eq!(result.related_items[0].principal_key, "user:shared");
    }

    #[tokio::test]
    async fn test_exact_flag_propagation() {
        let anchor_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, prin_id);
        let standing = a_standing().build();
        let traversal = TraversalResponse {
            nodes: vec![],
            exact: false,
        };

        let repos = repos_with_anchor_and_traversal(
            anchor,
            standing,
            traversal,
            vec![test_principal(prin_id, "user:test")],
        );

        let result = call_execute(&repos, default_params(anchor_id))
            .await
            .unwrap();

        assert!(!result.exact);
    }

    // -- Service: enrichment -----------------------------------------------

    #[tokio::test]
    async fn test_include_standing_enriches_related_items() {
        let anchor_id = KnowledgeItemId::new();
        let anchor_prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, anchor_prin_id);

        let related_prin_id = PrincipalId::new();
        let related_ki_id = KnowledgeItemId::new();
        let related = a_knowledge_item()
            .id(related_ki_id)
            .principal_id(related_prin_id)
            .build();
        let node = test_traversal_node(
            &related,
            RelationKind::Supports,
            TraversalDirection::Inbound,
            anchor_id,
            1,
        );
        let traversal = TraversalResponse {
            nodes: vec![node],
            exact: true,
        };

        let anchor_standing = a_standing().build();
        let related_standing = a_standing()
            .supporting_count(5)
            .contradicting_count(2)
            .observation_count(7)
            .supporting_episode_count(3)
            .supporting_project_count(1)
            .build();
        let expected_supporting = related_standing.supporting_count();

        let standing_mock = MockStandingRepository::builder()
            .when_compute(move |ids| ids.len() == 1 && ids[0] == anchor_id)
            .respond_with(vec![anchor_standing], None)
            .when_compute(move |ids| ids.len() == 1 && ids[0] == related_ki_id)
            .respond_with(vec![related_standing], None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(
            MockKnowledgeItemRepository::builder()
                .on_find_by_id(anchor, None)
                .build(),
        );
        repos.relation = Arc::new(
            MockRelationRepository::builder()
                .on_traverse(traversal, None)
                .build(),
        );
        repos.standing = Arc::new(standing_mock);
        repos.principal = Arc::new(
            MockPrincipalRepository::builder()
                .on_find_by_ids(
                    vec![
                        test_principal(anchor_prin_id, "user:anchor"),
                        test_principal(related_prin_id, "user:related"),
                    ],
                    None,
                )
                .build(),
        );

        let params = ExploreParams {
            include_standing: true,
            ..default_params(anchor_id)
        };
        let result = call_execute(&repos, params).await.unwrap();

        let item_standing = result.related_items[0]
            .standing
            .as_ref()
            .expect("standing present");
        assert_eq!(item_standing.supporting_count(), expected_supporting);
    }

    #[tokio::test]
    async fn test_standing_omitted_when_flag_false() {
        let anchor_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, prin_id);
        let standing = a_standing().build();

        let related = a_knowledge_item().principal_id(prin_id).build();
        let node = test_traversal_node(
            &related,
            RelationKind::Supports,
            TraversalDirection::Inbound,
            anchor_id,
            1,
        );
        let traversal = TraversalResponse {
            nodes: vec![node],
            exact: true,
        };

        let repos = repos_with_anchor_and_traversal(
            anchor,
            standing,
            traversal,
            vec![test_principal(prin_id, "user:test")],
        );

        let result = call_execute(&repos, default_params(anchor_id))
            .await
            .unwrap();

        assert!(result.related_items[0].standing.is_none());
    }

    #[tokio::test]
    async fn test_include_references_enriches_related_items() {
        let anchor_id = KnowledgeItemId::new();
        let anchor_prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, anchor_prin_id);
        let anchor_standing = a_standing().build();

        let related_prin_id = PrincipalId::new();
        let related_ki_id = KnowledgeItemId::new();
        let proj_id = ProjectId::new();
        let related = a_knowledge_item()
            .id(related_ki_id)
            .principal_id(related_prin_id)
            .project_id(proj_id)
            .build();
        let node = test_traversal_node(
            &related,
            RelationKind::Supports,
            TraversalDirection::Inbound,
            anchor_id,
            1,
        );
        let traversal = TraversalResponse {
            nodes: vec![node],
            exact: true,
        };

        let reference = a_reference()
            .knowledge_item_id(related_ki_id)
            .project_id(proj_id)
            .kind(ReferenceKind::FilePath)
            .value("src/auth.rs".to_owned())
            .build();

        let mut repos = repos_with_anchor_and_traversal(
            anchor,
            anchor_standing,
            traversal,
            vec![
                test_principal(anchor_prin_id, "user:anchor"),
                test_principal(related_prin_id, "user:related"),
            ],
        );
        repos.reference = Arc::new(
            MockReferenceRepository::builder()
                .on_find_by_knowledge_item_ids(vec![reference], None)
                .build(),
        );

        let params = ExploreParams {
            include_references: true,
            ..default_params(anchor_id)
        };
        let result = call_execute(&repos, params).await.unwrap();

        let refs = result.related_items[0]
            .references
            .as_ref()
            .expect("references present");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].value(), "src/auth.rs");
    }

    #[tokio::test]
    async fn test_references_omitted_when_flag_false() {
        let anchor_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, prin_id);
        let standing = a_standing().build();

        let related = a_knowledge_item().principal_id(prin_id).build();
        let node = test_traversal_node(
            &related,
            RelationKind::Supports,
            TraversalDirection::Inbound,
            anchor_id,
            1,
        );
        let traversal = TraversalResponse {
            nodes: vec![node],
            exact: true,
        };

        let repos = repos_with_anchor_and_traversal(
            anchor,
            standing,
            traversal,
            vec![test_principal(prin_id, "user:test")],
        );

        let result = call_execute(&repos, default_params(anchor_id))
            .await
            .unwrap();

        assert!(result.related_items[0].references.is_none());
    }

    #[tokio::test]
    async fn test_direction_tagging_at_depth_greater_than_one() {
        let anchor_id = KnowledgeItemId::new();
        let anchor_prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, anchor_prin_id);
        let standing = a_standing().build();

        let prin_id_1 = PrincipalId::new();
        let item_1 = a_knowledge_item().principal_id(prin_id_1).build();
        let node_1 = test_traversal_node(
            &item_1,
            RelationKind::Supports,
            TraversalDirection::Inbound,
            anchor_id,
            1,
        );

        let prin_id_2 = PrincipalId::new();
        let item_2 = a_knowledge_item().principal_id(prin_id_2).build();
        let node_2 = test_traversal_node(
            &item_2,
            RelationKind::DerivedFrom,
            TraversalDirection::Inbound,
            item_1.id(),
            2,
        );

        let traversal = TraversalResponse {
            nodes: vec![node_1, node_2],
            exact: true,
        };

        let repos = repos_with_anchor_and_traversal(
            anchor,
            standing,
            traversal,
            vec![
                test_principal(anchor_prin_id, "user:anchor"),
                test_principal(prin_id_1, "user:item1"),
                test_principal(prin_id_2, "user:item2"),
            ],
        );

        let result = call_execute(&repos, default_params(anchor_id))
            .await
            .unwrap();

        assert_eq!(result.related_items.len(), 2);
        assert_eq!(
            result.related_items[0].traversal_direction,
            TraversalDirection::Inbound,
        );
        assert_eq!(result.related_items[0].depth, 1);
        assert_eq!(
            result.related_items[1].traversal_direction,
            TraversalDirection::Inbound,
        );
        assert_eq!(result.related_items[1].depth, 2);
    }

    #[tokio::test]
    async fn test_both_directions_tagged_correctly() {
        let anchor_id = KnowledgeItemId::new();
        let anchor_prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, anchor_prin_id);
        let standing = a_standing().build();

        let inbound_prin_id = PrincipalId::new();
        let inbound_item = a_knowledge_item().principal_id(inbound_prin_id).build();
        let inbound_node = test_traversal_node(
            &inbound_item,
            RelationKind::Supports,
            TraversalDirection::Inbound,
            anchor_id,
            1,
        );

        let outbound_prin_id = PrincipalId::new();
        let outbound_item = a_knowledge_item().principal_id(outbound_prin_id).build();
        let outbound_node = test_traversal_node(
            &outbound_item,
            RelationKind::DerivedFrom,
            TraversalDirection::Outbound,
            anchor_id,
            1,
        );

        let traversal = TraversalResponse {
            nodes: vec![inbound_node, outbound_node],
            exact: true,
        };

        let repos = repos_with_anchor_and_traversal(
            anchor,
            standing,
            traversal,
            vec![
                test_principal(anchor_prin_id, "user:anchor"),
                test_principal(inbound_prin_id, "user:inbound"),
                test_principal(outbound_prin_id, "user:outbound"),
            ],
        );

        let params = ExploreParams {
            direction: Direction::Both,
            ..default_params(anchor_id)
        };
        let result = call_execute(&repos, params).await.unwrap();

        assert_eq!(result.related_items.len(), 2);
        assert_eq!(
            result.related_items[0].traversal_direction,
            TraversalDirection::Inbound,
        );
        assert_eq!(
            result.related_items[1].traversal_direction,
            TraversalDirection::Outbound,
        );
    }

    // -- Service: parameter pass-through ------------------------------------

    #[tokio::test]
    async fn test_direction_filtering() {
        let anchor_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, prin_id);
        let standing = a_standing().build();

        let related = a_knowledge_item().principal_id(prin_id).build();
        let node = test_traversal_node(
            &related,
            RelationKind::DerivedFrom,
            TraversalDirection::Outbound,
            anchor_id,
            1,
        );
        let traversal = TraversalResponse {
            nodes: vec![node],
            exact: true,
        };

        let relation_mock = MockRelationRepository::builder()
            .when_traverse(move |args| {
                let (_, dir, _, _, _) = args;
                *dir == Direction::Outbound
            })
            .respond_with(traversal, None)
            .build();

        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_id(anchor, None)
            .build();
        let standing_mock = MockStandingRepository::builder()
            .on_compute(vec![standing], None)
            .build();
        let prin_mock = MockPrincipalRepository::builder()
            .on_find_by_ids(vec![test_principal(prin_id, "user:test")], None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.standing = Arc::new(standing_mock);
        repos.relation = Arc::new(relation_mock);
        repos.principal = Arc::new(prin_mock);

        let params = ExploreParams {
            direction: Direction::Outbound,
            ..default_params(anchor_id)
        };
        let result = call_execute(&repos, params).await.unwrap();

        assert_eq!(result.related_items.len(), 1);
        assert_eq!(
            result.related_items[0].traversal_direction,
            TraversalDirection::Outbound,
        );
    }

    #[tokio::test]
    async fn test_relation_type_filtering() {
        let anchor_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, prin_id);
        let standing = a_standing().build();

        let related = a_knowledge_item().principal_id(prin_id).build();
        let node = test_traversal_node(
            &related,
            RelationKind::Contradicts,
            TraversalDirection::Inbound,
            anchor_id,
            1,
        );
        let traversal = TraversalResponse {
            nodes: vec![node],
            exact: true,
        };

        let relation_mock = MockRelationRepository::builder()
            .when_traverse(move |args| {
                let (_, _, _, _, types) = args;
                types
                    .as_ref()
                    .is_some_and(|t| t == &[RelationKind::Contradicts])
            })
            .respond_with(traversal, None)
            .build();

        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_id(anchor, None)
            .build();
        let standing_mock = MockStandingRepository::builder()
            .on_compute(vec![standing], None)
            .build();
        let prin_mock = MockPrincipalRepository::builder()
            .on_find_by_ids(vec![test_principal(prin_id, "user:test")], None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.standing = Arc::new(standing_mock);
        repos.relation = Arc::new(relation_mock);
        repos.principal = Arc::new(prin_mock);

        let params = ExploreParams {
            relation_types: Some(vec![RelationKind::Contradicts]),
            ..default_params(anchor_id)
        };
        let result = call_execute(&repos, params).await.unwrap();

        assert_eq!(result.related_items.len(), 1);
        assert_eq!(
            result.related_items[0].relation_type,
            RelationKind::Contradicts,
        );
    }

    // -- Adapter: validation -----------------------------------------------

    #[tokio::test]
    async fn test_malformed_json_returns_invalid_params() {
        let pool = lazy_pool();
        let repos = test_repositories();

        let err =
            TribalServerHandler::apply_explore(&pool, &repos, serde_json::json!({"item_id": 123}))
                .await
                .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_invalid_item_id_prefix() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let wrong_id = ProjectId::new().to_string();

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({"item_id": wrong_id}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_invalid_direction_returns_error() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let ki_id = KnowledgeItemId::new().to_string();

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({"item_id": ki_id, "direction": "sideways"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_invalid_relation_type_returns_error() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let ki_id = KnowledgeItemId::new().to_string();

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({"item_id": ki_id, "relation_types": ["causes"]}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_depth_below_one_is_invalid() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let ki_id = KnowledgeItemId::new().to_string();

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({"item_id": ki_id, "depth": 0}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_depth_above_max_is_invalid() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let ki_id = KnowledgeItemId::new().to_string();

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({"item_id": ki_id, "depth": MAX_DEPTH + 1}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_limit_below_one_is_invalid() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let ki_id = KnowledgeItemId::new().to_string();

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({"item_id": ki_id, "limit": 0}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_limit_above_max_is_invalid() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let ki_id = KnowledgeItemId::new().to_string();

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({"item_id": ki_id, "limit": MAX_LIMIT + 1}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_defaults_for_direction_depth_limit() {
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("pool");
        let anchor_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, prin_id);
        let standing = a_standing().build();
        let traversal = TraversalResponse {
            nodes: vec![],
            exact: true,
        };

        let relation_mock = MockRelationRepository::builder()
            .when_traverse(move |args| {
                let (id, dir, depth, limit, _) = args;
                *id == anchor_id
                    && *dir == Direction::Inbound
                    && *depth == DEFAULT_DEPTH
                    && *limit == DEFAULT_LIMIT
            })
            .respond_with(traversal, None)
            .build();

        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_id(anchor, None)
            .build();
        let standing_mock = MockStandingRepository::builder()
            .on_compute(vec![standing], None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.standing = Arc::new(standing_mock);
        repos.relation = Arc::new(relation_mock);
        repos.principal = Arc::new(
            MockPrincipalRepository::builder()
                .on_find_by_ids(vec![test_principal(prin_id, "user:test")], None)
                .build(),
        );

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({"item_id": anchor_id.to_string()}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
    }

    #[tokio::test]
    async fn test_session_trace_id_echoed_when_valid() {
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("pool");
        let anchor_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, prin_id);
        let standing = a_standing().build();
        let traversal = TraversalResponse {
            nodes: vec![],
            exact: true,
        };

        let repos = repos_with_anchor_and_traversal(
            anchor,
            standing,
            traversal,
            vec![test_principal(prin_id, "user:test")],
        );

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({
                "item_id": anchor_id.to_string(),
                "session_trace_id": "my-trace-42"
            }),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["trace_id"], "my-trace-42");
    }

    #[tokio::test]
    async fn test_session_trace_id_generated_when_absent() {
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("pool");
        let anchor_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let anchor = test_anchor(anchor_id, prin_id);
        let standing = a_standing().build();
        let traversal = TraversalResponse {
            nodes: vec![],
            exact: true,
        };

        let repos = repos_with_anchor_and_traversal(
            anchor,
            standing,
            traversal,
            vec![test_principal(prin_id, "user:test")],
        );

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({"item_id": anchor_id.to_string()}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        let trace_id = structured["trace_id"].as_str().expect("trace_id is string");
        assert_eq!(trace_id.len(), 32);
    }

    #[tokio::test]
    async fn test_session_trace_id_empty_string_rejected() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let ki_id = KnowledgeItemId::new().to_string();

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({"item_id": ki_id, "session_trace_id": ""}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_session_trace_id_too_long_rejected() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let ki_id = KnowledgeItemId::new().to_string();
        let long_trace = "x".repeat(MAX_TRACE_ID_LEN + 1);

        let result = TribalServerHandler::apply_explore(
            &pool,
            &repos,
            serde_json::json!({"item_id": ki_id, "session_trace_id": long_trace}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }
}
