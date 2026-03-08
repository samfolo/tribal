//! Handler for `tribal_discover` — semantic search across the knowledge base.

use std::{collections::HashMap, str::FromStr};

use chrono::{DateTime, Utc};
use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::PgConnection;
use tribal_db::{DbError, SemanticSearchParams};
use tribal_domain::{
    EmbeddingPurpose, KnowledgeItemId, KnowledgeKind, McpErrorCode, PrincipalId, ProjectId,
    Reference, Standing,
};
use tribal_inference::{EmbeddingProvider, EmbeddingRequest, InferenceError};

use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    mapping::{
        McpDiscoverRequest, McpDiscoverResponse, McpDiscoveryResult, McpKnowledgeItem,
        McpReference, McpStanding,
    },
    server_handler::{ConnectionRepositories, POOL_NAME, TribalServerHandler},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_LIMIT: u32 = 10;
const MAX_LIMIT: u32 = 50;

// ---------------------------------------------------------------------------
// Service types
// ---------------------------------------------------------------------------

/// Resolved domain parameters for the discover operation.
struct DiscoverParams {
    query: String,
    project_id: Option<ProjectId>,
    project_name: Option<String>,
    kinds: Option<Vec<KnowledgeKind>>,
    tags: Option<Vec<String>>,
    time_range_from: Option<DateTime<Utc>>,
    time_range_to: Option<DateTime<Utc>>,
    include_superseded: bool,
    include_standing: bool,
    include_references: bool,
    limit: u32,
    cursor: Option<String>,
}

/// A single item in the discover result set.
struct DiscoverResultItem {
    item: tribal_domain::KnowledgeItem,
    similarity: f64,
    principal_key: String,
    standing: Option<Standing>,
    references: Option<Vec<Reference>>,
}

/// Domain-level result from [`execute_discover`].
struct DiscoverResult {
    items: Vec<DiscoverResultItem>,
    next_cursor: Option<String>,
    exact: bool,
    applied_project_id: Option<ProjectId>,
    project_name: Option<String>,
    embedding_model: String,
}

/// Service-boundary error for [`execute_discover`].
#[derive(Debug, thiserror::Error)]
enum DiscoverError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Inference(#[from] InferenceError),
}

impl IntoMcpError for DiscoverError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
            Self::Inference(e) => e.into_mcp_error(),
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

impl TribalServerHandler {
    /// Handles the `tribal_discover` tool call.
    pub(crate) async fn handle_discover(
        &self,
        params: serde_json::Value,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        Self::apply_discover(
            &self.pool,
            &self.repositories,
            self.embedding_provider.as_ref(),
            &self.session,
            params,
        )
        .await
    }

    /// Core logic for `tribal_discover`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    ///
    /// Parses the request, validates and resolves a project ID (if supplied),
    /// then delegates to [`execute_discover`] for all domain logic. Domain
    /// errors are returned as error `CallToolResult` values via `IntoMcpError`
    /// / `IntoCallToolResult`. Only protocol-level errors (malformed JSON)
    /// return `Err(McpError)`.
    async fn apply_discover(
        pool: &sqlx::PgPool,
        repositories: &ConnectionRepositories,
        embedding_provider: &(dyn EmbeddingProvider + Send + Sync),
        session: &tokio::sync::RwLock<crate::session::SessionContext>,
        params: serde_json::Value,
    ) -> Result<CallToolResult, McpError> {
        let request: McpDiscoverRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;

        let limit = request.limit.unwrap_or(DEFAULT_LIMIT);
        if !(1..=MAX_LIMIT).contains(&limit) {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: format!("limit must be between 1 and {MAX_LIMIT}, got {limit}"),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        }

        let session_project = {
            let guard = session.read().await;
            guard.project.as_ref().map(|p| (p.id, p.name.clone()))
        };

        let (project_id, project_name) =
            match resolve_project_scope(&request.project_id, session_project) {
                Ok(pair) => pair,
                Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
            };

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

        let discover_params = DiscoverParams {
            query: request.query.clone(),
            project_id,
            project_name,
            kinds: request.kinds,
            tags: request.tags,
            time_range_from: request.time_range.as_ref().and_then(|r| r.from),
            time_range_to: request.time_range.as_ref().and_then(|r| r.to),
            include_superseded: request.include_superseded.unwrap_or(false),
            include_standing: request.include_standing.unwrap_or(false),
            include_references: request.include_references.unwrap_or(false),
            limit,
            cursor: request.cursor,
        };

        let result =
            match execute_discover(&mut conn, repositories, embedding_provider, discover_params)
                .await
            {
                Ok(r) => r,
                Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
            };

        let trace_id = uuid::Uuid::new_v4().simple().to_string();

        let items: Vec<McpDiscoveryResult> = result
            .items
            .iter()
            .map(|r| McpDiscoveryResult {
                item: McpKnowledgeItem::from_item_with_principal_key(&r.item, &r.principal_key),
                similarity: r.similarity,
                standing: r.standing.as_ref().map(McpStanding::from),
                references: r
                    .references
                    .as_ref()
                    .map(|refs| refs.iter().map(McpReference::from).collect()),
            })
            .collect();

        let response = McpDiscoverResponse {
            items,
            next_cursor: result.next_cursor,
            applied_project_id: result.applied_project_id.map(|id| id.to_string()),
            embedding_model: result.embedding_model,
            trace_id,
            exact: result.exact,
            query: request.query,
            project_name: result.project_name,
        };

        Ok(response.into_call_tool_result())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolves the four-way `project_id` semantics from the MCP request and
/// session state.
///
/// The `project_id` field on `McpDiscoverRequest` uses `Option<Option<String>>`
/// with three-way semantics:
///
/// | Request `project_id`    | Session project | Resolution                          |
/// |-------------------------|-----------------|-------------------------------------|
/// | **absent** (`None`)     | present         | Use session project (ID + name)     |
/// | **absent** (`None`)     | absent          | Global search (`None, None`)        |
/// | **null** (`Some(None)`) | any             | Global search regardless of session |
/// | **present** string      | any             | Parse ID; name resolved later by DB |
fn resolve_project_scope(
    request_project_id: &Option<Option<String>>,
    session_project: Option<(ProjectId, String)>,
) -> Result<(Option<ProjectId>, Option<String>), tribal_domain::IdParseError> {
    match request_project_id {
        None => Ok(match session_project {
            Some((id, name)) => (Some(id), Some(name)),
            None => (None, None),
        }),
        Some(None) => Ok((None, None)),
        Some(Some(raw_id)) => {
            let proj_id = ProjectId::from_str(raw_id)?;
            Ok((Some(proj_id), None))
        }
    }
}

// ---------------------------------------------------------------------------
// Service function
// ---------------------------------------------------------------------------

/// Executes the discover operation against the knowledge base.
///
/// This is the single application boundary called by the handler adapter.
/// All inputs and outputs are domain types — no MCP types cross this
/// boundary.
async fn execute_discover(
    conn: &mut PgConnection,
    repositories: &ConnectionRepositories,
    embedding_provider: &(dyn EmbeddingProvider + Send + Sync),
    params: DiscoverParams,
) -> Result<DiscoverResult, DiscoverError> {
    let project_name = match (params.project_id, params.project_name) {
        (Some(proj_id), None) => {
            let project = repositories.project.find_by_id(conn, proj_id).await?;
            Some(project.name().to_owned())
        }
        (_, name) => name,
    };

    let embedding_response = embedding_provider
        .embed(EmbeddingRequest {
            input: params.query,
            purpose: EmbeddingPurpose::Query,
        })
        .await?;

    let embedding_model = embedding_provider.identity().model.clone();

    let search_params = SemanticSearchParams::builder()
        .query_embedding(embedding_response.vector)
        .embedding_model(embedding_model.clone())
        .project_id(params.project_id)
        .kinds(params.kinds)
        .tags(params.tags)
        .time_range_from(params.time_range_from)
        .time_range_to(params.time_range_to)
        .include_superseded(params.include_superseded)
        .limit(params.limit)
        .cursor(params.cursor)
        .build();

    let search_response = repositories
        .knowledge_item
        .semantic_search(conn, &search_params)
        .await?;

    if search_response.results.is_empty() {
        return Ok(DiscoverResult {
            items: Vec::new(),
            next_cursor: search_response.next_cursor,
            exact: search_response.exact,
            applied_project_id: params.project_id,
            project_name,
            embedding_model,
        });
    }

    let ki_ids: Vec<KnowledgeItemId> = search_response
        .results
        .iter()
        .map(|r| r.item.id())
        .collect();

    let unique_principal_ids: Vec<PrincipalId> = {
        let mut ids: Vec<PrincipalId> = search_response
            .results
            .iter()
            .map(|r| r.item.principal_id())
            .collect();
        ids.sort();
        ids.dedup();
        ids
    };

    let mut principal_map: HashMap<PrincipalId, String> =
        HashMap::with_capacity(unique_principal_ids.len());
    for prin_id in unique_principal_ids {
        let principal = repositories.principal.find_by_id(conn, prin_id).await?;
        principal_map.insert(prin_id, principal.principal_key().to_owned());
    }

    let standings: Option<Vec<Standing>> = if params.include_standing {
        Some(repositories.standing.compute(conn, &ki_ids).await?)
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

    let items: Vec<DiscoverResultItem> = search_response
        .results
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            let principal_key = principal_map
                .get(&r.item.principal_id())
                .cloned()
                .unwrap_or_else(|| r.item.principal_id().to_string());

            let standing = standings.as_ref().map(|s| s[i].clone());
            let references = references_map
                .as_ref()
                .map(|m| m.get(&r.item.id()).cloned().unwrap_or_default());

            DiscoverResultItem {
                item: r.item,
                similarity: r.similarity,
                principal_key,
                standing,
                references,
            }
        })
        .collect();

    Ok(DiscoverResult {
        items,
        next_cursor: search_response.next_cursor,
        exact: search_response.exact,
        applied_project_id: params.project_id,
        project_name,
        embedding_model,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rmcp::model::ErrorCode;
    use tokio::sync::RwLock;
    use tribal_db::SemanticSearchResponse;
    use tribal_domain::{KnowledgeItemId, PrincipalId, ProjectId, ReferenceKind};
    use tribal_test_utils::{
        ExhaustBehaviour, MockEmbeddingProvider, MockKnowledgeItemRepository,
        MockPrincipalRepository, MockProjectRepository, MockReferenceRepository,
        MockStandingRepository, a_knowledge_item, a_not_found, a_principal, a_project, a_reference,
        a_standing, an_embedding_response, lazy_pool, test_context,
    };

    use super::*;
    use crate::{
        server_handler::ConnectionRepositories,
        session::{SessionContext, SessionProject},
        test_utils::test_repositories,
    };

    // -- Constants ---------------------------------------------------------

    const STRUCTURED_CONTENT: &str = "structured_content must be present";
    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";

    // -- Helpers -----------------------------------------------------------

    fn test_vector() -> Vec<f32> {
        vec![0.1, 0.2, 0.3]
    }

    fn test_embedding_provider() -> Arc<dyn EmbeddingProvider> {
        Arc::new(MockEmbeddingProvider::builder().build())
    }

    fn embedding_provider_with_response() -> Arc<dyn EmbeddingProvider> {
        Arc::new(
            MockEmbeddingProvider::builder()
                .on_embed(an_embedding_response(test_vector()), None)
                .build(),
        )
    }

    fn a_search_result(
        item: &tribal_domain::KnowledgeItem,
        similarity: f64,
    ) -> tribal_db::SemanticSearchResult {
        tribal_db::SemanticSearchResult {
            item: item.clone(),
            similarity,
        }
    }

    fn a_search_response(results: Vec<tribal_db::SemanticSearchResult>) -> SemanticSearchResponse {
        SemanticSearchResponse {
            results,
            next_cursor: None,
            exact: true,
        }
    }

    fn test_principal(id: PrincipalId, key: &str) -> tribal_domain::Principal {
        a_principal().id(id).principal_key(key.to_owned()).build()
    }

    fn repos_with_search_and_principal(
        search_response: SemanticSearchResponse,
        principals: Vec<tribal_domain::Principal>,
    ) -> ConnectionRepositories {
        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_semantic_search(search_response, None)
            .build();

        let mut prin_mock = MockPrincipalRepository::builder();
        for p in principals {
            prin_mock = prin_mock.on_find_by_id(p, None);
        }

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.principal = Arc::new(prin_mock.build());
        repos
    }

    fn session_without_project() -> Arc<RwLock<SessionContext>> {
        Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())))
    }

    fn session_with_project(id: ProjectId, name: &str) -> Arc<RwLock<SessionContext>> {
        Arc::new(RwLock::new(SessionContext::new(
            Some(SessionProject {
                id,
                name: name.into(),
                git_remote: "git@github.com:user/tribal.git".into(),
            }),
            "user:test".into(),
        )))
    }

    async fn call_discover(
        repos: &ConnectionRepositories,
        embedding_provider: &(dyn EmbeddingProvider + Send + Sync),
        session: &RwLock<SessionContext>,
        params: serde_json::Value,
    ) -> Result<CallToolResult, McpError> {
        let ctx = test_context().await;
        TribalServerHandler::apply_discover(ctx.pool(), repos, embedding_provider, session, params)
            .await
    }

    // -- Happy path -------------------------------------------------------

    #[tokio::test]
    async fn test_response_includes_required_fields() {
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item().principal_id(prin_id).build();
        let search = a_search_response(vec![a_search_result(&item, 0.95)]);
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);
        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth patterns"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert!(structured.get("items").is_some());
        assert!(structured.get("next_cursor").is_some());
        assert!(structured.get("applied_project_id").is_some());
        assert!(structured.get("embedding_model").is_some());
        assert!(structured.get("trace_id").is_some());
        assert!(structured.get("exact").is_some());
        // serde(skip) fields must not appear
        assert!(structured.get("query").is_none());
        assert!(structured.get("project_name").is_none());
    }

    #[tokio::test]
    async fn test_trace_id_is_32_char_lowercase_hex() {
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item().principal_id(prin_id).build();
        let search = a_search_response(vec![a_search_result(&item, 0.9)]);
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);
        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "test"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        let trace_id = structured["trace_id"].as_str().expect("trace_id is string");
        assert_eq!(trace_id.len(), 32);
        assert!(trace_id.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(trace_id, trace_id.to_lowercase());
    }

    #[tokio::test]
    async fn test_empty_results_returns_empty_items() {
        let search = a_search_response(vec![]);
        let repos = repos_with_search_and_principal(search, vec![]);
        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "nonexistent"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert!(structured["items"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_principal_key_is_human_readable() {
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item().principal_id(prin_id).build();
        let search = a_search_response(vec![a_search_result(&item, 0.9)]);
        let repos = repos_with_search_and_principal(
            search,
            vec![test_principal(prin_id, "user:sam@example.com")],
        );
        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(
            structured["items"][0]["item"]["principal_key"],
            "user:sam@example.com",
        );
    }

    // -- Project scoping --------------------------------------------------

    #[tokio::test]
    async fn test_absent_project_with_session_uses_session() {
        let proj_id = ProjectId::new();
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item()
            .principal_id(prin_id)
            .project_id(proj_id)
            .build();
        let search = a_search_response(vec![a_search_result(&item, 0.8)]);
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);
        let provider = embedding_provider_with_response();
        let session = session_with_project(proj_id, "tribal");

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["applied_project_id"], proj_id.to_string());
    }

    #[tokio::test]
    async fn test_absent_project_without_session_is_global() {
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item().principal_id(prin_id).build();
        let search = a_search_response(vec![a_search_result(&item, 0.8)]);
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);
        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert!(structured["applied_project_id"].is_null());
    }

    #[tokio::test]
    async fn test_explicit_null_project_is_global() {
        let proj_id = ProjectId::new();
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item().principal_id(prin_id).build();
        let search = a_search_response(vec![a_search_result(&item, 0.8)]);
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);
        let provider = embedding_provider_with_response();
        let session = session_with_project(proj_id, "tribal");

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth", "project_id": null}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert!(structured["applied_project_id"].is_null());
    }

    #[tokio::test]
    async fn test_present_project_id_scopes_to_project() {
        let proj_id = ProjectId::new();
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item()
            .principal_id(prin_id)
            .project_id(proj_id)
            .build();
        let search = a_search_response(vec![a_search_result(&item, 0.8)]);
        let project = a_project().id(proj_id).build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(
            MockKnowledgeItemRepository::builder()
                .on_semantic_search(search, None)
                .build(),
        );
        repos.principal = Arc::new(
            MockPrincipalRepository::builder()
                .on_find_by_id(test_principal(prin_id, "user:test"), None)
                .build(),
        );
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );

        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth", "project_id": proj_id.to_string()}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["applied_project_id"], proj_id.to_string());
    }

    // -- Limit validation -------------------------------------------------

    #[tokio::test]
    async fn test_limit_defaults_to_10() {
        let search = a_search_response(vec![]);
        let ki_mock = MockKnowledgeItemRepository::builder()
            .when_semantic_search(|params| params.limit == DEFAULT_LIMIT)
            .respond_with(search, None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);

        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "test"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
    }

    #[tokio::test]
    async fn test_limit_below_one_is_invalid() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let provider = test_embedding_provider();
        let session = session_without_project();

        let result = TribalServerHandler::apply_discover(
            &pool,
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "test", "limit": 0}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_limit_above_fifty_is_invalid() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let provider = test_embedding_provider();
        let session = session_without_project();

        let result = TribalServerHandler::apply_discover(
            &pool,
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "test", "limit": 51}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    // -- Enrichment -------------------------------------------------------

    #[tokio::test]
    async fn test_include_standing_enriches_results() {
        let prin_id = PrincipalId::new();
        let ki_id = KnowledgeItemId::new();
        let item = a_knowledge_item().id(ki_id).principal_id(prin_id).build();
        let search = a_search_response(vec![a_search_result(&item, 0.9)]);
        let standing = a_standing()
            .supporting_count(3)
            .contradicting_count(1)
            .observation_count(5)
            .supporting_episode_count(2)
            .supporting_project_count(1)
            .build();

        let mut repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);
        repos.standing = Arc::new(
            MockStandingRepository::builder()
                .on_compute(vec![standing], None)
                .build(),
        );

        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth", "include_standing": true}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        let first = &structured["items"][0];
        assert!(first.get("standing").is_some());
        assert_eq!(first["standing"]["supporting_count"], 3);
        assert_eq!(first["standing"]["contradicting_count"], 1);
    }

    #[tokio::test]
    async fn test_standing_omitted_when_flag_false() {
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item().principal_id(prin_id).build();
        let search = a_search_response(vec![a_search_result(&item, 0.9)]);
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);
        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        let first = &structured["items"][0];
        assert!(first.get("standing").is_none());
    }

    #[tokio::test]
    async fn test_include_references_enriches_results() {
        let prin_id = PrincipalId::new();
        let ki_id = KnowledgeItemId::new();
        let proj_id = ProjectId::new();
        let item = a_knowledge_item()
            .id(ki_id)
            .principal_id(prin_id)
            .project_id(proj_id)
            .build();
        let search = a_search_response(vec![a_search_result(&item, 0.9)]);
        let reference = a_reference()
            .knowledge_item_id(ki_id)
            .project_id(proj_id)
            .kind(ReferenceKind::FilePath)
            .value("src/auth.rs".to_owned())
            .build();

        let mut repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);
        repos.reference = Arc::new(
            MockReferenceRepository::builder()
                .on_find_by_knowledge_item_ids(vec![reference], None)
                .build(),
        );

        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth", "include_references": true}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        let first = &structured["items"][0];
        let refs = first["references"].as_array().expect("references is array");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0]["value"], "src/auth.rs");
    }

    #[tokio::test]
    async fn test_references_omitted_when_flag_false() {
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item().principal_id(prin_id).build();
        let search = a_search_response(vec![a_search_result(&item, 0.9)]);
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);
        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        let first = &structured["items"][0];
        assert!(first.get("references").is_none());
    }

    // -- Text summary -----------------------------------------------------

    #[tokio::test]
    async fn test_text_summary_global_search() {
        let search = a_search_response(vec![]);
        let repos = repos_with_search_and_principal(search, vec![]);
        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth patterns"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let text = &result.content[0];
        let text_str = format!("{text:?}");
        assert!(text_str.contains("auth patterns"));
        assert!(text_str.contains("global search"));
    }

    #[tokio::test]
    async fn test_text_summary_scoped_to_project() {
        let proj_id = ProjectId::new();
        let search = a_search_response(vec![]);
        let repos = repos_with_search_and_principal(search, vec![]);
        let provider = embedding_provider_with_response();
        let session = session_with_project(proj_id, "tribal");

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "auth"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let text = &result.content[0];
        let text_str = format!("{text:?}");
        assert!(text_str.contains("scoped to project 'tribal'"));
    }

    #[tokio::test]
    async fn test_next_cursor_null_not_omitted() {
        let search = a_search_response(vec![]);
        let repos = repos_with_search_and_principal(search, vec![]);
        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "test"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert!(
            structured.get("next_cursor").is_some(),
            "next_cursor must be present",
        );
        assert!(structured["next_cursor"].is_null());
    }

    // -- Error paths ------------------------------------------------------

    #[tokio::test]
    async fn test_invalid_project_id_prefix() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let provider = test_embedding_provider();
        let session = session_without_project();
        let wrong_type_id = KnowledgeItemId::new().to_string();

        let result = TribalServerHandler::apply_discover(
            &pool,
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "test", "project_id": wrong_type_id}),
        )
        .await
        .expect("should return Ok with error result, not Err");

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_malformed_json_returns_invalid_params() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let provider = test_embedding_provider();
        let session = session_without_project();

        let err = TribalServerHandler::apply_discover(
            &pool,
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": 123}),
        )
        .await
        .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_embedding_provider_failure_returns_error() {
        let repos = test_repositories();
        let provider: Arc<dyn EmbeddingProvider> = Arc::new(
            MockEmbeddingProvider::builder()
                .on_embed_error(
                    || InferenceError::EmbeddingFailed {
                        model: "test-model".into(),
                        context: "test failure".into(),
                        source: None,
                    },
                    None,
                )
                .build(),
        );
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "test"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "internal");
    }

    #[tokio::test]
    async fn test_nonexistent_project_returns_not_found() {
        let proj_id = ProjectId::new();
        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id_exhaust(ExhaustBehaviour::Error(a_not_found(
                    "project",
                    proj_id.to_string(),
                )))
                .build(),
        );

        let provider = embedding_provider_with_response();
        let session = session_without_project();

        let result = call_discover(
            &repos,
            provider.as_ref(),
            &session,
            serde_json::json!({"query": "test", "project_id": proj_id.to_string()}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "not_found");
    }
}
