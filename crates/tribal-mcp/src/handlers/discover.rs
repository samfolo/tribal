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
use tribal_inference::EmbeddingRequest;

use super::common::acquire_connection;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    mapping::{
        McpDiscoverRequest, McpDiscoverResponse, McpDiscoveryResult, McpKnowledgeItem,
        McpReference, McpStanding,
    },
    server_handler::{ConnectionRepositories, TribalServerHandler},
};

// ---------------------------------------------------------------------------
// Service types
// ---------------------------------------------------------------------------

/// Resolved domain parameters for the discover operation.
struct DiscoverParams {
    query_embedding: Vec<f32>,
    embedding_model: String,
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
#[derive(Debug)]
struct DiscoverResultItem {
    item: tribal_domain::KnowledgeItem,
    similarity: f64,
    principal_key: String,
    standing: Option<Standing>,
    references: Option<Vec<Reference>>,
}

/// Domain-level result from [`execute_discover`].
#[derive(Debug)]
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
}

impl IntoMcpError for DiscoverError {
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
    /// Handles the `tribal_discover` tool call.
    pub(crate) async fn handle_discover(
        &self,
        params: serde_json::Value,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        self.apply_discover(params).await
    }

    /// Core logic for `tribal_discover`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    ///
    /// Parses the request, validates and resolves a project ID (if supplied),
    /// embeds the query, then delegates to [`execute_discover`] for all
    /// domain logic. Embedding is performed before pool acquisition so
    /// a connection is not held idle during the network call.
    ///
    /// Domain errors are returned as error `CallToolResult` values via
    /// `IntoMcpError` / `IntoCallToolResult`. Only protocol-level errors
    /// (malformed JSON) return `Err(McpError)`.
    async fn apply_discover(
        &self,
        params: serde_json::Value,
    ) -> Result<CallToolResult, McpError> {
        let request: McpDiscoverRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;

        let default_limit = self.config.discovery.default_limit;
        let max_limit = self.config.discovery.max_limit;

        let limit = request.limit.unwrap_or(default_limit);
        if !(1..=max_limit).contains(&limit) {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: format!("limit must be between 1 and {max_limit}, got {limit}"),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        }

        let session_project = {
            let guard = self.session.read().await;
            guard.project.as_ref().map(|p| (p.id, p.name.clone()))
        };

        let (project_id, project_name) =
            match resolve_project_scope(&request.project_id, session_project) {
                Ok(scope) => scope.into_parts(),
                Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
            };

        let embedding_response = match self
            .embedding_provider
            .embed(EmbeddingRequest {
                input: request.query.clone(),
                purpose: EmbeddingPurpose::Query,
            })
            .await
        {
            Ok(r) => r,
            Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
        };

        let embedding_model = embedding_response.usage.model.clone();

        let mut conn = match acquire_connection(&self.pool).await {
            Ok(c) => c,
            Err(call_result) => return Ok(call_result),
        };

        let discover_params = DiscoverParams {
            query_embedding: embedding_response.vector,
            embedding_model,
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

        let result = match execute_discover(&mut conn, &self.repositories, discover_params).await {
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

/// Resolved project scope for a discover query.
enum ProjectScope {
    /// Use the session project — ID and name are already known.
    Session { id: ProjectId, name: String },
    /// Global search — no project filter applied.
    Global,
    /// Explicit project ID from the request — name must be resolved from DB.
    Explicit(ProjectId),
}

impl ProjectScope {
    /// Splits into the `(Option<ProjectId>, Option<String>)` pair consumed
    /// by `DiscoverParams`.
    fn into_parts(self) -> (Option<ProjectId>, Option<String>) {
        match self {
            Self::Session { id, name } => (Some(id), Some(name)),
            Self::Global => (None, None),
            Self::Explicit(id) => (Some(id), None),
        }
    }
}

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
#[allow(clippy::option_option, clippy::ref_option)]
fn resolve_project_scope(
    request_project_id: &Option<Option<String>>,
    session_project: Option<(ProjectId, String)>,
) -> Result<ProjectScope, tribal_domain::IdParseError> {
    match request_project_id {
        None => Ok(match session_project {
            Some((id, name)) => ProjectScope::Session { id, name },
            None => ProjectScope::Global,
        }),
        Some(None) => Ok(ProjectScope::Global),
        Some(Some(raw_id)) => {
            let proj_id = ProjectId::from_str(raw_id)?;
            Ok(ProjectScope::Explicit(proj_id))
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
/// boundary. The query embedding is pre-computed by the adapter so a
/// pool connection is not held idle during the embedding network call.
async fn execute_discover(
    conn: &mut PgConnection,
    repositories: &ConnectionRepositories,
    params: DiscoverParams,
) -> Result<DiscoverResult, DiscoverError> {
    let project_name = match (params.project_id, params.project_name) {
        (Some(proj_id), None) => {
            let project = repositories.project.find_by_id(conn, proj_id).await?;
            Some(project.name().to_owned())
        }
        (_, name) => name,
    };

    let search_params = SemanticSearchParams::builder()
        .query_embedding(params.query_embedding)
        .embedding_model(params.embedding_model.clone())
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
            embedding_model: params.embedding_model,
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

    let principals = repositories
        .principal
        .find_by_ids(conn, &unique_principal_ids)
        .await?;
    let principal_map: HashMap<PrincipalId, String> = principals
        .into_iter()
        .map(|p| (p.id(), p.principal_key().to_owned()))
        .collect();

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

    let items: Vec<DiscoverResultItem> = search_response
        .results
        .into_iter()
        .map(|r| {
            // Missing principals fall back to the raw ID string.
            let principal_key = principal_map
                .get(&r.item.principal_id())
                .cloned()
                .unwrap_or_else(|| r.item.principal_id().to_string());

            let standing = standings_map
                .as_ref()
                .and_then(|m| m.get(&r.item.id()).cloned());
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
        embedding_model: params.embedding_model,
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
    use tribal_domain::{PromptVersionId, ReferenceKind};
    use tribal_inference::{EmbeddingProvider, InferenceError};
    use tribal_test_utils::{
        ExhaustBehaviour, MockEmbeddingProvider, MockKnowledgeItemRepository,
        MockPrincipalRepository, MockProjectRepository, MockReferenceRepository,
        MockStandingRepository, a_knowledge_item, a_not_found, a_principal, a_project, a_reference,
        a_standing, lazy_pool, test_context,
    };

    use super::*;
    use crate::{
        config::HandlerConfig,
        server_handler::{ActivePromptVersions, TribalServerHandler},
        session::SessionContext,
        test_utils::test_repositories,
    };

    // -- Constants ---------------------------------------------------------

    const STRUCTURED_CONTENT: &str = "structured_content must be present";
    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";

    // -- Helpers -----------------------------------------------------------

    fn test_vector() -> Vec<f32> {
        vec![0.1, 0.2, 0.3]
    }

    fn test_prompt_versions() -> Arc<RwLock<ActivePromptVersions>> {
        Arc::new(RwLock::new(ActivePromptVersions {
            extraction_system_prompt_version_id: PromptVersionId::new(),
            extraction_user_prompt_version_id: PromptVersionId::new(),
            triage_system_prompt_version_id: PromptVersionId::new(),
            triage_user_prompt_version_id: PromptVersionId::new(),
            relation_system_prompt_version_id: PromptVersionId::new(),
            relation_user_prompt_version_id: PromptVersionId::new(),
        }))
    }

    fn test_embedding_provider() -> Arc<dyn EmbeddingProvider> {
        Arc::new(MockEmbeddingProvider::builder().build())
    }

    fn test_handler_with_repos_and_provider(
        repos: ConnectionRepositories,
        provider: Arc<dyn EmbeddingProvider>,
    ) -> TribalServerHandler {
        TribalServerHandler::new(
            lazy_pool(),
            repos,
            provider,
            test_prompt_versions(),
            SessionContext::new(None, "user:test".into()),
            HandlerConfig::default(),
        )
    }

    fn test_handler_with_repos(repos: ConnectionRepositories) -> TribalServerHandler {
        test_handler_with_repos_and_provider(repos, test_embedding_provider())
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

        let prin_mock = MockPrincipalRepository::builder().on_find_by_ids(principals, None);

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.principal = Arc::new(prin_mock.build());
        repos
    }

    fn default_params() -> DiscoverParams {
        let config = HandlerConfig::default();
        DiscoverParams {
            query_embedding: test_vector(),
            embedding_model: "mock-model".into(),
            project_id: None,
            project_name: None,
            kinds: None,
            tags: None,
            time_range_from: None,
            time_range_to: None,
            include_superseded: false,
            include_standing: false,
            include_references: false,
            limit: config.discovery.default_limit,
            cursor: None,
        }
    }

    async fn call_execute(
        repos: &ConnectionRepositories,
        params: DiscoverParams,
    ) -> Result<DiscoverResult, DiscoverError> {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");
        execute_discover(&mut tx, repos, params).await
    }

    // -- Project scope resolution -----------------------------------------

    #[test]
    fn test_resolve_absent_project_with_session() {
        let id = ProjectId::new();
        let result =
            resolve_project_scope(&None, Some((id, "tribal".into()))).expect("should resolve");
        let (proj_id, proj_name) = result.into_parts();
        assert_eq!(proj_id, Some(id));
        assert_eq!(proj_name.as_deref(), Some("tribal"));
    }

    #[test]
    fn test_resolve_absent_project_without_session() {
        let result = resolve_project_scope(&None, None).expect("should resolve");
        let (proj_id, proj_name) = result.into_parts();
        assert!(proj_id.is_none());
        assert!(proj_name.is_none());
    }

    #[test]
    fn test_resolve_explicit_null_is_global() {
        let id = ProjectId::new();
        let result = resolve_project_scope(&Some(None), Some((id, "tribal".into())))
            .expect("should resolve");
        let (proj_id, proj_name) = result.into_parts();
        assert!(proj_id.is_none());
        assert!(proj_name.is_none());
    }

    #[test]
    fn test_resolve_present_project_id() {
        let id = ProjectId::new();
        let result =
            resolve_project_scope(&Some(Some(id.to_string())), None).expect("should resolve");
        let (proj_id, proj_name) = result.into_parts();
        assert_eq!(proj_id, Some(id));
        assert!(proj_name.is_none());
    }

    #[test]
    fn test_resolve_invalid_project_id_returns_error() {
        let ki_id = KnowledgeItemId::new().to_string();
        let result = resolve_project_scope(&Some(Some(ki_id)), None);
        assert!(result.is_err());
    }

    // -- Service: happy path ----------------------------------------------

    #[tokio::test]
    async fn test_query_returns_items() {
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item().principal_id(prin_id).build();
        let search = a_search_response(vec![a_search_result(&item, 0.95)]);
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);

        let result = call_execute(&repos, default_params()).await.unwrap();

        assert_eq!(result.items.len(), 1);
        assert!((result.items[0].similarity - 0.95).abs() < f64::EPSILON);
        assert!(result.applied_project_id.is_none());
        assert_eq!(result.embedding_model, "mock-model");
        assert!(result.exact);
    }

    #[tokio::test]
    async fn test_empty_results_returns_empty_items() {
        let search = a_search_response(vec![]);
        let repos = repos_with_search_and_principal(search, vec![]);

        let result = call_execute(&repos, default_params()).await.unwrap();

        assert!(result.items.is_empty());
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

        let result = call_execute(&repos, default_params()).await.unwrap();

        assert_eq!(result.items[0].principal_key, "user:sam@example.com");
    }

    #[tokio::test]
    async fn test_shared_principal_deduplicates_lookups() {
        let prin_id = PrincipalId::new();
        let item_a = a_knowledge_item().principal_id(prin_id).build();
        let item_b = a_knowledge_item().principal_id(prin_id).build();
        let search = a_search_response(vec![
            a_search_result(&item_a, 0.9),
            a_search_result(&item_b, 0.8),
        ]);

        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:shared")]);

        let result = call_execute(&repos, default_params()).await.unwrap();

        assert_eq!(result.items.len(), 2);
        assert_eq!(result.items[0].principal_key, "user:shared");
        assert_eq!(result.items[1].principal_key, "user:shared");
    }

    // -- Service: project resolution --------------------------------------

    #[tokio::test]
    async fn test_explicit_project_id_resolves_name() {
        let proj_id = ProjectId::new();
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item()
            .principal_id(prin_id)
            .project_id(proj_id)
            .build();
        let search = a_search_response(vec![a_search_result(&item, 0.8)]);
        let project = a_project().id(proj_id).build();
        let expected_name = project.name().to_owned();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(
            MockKnowledgeItemRepository::builder()
                .on_semantic_search(search, None)
                .build(),
        );
        repos.principal = Arc::new(
            MockPrincipalRepository::builder()
                .on_find_by_ids(vec![test_principal(prin_id, "user:test")], None)
                .build(),
        );
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );

        let params = DiscoverParams {
            project_id: Some(proj_id),
            ..default_params()
        };
        let result = call_execute(&repos, params).await.unwrap();

        assert_eq!(result.applied_project_id, Some(proj_id));
        assert_eq!(result.project_name.as_deref(), Some(expected_name.as_str()));
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

        let params = DiscoverParams {
            project_id: Some(proj_id),
            ..default_params()
        };
        let err = call_execute(&repos, params).await.unwrap_err();

        assert!(matches!(err, DiscoverError::Db(DbError::NotFound { .. })));
    }

    // -- Service: parameter propagation -----------------------------------

    #[tokio::test]
    async fn test_cursor_propagated_to_search_params() {
        let search = a_search_response(vec![]);
        let ki_mock = MockKnowledgeItemRepository::builder()
            .when_semantic_search(|params| params.cursor.as_deref() == Some("cursor_xyz"))
            .respond_with(search, None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);

        let params = DiscoverParams {
            cursor: Some("cursor_xyz".into()),
            ..default_params()
        };
        let result = call_execute(&repos, params).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_filters_passed_to_search_params() {
        let from = Utc::now();
        let to = from + chrono::Duration::hours(1);
        let search = a_search_response(vec![]);
        let ki_mock = MockKnowledgeItemRepository::builder()
            .when_semantic_search(move |params| {
                params.kinds.as_ref().is_some_and(|k| k.len() == 1)
                    && params.tags.as_ref().is_some_and(|t| t == &["auth"])
                    && params.time_range_from == Some(from)
                    && params.time_range_to == Some(to)
            })
            .respond_with(search, None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);

        let params = DiscoverParams {
            kinds: Some(vec![KnowledgeKind::Fact]),
            tags: Some(vec!["auth".into()]),
            time_range_from: Some(from),
            time_range_to: Some(to),
            ..default_params()
        };
        let result = call_execute(&repos, params).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_include_superseded_passed_through() {
        let search = a_search_response(vec![]);
        let ki_mock = MockKnowledgeItemRepository::builder()
            .when_semantic_search(|params| params.include_superseded)
            .respond_with(search, None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);

        let params = DiscoverParams {
            include_superseded: true,
            ..default_params()
        };
        let result = call_execute(&repos, params).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_next_cursor_populated_when_more_results() {
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item().principal_id(prin_id).build();
        let search = SemanticSearchResponse {
            results: vec![a_search_result(&item, 0.9)],
            next_cursor: Some("cursor_abc123".into()),
            exact: false,
        };
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);

        let result = call_execute(&repos, default_params()).await.unwrap();

        assert_eq!(result.next_cursor.as_deref(), Some("cursor_abc123"));
        assert!(!result.exact);
    }

    // -- Service: enrichment ----------------------------------------------

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
                .on_compute(vec![standing.clone()], None)
                .build(),
        );

        let params = DiscoverParams {
            include_standing: true,
            ..default_params()
        };
        let result = call_execute(&repos, params).await.unwrap();

        let item_standing = result.items[0].standing.as_ref().expect("standing present");
        assert_eq!(
            item_standing.supporting_count(),
            standing.supporting_count()
        );
        assert_eq!(
            item_standing.contradicting_count(),
            standing.contradicting_count(),
        );
    }

    #[tokio::test]
    async fn test_standing_omitted_when_flag_false() {
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item().principal_id(prin_id).build();
        let search = a_search_response(vec![a_search_result(&item, 0.9)]);
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);

        let result = call_execute(&repos, default_params()).await.unwrap();

        assert!(result.items[0].standing.is_none());
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

        let params = DiscoverParams {
            include_references: true,
            ..default_params()
        };
        let result = call_execute(&repos, params).await.unwrap();

        let refs = result.items[0]
            .references
            .as_ref()
            .expect("references present");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].value(), "src/auth.rs");
    }

    #[tokio::test]
    async fn test_references_omitted_when_flag_false() {
        let prin_id = PrincipalId::new();
        let item = a_knowledge_item().principal_id(prin_id).build();
        let search = a_search_response(vec![a_search_result(&item, 0.9)]);
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);

        let result = call_execute(&repos, default_params()).await.unwrap();

        assert!(result.items[0].references.is_none());
    }

    // -- Adapter: validation ----------------------------------------------

    #[tokio::test]
    async fn test_limit_below_one_is_invalid() {
        let handler = test_handler_with_repos(test_repositories());

        let result = handler
            .apply_discover(serde_json::json!({"query": "test", "limit": 0}))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_limit_above_max_is_invalid() {
        let handler = test_handler_with_repos(test_repositories());
        let max_limit = HandlerConfig::default().discovery.max_limit;

        let result = handler
            .apply_discover(serde_json::json!({"query": "test", "limit": max_limit + 1}))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_invalid_project_id_prefix() {
        let handler = test_handler_with_repos(test_repositories());
        let wrong_type_id = KnowledgeItemId::new().to_string();

        let result = handler
            .apply_discover(serde_json::json!({"query": "test", "project_id": wrong_type_id}))
            .await
            .expect("should return Ok with error result, not Err");

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_malformed_json_returns_invalid_params() {
        let handler = test_handler_with_repos(test_repositories());

        let err = handler
            .apply_discover(serde_json::json!({"query": 123}))
            .await
            .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_embedding_provider_failure_returns_error() {
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
        let handler = test_handler_with_repos_and_provider(test_repositories(), provider);

        let result = handler
            .apply_discover(serde_json::json!({"query": "test"}))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "internal");
    }
}
