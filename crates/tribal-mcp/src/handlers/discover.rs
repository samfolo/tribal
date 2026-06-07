//! Handler for `tribal_discover` — semantic search across the knowledge base.

use std::{collections::HashMap, str::FromStr, sync::Arc, time::Instant};

use chrono::{DateTime, Utc};
use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::PgConnection;
use tracing::Instrument;
use tribal_db::{
    DbError, EmbeddingProfileRepository, PgEmbeddingProfileRepository, SemanticSearchParams,
    encode_cursor,
};
use tribal_domain::{
    EmbeddingProfile, EmbeddingProfileId, EmbeddingPurpose, KnowledgeItemId, KnowledgeKind,
    McpErrorCode, PrincipalId, ProjectId, Reference, Standing, span_attrs,
};
use tribal_inference::{EmbeddingRequest, EmbeddingResponse, InferenceError};
use tribal_worker::build_target_provider;

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
    original_limit: u32,
    overfetch_limit: u32,
    similarity_threshold: f64,
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
    embedding_profile_id: EmbeddingProfileId,
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
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let principal = self.resolve_principal(&context)?;
        let span = tracing::info_span!(
            parent: None,
            "tribal.discover",
            { span_attrs::PRINCIPAL_KEY } = principal.principal_key(),
            { span_attrs::TRANSPORT } = self.transport_name,
            { span_attrs::PROJECT_ID } = tracing::field::Empty,
        );
        self.apply_discover(params).instrument(span).await
    }

    /// Resolves the live embedding provider from the active profile and embeds
    /// the query against it, so the query lands in the geometry it is searched
    /// against rather than the boot-time provider's. Returns the active profile
    /// (the search and the next cursor bind to it) and the embedding response,
    /// or an error `CallToolResult` to return verbatim.
    ///
    /// The active profile is read on a short-lived connection that is released
    /// before the embedding network call, so a pool connection is never held
    /// idle across it.
    async fn embed_query_against_active(
        &self,
        query: &str,
    ) -> Result<(EmbeddingProfile, EmbeddingResponse), CallToolResult> {
        let active_profile = {
            let mut conn = acquire_connection(
                &self.state.pool_mcp,
                self.config.pool_name,
                &self.state.metrics,
            )
            .await?;
            match PgEmbeddingProfileRepository.find_active(&mut conn).await {
                Ok(Some(profile)) => profile,
                Ok(None) => {
                    return Err(DbError::NotFound {
                        entity: "embedding_profile",
                        id: "active".to_owned(),
                    }
                    .into_mcp_error()
                    .into_call_tool_result());
                }
                Err(e) => return Err(e.into_mcp_error().into_call_tool_result()),
            }
        };

        let (provider, semaphore) = build_target_provider(
            &self.state.provider_registry,
            &self.state.embedding_providers,
            &self.state.credentials,
            &active_profile,
        )
        .map_err(|e| {
            InferenceError::provider_unavailable(
                active_profile.provider_kind().to_string(),
                e.to_string(),
            )
            .into_mcp_error()
            .into_call_tool_result()
        })?;

        let _permit = {
            let sem_start = Instant::now();
            let permit = Arc::clone(&semaphore)
                .acquire_owned()
                .await
                .expect("embedding semaphore closed");
            self.state.metrics.record_semaphore_acquire(
                &self.state.embedding_key.to_string(),
                sem_start.elapsed(),
            );
            permit
        };

        let provider_start = Instant::now();
        let embedding_response = provider
            .embed(EmbeddingRequest {
                input: query.to_owned(),
                purpose: EmbeddingPurpose::Query,
            })
            .await
            .map_err(|e| e.into_mcp_error().into_call_tool_result())?;
        let identity = provider.identity();
        self.state.metrics.record_provider_call(
            &identity.name,
            &identity.model,
            "discover",
            provider_start.elapsed(),
        );

        Ok((active_profile, embedding_response))
    }

    /// Core logic for `tribal_discover`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    ///
    /// Parses the request, validates and resolves a project ID (if supplied),
    /// resolves the live provider from the active profile and embeds the query
    /// against it (see [`Self::embed_query_against_active`]), then delegates to
    /// [`execute_discover`] for all domain logic.
    ///
    /// Domain errors are returned as error `CallToolResult` values via
    /// `IntoMcpError` / `IntoCallToolResult`. Only protocol-level errors
    /// (malformed JSON) return `Err(McpError)`.
    async fn apply_discover(&self, params: serde_json::Value) -> Result<CallToolResult, McpError> {
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

        if let Some(pid) = &project_id {
            tracing::Span::current().record(span_attrs::PROJECT_ID, tracing::field::display(pid));
        }

        let (active_profile, embedding_response) =
            match self.embed_query_against_active(&request.query).await {
                Ok(resolved) => resolved,
                Err(call_result) => return Ok(call_result),
            };

        let embedding_model = embedding_response.usage.model.clone();

        let mut conn = match acquire_connection(
            &self.state.pool_mcp,
            self.config.pool_name,
            &self.state.metrics,
        )
        .await
        {
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
            original_limit: limit,
            overfetch_limit: limit.saturating_mul(self.config.discovery.overfetch_multiplier),
            similarity_threshold: self.config.discovery.similarity_threshold,
            cursor: normalise_cursor(request.cursor),
        };

        let result = match execute_discover(
            &mut conn,
            &self.repositories,
            &active_profile,
            discover_params,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
        };

        let trace_id = tribal_telemetry::current_trace_id()
            .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());

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
            embedding_profile_id: result.embedding_profile_id.to_string(),
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

/// Normalises an incoming pagination cursor: a present-but-empty (or blank)
/// cursor is treated as absent (first page) rather than handed to the strict
/// 48-hex validator. Some harnesses auto-fill an optional `cursor` with `""`
/// instead of omitting it; treating that as "no cursor" keeps the first-page
/// path working while real tokens stay strictly validated.
fn normalise_cursor(cursor: Option<String>) -> Option<String> {
    cursor.filter(|c| !c.trim().is_empty())
}

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
    active_profile: &EmbeddingProfile,
    params: DiscoverParams,
) -> Result<DiscoverResult, DiscoverError> {
    let project_name = match (params.project_id, params.project_name) {
        (Some(proj_id), None) => {
            let project = repositories.project.find_by_id(conn, proj_id).await?;
            Some(project.name().to_owned())
        }
        (_, name) => name,
    };

    // The adapter resolved the active profile and embedded the query against its
    // geometry; the search and the next cursor bind to that same profile.
    let search_params = SemanticSearchParams::builder()
        .query_embedding(params.query_embedding)
        .embedding_profile_id(active_profile.id())
        .dimensions(active_profile.dimensions())
        .project_id(params.project_id)
        .kinds(params.kinds)
        .tags(params.tags)
        .time_range_from(params.time_range_from)
        .time_range_to(params.time_range_to)
        .include_superseded(params.include_superseded)
        .limit(params.overfetch_limit)
        .cursor(params.cursor)
        .build();

    let mut search_response = repositories
        .knowledge_item
        .semantic_search(conn, &search_params)
        .await?;

    // -- Overfetch filtering --------------------------------------------------
    // The search fetched `original_limit * overfetch_multiplier` rows.
    // Filter by similarity, compute whether the result set is complete,
    // then trim to the caller's requested limit — all before enrichment
    // to avoid unnecessary lookups for items that will be discarded.

    let pre_filter_count = search_response.results.len();
    search_response
        .results
        .retain(|r| r.similarity >= params.similarity_threshold);
    let threshold_cut_tail = search_response.results.len() < pre_filter_count;

    let post_filter_count = search_response.results.len();
    search_response
        .results
        .truncate(params.original_limit as usize);

    // Determine whether more results can exist beyond this page.
    // Pagination continues when above-threshold results were truncated,
    // or when the repo indicates more rows exist. The threshold cutting
    // the tail only terminates pagination when the surviving results
    // fit within the original limit.
    let truncated = post_filter_count > params.original_limit as usize;
    let has_more = truncated || (!threshold_cut_tail && search_response.next_cursor.is_some());

    let next_cursor = if has_more {
        search_response.results.last().map(|r| {
            encode_cursor(
                r.similarity,
                *r.item.id().inner(),
                *active_profile.id().inner(),
            )
        })
    } else {
        None
    };

    // `exact` tells the client whether all matching results are
    // included. Both conditions must hold: no more pages to fetch,
    // and the repository's search was itself complete (the ANN
    // widening heuristic was not exhausted).
    let exact = next_cursor.is_none() && search_response.exact;

    if search_response.results.is_empty() {
        return Ok(DiscoverResult {
            items: Vec::new(),
            next_cursor,
            exact,
            applied_project_id: params.project_id,
            project_name,
            embedding_model: params.embedding_model,
            embedding_profile_id: active_profile.id(),
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
        next_cursor,
        exact,
        applied_project_id: params.project_id,
        project_name,
        embedding_model: params.embedding_model,
        embedding_profile_id: active_profile.id(),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use rmcp::model::ErrorCode;
    use tribal_db::SemanticSearchResponse;
    use tribal_domain::{ProviderKind, ReferenceKind};
    use tribal_inference::{
        EmbeddingProvider, InferenceError, ProviderIdentity, ProviderKey, ProviderLimits,
        RequestClass,
    };
    use tribal_test_utils::{
        ExhaustBehaviour, MockEmbeddingProvider, MockKnowledgeItemRepository,
        MockPrincipalRepository, MockProjectRepository, MockReferenceRepository,
        MockStandingRepository, TestContext, a_knowledge_item, a_not_found, a_principal, a_project,
        a_reference, a_standing, an_embedding_response, create_complete_profile,
        ensure_genesis_profile, test_context,
    };

    use super::*;
    use crate::{
        config::HandlerConfig,
        test_utils::{TestHandler, first_text_content, test_repositories},
    };

    // -- Constants ---------------------------------------------------------

    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";
    const NO_STRUCTURED_CONTENT: &str = "error results carry no structured content";

    // -- Helpers -----------------------------------------------------------

    fn test_vector() -> Vec<f32> {
        vec![0.1, 0.2, 0.3]
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
            original_limit: config.discovery.default_limit,
            overfetch_limit: config
                .discovery
                .default_limit
                .saturating_mul(config.discovery.overfetch_multiplier),
            similarity_threshold: config.discovery.similarity_threshold,
            cursor: None,
        }
    }

    async fn call_execute(
        repos: &ConnectionRepositories,
        params: DiscoverParams,
    ) -> Result<DiscoverResult, DiscoverError> {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");
        // The handler resolves the active embedding profile; seed one and read
        // it back so the mocked search runs against it.
        ensure_genesis_profile(&mut tx, "mock-model", 768).await;
        let active_profile = PgEmbeddingProfileRepository
            .find_active(&mut tx)
            .await
            .expect("find active")
            .expect("genesis profile seeded");
        execute_discover(&mut tx, repos, &active_profile, params).await
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

    #[test]
    fn test_normalise_cursor_treats_empty_or_blank_as_first_page() {
        // A harness that auto-fills the optional cursor with "" must not trip
        // the strict 48-hex validator; empty or blank normalises to first page,
        // while a real token is passed through untouched.
        assert_eq!(normalise_cursor(None), None);
        assert_eq!(normalise_cursor(Some(String::new())), None);
        assert_eq!(normalise_cursor(Some("   ".into())), None);
        assert_eq!(
            normalise_cursor(Some("realtoken".into())).as_deref(),
            Some("realtoken"),
        );
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
    async fn test_next_cursor_recomputed_from_last_returned_item() {
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

        let expected_cursor = encode_cursor(
            0.9,
            *item.id().inner(),
            *result.embedding_profile_id.inner(),
        );
        assert_eq!(
            result.next_cursor.as_deref(),
            Some(expected_cursor.as_str())
        );
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
        let handler = TestHandler::builder().build();

        let result = handler
            .apply_discover(serde_json::json!({"query": "test", "limit": 0}))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("limit must be between"));
    }

    #[tokio::test]
    async fn test_limit_above_max_is_invalid() {
        let handler = TestHandler::builder().build();
        let max_limit = HandlerConfig::default().discovery.max_limit;

        let result = handler
            .apply_discover(serde_json::json!({"query": "test", "limit": max_limit + 1}))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("limit must be between"));
    }

    #[tokio::test]
    async fn test_invalid_project_id_prefix() {
        let handler = TestHandler::builder().build();
        let wrong_type_id = KnowledgeItemId::new().to_string();

        let result = handler
            .apply_discover(serde_json::json!({"query": "test", "project_id": wrong_type_id}))
            .await
            .expect("should return Ok with error result, not Err");

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("expected prefix"));
    }

    #[tokio::test]
    async fn test_malformed_json_returns_invalid_params() {
        let handler = TestHandler::builder().build();

        let err = handler
            .apply_discover(serde_json::json!({"query": 123}))
            .await
            .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_embedding_provider_failure_returns_error() {
        // The read path resolves the provider from the active profile, so seed
        // one in a dedicated database and bind a failing provider to it; the
        // embed error must surface as an internal error result. A dummy pool
        // would instead fail at find_active, with an environment-dependent code.
        let ctx = TestContext::new().await.expect("dedicated test database");
        let pool = ctx.pool().clone();
        let active = {
            let mut seed = ctx.raw_connection().await.expect("seed connection");
            ensure_genesis_profile(&mut seed, "model-a", 768).await
        };

        let failing: Arc<dyn EmbeddingProvider> = Arc::new(
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

        let handler = TestHandler::builder().pool(pool).build();
        handler
            .state
            .provider_registry
            .register_building(
                ProviderKey::new(
                    ProviderKind::Ollama.to_string(),
                    ProviderKind::DEFAULT_OLLAMA_BASE_URL,
                    RequestClass::Embedding,
                )
                .expect("embedding provider key"),
                &ProviderLimits {
                    max_in_flight: 1,
                    request_timeout: Duration::from_secs(5),
                },
            )
            .expect("register the embedding endpoint");
        handler
            .state
            .embedding_providers
            .insert(active.id(), failing);

        let result = handler
            .apply_discover(serde_json::json!({"query": "test"}))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("embedding generation failed"));
    }

    // -- Service: overfetch behaviour ----------------------------------------

    #[tokio::test]
    async fn test_execute_discover_overfetch_multiplier_applied() {
        let search = a_search_response(vec![]);
        let ki_mock = MockKnowledgeItemRepository::builder()
            .when_semantic_search(|params| params.limit == 15)
            .respond_with(search, None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);

        let params = DiscoverParams {
            original_limit: 5,
            overfetch_limit: 15,
            ..default_params()
        };
        let result = call_execute(&repos, params).await;

        assert!(
            result.is_ok(),
            "predicate on limit == 15 should have matched"
        );
    }

    // -- Overfetch helper ----------------------------------------------------

    /// Builds a discover scenario and returns the result.
    ///
    /// `similarities` defines the items returned by the mock repo (must
    /// be in descending order). `repo_exact` and `repo_cursor` configure
    /// the mock's `SemanticSearchResponse`. The remaining parameters map
    /// directly to `DiscoverParams`.
    async fn run_overfetch_scenario(
        similarities: &[f64],
        repo_exact: bool,
        repo_cursor: Option<&str>,
        original_limit: u32,
        similarity_threshold: f64,
    ) -> DiscoverResult {
        let prin_id = PrincipalId::new();
        let items: Vec<_> = similarities
            .iter()
            .map(|_| a_knowledge_item().principal_id(prin_id).build())
            .collect();

        let search = SemanticSearchResponse {
            results: items
                .iter()
                .zip(similarities)
                .map(|(item, &sim)| a_search_result(item, sim))
                .collect(),
            next_cursor: repo_cursor.map(String::from),
            exact: repo_exact,
        };
        let repos =
            repos_with_search_and_principal(search, vec![test_principal(prin_id, "user:test")]);

        let params = DiscoverParams {
            original_limit,
            overfetch_limit: original_limit.saturating_mul(3),
            similarity_threshold,
            ..default_params()
        };
        call_execute(&repos, params).await.unwrap()
    }

    // -- Overfetch: filtering -------------------------------------------------

    #[tokio::test]
    async fn test_overfetch_filters_below_similarity_threshold() {
        let result = run_overfetch_scenario(&[0.9, 0.7, 0.3], true, None, 10, 0.5).await;
        assert_eq!(result.items.len(), 2);
        assert!(result.items.iter().all(|r| r.similarity >= 0.5));
    }

    #[tokio::test]
    async fn test_overfetch_truncates_to_original_limit() {
        let result = run_overfetch_scenario(&[0.9, 0.8, 0.7, 0.6], true, None, 2, 0.0).await;
        assert_eq!(result.items.len(), 2);
        assert!(!result.exact);
        assert!(result.next_cursor.is_some());
    }

    // -- Overfetch: exact flag ------------------------------------------------

    #[tokio::test]
    async fn test_overfetch_exact_true_when_complete_and_fits() {
        let result = run_overfetch_scenario(&[0.9, 0.8, 0.7], true, None, 5, 0.0).await;
        assert!(result.exact);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn test_overfetch_exact_false_when_repo_search_incomplete() {
        let result = run_overfetch_scenario(&[0.9, 0.8, 0.7], false, None, 5, 0.0).await;
        assert!(!result.exact);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn test_overfetch_exact_false_when_threshold_cuts_incomplete_search() {
        let result = run_overfetch_scenario(&[0.9, 0.7, 0.2, 0.1], false, None, 5, 0.5).await;
        assert_eq!(result.items.len(), 2);
        assert!(!result.exact);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn test_overfetch_exact_true_when_threshold_cuts_complete_search() {
        let result = run_overfetch_scenario(&[0.9, 0.7, 0.2, 0.1], true, None, 5, 0.5).await;
        assert_eq!(result.items.len(), 2);
        assert!(result.exact);
        assert!(result.next_cursor.is_none());
    }

    // -- Overfetch: cursor ----------------------------------------------------

    #[tokio::test]
    async fn test_overfetch_cursor_preserved_when_repo_has_more() {
        let result =
            run_overfetch_scenario(&[0.9, 0.8, 0.7], true, Some("repo_cursor"), 5, 0.0).await;
        assert_eq!(result.items.len(), 3);
        assert!(!result.exact);
        assert!(result.next_cursor.is_some());
    }

    #[tokio::test]
    async fn test_overfetch_cursor_none_when_threshold_cuts_tail() {
        let result =
            run_overfetch_scenario(&[0.9, 0.7, 0.2], true, Some("repo_cursor"), 5, 0.5).await;
        assert_eq!(result.items.len(), 2);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn test_overfetch_cursor_present_when_threshold_cuts_but_above_threshold_exceeds_limit() {
        // 8 above threshold (0.5), 2 below. original_limit is 5, so
        // the 8 surviving items are truncated to 5. Pagination must
        // continue despite the threshold cutting the tail.
        let result = run_overfetch_scenario(
            &[0.95, 0.9, 0.85, 0.8, 0.75, 0.7, 0.65, 0.6, 0.2, 0.1],
            true,
            None,
            5,
            0.5,
        )
        .await;
        assert_eq!(result.items.len(), 5);
        assert!(result.next_cursor.is_some());
        assert!(!result.exact);
    }

    // -- Adapter: live-identity seam across a cutover (§5.7) ------------------

    /// Builds a mock embedding provider tagged with `model` that returns
    /// `vector` once, for inserting into the per-profile provider cache.
    fn profile_provider(model: &str, vector: Vec<f32>) -> Arc<dyn EmbeddingProvider> {
        Arc::new(
            MockEmbeddingProvider::builder()
                .with_identity(ProviderIdentity {
                    name: "ollama".into(),
                    model: model.into(),
                })
                .on_embed(an_embedding_response(vector), None)
                .build(),
        )
    }

    /// Calls the private live-identity embed seam, panicking with a clear
    /// message if it returns an error `CallToolResult` (opaque to assert on).
    async fn embed_active(
        handler: &TribalServerHandler,
        query: &str,
    ) -> (EmbeddingProfile, EmbeddingResponse) {
        match handler.embed_query_against_active(query).await {
            Ok(resolved) => resolved,
            Err(call_result) => {
                panic!("embed_query_against_active returned an error result: {call_result:?}")
            }
        }
    }

    /// `tribal_discover` resolves the embedding provider from the active profile
    /// on every call, so a live cutover embeds the query against the
    /// newly-active profile's geometry rather than the boot-time provider.
    ///
    /// This is the regression guard for the §5.7 read-path gap: the per-area
    /// suites all passed while discover still embedded against the static
    /// boot-time provider, because the discrepancy surfaces only once the active
    /// profile changes under a running server. The test flips the active profile
    /// between two embed calls and asserts each call binds to the profile live at
    /// that moment.
    #[tokio::test]
    async fn test_discover_embeds_against_the_active_profile_across_a_cutover() {
        // This test commits embedding-profile rows, and the handler reads the
        // global active profile on its own pool connection, so committed state
        // would otherwise leak into the parallel suite's transaction-rollback
        // tests. A dedicated database (its own container) isolates it entirely:
        // no shared pool, no committed-state leakage, so no serial lock or
        // truncation is needed. A single raw connection seeds it.
        let ctx = TestContext::new().await.expect("dedicated test database");
        let pool = ctx.pool().clone();
        let mut seed = ctx.raw_connection().await.expect("seed connection");

        let handler = TestHandler::builder().pool(pool).build();

        // Both profiles share the Ollama default endpoint, so a single dynamic
        // registration supplies the semaphore the read path resolves on a cache
        // hit for either profile.
        handler
            .state
            .provider_registry
            .register_building(
                ProviderKey::new(
                    ProviderKind::Ollama.to_string(),
                    ProviderKind::DEFAULT_OLLAMA_BASE_URL,
                    RequestClass::Embedding,
                )
                .expect("embedding provider key"),
                &ProviderLimits {
                    max_in_flight: 1,
                    request_timeout: Duration::from_secs(5),
                },
            )
            .expect("register the shared embedding endpoint");

        // -- Profile A: the genesis active profile ----------------------------
        let profile_a = ensure_genesis_profile(&mut seed, "model-a", 768).await;
        handler.state.embedding_providers.insert(
            profile_a.id(),
            profile_provider("model-a", vec![1.0, 0.0, 0.0]),
        );

        let (resolved_a, response_a) = embed_active(&handler, "where is auth handled").await;
        assert_eq!(resolved_a.id(), profile_a.id());
        assert_eq!(resolved_a.model(), "model-a");
        assert_eq!(response_a.vector, vec![1.0, 0.0, 0.0]);

        // -- Cut over to profile B (higher epoch, now active) -----------------
        let profile_b_id = create_complete_profile(&mut seed, "model-b", 768).await;
        handler.state.embedding_providers.insert(
            profile_b_id,
            profile_provider("model-b", vec![0.0, 1.0, 0.0]),
        );

        let (resolved_b, response_b) = embed_active(&handler, "where is auth handled").await;
        assert_eq!(
            resolved_b.id(),
            profile_b_id,
            "after the cutover the read path must bind to the newly-active profile",
        );
        assert_eq!(resolved_b.model(), "model-b");
        assert_eq!(
            response_b.vector,
            vec![0.0, 1.0, 0.0],
            "the query must embed against the active profile's geometry, not the prior one",
        );
    }
}
