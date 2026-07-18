use std::{collections::HashMap, sync::Arc};

use rmcp::{
    handler::server::ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, ErrorCode, ErrorData as McpError, Implementation,
        InitializeRequestParams, InitializeResult, ListResourcesResult, ListToolsResult,
        PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResult, ResourceContents,
        ServerCapabilities, ServerInfo, SubscribeRequestParams, Tool, UnsubscribeRequestParams,
    },
    service::{RequestContext, RoleServer},
};
use tokio::sync::RwLock;
use tribal_auth::{AuthenticatedPrincipal, TransportAuthStrategy};
use tribal_db::{
    JobRepository, KnowledgeItemRepository, PgJobRepository, PgKnowledgeItemRepository,
    PgPrincipalRepository, PgProjectRepository, PgPromptVersionRepository, PgReferenceRepository,
    PgRelationRepository, PgRetrievalFeedbackRepository, PgStandingRepository,
    PgSystemFingerprintRepository, PgTaskRepository, PgTriageResultRepository, PrincipalRepository,
    ProjectRepository, PromptVersionRepository, ReferenceRepository, RelationRepository,
    RetrievalFeedbackRepository, StandingRepository, SystemFingerprintRepository, TaskRepository,
    TriageResultRepository,
};
use tribal_domain::{
    IngestChannel, PromptClass, PromptRole, PromptStage, PromptVersionId, is_authorised,
};

use crate::{
    app_state::AppState,
    config::HandlerConfig,
    error::method_not_found,
    mapping::session_to_json,
    session::{self, SESSION_RESOURCE_URI, SessionContext},
    tools::{PARSED_TOOLS, to_tool},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERVER_NAME: &str = "tribal";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const MISSING_PRINCIPAL_CONTEXT: &str = "missing principal context";

/// Tool names with explicit `call_tool` match arms.
#[cfg(test)]
pub(crate) const DISPATCHED_TOOLS: &[&str] = &[
    "tribal_set_context",
    "tribal_ingest",
    "tribal_discover",
    "tribal_explore",
    "tribal_get_item",
    "tribal_feedback",
    "tribal_job_status",
    "tribal_reindex",
    "tribal_reindex_cancel",
    "tribal_reindex_prune",
];

// ---------------------------------------------------------------------------
// ConnectionRepositories
// ---------------------------------------------------------------------------

/// Repository trait objects shared across MCP tool handlers.
///
/// Each field holds a trait-object-wrapped repository implementation.
pub struct ConnectionRepositories {
    pub(crate) knowledge_item: Arc<dyn KnowledgeItemRepository + Send + Sync>,
    pub(crate) project: Arc<dyn ProjectRepository + Send + Sync>,
    pub(crate) job: Arc<dyn JobRepository + Send + Sync>,
    pub(crate) task: Arc<dyn TaskRepository + Send + Sync>,
    pub(crate) retrieval_feedback: Arc<dyn RetrievalFeedbackRepository + Send + Sync>,
    pub(crate) standing: Arc<dyn StandingRepository + Send + Sync>,
    pub(crate) reference: Arc<dyn ReferenceRepository + Send + Sync>,
    pub(crate) relation: Arc<dyn RelationRepository + Send + Sync>,
    pub(crate) principal: Arc<dyn PrincipalRepository + Send + Sync>,
    pub(crate) prompt_version: Arc<dyn PromptVersionRepository + Send + Sync>,
    pub(crate) system_fingerprint: Arc<dyn SystemFingerprintRepository + Send + Sync>,
    pub(crate) triage_result: Arc<dyn TriageResultRepository + Send + Sync>,
}

impl Default for ConnectionRepositories {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionRepositories {
    /// Constructs real database-backed repositories for all MCP handlers.
    #[must_use]
    pub fn new() -> Self {
        Self {
            knowledge_item: Arc::new(PgKnowledgeItemRepository),
            project: Arc::new(PgProjectRepository),
            job: Arc::new(PgJobRepository),
            task: Arc::new(PgTaskRepository),
            retrieval_feedback: Arc::new(PgRetrievalFeedbackRepository),
            standing: Arc::new(PgStandingRepository),
            reference: Arc::new(PgReferenceRepository),
            relation: Arc::new(PgRelationRepository),
            principal: Arc::new(PgPrincipalRepository),
            prompt_version: Arc::new(PgPromptVersionRepository),
            system_fingerprint: Arc::new(PgSystemFingerprintRepository),
            triage_result: Arc::new(PgTriageResultRepository),
        }
    }
}

// ---------------------------------------------------------------------------
// ActivePromptVersions
// ---------------------------------------------------------------------------

/// Active prompt version IDs used when creating new jobs.
///
/// Each field corresponds to a prompt version ID on [`tribal_db::NewJob`].
/// At startup, the factory closure populates this from the database and
/// passes it into [`TribalServerHandler::new`].
#[derive(Clone, Debug)]
// All fields share the `_prompt_version_id` suffix by convention, matching
// the column names on the `jobs` table.
#[allow(clippy::struct_field_names)]
pub struct ActivePromptVersions {
    pub(crate) extraction_system_prompt_version_id: PromptVersionId,
    pub(crate) extraction_user_prompt_version_id: PromptVersionId,
    pub(crate) triage_system_prompt_version_id: PromptVersionId,
    pub(crate) triage_user_prompt_version_id: PromptVersionId,
    pub(crate) relation_system_prompt_version_id: PromptVersionId,
    pub(crate) relation_user_prompt_version_id: PromptVersionId,
    /// Versions for the non-one-shot classes, keyed by slot. The six
    /// named fields mirror the jobs table's prompt columns; classes the
    /// schema does not reference ride here additively.
    pub(crate) agentic: HashMap<(PromptStage, PromptClass, PromptRole), PromptVersionId>,
}

/// The worker-facing view of the shared active prompt set: the same
/// `Arc<RwLock<…>>` the hot-reload watcher mutates, behind the worker's
/// claim-time resolution trait, so no second prompt source exists.
pub struct SharedActivePrompts(pub Arc<RwLock<ActivePromptVersions>>);

#[async_trait::async_trait]
impl tribal_worker::ActiveAgenticPrompts for SharedActivePrompts {
    async fn version_id(
        &self,
        stage: PromptStage,
        class: PromptClass,
        role: PromptRole,
    ) -> Option<PromptVersionId> {
        self.0.read().await.get_version(stage, class, role)
    }
}

impl ActivePromptVersions {
    /// Creates a new set of active prompt version IDs.
    #[must_use]
    // Six prompt version IDs (3 stages × 2 roles) cannot be reduced further.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        extraction_system: PromptVersionId,
        extraction_user: PromptVersionId,
        triage_system: PromptVersionId,
        triage_user: PromptVersionId,
        relation_system: PromptVersionId,
        relation_user: PromptVersionId,
    ) -> Self {
        Self {
            extraction_system_prompt_version_id: extraction_system,
            extraction_user_prompt_version_id: extraction_user,
            triage_system_prompt_version_id: triage_system,
            triage_user_prompt_version_id: triage_user,
            relation_system_prompt_version_id: relation_system,
            relation_user_prompt_version_id: relation_user,
            agentic: HashMap::new(),
        }
    }

    /// Updates the active version ID for a single slot.
    ///
    /// Exclusive access is enforced by the `&mut self` receiver — in
    /// practice this means the caller holds the `RwLock` write guard.
    pub fn set_version(
        &mut self,
        stage: PromptStage,
        class: PromptClass,
        role: PromptRole,
        id: PromptVersionId,
    ) {
        if class != PromptClass::OneShot {
            self.agentic.insert((stage, class, role), id);
            return;
        }
        match (stage, role) {
            (PromptStage::Extraction, PromptRole::System) => {
                self.extraction_system_prompt_version_id = id;
            }
            (PromptStage::Extraction, PromptRole::User) => {
                self.extraction_user_prompt_version_id = id;
            }
            (PromptStage::Triage, PromptRole::System) => {
                self.triage_system_prompt_version_id = id;
            }
            (PromptStage::Triage, PromptRole::User) => {
                self.triage_user_prompt_version_id = id;
            }
            (PromptStage::Relation, PromptRole::System) => {
                self.relation_system_prompt_version_id = id;
            }
            (PromptStage::Relation, PromptRole::User) => {
                self.relation_user_prompt_version_id = id;
            }
        }
    }

    /// Returns all version IDs in canonical order, matching the
    /// `SystemFingerprint` struct field declaration order.
    #[must_use]
    pub fn version_ids(&self) -> [PromptVersionId; 6] {
        [
            self.extraction_system_prompt_version_id,
            self.extraction_user_prompt_version_id,
            self.triage_system_prompt_version_id,
            self.triage_user_prompt_version_id,
            self.relation_system_prompt_version_id,
            self.relation_user_prompt_version_id,
        ]
    }

    /// Returns the active version ID for a slot, `None` for a slot no
    /// loader has populated.
    #[must_use]
    pub fn get_version(
        &self,
        stage: PromptStage,
        class: PromptClass,
        role: PromptRole,
    ) -> Option<PromptVersionId> {
        if class != PromptClass::OneShot {
            return self.agentic.get(&(stage, class, role)).copied();
        }
        Some(match (stage, role) {
            (PromptStage::Extraction, PromptRole::System) => {
                self.extraction_system_prompt_version_id
            }
            (PromptStage::Extraction, PromptRole::User) => self.extraction_user_prompt_version_id,
            (PromptStage::Triage, PromptRole::System) => self.triage_system_prompt_version_id,
            (PromptStage::Triage, PromptRole::User) => self.triage_user_prompt_version_id,
            (PromptStage::Relation, PromptRole::System) => self.relation_system_prompt_version_id,
            (PromptStage::Relation, PromptRole::User) => self.relation_user_prompt_version_id,
        })
    }
}

// ---------------------------------------------------------------------------
// TribalServerHandler
// ---------------------------------------------------------------------------

/// MCP server handler for the Tribal knowledge system.
///
/// Implements the [`ServerHandler`] trait from `rmcp`, dispatching tool
/// calls to individual handler methods and managing the per-connection
/// session state.
///
/// Wraps [`AppState`] (shared process-level state) alongside per-connection
/// fields (`repositories`, `session`, `config`).
pub struct TribalServerHandler {
    pub(crate) state: Arc<AppState>,
    pub(crate) auth_strategy: TransportAuthStrategy,
    pub(crate) repositories: ConnectionRepositories,
    pub(crate) session: Arc<RwLock<SessionContext>>,
    pub(crate) config: HandlerConfig,
    /// The transport this connection arrived over; also the source of the
    /// span-attribute transport label.
    pub(crate) channel: IngestChannel,
}

impl TribalServerHandler {
    /// Creates a new handler wrapping the shared application state plus
    /// per-connection state.
    ///
    /// The session is wrapped in an `Arc<RwLock<…>>` internally.
    #[must_use]
    pub fn new(
        state: Arc<AppState>,
        auth_strategy: TransportAuthStrategy,
        repositories: ConnectionRepositories,
        session: SessionContext,
        config: HandlerConfig,
        channel: IngestChannel,
    ) -> Self {
        Self {
            state,
            auth_strategy,
            repositories,
            session: Arc::new(RwLock::new(session)),
            config,
            channel,
        }
    }

    /// The transport label recorded on request spans.
    pub(crate) fn transport_name(&self) -> &'static str {
        match self.channel {
            IngestChannel::McpHttp => "http",
            IngestChannel::McpSse => "sse",
            IngestChannel::McpStdio => "stdio",
        }
    }

    /// Resolves the authenticated principal for the current request.
    ///
    /// Checks request context extensions first (per-request transports
    /// such as HTTP inject the principal via middleware), then falls back
    /// to handler-level auth (per-connection transports such as stdio
    /// resolve the principal once at handler creation).
    ///
    /// Adding a new transport requires no changes here — it either
    /// injects into request extensions or passes
    /// [`TransportAuthStrategy::AtCreation`] at construction time.
    pub(crate) fn resolve_principal<'a>(
        &'a self,
        context: &'a RequestContext<RoleServer>,
    ) -> Result<&'a AuthenticatedPrincipal, McpError> {
        // Per-request path: principal injected into http::request::Parts
        // extensions by the transport middleware, then propagated into
        // rmcp Extensions by StreamableHttpService.
        if let Some(parts) = context.extensions.get::<http::request::Parts>()
            && let Some(principal) = parts.extensions.get::<AuthenticatedPrincipal>()
        {
            return Ok(principal);
        }

        // Per-connection path: principal resolved at handler creation.
        if let TransportAuthStrategy::AtCreation(auth) = &self.auth_strategy {
            return Ok(auth.principal());
        }

        // Defence-in-depth: no principal from either source.
        Err(McpError::new(
            ErrorCode::INTERNAL_ERROR,
            MISSING_PRINCIPAL_CONTEXT,
            None,
        ))
    }

    fn list_resources_inner() -> ListResourcesResult {
        ListResourcesResult {
            resources: vec![session::session_resource()],
            ..Default::default()
        }
    }

    async fn read_resource_inner(
        &self,
        uri: &str,
        principal: &AuthenticatedPrincipal,
    ) -> Result<ReadResourceResult, McpError> {
        if uri != SESSION_RESOURCE_URI {
            return Err(McpError::invalid_params("unknown resource URI", None));
        }

        let json = {
            let session = self.session.read().await;
            session_to_json(&session, principal.principal_key())
        };

        let text =
            serde_json::to_string(&json).expect("session context serialisation must not fail");

        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(text, SESSION_RESOURCE_URI).with_mime_type("application/json"),
        ]))
    }

    async fn subscribe_inner(&self, uri: &str) -> Result<(), McpError> {
        if uri != SESSION_RESOURCE_URI {
            return Err(McpError::invalid_params("unknown resource URI", None));
        }

        self.session.write().await.subscribed = true;
        Ok(())
    }

    async fn unsubscribe_inner(&self, uri: &str) -> Result<(), McpError> {
        if uri != SESSION_RESOURCE_URI {
            return Err(McpError::invalid_params("unknown resource URI", None));
        }

        self.session.write().await.subscribed = false;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ServerHandler impl
// ---------------------------------------------------------------------------

impl ServerHandler for TribalServerHandler {
    /// Captures the client's declared identity as the session's client
    /// claim before delegating to the protocol's default behaviour.
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        {
            let mut guard = self.session.write().await;
            guard.actor.client_name = Some(request.client_info.name.clone());
            guard.actor.client_version = Some(request.client_info.version.clone());
        }
        if context.peer.peer_info().is_none() {
            context.peer.set_peer_info(request);
        }
        Ok(self.get_info())
    }

    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_resources_subscribe()
                .build(),
        )
        .with_server_info(Implementation::new(SERVER_NAME, VERSION))
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        let principal = self.resolve_principal(&context);
        std::future::ready(match principal {
            Ok(p) => {
                let scopes = p.scopes();
                Ok(ListToolsResult {
                    tools: PARSED_TOOLS
                        .iter()
                        .filter(|t| is_authorised(scopes, &t.required_scope))
                        .map(to_tool)
                        .collect(),
                    ..Default::default()
                })
            }
            Err(e) => Err(e),
        })
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        // get_tool receives no RequestContext (rmcp trait limitation), so
        // per-request scope filtering is impossible here.  For
        // per-connection transports, filter by scopes; for per-request
        // transports, return the tool unconditionally.  This exposes tool
        // metadata but not tool execution — call_tool is the authoritative
        // enforcement point with full scope checking.  Returning None
        // would break tool discovery for HTTP clients.
        match &self.auth_strategy {
            TransportAuthStrategy::AtCreation(auth) => {
                let scopes = auth.principal().scopes();
                PARSED_TOOLS
                    .iter()
                    .find(|t| t.name == name && is_authorised(scopes, &t.required_scope))
                    .map(to_tool)
            }
            TransportAuthStrategy::PerRequest => {
                PARSED_TOOLS.iter().find(|t| t.name == name).map(to_tool)
            }
        }
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourcesResult, McpError>> + Send + '_ {
        std::future::ready(Ok(Self::list_resources_inner()))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let principal = self.resolve_principal(&context)?;
        self.read_resource_inner(&request.uri, principal).await
    }

    async fn subscribe(
        &self,
        request: SubscribeRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), McpError> {
        self.subscribe_inner(&request.uri).await
    }

    async fn unsubscribe(
        &self,
        request: UnsubscribeRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), McpError> {
        self.unsubscribe_inner(&request.uri).await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let principal = self.resolve_principal(&context)?;
        let scopes = principal.scopes();
        let entry = PARSED_TOOLS
            .iter()
            .find(|t| t.name == request.name.as_ref() && is_authorised(scopes, &t.required_scope))
            .ok_or_else(|| method_not_found(&request.name))?;

        let params = request.arguments.map_or_else(
            || serde_json::Value::Object(serde_json::Map::default()),
            serde_json::Value::Object,
        );

        match entry.name {
            "tribal_set_context" => self.handle_set_context(params, context).await,
            "tribal_ingest" => self.handle_ingest(params, context).await,
            "tribal_discover" => self.handle_discover(params, context).await,
            "tribal_explore" => self.handle_explore(params, context).await,
            "tribal_get_item" => self.handle_get_item(params, context).await,
            "tribal_feedback" => self.handle_feedback(params, context).await,
            "tribal_job_status" => self.handle_job_status(params, context).await,
            "tribal_reindex" => self.handle_reindex(params, context).await,
            "tribal_reindex_cancel" => self.handle_reindex_cancel(params, context).await,
            "tribal_reindex_prune" => self.handle_reindex_prune(params, context).await,
            _ => Err(method_not_found(&request.name)),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use rmcp::{
        handler::server::ServerHandler,
        model::{ErrorCode, Extensions as RmcpExtensions, ResourceContents},
    };
    use tribal_auth::{AuthenticatedPrincipal, TransportAuthStrategy};
    use tribal_domain::PrincipalId;
    use tribal_test_utils::TEST_PRINCIPAL_KEY;

    use super::*;
    use crate::{
        session::SESSION_RESOURCE_URI,
        test_utils::{TestHandler, session_with_project, test_request_context},
    };

    fn test_principal() -> AuthenticatedPrincipal {
        AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            TEST_PRINCIPAL_KEY,
            tribal_domain::full_access_scopes(),
        )
    }

    // -- get_info -----------------------------------------------------------

    #[tokio::test]
    async fn test_get_info_advertises_resources() {
        let handler = TestHandler::builder().build();
        let info = handler.get_info();

        assert!(
            info.capabilities.resources.is_some(),
            "resources capability must be advertised",
        );
    }

    #[tokio::test]
    async fn test_get_info_advertises_resource_subscription() {
        let handler = TestHandler::builder().build();
        let info = handler.get_info();
        let resources = info.capabilities.resources.expect("resources must be set");

        assert_eq!(
            resources.subscribe,
            Some(true),
            "resource subscription capability must be advertised",
        );
    }

    #[tokio::test]
    async fn test_get_info_advertises_tools() {
        let handler = TestHandler::builder().build();
        let info = handler.get_info();

        assert!(
            info.capabilities.tools.is_some(),
            "tools capability must be advertised",
        );
    }

    #[tokio::test]
    async fn test_get_info_server_identity() {
        let handler = TestHandler::builder().build();
        let info = handler.get_info();

        assert_eq!(info.server_info.name, SERVER_NAME);
        assert_eq!(info.server_info.version, VERSION);
    }

    // -- get_tool -----------------------------------------------------------

    #[tokio::test]
    async fn test_get_tool_found() {
        let handler = TestHandler::builder().build();
        let tool = handler.get_tool("tribal_discover");

        assert!(tool.is_some());
        assert_eq!(tool.unwrap().name.as_ref(), "tribal_discover");
    }

    #[tokio::test]
    async fn test_get_tool_not_found() {
        let handler = TestHandler::builder().build();
        assert!(handler.get_tool("nonexistent").is_none());
    }

    // -- list_resources -----------------------------------------------------

    #[test]
    fn test_list_resources_returns_session() {
        let result = TribalServerHandler::list_resources_inner();

        assert_eq!(result.resources.len(), 1);
        let resource = &result.resources[0];
        assert_eq!(resource.uri, SESSION_RESOURCE_URI);
        assert_eq!(resource.name, "session_context");
        assert_eq!(resource.mime_type.as_deref(), Some("application/json"));
    }

    // -- read_resource ------------------------------------------------------

    #[tokio::test]
    async fn test_read_resource_success() {
        let handler = TestHandler::builder()
            .session(session_with_project())
            .build();
        let principal = test_principal();
        let result = handler
            .read_resource_inner(SESSION_RESOURCE_URI, &principal)
            .await
            .expect("read_resource must succeed for known URI");

        assert_eq!(result.contents.len(), 1);

        let ResourceContents::TextResourceContents {
            mime_type, text, ..
        } = &result.contents[0]
        else {
            panic!("expected text resource content");
        };

        assert_eq!(mime_type.as_deref(), Some("application/json"));

        let json: serde_json::Value =
            serde_json::from_str(text).expect("content must be valid JSON");

        assert!(json["project"].is_object());
        assert_eq!(json["principal_key"], "user:test");
        assert_eq!(json["project"]["name"], "tribal");
    }

    #[tokio::test]
    async fn test_read_resource_unknown_uri() {
        let handler = TestHandler::builder().build();
        let principal = test_principal();
        let err = handler
            .read_resource_inner("tribal://unknown", &principal)
            .await
            .expect_err("unknown URI must return error");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    // -- subscribe ----------------------------------------------------------

    #[tokio::test]
    async fn test_subscribe_success() {
        let handler = TestHandler::builder().build();
        assert!(!handler.session.read().await.subscribed);

        handler
            .subscribe_inner(SESSION_RESOURCE_URI)
            .await
            .expect("subscribe must succeed for known URI");

        assert!(handler.session.read().await.subscribed);
    }

    #[tokio::test]
    async fn test_subscribe_unknown_uri() {
        let handler = TestHandler::builder().build();
        let err = handler
            .subscribe_inner("tribal://unknown")
            .await
            .expect_err("unknown URI must return error");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    // -- unsubscribe --------------------------------------------------------

    #[tokio::test]
    async fn test_unsubscribe_success() {
        let handler = TestHandler::builder().build();
        handler.session.write().await.subscribed = true;

        handler
            .unsubscribe_inner(SESSION_RESOURCE_URI)
            .await
            .expect("unsubscribe must succeed for known URI");

        assert!(!handler.session.read().await.subscribed);
    }

    #[tokio::test]
    async fn test_unsubscribe_unknown_uri() {
        let handler = TestHandler::builder().build();
        let err = handler
            .unsubscribe_inner("tribal://unknown")
            .await
            .expect_err("unknown URI must return error");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    // -- resolve_principal --------------------------------------------------

    #[tokio::test]
    async fn test_resolve_principal_from_auth_context() {
        let handler = TestHandler::builder().build();
        let context = test_request_context(RmcpExtensions::new());

        let principal = handler
            .resolve_principal(&context)
            .expect("resolve_principal must succeed with AtCreation strategy");

        assert_eq!(principal.principal_key(), TEST_PRINCIPAL_KEY);
    }

    #[tokio::test]
    async fn test_resolve_principal_from_http_extensions() {
        let mut handler = TestHandler::builder().build();
        handler.auth_strategy = TransportAuthStrategy::PerRequest;

        let principal = AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            "user:http-test",
            tribal_domain::full_access_scopes(),
        );
        let (mut parts, ()) = http::Request::builder().body(()).unwrap().into_parts();
        parts.extensions.insert(principal);

        let mut extensions = RmcpExtensions::new();
        extensions.insert(parts);

        let context = test_request_context(extensions);

        let resolved = handler
            .resolve_principal(&context)
            .expect("resolve_principal must succeed with HTTP extensions");

        assert_eq!(resolved.principal_key(), "user:http-test");
    }

    #[tokio::test]
    async fn test_resolve_principal_missing_both_returns_internal_error() {
        let mut handler = TestHandler::builder().build();
        handler.auth_strategy = TransportAuthStrategy::PerRequest;

        let context = test_request_context(RmcpExtensions::new());

        let err = handler
            .resolve_principal(&context)
            .expect_err("resolve_principal must fail when neither source provides a principal");

        assert!(matches!(err.code, ErrorCode::INTERNAL_ERROR));
        assert!(
            err.message.contains(MISSING_PRINCIPAL_CONTEXT),
            "error message must contain the expected constant",
        );
    }

    // -- set_version --------------------------------------------------------

    #[test]
    fn test_set_version_updates_correct_field() {
        let original = PromptVersionId::new();
        let mut versions =
            ActivePromptVersions::new(original, original, original, original, original, original);

        let new_id = PromptVersionId::new();
        versions.set_version(
            PromptStage::Triage,
            PromptClass::OneShot,
            PromptRole::User,
            new_id,
        );

        assert_eq!(versions.extraction_system_prompt_version_id, original);
        assert_eq!(versions.extraction_user_prompt_version_id, original);
        assert_eq!(versions.triage_system_prompt_version_id, original);
        assert_eq!(versions.triage_user_prompt_version_id, new_id);
        assert_eq!(versions.relation_system_prompt_version_id, original);
        assert_eq!(versions.relation_user_prompt_version_id, original);
    }
}
