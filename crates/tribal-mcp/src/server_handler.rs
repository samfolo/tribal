use std::sync::Arc;

use rmcp::{
    handler::server::ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, ErrorData as McpError, Implementation,
        ListResourcesResult, ListToolsResult, PaginatedRequestParams, ReadResourceRequestParams,
        ReadResourceResult, ResourceContents, ServerCapabilities, ServerInfo,
        SubscribeRequestParams, Tool, UnsubscribeRequestParams,
    },
    service::{RequestContext, RoleServer},
};
use tokio::sync::RwLock;
use tribal_db::{
    JobRepository, KnowledgeItemRepository, ProjectRepository, RetrievalFeedbackRepository,
};

use crate::{
    auth::AuthContext,
    error::method_not_found,
    session::{self, SESSION_RESOURCE_URI, SessionContext},
    tools::{PARSED_TOOLS, to_tool},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERVER_NAME: &str = "tribal";
const VERSION: &str = env!("CARGO_PKG_VERSION");

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
];

// ---------------------------------------------------------------------------
// ConnectionRepositories
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub struct ConnectionRepositories {
    pub(crate) knowledge: Arc<dyn KnowledgeItemRepository + Send + Sync>,
    pub(crate) project: Arc<dyn ProjectRepository + Send + Sync>,
    pub(crate) job: Arc<dyn JobRepository + Send + Sync>,
    pub(crate) feedback: Arc<dyn RetrievalFeedbackRepository + Send + Sync>,
}

// ---------------------------------------------------------------------------
// TribalServerHandler
// ---------------------------------------------------------------------------

pub struct TribalServerHandler {
    #[allow(dead_code)]
    repositories: ConnectionRepositories,
    pub(crate) session: Arc<RwLock<SessionContext>>,
}

impl TribalServerHandler {
    #[must_use]
    pub fn new(repositories: ConnectionRepositories, session: SessionContext) -> Self {
        Self {
            repositories,
            session: Arc::new(RwLock::new(session)),
        }
    }
}

// ---------------------------------------------------------------------------
// ServerHandler impl
// ---------------------------------------------------------------------------

impl ServerHandler for TribalServerHandler {
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
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        std::future::ready(Ok(ListToolsResult {
            tools: PARSED_TOOLS.iter().map(to_tool).collect(),
            ..Default::default()
        }))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        PARSED_TOOLS.iter().find(|t| t.name == name).map(to_tool)
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourcesResult, McpError>> + Send + '_ {
        std::future::ready(Ok(ListResourcesResult {
            resources: vec![session::session_resource()],
            ..Default::default()
        }))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        if request.uri != SESSION_RESOURCE_URI {
            return Err(McpError::invalid_params("unknown resource URI", None));
        }

        let json: serde_json::Value = {
            let session = self.session.read().await;
            (&*session).into()
        };

        let text =
            serde_json::to_string(&json).expect("session context serialisation must not fail");

        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(text, SESSION_RESOURCE_URI).with_mime_type("application/json"),
        ]))
    }

    async fn subscribe(
        &self,
        request: SubscribeRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), McpError> {
        if request.uri != SESSION_RESOURCE_URI {
            return Err(McpError::invalid_params("unknown resource URI", None));
        }

        self.session.write().await.subscribed = true;
        Ok(())
    }

    async fn unsubscribe(
        &self,
        request: UnsubscribeRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), McpError> {
        if request.uri != SESSION_RESOURCE_URI {
            return Err(McpError::invalid_params("unknown resource URI", None));
        }

        self.session.write().await.subscribed = false;
        Ok(())
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let entry = PARSED_TOOLS
            .iter()
            .find(|t| t.name == request.name.as_ref())
            .ok_or_else(|| method_not_found(&request.name))?;

        let auth = AuthContext::from_context(&context);
        auth.require_scope(entry.required_scope)?;

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
            _ => Err(method_not_found(&request.name)),
        }
    }
}
