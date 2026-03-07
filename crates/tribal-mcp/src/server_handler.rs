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

    fn list_resources_inner() -> ListResourcesResult {
        ListResourcesResult {
            resources: vec![session::session_resource()],
            ..Default::default()
        }
    }

    async fn read_resource_inner(&self, uri: &str) -> Result<ReadResourceResult, McpError> {
        if uri != SESSION_RESOURCE_URI {
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
        std::future::ready(Ok(Self::list_resources_inner()))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        self.read_resource_inner(&request.uri).await
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rmcp::{
        handler::server::ServerHandler,
        model::{ErrorCode, ResourceContents},
    };
    use tribal_domain::ProjectId;
    use tribal_test_utils::stub_repositories::{
        StubJobRepository, StubKnowledgeItemRepository, StubProjectRepository,
        StubRetrievalFeedbackRepository,
    };

    use super::*;
    use crate::session::{SESSION_RESOURCE_URI, SessionContext, SessionProject};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn test_repositories() -> ConnectionRepositories {
        ConnectionRepositories {
            knowledge: Arc::new(StubKnowledgeItemRepository),
            project: Arc::new(StubProjectRepository),
            job: Arc::new(StubJobRepository),
            feedback: Arc::new(StubRetrievalFeedbackRepository),
        }
    }

    fn test_handler() -> TribalServerHandler {
        let session = SessionContext::new(None, "user:test".into());
        TribalServerHandler::new(test_repositories(), session)
    }

    fn test_handler_with_project() -> TribalServerHandler {
        let project = SessionProject {
            id: ProjectId::new(),
            name: "tribal".into(),
            git_remote: "git@github.com:user/tribal.git".into(),
        };
        let session = SessionContext::new(Some(project), "user:test".into());
        TribalServerHandler::new(test_repositories(), session)
    }

    // -----------------------------------------------------------------------
    // get_info tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_info_advertises_resources() {
        let handler = test_handler();
        let info = handler.get_info();

        assert!(
            info.capabilities.resources.is_some(),
            "resources capability must be advertised",
        );
    }

    #[test]
    fn test_get_info_advertises_resource_subscription() {
        let handler = test_handler();
        let info = handler.get_info();
        let resources = info.capabilities.resources.expect("resources must be set");

        assert_eq!(
            resources.subscribe,
            Some(true),
            "resource subscription capability must be advertised",
        );
    }

    #[test]
    fn test_get_info_advertises_tools() {
        let handler = test_handler();
        let info = handler.get_info();

        assert!(
            info.capabilities.tools.is_some(),
            "tools capability must be advertised",
        );
    }

    #[test]
    fn test_get_info_server_identity() {
        let handler = test_handler();
        let info = handler.get_info();

        assert_eq!(info.server_info.name, SERVER_NAME);
        assert_eq!(info.server_info.version, VERSION);
    }

    // -----------------------------------------------------------------------
    // get_tool tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_tool_found() {
        let handler = test_handler();
        let tool = handler.get_tool("tribal_discover");

        assert!(tool.is_some());
        assert_eq!(tool.unwrap().name.as_ref(), "tribal_discover");
    }

    #[test]
    fn test_get_tool_not_found() {
        let handler = test_handler();
        assert!(handler.get_tool("nonexistent").is_none());
    }

    // -----------------------------------------------------------------------
    // list_resources tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_list_resources_returns_session() {
        let result = TribalServerHandler::list_resources_inner();

        assert_eq!(result.resources.len(), 1);
        let resource = &result.resources[0];
        assert_eq!(resource.uri, SESSION_RESOURCE_URI);
        assert_eq!(resource.name, "session_context");
        assert_eq!(resource.mime_type.as_deref(), Some("application/json"));
    }

    // -----------------------------------------------------------------------
    // read_resource tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_read_resource_success() {
        let handler = test_handler_with_project();
        let result = handler
            .read_resource_inner(SESSION_RESOURCE_URI)
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
        let handler = test_handler();
        let err = handler
            .read_resource_inner("tribal://unknown")
            .await
            .expect_err("unknown URI must return error");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    // -----------------------------------------------------------------------
    // subscribe tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_subscribe_success() {
        let handler = test_handler();
        assert!(!handler.session.read().await.subscribed);

        handler
            .subscribe_inner(SESSION_RESOURCE_URI)
            .await
            .expect("subscribe must succeed for known URI");

        assert!(handler.session.read().await.subscribed);
    }

    #[tokio::test]
    async fn test_subscribe_unknown_uri() {
        let handler = test_handler();
        let err = handler
            .subscribe_inner("tribal://unknown")
            .await
            .expect_err("unknown URI must return error");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    // -----------------------------------------------------------------------
    // unsubscribe tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_unsubscribe_success() {
        let handler = test_handler();
        handler.session.write().await.subscribed = true;

        handler
            .unsubscribe_inner(SESSION_RESOURCE_URI)
            .await
            .expect("unsubscribe must succeed for known URI");

        assert!(!handler.session.read().await.subscribed);
    }

    #[tokio::test]
    async fn test_unsubscribe_unknown_uri() {
        let handler = test_handler();
        let err = handler
            .unsubscribe_inner("tribal://unknown")
            .await
            .expect_err("unknown URI must return error");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }
}
