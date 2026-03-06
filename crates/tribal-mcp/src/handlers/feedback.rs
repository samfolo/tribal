use rmcp::handler::server::RequestContext;
use rmcp::model::{CallToolResult, ErrorData as McpError};
use rmcp::service::RoleServer;
use tribal_domain::McpErrorCode;

use crate::error::{IntoCallToolResult, McpToolError};
use crate::server_handler::TribalServerHandler;

impl TribalServerHandler {
    pub(crate) async fn handle_feedback(
        &self,
        _params: serde_json::Value,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        Ok(McpToolError {
            code: McpErrorCode::Internal,
            message: "tribal_feedback is not yet implemented".into(),
            details: serde_json::json!({}),
        }
        .into_call_tool_result())
    }
}
