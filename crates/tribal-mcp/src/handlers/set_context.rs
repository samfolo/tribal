use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use tribal_domain::McpErrorCode;

use crate::{
    error::{IntoCallToolResult, McpToolError},
    server_handler::TribalServerHandler,
};

#[allow(clippy::unused_async)]
impl TribalServerHandler {
    pub(crate) async fn handle_set_context(
        &self,
        _params: serde_json::Value,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        Ok(McpToolError {
            code: McpErrorCode::Internal,
            message: "tribal_set_context is not yet implemented".into(),
            details: serde_json::json!({}),
        }
        .into_call_tool_result())
    }
}
