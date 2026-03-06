use rmcp::handler::server::RequestContext;
use rmcp::model::ErrorData as McpError;
use rmcp::service::RoleServer;

// ---------------------------------------------------------------------------
// AuthContext
// ---------------------------------------------------------------------------

/// Compatibility seam for scope-based auth.
///
/// Grants all scopes unconditionally.
pub struct AuthContext;

impl AuthContext {
    pub fn from_context(_context: &RequestContext<RoleServer>) -> Self {
        AuthContext
    }

    pub fn require_scope(&self, _scope: &str) -> Result<(), McpError> {
        Ok(())
    }
}
