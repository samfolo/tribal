#![warn(clippy::pedantic)]
#![deny(warnings)]
//! MCP layer for Tribal: rmcp integration, tool handlers, session
//! state management, and transport setup (stdio, HTTP, SSE).

mod app_state;
mod auth;
mod config;
mod error;
mod fingerprint;
mod format;
mod handlers;
mod mapping;
mod middleware;
mod polling;
mod server_handler;
mod session;
pub mod sweep;
#[cfg(test)]
mod test_utils;
mod tools;

pub use app_state::{AppState, ResolvedProject};
pub use auth::{
    AuthContext, AuthError, AuthenticatedPrincipal, Authenticator, DISPLAY_INVALID_TOKEN,
    DISPLAY_MISSING_TOKEN, DISPLAY_TOKEN_EXPIRED, DISPLAY_TOKEN_REVOKED, TransportAuthStrategy,
};
pub use config::{HandlerConfig, HandlerDiscoveryConfig, HandlerExplorationConfig};
pub use error::{IntoCallToolResult, IntoMcpError, McpToolError};
pub use middleware::{AuthMiddlewareState, require_bearer_auth};
pub use server_handler::{ActivePromptVersions, ConnectionRepositories, TribalServerHandler};
pub use session::{SessionActor, SessionContext, SessionProject};
#[cfg(feature = "test-helpers")]
pub use tools::tool_scope_registry;
