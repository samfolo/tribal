#![warn(clippy::pedantic)]
#![deny(warnings)]
//! MCP layer for Tribal: rmcp integration, tool handlers, session
//! state management, and transport setup (stdio, HTTP, SSE).

mod app_state;
mod auth;
mod config;
mod error;
mod format;
mod handlers;
mod mapping;
mod polling;
mod server_handler;
mod session;
#[cfg(test)]
mod test_utils;
mod tools;

pub use app_state::{AppState, ResolvedProject};
pub use config::{HandlerConfig, HandlerDiscoveryConfig, HandlerExplorationConfig};
pub use error::{IntoCallToolResult, IntoMcpError, McpToolError};
pub use server_handler::{ActivePromptVersions, ConnectionRepositories, TribalServerHandler};
pub use session::{SessionActor, SessionContext, SessionProject};
