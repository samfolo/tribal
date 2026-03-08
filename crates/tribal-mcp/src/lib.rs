#![warn(clippy::pedantic)]
#![deny(warnings)]
//! MCP layer for Tribal: rmcp integration, tool handlers, session
//! state management, and transport setup (stdio, HTTP, SSE).

mod auth;
mod error;
mod handlers;
mod mapping;
mod server_handler;
mod session;
#[cfg(test)]
mod test_utils;
mod tools;

pub use error::{IntoCallToolResult, IntoMcpError, McpToolError};
pub use server_handler::{ConnectionRepositories, TribalServerHandler};
pub use session::{SessionActor, SessionContext, SessionProject};
