#![warn(clippy::pedantic)]
#![deny(warnings)]
//! MCP layer for Tribal: rmcp integration, tool handlers, session
//! state management, and transport setup (stdio, HTTP, SSE).

mod auth;
mod error;
mod handlers;
mod mapping;
mod server_handler;

pub use error::{IntoCallToolResult, IntoMcpError, McpToolError};
pub use server_handler::{ConnectionRepositories, TribalServerHandler};
