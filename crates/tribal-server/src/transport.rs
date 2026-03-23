//! Transport layer for the Tribal MCP server.
//!
//! Each transport module handles protocol-specific setup (binding,
//! middleware, auth resolution) and produces a running server that
//! integrates with the shared [`CancellationToken`] for graceful
//! shutdown.  Adding a new transport is additive — no changes to
//! handler code are required.

mod common;
mod http;
mod sse;
mod sse_lifecycle;
mod stdio;

pub(crate) use self::stdio::run_stdio_transport;
pub use self::{http::run_http_transport, sse::run_sse_transport};
