//! MCP (Model Context Protocol) wire DTOs — the request/response contract for
//! the `tribal_*` MCP tools. Domain → wire conversions (`From`/`from_domain`)
//! live alongside the types; the rmcp response glue stays in `tribal-mcp`.

pub mod feedback;
pub mod job;
pub mod knowledge;
pub mod reindex;
pub mod session;

pub use feedback::*;
pub use job::*;
pub use knowledge::*;
pub use reindex::*;
pub use session::*;
