//! Wire DTOs for the Tribal MCP contract — the request/response types exchanged
//! over the MCP/JSON-RPC surface. Shared by the server (`tribal-mcp`) and
//! clients (the eval harness, future SDKs) so the contract has one source of
//! truth; drift between producer and consumer becomes a compile error.
//!
//! These types are pure data: every type derives `Serialize` + `Deserialize`
//! and depends only on `tribal-domain`, `serde`, and `chrono` — never on the
//! server, rmcp, or the database. Domain → wire conversions (`From`/`from_domain`)
//! live here; the rmcp response glue stays in `tribal-mcp`.

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
