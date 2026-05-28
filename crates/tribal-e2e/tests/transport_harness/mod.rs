// mod.rs is required here — cargo discovers top-level files in tests/ as
// separate binaries, so transport_harness.rs would conflict with the
// transport_harness/ directory.
//
// Shared helpers for HTTP and SSE transport integration tests.
// Each test file in tests/ is a separate binary that compiles this
// module independently.  Not every binary uses every export, so Rust
// warns about the unused items in each compilation.  The allows are
// unavoidable for shared test modules under this compilation model.
#![allow(dead_code, unused_imports)]

mod duration;
mod fixtures;
mod mcp_client;
mod state;
mod transport;

pub use duration::LIFECYCLE_FAR_FUTURE_MS;
pub use fixtures::{INITIALIZE_BODY, MINIMAL_INITIALIZE_BODY};
pub use mcp_client::{McpTestClient, assert_tool_visibility};
pub use state::{TEST_CANONICAL_RESOURCE, fresh_pool, seed_auth, seed_scoped_auth, test_app_state};
pub use transport::{TransportHandle, spawn_transport, test_client};
