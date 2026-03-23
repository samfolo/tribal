//! Shared JSON-RPC request fixtures for transport tests.

/// Minimal MCP initialize request body.
///
/// Used by auth-rejection tests where the server never reaches the
/// handler — the params are intentionally incomplete.
pub const MINIMAL_INITIALIZE_BODY: &str =
    r#"{"jsonrpc":"2.0","method":"initialize","id":1,"params":{}}"#;

/// Full MCP initialize request body with valid protocol parameters.
///
/// Used by tests that need the handler to process the request
/// successfully and return a session.
pub const INITIALIZE_BODY: &str = r#"{"jsonrpc":"2.0","method":"initialize","id":1,"params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}"#;
