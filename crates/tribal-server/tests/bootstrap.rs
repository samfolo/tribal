//! Integration tests for `tribal bootstrap`, `tribal mcp-config`, and the
//! shared credentials.json persistence path.
//!
//! Each test owns an isolated database via `TestDb` and uses scoped
//! guards for its env-var manipulation (`XDG_CONFIG_HOME`, current
//! directory).

#[path = "bootstrap/common.rs"]
mod common;

#[path = "bootstrap/bootstrap_flow.rs"]
mod bootstrap_flow;
#[path = "bootstrap/credentials.rs"]
mod credentials;
#[path = "bootstrap/mcp_config.rs"]
mod mcp_config;
