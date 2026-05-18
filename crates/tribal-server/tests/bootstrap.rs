//! Integration tests for `tribal bootstrap`, `tribal mcp-config`, and the
//! shared credentials.json persistence path.
//!
//! Each test holds `serial_lock` for the duration of its env-var
//! manipulation (XDG_CONFIG_HOME, current directory) and uses a fresh
//! testcontainer-backed pool drained at the start.

#[path = "bootstrap/common.rs"]
mod common;

#[path = "bootstrap/bootstrap_flow.rs"]
mod bootstrap_flow;
#[path = "bootstrap/credentials.rs"]
mod credentials;
#[path = "bootstrap/mcp_config.rs"]
mod mcp_config;
