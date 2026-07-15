//! Cross-command output building blocks.
//!
//! Modules here build values consumed by more than one CLI subcommand
//! (e.g. `tribal project register`, `tribal bootstrap`,
//! `tribal integration mcp-config`). Per-command presentation lives next to each
//! command in `commands/<name>/output.rs`.

mod mcp_config;

pub(crate) use mcp_config::{resolved_advertised_url, snippet_key};
