//! Outcome of a successful register run.

use serde_json::Value;
use tribal_domain::{GitRemote, ProjectId};

/// Result of [`super::register::run`] when registration completes
/// successfully.
///
/// Holds the resolved project identifiers, git remote, and the
/// JSON-shaped MCP server entry. The standalone `register` wrapper
/// discards this value after printing its own output; `bootstrap`
/// consumes it to compose its own polished presentation across the
/// setup + register pair.
#[derive(Debug)]
pub struct RegisterOutcome {
    /// Database id of the registered (or pre-existing) project.
    pub project_id: ProjectId,
    /// Human-friendly project name.
    pub project_name: String,
    /// Git remote recorded on the project row.
    pub git_remote: GitRemote,
    /// JSON-shaped MCP server entry (transport-specific shape).
    pub mcp_config: Value,
}
