//! Outcome of a successful register run.

use serde_json::Value;
use tribal_domain::{GitRemote, ProjectId};

/// Resolved project fields shared by every [`RegisterOutcome`] variant.
#[derive(Debug)]
pub(crate) struct RegisteredProject {
    /// Database id of the registered project.
    pub project_id: ProjectId,
    /// Human-friendly project name.
    pub project_name: String,
    /// Git remote recorded on the project row.
    pub git_remote: GitRemote,
    /// JSON-shaped MCP server entry (transport-specific shape).
    pub mcp_config: Value,
}

/// Result of [`super::register::compute`].
#[derive(Debug)]
pub(crate) enum RegisterOutcome {
    /// The project row was just inserted by this run.
    Registered(RegisteredProject),
    /// A project for this git remote already existed and was returned
    /// via `find_by_git_remote`.
    AlreadyExists(RegisteredProject),
}

impl RegisterOutcome {
    /// Returns the resolved [`RegisteredProject`] regardless of variant.
    pub(crate) fn project(&self) -> &RegisteredProject {
        match self {
            Self::Registered(p) | Self::AlreadyExists(p) => p,
        }
    }

    /// Consumes the outcome, returning the inner [`RegisteredProject`].
    pub(crate) fn into_project(self) -> RegisteredProject {
        match self {
            Self::Registered(p) | Self::AlreadyExists(p) => p,
        }
    }
}
