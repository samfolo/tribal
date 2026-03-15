//! Project entity — the top-level organisational unit.
//!
//! A project is identified by its `git_remote`, not by local filesystem
//! path. The same repository cloned on two machines resolves to the same
//! project.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{GitRemote, ProjectId};

/// A project in the Tribal knowledge graph.
///
/// `git_remote` is the stable identity in canonical form (e.g.
/// `github.com/user/tribal`). `name` is human-friendly and mutable.
/// `settings` is opaque JSONB for project-specific configuration,
/// versioned by `schema_version`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
#[allow(clippy::struct_field_names)]
pub struct Project {
    /// Unique identifier with `proj_` prefix.
    id: ProjectId,
    /// Stable identity — the git remote in canonical form.
    git_remote: GitRemote,
    /// Human-friendly project name (mutable).
    name: String,
    /// Default branch (e.g. `"main"`).
    default_branch: String,
    /// Loose project type hint (e.g. `"cli_tool"`, `"web_service"`).
    #[builder(default)]
    project_type: Option<String>,
    /// Schema version for settings migration.
    schema_version: u32,
    /// Project-specific configuration (opaque JSONB).
    settings: serde_json::Value,
    /// When this project was created.
    created_at: DateTime<Utc>,
    /// When this project was last updated.
    updated_at: DateTime<Utc>,
}

impl Project {
    /// Returns the project identifier.
    pub fn id(&self) -> ProjectId {
        self.id
    }

    /// Returns the git remote identity in canonical form.
    pub fn git_remote(&self) -> &GitRemote {
        &self.git_remote
    }

    /// Returns the human-friendly name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the default branch.
    pub fn default_branch(&self) -> &str {
        &self.default_branch
    }

    /// Returns the project type hint, if set.
    pub fn project_type(&self) -> Option<&str> {
        self.project_type.as_deref()
    }

    /// Returns the settings schema version.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Returns the project settings.
    pub fn settings(&self) -> &serde_json::Value {
        &self.settings
    }

    /// Returns when this project was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns when this project was last updated.
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}
