//! Project entity — the top-level organisational unit.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{GitRemote, ProjectId};

/// The source of a project's stable identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProjectOrigin {
    /// The graph-wide project maintained by Tribal.
    System,
    /// A project established from a Git remote.
    Git {
        /// Canonical remote identity.
        remote: GitRemote,
        /// Branch used when a caller supplies no branch.
        default_branch: String,
    },
}

/// A project in the Tribal knowledge graph.
///
/// The origin is immutable identity; the name and settings may change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
#[allow(clippy::struct_field_names)]
pub struct Project {
    /// Unique identifier with `proj_` prefix.
    id: ProjectId,
    /// Typed source of project identity.
    origin: ProjectOrigin,
    /// Human-friendly project name (mutable).
    name: String,
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

    /// Returns the persisted project origin.
    pub fn origin(&self) -> &ProjectOrigin {
        &self.origin
    }

    /// Returns the human-friendly name.
    pub fn name(&self) -> &str {
        &self.name
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
