//! Reference entity — external pointers attached to knowledge items.
//!
//! References are pointers, not content stores. File paths use the
//! project-relative sigil (`//src/...`). The `description` field
//! preserves utility when the target is gone (e.g. git-ignored files).

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{KnowledgeItemId, ProjectId, ReferenceId, ReferenceKind};

/// An external reference attached to a knowledge item.
///
/// References can break — staleness is a signal, not an error.
/// The `description` captures what was at the location and why it
/// mattered, remaining useful even after the target disappears.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
pub struct Reference {
    /// Unique identifier with `ref_` prefix.
    id: ReferenceId,
    /// The knowledge item this reference is attached to.
    knowledge_item_id: KnowledgeItemId,
    /// Classification of the reference target.
    kind: ReferenceKind,
    /// The pointer value (e.g. `"//src/auth/middleware.rs"`, a URL).
    value: String,
    /// What was at the location and why it mattered.
    #[builder(default)]
    description: Option<String>,
    /// Which project context this reference belongs to.
    project_id: ProjectId,
    /// Git commit SHA when the reference was known valid.
    #[builder(default)]
    commit: Option<String>,
    /// Branch name (informational only).
    #[builder(default)]
    branch: Option<String>,
}

impl Reference {
    /// Returns the reference identifier.
    pub fn id(&self) -> ReferenceId {
        self.id
    }

    /// Returns the knowledge item this reference is attached to.
    pub fn knowledge_item_id(&self) -> KnowledgeItemId {
        self.knowledge_item_id
    }

    /// Returns the reference kind.
    pub fn kind(&self) -> ReferenceKind {
        self.kind
    }

    /// Returns the pointer value.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Returns the description, if set.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the project identifier.
    pub fn project_id(&self) -> ProjectId {
        self.project_id
    }

    /// Returns the commit SHA, if set.
    pub fn commit(&self) -> Option<&str> {
        self.commit.as_deref()
    }

    /// Returns the branch name, if set.
    pub fn branch(&self) -> Option<&str> {
        self.branch.as_deref()
    }
}
