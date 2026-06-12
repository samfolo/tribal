//! Prompt version entity — content-addressed prompt versioning.
//!
//! Identical prompt content produces one row; changed content produces a
//! new version. Jobs reference the active prompt versions at creation time.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{PromptClass, PromptRole, PromptStage, PromptVersionId};

/// A content-addressed prompt version.
///
/// Prompt versions are immutable. The `content_hash` (SHA-256, hex-encoded)
/// together with `stage`, `class`, and `role` forms a unique constraint —
/// identical content for the same slot is deduplicated to one row.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
pub struct PromptVersion {
    /// Unique identifier with `pv_` prefix.
    id: PromptVersionId,
    /// Pipeline stage this prompt version applies to.
    stage: PromptStage,
    /// The executor surface this prompt version serves.
    class: PromptClass,
    /// Whether this is a system or user prompt.
    role: PromptRole,
    /// SHA-256 hash of the prompt content (hex-encoded, 64 chars).
    content_hash: String,
    /// The full prompt content.
    content: String,
    /// When this version was created.
    created_at: DateTime<Utc>,
}

impl PromptVersion {
    /// Returns the prompt version identifier.
    pub fn id(&self) -> PromptVersionId {
        self.id
    }

    /// Returns the pipeline stage.
    pub fn stage(&self) -> PromptStage {
        self.stage
    }

    /// Returns the executor surface this version serves.
    pub fn class(&self) -> PromptClass {
        self.class
    }

    /// Returns the prompt role.
    pub fn role(&self) -> PromptRole {
        self.role
    }

    /// Returns the content hash.
    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }

    /// Returns the full prompt content.
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Returns when this version was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
