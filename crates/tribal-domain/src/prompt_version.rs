//! Prompt version entity — content-addressed prompt versioning.
//!
//! Identical prompt content produces one row; changed content produces a
//! new version. Jobs reference the active prompt versions at creation time.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::PromptVersionId;

/// A content-addressed prompt version.
///
/// Prompt versions are immutable. The `content_hash` (SHA-256, hex-encoded)
/// together with `stage` forms a unique constraint — identical content for
/// the same stage is deduplicated to one row.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
pub struct PromptVersion {
    /// Unique identifier with `pv_` prefix.
    id: PromptVersionId,
    /// Pipeline stage: `"extraction"`, `"triage"`, or `"relation"`.
    stage: String,
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
    pub fn stage(&self) -> &str {
        &self.stage
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
