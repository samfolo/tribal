//! Tag registry entity — canonical tag tracking.
//!
//! The tag registry is global (not per-project) and prevents tag drift
//! across models, versions, and prompts. Tags are normalised to lowercase
//! during ingest with embedding-based semantic matching against existing
//! entries.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

/// An entry in the global tag registry.
///
/// Each row represents a canonical tag. The database table is the registry;
/// each row is an entry. Tags are stored in canonical form (lowercase, trimmed).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
pub struct TagRegistryEntry {
    /// The canonical tag string (lowercase).
    tag: String,
    /// When this tag was first encountered.
    first_seen_at: DateTime<Utc>,
    /// Number of times this tag has been resolved to (exact or semantic match).
    #[builder(default = 0)]
    usage_count: i32,
}

impl TagRegistryEntry {
    /// Creates a new tag registry entry.
    pub fn new(tag: String, first_seen_at: DateTime<Utc>, usage_count: i32) -> Self {
        Self {
            tag,
            first_seen_at,
            usage_count,
        }
    }

    /// Returns the canonical tag string.
    pub fn tag(&self) -> &str {
        &self.tag
    }

    /// Returns when this tag was first encountered.
    pub fn first_seen_at(&self) -> DateTime<Utc> {
        self.first_seen_at
    }

    /// Returns the usage count for this tag.
    pub fn usage_count(&self) -> i32 {
        self.usage_count
    }
}
