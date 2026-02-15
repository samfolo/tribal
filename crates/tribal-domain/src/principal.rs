//! Principal entity — user or agent identity.
//!
//! Every write operation is attributed to a principal. The `principal_key`
//! is a human-readable identifier (e.g. `"user:sam"`, `"principal:local"`)
//! exposed via MCP and used in logs. The UUID `id` is used for all FK
//! relationships.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::PrincipalId;

/// A principal (user or agent) in the system.
///
/// Principals are the attribution target for all write operations.
/// The `principal_key` is human-readable; the `id` is used in foreign keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Principal {
    /// Unique identifier with `prin_` prefix.
    id: PrincipalId,
    /// Human-readable key (e.g. `"user:sam"`, `"principal:local"`).
    principal_key: String,
    /// Optional display name.
    display_name: Option<String>,
    /// When this principal was created.
    created_at: DateTime<Utc>,
}

impl Principal {
    /// Creates a new principal.
    pub fn new(
        id: PrincipalId,
        principal_key: String,
        display_name: Option<String>,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            principal_key,
            display_name,
            created_at,
        }
    }

    /// Returns the principal identifier.
    pub fn id(&self) -> PrincipalId {
        self.id
    }

    /// Returns the human-readable principal key.
    pub fn principal_key(&self) -> &str {
        &self.principal_key
    }

    /// Returns the display name, if set.
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    /// Returns when this principal was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
