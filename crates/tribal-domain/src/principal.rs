//! Principal entity — user or agent identity.
//!
//! Every write operation is attributed to a principal. The `principal_key`
//! is a human-readable identifier (e.g. `"user:sam"`, `"principal:local"`)
//! exposed via MCP and used in logs. The UUID `id` is used for all FK
//! relationships.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::PrincipalId;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Principal key for the local stdio transport identity.
///
/// Created during `tribal setup` and used as the hardcoded bypass
/// identity for stdio connections.
pub const LOCAL_PRINCIPAL_KEY: &str = "principal:local";

// ---------------------------------------------------------------------------
// PlatformBinding
// ---------------------------------------------------------------------------

/// A principal's binding to its platform `(user, account)`.
///
/// The two fields are present together or not at all — the paired-null CHECK
/// on `principals` makes a half-binding unrepresentable in the database, and
/// wrapping them makes it unrepresentable in the domain. Both are opaque
/// cross-database pointers with no enforceable foreign key; `external_subject`
/// is never among them — that IdP-sourced PII stays platform-side, so a
/// platform erase reaches every copy of it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PlatformBinding {
    /// The platform account id (`account_…`) this principal contributes to.
    account_reference: String,
    /// The opaque platform user id — the linkage key mirrored platform-side by
    /// `PrincipalMap.local_principal_key`.
    platform_user_id: String,
}

impl PlatformBinding {
    /// Binds a principal to a platform `(user, account)`.
    pub fn new(account_reference: String, platform_user_id: String) -> Self {
        Self {
            account_reference,
            platform_user_id,
        }
    }

    /// Returns the platform account reference.
    pub fn account_reference(&self) -> &str {
        &self.account_reference
    }

    /// Returns the opaque platform user id.
    pub fn platform_user_id(&self) -> &str {
        &self.platform_user_id
    }
}

// ---------------------------------------------------------------------------
// Principal
// ---------------------------------------------------------------------------

/// A principal (user or agent) in the system.
///
/// Principals are the attribution target for all write operations.
/// The `principal_key` is human-readable; the `id` is used in foreign keys.
/// A platform-bound principal carries its [`PlatformBinding`] and derives its
/// display name from the platform user, storing none of its own; a local-only
/// principal keeps a local name and no binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
#[allow(clippy::struct_field_names)]
pub struct Principal {
    /// Unique identifier with `prin_` prefix.
    id: PrincipalId,
    /// Human-readable key (e.g. `"user:sam"`, `"principal:local"`).
    principal_key: String,
    /// Local display name — `None` for a platform-bound principal.
    #[builder(default)]
    display_name: Option<String>,
    /// The platform `(user, account)` this principal is bound to, or `None`
    /// for a local-only principal.
    #[builder(default)]
    platform_binding: Option<PlatformBinding>,
    /// When this principal was created.
    created_at: DateTime<Utc>,
}

impl Principal {
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

    /// Returns the platform binding, if this principal is platform-bound.
    pub fn platform_binding(&self) -> Option<&PlatformBinding> {
        self.platform_binding.as_ref()
    }

    /// Returns when this principal was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
