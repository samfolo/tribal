//! Verification-proof principal identity.
//!
//! The [`AuthenticatedPrincipal`] type is a verification-proof wrapper;
//! its existence guarantees the holder passed either token verification
//! or the stdio bypass path.

use tribal_domain::{PrincipalId, Scope};

/// Verification-proof principal identity.
///
/// Carries the resolved [`PrincipalId`] and human-readable principal
/// key. Its existence guarantees the holder passed either token
/// verification or the stdio bypass path.
#[derive(Debug, Clone)]
pub struct AuthenticatedPrincipal {
    principal_id: PrincipalId,
    principal_key: String,
    scopes: Vec<Scope>,
}

impl AuthenticatedPrincipal {
    /// Constructs a verified principal from its three fields.
    ///
    /// Reserved for the authenticator and stdio resolution paths;
    /// construction implies the caller has performed verification.
    #[must_use]
    pub(crate) fn new(
        principal_id: PrincipalId,
        principal_key: String,
        scopes: Vec<Scope>,
    ) -> Self {
        Self {
            principal_id,
            principal_key,
            scopes,
        }
    }

    /// Returns the principal's database identifier.
    #[must_use]
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Returns the human-readable principal key.
    #[must_use]
    pub fn principal_key(&self) -> &str {
        &self.principal_key
    }

    /// Returns the permission scopes granted to this principal.
    #[must_use]
    pub fn scopes(&self) -> &[Scope] {
        &self.scopes
    }
}

#[cfg(any(test, feature = "test-helpers"))]
impl AuthenticatedPrincipal {
    /// Test-only constructor that bypasses verification.
    ///
    /// The caller supplies the [`PrincipalId`] and scopes directly. Use
    /// a random ID for mock-backed tests; use the real ID from the
    /// database for tests that hit FK-constrained tables.
    #[must_use]
    pub fn for_test(principal_id: PrincipalId, principal_key: &str, scopes: Vec<Scope>) -> Self {
        Self {
            principal_id,
            principal_key: principal_key.to_owned(),
            scopes,
        }
    }
}

#[cfg(test)]
mod tests {
    use tribal_domain::{PrincipalId, full_access_scopes};

    use super::*;

    #[test]
    fn test_authenticated_principal_accessors() {
        let id = PrincipalId::new();
        let principal = AuthenticatedPrincipal::for_test(id, "user:sam", full_access_scopes());

        assert_eq!(principal.principal_id(), id);
        assert_eq!(principal.principal_key(), "user:sam");
        assert_eq!(principal.scopes().len(), 2);
    }
}
