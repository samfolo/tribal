//! Authentication context carried on the handler.

use tribal_domain::{Scope, is_authorised};

use crate::{error::AuthError, principal::AuthenticatedPrincipal};

/// Authentication context carried on the handler.
///
/// Constructed once at handler creation from the resolved principal
/// identity and stored as a field on the handler.
pub struct AuthContext {
    principal: AuthenticatedPrincipal,
}

impl AuthContext {
    /// Creates a new authentication context from a verified principal.
    #[must_use]
    pub fn new(principal: AuthenticatedPrincipal) -> Self {
        Self { principal }
    }

    /// Returns the authenticated principal.
    #[must_use]
    pub fn principal(&self) -> &AuthenticatedPrincipal {
        &self.principal
    }

    /// Checks whether the authenticated principal has the required scope.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::InsufficientScope`] if no granted scope
    /// satisfies the required scope.
    pub fn require_scope(&self, scope: &Scope) -> Result<(), AuthError> {
        if is_authorised(self.principal.scopes(), scope) {
            Ok(())
        } else {
            Err(AuthError::InsufficientScope {
                required_scope: scope.clone(),
                granted_scopes: self.principal.scopes().to_vec(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use tribal_domain::{PrincipalId, full_access_scopes};

    use super::*;

    #[test]
    fn test_require_scope_full_access_accepts_all_tool_scopes() {
        let auth = AuthContext::new(AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            "user:test",
            full_access_scopes(),
        ));

        let tool_scopes: Vec<Scope> = [
            "tribal:write",
            "tribal.knowledge:read",
            "tribal.knowledge:write",
            "tribal.jobs:read",
        ]
        .iter()
        .map(|s| Scope::parse(s).unwrap())
        .collect();

        for scope in &tool_scopes {
            assert!(
                auth.require_scope(scope).is_ok(),
                "expected {scope} to pass"
            );
        }
    }

    #[test]
    fn test_require_scope_insufficient_scope() {
        let scopes = vec![Scope::parse("tribal.knowledge:read").unwrap()];
        let auth = AuthContext::new(AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            "user:test",
            scopes,
        ));

        let required = Scope::parse("tribal:write").unwrap();
        let err = auth
            .require_scope(&required)
            .expect_err("should reject missing scope");

        assert!(matches!(err, AuthError::InsufficientScope { .. }));
    }

    #[test]
    fn test_require_scope_prefix_satisfaction() {
        let scopes = vec![Scope::parse("tribal:read").unwrap()];
        let auth = AuthContext::new(AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            "user:test",
            scopes,
        ));

        let required = Scope::parse("tribal.knowledge:read").unwrap();
        assert!(auth.require_scope(&required).is_ok());
    }
}
