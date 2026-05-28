use chrono::Utc;
use tribal_db::NewAuthToken;
use tribal_domain::{AuthToken, AuthTokenId, PrincipalId, Scope, full_access_scopes};

define_factory! {
    /// Factory for [`AuthToken`] instances.
    pub struct AuthTokenFactory for AuthToken {
        id: AuthTokenId = AuthTokenId::new(),
        token_hash: String = "a".repeat(64),
        principal_id: PrincipalId = PrincipalId::new(),
        scopes: Vec<Scope> = full_access_scopes(),
        audience: String = String::new(),
        expires_at: chrono::DateTime<Utc> = Utc::now() + chrono::Duration::hours(24),
        created_at: chrono::DateTime<Utc> = Utc::now(),
        revoked_at: Option<chrono::DateTime<Utc>> = None,
    }
}

/// Returns an [`AuthTokenFactory`] with sensible defaults.
pub fn an_auth_token() -> AuthTokenFactory {
    AuthTokenFactory::new()
}

define_factory! {
    /// Factory for [`NewAuthToken`] instances used in repository
    /// insert operations.
    pub struct NewAuthTokenFactory for NewAuthToken {
        token_hash: String = "a".repeat(64),
        principal_id: PrincipalId = PrincipalId::new(),
        scopes: Vec<Scope> = full_access_scopes(),
        audience: String = String::new(),
        expires_at: chrono::DateTime<Utc> = Utc::now() + chrono::Duration::hours(24),
    }
}

/// Returns a [`NewAuthTokenFactory`] with sensible defaults.
pub fn a_new_auth_token() -> NewAuthTokenFactory {
    NewAuthTokenFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let t = an_auth_token().build();
        assert_eq!(t.token_hash().len(), 64);
        assert!(t.revoked_at().is_none());
    }

    #[test]
    fn test_new_builds_with_defaults() {
        let new = a_new_auth_token().build();
        assert_eq!(new.token_hash.len(), 64);
        assert!(new.expires_at > Utc::now());
    }
}
