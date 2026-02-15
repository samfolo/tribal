use chrono::Utc;
use tribal_domain::{AuthToken, AuthTokenId, PrincipalId};

define_factory! {
    /// Factory for [`AuthToken`] instances.
    pub struct AuthTokenFactory for AuthToken {
        id: AuthTokenId = AuthTokenId::new(),
        token_hash: String = "a".repeat(64),
        principal_id: PrincipalId = PrincipalId::new(),
        expires_at: chrono::DateTime<Utc> = Utc::now() + chrono::Duration::hours(24),
        created_at: chrono::DateTime<Utc> = Utc::now(),
        revoked_at: Option<chrono::DateTime<Utc>> = None,
    }
}

/// Returns an [`AuthTokenFactory`] with sensible defaults.
pub fn a_auth_token() -> AuthTokenFactory {
    AuthTokenFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let t = a_auth_token().build();
        assert_eq!(t.token_hash().len(), 64);
        assert!(t.revoked_at().is_none());
    }
}
