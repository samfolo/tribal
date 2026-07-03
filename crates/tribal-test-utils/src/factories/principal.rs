use chrono::Utc;
use tribal_db::NewPrincipal;
use tribal_domain::{PlatformBinding, Principal, PrincipalId};

/// Default principal key used across test factories and handler tests.
pub const TEST_PRINCIPAL_KEY: &str = "user:test";

define_factory! {
    /// Factory for [`Principal`] instances.
    pub struct PrincipalFactory for Principal {
        id: PrincipalId = PrincipalId::new(),
        principal_key: String = TEST_PRINCIPAL_KEY.to_owned(),
        display_name: Option<String> = None,
        platform_binding: Option<PlatformBinding> = None,
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

define_factory! {
    /// Factory for [`NewPrincipal`] instances used in repository insert operations.
    pub struct NewPrincipalFactory for NewPrincipal {
        principal_key: String = TEST_PRINCIPAL_KEY.to_owned(),
        display_name: Option<String> = None,
        platform_binding: Option<PlatformBinding> = None,
    }
}

/// Returns a [`PrincipalFactory`] with sensible defaults.
pub fn a_principal() -> PrincipalFactory {
    PrincipalFactory::new()
}

/// Returns a [`NewPrincipalFactory`] with sensible defaults.
pub fn a_new_principal() -> NewPrincipalFactory {
    NewPrincipalFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let p = a_principal().build();
        assert_eq!(p.principal_key(), TEST_PRINCIPAL_KEY);
        assert!(p.display_name().is_none());
    }

    #[test]
    fn test_new_principal_builds_with_defaults() {
        let new = a_new_principal().build();
        assert_eq!(new.principal_key, TEST_PRINCIPAL_KEY);
        assert!(new.display_name.is_none());
    }
}
