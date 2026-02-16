use chrono::Utc;
use tribal_db::NewPrincipal;
use tribal_domain::{Principal, PrincipalId};

define_factory! {
    /// Factory for [`Principal`] instances.
    pub struct PrincipalFactory for Principal {
        id: PrincipalId = PrincipalId::new(),
        principal_key: String = "user:test".to_owned(),
        display_name: Option<String> = None,
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

define_factory! {
    /// Factory for [`NewPrincipal`] instances used in repository insert operations.
    pub struct NewPrincipalFactory for NewPrincipal {
        principal_key: String = "user:test".to_owned(),
        display_name: Option<String> = None,
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
        assert_eq!(p.principal_key(), "user:test");
        assert!(p.display_name().is_none());
    }

    #[test]
    fn test_new_principal_builds_with_defaults() {
        let new = a_new_principal().build();
        assert_eq!(new.principal_key, "user:test");
        assert!(new.display_name.is_none());
    }
}
