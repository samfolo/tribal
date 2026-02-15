use chrono::Utc;
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

/// Returns a [`PrincipalFactory`] with sensible defaults.
pub fn a_principal() -> PrincipalFactory {
    PrincipalFactory::new()
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
}
