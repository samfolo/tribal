use chrono::Utc;
use tribal_domain::TagRegistryEntry;

define_factory! {
    /// Factory for [`TagRegistryEntry`] instances.
    pub struct TagRegistryEntryFactory for TagRegistryEntry {
        tag: String = "test-tag".to_owned(),
        first_seen_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`TagRegistryEntryFactory`] with sensible defaults.
pub fn a_tag_registry_entry() -> TagRegistryEntryFactory {
    TagRegistryEntryFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let t = a_tag_registry_entry().build();
        assert_eq!(t.tag(), "test-tag");
    }
}
