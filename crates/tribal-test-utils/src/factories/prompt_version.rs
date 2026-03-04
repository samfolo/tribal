use chrono::Utc;
use tribal_db::NewPromptVersion;
use tribal_domain::{PromptRole, PromptStage, PromptVersion, PromptVersionId};

define_factory! {
    /// Factory for [`PromptVersion`] instances.
    pub struct PromptVersionFactory for PromptVersion {
        id: PromptVersionId = PromptVersionId::new(),
        stage: PromptStage = PromptStage::Extraction,
        role: PromptRole = PromptRole::System,
        content_hash: String = "b".repeat(64),
        content: String = "test prompt content".to_owned(),
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`PromptVersionFactory`] with sensible defaults.
pub fn a_prompt_version() -> PromptVersionFactory {
    PromptVersionFactory::new()
}

define_factory! {
    /// Factory for [`NewPromptVersion`] instances used in repository
    /// insert operations.
    pub struct NewPromptVersionFactory for NewPromptVersion {
        stage: PromptStage = PromptStage::Extraction,
        role: PromptRole = PromptRole::System,
        content_hash: String = "b".repeat(64),
        content: String = "test prompt content".to_owned(),
    }
}

/// Returns a [`NewPromptVersionFactory`] with sensible defaults.
pub fn a_new_prompt_version() -> NewPromptVersionFactory {
    NewPromptVersionFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let pv = a_prompt_version().build();
        assert_eq!(pv.stage(), PromptStage::Extraction);
        assert_eq!(pv.role(), PromptRole::System);
        assert_eq!(pv.content(), "test prompt content");
    }

    #[test]
    fn test_new_builds_with_defaults() {
        let new = a_new_prompt_version().build();
        assert_eq!(new.stage, PromptStage::Extraction);
        assert_eq!(new.role, PromptRole::System);
        assert_eq!(new.content_hash.len(), 64);
        assert_eq!(new.content, "test prompt content");
    }
}
