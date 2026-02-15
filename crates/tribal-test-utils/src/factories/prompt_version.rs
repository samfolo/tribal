use chrono::Utc;
use tribal_domain::{PromptStage, PromptVersion, PromptVersionId};

define_factory! {
    /// Factory for [`PromptVersion`] instances.
    pub struct PromptVersionFactory for PromptVersion {
        id: PromptVersionId = PromptVersionId::new(),
        stage: PromptStage = PromptStage::Extraction,
        content_hash: String = "b".repeat(64),
        content: String = "test prompt content".to_owned(),
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`PromptVersionFactory`] with sensible defaults.
pub fn a_prompt_version() -> PromptVersionFactory {
    PromptVersionFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let pv = a_prompt_version().build();
        assert_eq!(pv.stage(), PromptStage::Extraction);
        assert_eq!(pv.content(), "test prompt content");
    }
}
