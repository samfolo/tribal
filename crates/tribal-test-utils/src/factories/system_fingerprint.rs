use chrono::Utc;
use tribal_db::NewSystemFingerprint;
use tribal_domain::{InferenceParameters, PromptVersionId, SystemFingerprint, SystemFingerprintId};

define_factory! {
    /// Factory for [`SystemFingerprint`] instances.
    pub struct SystemFingerprintFactory for SystemFingerprint {
        id: SystemFingerprintId = SystemFingerprintId::new(),
        content_hash: String = "a".repeat(64),
        build_version: String = "test-build".to_owned(),
        extraction_system_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        extraction_user_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        triage_system_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        triage_user_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        relation_system_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        relation_user_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        extraction_inference_provider: String = "ollama".to_owned(),
        extraction_inference_model: String = "llama3:70b".to_owned(),
        triage_inference_provider: String = "ollama".to_owned(),
        triage_inference_model: String = "llama3:8b".to_owned(),
        relation_inference_provider: String = "ollama".to_owned(),
        relation_inference_model: String = "llama3:8b".to_owned(),
        embedding_provider: String = "ollama".to_owned(),
        embedding_model: String = "nomic-embed-text".to_owned(),
        inference_parameters: InferenceParameters = InferenceParameters::default(),
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`SystemFingerprintFactory`] with sensible defaults.
pub fn a_system_fingerprint() -> SystemFingerprintFactory {
    SystemFingerprintFactory::new()
}

define_factory! {
    /// Factory for [`NewSystemFingerprint`] instances used in repository
    /// upsert operations.
    pub struct NewSystemFingerprintFactory for NewSystemFingerprint {
        content_hash: String = "a".repeat(64),
        build_version: String = "test-build".to_owned(),
        extraction_system_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        extraction_user_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        triage_system_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        triage_user_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        relation_system_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        relation_user_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        extraction_inference_provider: String = "ollama".to_owned(),
        extraction_inference_model: String = "llama3:70b".to_owned(),
        triage_inference_provider: String = "ollama".to_owned(),
        triage_inference_model: String = "llama3:8b".to_owned(),
        relation_inference_provider: String = "ollama".to_owned(),
        relation_inference_model: String = "llama3:8b".to_owned(),
        embedding_provider: String = "ollama".to_owned(),
        embedding_model: String = "nomic-embed-text".to_owned(),
        inference_parameters: serde_json::Value = serde_json::json!({}),
    }
}

/// Returns a [`NewSystemFingerprintFactory`] with sensible defaults.
pub fn a_new_system_fingerprint() -> NewSystemFingerprintFactory {
    NewSystemFingerprintFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let f = a_system_fingerprint().build();
        assert_eq!(f.content_hash().len(), 64);
        assert_eq!(f.build_version(), "test-build");
    }

    #[test]
    fn test_new_builds_with_defaults() {
        let new = a_new_system_fingerprint().build();
        assert_eq!(new.content_hash.len(), 64);
        assert_eq!(new.build_version, "test-build");
    }
}
