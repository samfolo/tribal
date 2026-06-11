use chrono::Utc;
use tribal_db::NewSystemFingerprint;
use tribal_domain::{PipelineParameters, SystemFingerprint, SystemFingerprintId};

/// A well-formed placeholder binding hash (64 hex characters).
const PLACEHOLDER_BINDING_HASH: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

define_factory! {
    /// Factory for [`SystemFingerprint`] instances.
    pub struct SystemFingerprintFactory for SystemFingerprint {
        id: SystemFingerprintId = SystemFingerprintId::new(),
        content_hash: String = "a".repeat(64),
        build_version: String = "test-build".to_owned(),
        extraction_binding_hash: String = PLACEHOLDER_BINDING_HASH.to_owned(),
        triage_binding_hash: String = PLACEHOLDER_BINDING_HASH.to_owned(),
        relation_binding_hash: String = PLACEHOLDER_BINDING_HASH.to_owned(),
        embedding_provider: String = "ollama".to_owned(),
        embedding_model: String = "nomic-embed-text".to_owned(),
        embedding_dimensions: u32 = 768,
        pipeline_parameters: PipelineParameters = PipelineParameters::default(),
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
        extraction_binding_hash: String = PLACEHOLDER_BINDING_HASH.to_owned(),
        triage_binding_hash: String = PLACEHOLDER_BINDING_HASH.to_owned(),
        relation_binding_hash: String = PLACEHOLDER_BINDING_HASH.to_owned(),
        embedding_provider: String = "ollama".to_owned(),
        embedding_model: String = "nomic-embed-text".to_owned(),
        embedding_dimensions: u32 = 768,
        pipeline_parameters: serde_json::Value = serde_json::json!({}),
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
