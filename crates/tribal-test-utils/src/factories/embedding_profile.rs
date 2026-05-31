use chrono::Utc;
use tribal_db::NewEmbeddingProfile;
use tribal_domain::{
    DistanceMetric, EmbeddingProfile, EmbeddingProfileId, EmbeddingProfileState, ProviderKind,
};

define_factory! {
    /// Factory for [`EmbeddingProfile`] instances.
    ///
    /// Defaults to a `complete` profile (with a `completed_at`), the usable
    /// active-profile shape; tests override `state` for building or failed rows.
    pub struct EmbeddingProfileFactory for EmbeddingProfile {
        id: EmbeddingProfileId = EmbeddingProfileId::new(),
        epoch: i64 = 1,
        provider_kind: ProviderKind = ProviderKind::Ollama,
        normalised_base_url: String = "http://localhost:11434".to_owned(),
        model: String = "nomic-embed-text:v1.5".to_owned(),
        dimensions: u32 = 768,
        distance_metric: DistanceMetric = DistanceMetric::Cosine,
        revision_token: String = String::new(),
        probe_digest: Option<String> = None,
        state: EmbeddingProfileState = EmbeddingProfileState::Complete,
        fingerprint_hash: String = "test-fingerprint".to_owned(),
        created_at: chrono::DateTime<Utc> = Utc::now(),
        completed_at: Option<chrono::DateTime<Utc>> = Some(Utc::now()),
    }
}

/// Returns an [`EmbeddingProfileFactory`] with sensible defaults.
pub fn an_embedding_profile() -> EmbeddingProfileFactory {
    EmbeddingProfileFactory::new()
}

define_factory! {
    /// Factory for [`NewEmbeddingProfile`] instances used in repository inserts.
    pub struct NewEmbeddingProfileFactory for NewEmbeddingProfile {
        provider_kind: ProviderKind = ProviderKind::Ollama,
        normalised_base_url: String = "http://localhost:11434".to_owned(),
        model: String = "nomic-embed-text:v1.5".to_owned(),
        dimensions: u32 = 768,
        distance_metric: DistanceMetric = DistanceMetric::Cosine,
        revision_token: String = String::new(),
        probe_digest: Option<String> = None,
        fingerprint_hash: String = "test-fingerprint".to_owned(),
    }
}

/// Returns a [`NewEmbeddingProfileFactory`] with sensible defaults.
pub fn a_new_embedding_profile() -> NewEmbeddingProfileFactory {
    NewEmbeddingProfileFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let profile = an_embedding_profile().build();
        assert_eq!(profile.state(), EmbeddingProfileState::Complete);
        assert_eq!(profile.dimensions(), 768);
        assert!(profile.completed_at().is_some());
    }

    #[test]
    fn test_new_embedding_profile_builds_with_defaults() {
        let new = a_new_embedding_profile().build();
        assert_eq!(new.provider_kind, ProviderKind::Ollama);
        assert_eq!(new.dimensions, 768);
    }
}
