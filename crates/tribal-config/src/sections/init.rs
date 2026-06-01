//! Fresh-system genesis seed configuration.
//!
//! `init` holds values used only when a corpus is first created: the genesis
//! embedding profile's identity. Once the corpus exists the active profile,
//! not this section, is the live embedding identity (the model is corpus
//! state, changed only by a reindex), so editing `init` afterwards is inert
//! and `tribal check` reports any divergence as informational state. The
//! section stays in the file for legibility: it records the seed a corpus
//! began from, and carries no live authority.

use serde::{Deserialize, Serialize};
use tribal_domain::ProviderKind;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default genesis embedding model.
pub const DEFAULT_MODEL: &str = "nomic-embed-text:v1.5";

/// Native output dimensionality of [`DEFAULT_MODEL`].
///
/// `init.embedding.dimensions` is `None` by default and resolves through the
/// embedding service's native-dimension chain at provisioning; this is the
/// value that chain yields for the default model, exposed for tests and the
/// rendered-config reference.
pub const DEFAULT_DIMENSIONS: u32 = 768;

fn default_model() -> String {
    String::from(DEFAULT_MODEL)
}

// ---------------------------------------------------------------------------
// InitEmbeddingConfig
// ---------------------------------------------------------------------------

/// The genesis embedding identity used to seed the first profile.
///
/// Carries no credential: the runtime resolves the active provider's
/// credential through the credential catalogue, keyed by
/// `(provider_kind, normalised_base_url)`, because after a migration the
/// active provider can differ from this genesis provider and the runtime
/// must follow the active one. `dimensions` is optional: `None` resolves
/// through the embedding service's native-dimension chain at provisioning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitEmbeddingConfig {
    /// Genesis embedding provider.
    #[serde(default)]
    pub provider: ProviderKind,

    /// Genesis embedding model name.
    #[serde(default = "default_model")]
    pub model: String,

    /// Target output dimensionality. `None` resolves through the embedding
    /// service's native-dimension chain when the genesis profile is created.
    #[serde(default)]
    pub dimensions: Option<u32>,

    /// Base URL for the provider API.
    ///
    /// When `None`, the provider supplies its own default.
    #[serde(default)]
    pub base_url: Option<String>,
}

impl Default for InitEmbeddingConfig {
    fn default() -> Self {
        Self {
            provider: ProviderKind::default(),
            model: default_model(),
            dimensions: None,
            base_url: None,
        }
    }
}

// ---------------------------------------------------------------------------
// InitConfig
// ---------------------------------------------------------------------------

/// Fresh-system seed values, applied only when first creating a corpus.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitConfig {
    /// Genesis embedding identity.
    #[serde(default)]
    pub embedding: InitEmbeddingConfig,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_init_embedding_values() {
        let init = InitConfig::default();
        assert_eq!(init.embedding.provider, ProviderKind::default());
        assert_eq!(init.embedding.model, DEFAULT_MODEL);
        assert_eq!(init.embedding.dimensions, None);
        assert_eq!(init.embedding.base_url, None);
    }

    #[test]
    fn test_deserialises_from_empty_object() {
        let init: InitConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(init, InitConfig::default());
    }

    #[test]
    fn test_deserialises_partial_embedding() {
        let init: InitConfig = serde_yaml::from_str(
            "embedding:\n  provider: openai\n  model: text-embedding-3-small\n  dimensions: 1536\n",
        )
        .unwrap();
        assert_eq!(init.embedding.provider, ProviderKind::OpenAi);
        assert_eq!(init.embedding.model, "text-embedding-3-small");
        assert_eq!(init.embedding.dimensions, Some(1536));
    }

    #[test]
    fn test_rejects_unknown_field() {
        assert!(serde_yaml::from_str::<InitConfig>("embedding:\n  api_key: sk-x\n").is_err());
    }
}
