//! Fresh-system genesis seed configuration.
//!
//! `init` holds values used only when a corpus is first created: the genesis
//! embedding profile's identity. Once the corpus exists the active profile,
//! not this section, is the live embedding identity (the model is corpus
//! state, changed only by a reindex), so the config facade's genesis gate
//! refuses post-genesis drift and `tribal check` reports existing divergence
//! as informational state. The section stays in the file for legibility: it
//! records the seed a corpus began from, and carries no live authority.

use serde::{Deserialize, Serialize};
use tribal_domain::ProviderConnectionName;

use super::provider_connections::default_provider_connection_name;

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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct InitEmbeddingConfig {
    /// Reusable embedding-provider connection.
    #[serde(default = "default_provider_connection_name")]
    pub connection: ProviderConnectionName,

    /// Genesis embedding model name.
    #[serde(default = "default_model")]
    pub model: String,

    /// Target output dimensionality. `None` resolves through the embedding
    /// service's native-dimension chain when the genesis profile is created.
    #[serde(default)]
    pub dimensions: Option<u32>,
}

impl Default for InitEmbeddingConfig {
    fn default() -> Self {
        Self {
            connection: default_provider_connection_name(),
            model: default_model(),
            dimensions: None,
        }
    }
}

// ---------------------------------------------------------------------------
// InitConfig
// ---------------------------------------------------------------------------

/// Fresh-system seed values, applied only when first creating a corpus.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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
        assert_eq!(init.embedding.connection.as_str(), "ollama_default");
        assert_eq!(init.embedding.model, DEFAULT_MODEL);
        assert_eq!(init.embedding.dimensions, None);
    }

    #[test]
    fn test_deserialises_from_empty_object() {
        let init: InitConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(init, InitConfig::default());
    }

    #[test]
    fn test_deserialises_partial_embedding() {
        let init: InitConfig = serde_yaml::from_str(
            "embedding:\n  connection: openai_default\n  model: text-embedding-3-small\n  dimensions: 1536\n",
        )
        .unwrap();
        assert_eq!(init.embedding.connection.as_str(), "openai_default");
        assert_eq!(init.embedding.model, "text-embedding-3-small");
        assert_eq!(init.embedding.dimensions, Some(1536));
    }

    #[test]
    fn test_rejects_unknown_field() {
        assert!(serde_yaml::from_str::<InitConfig>("embedding:\n  provider: openai\n").is_err());
    }
}
