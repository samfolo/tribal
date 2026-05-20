//! Embedding provider configuration.

use serde::{Deserialize, Serialize};
use tribal_domain::ApiKey;

use super::provider_kind::ProviderKind;
use crate::validation::{ConfigPath, EnumerateFields};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default embedding model name.
pub const DEFAULT_MODEL: &str = "nomic-embed-text:v1.5";

/// Default vector dimensions.
pub const DEFAULT_DIMENSIONS: u32 = 768;

fn default_model() -> String {
    String::from(DEFAULT_MODEL)
}

const fn default_dimensions() -> u32 {
    DEFAULT_DIMENSIONS
}

// ---------------------------------------------------------------------------
// EmbeddingConfig
// ---------------------------------------------------------------------------

/// Configuration for the embedding provider.
///
/// Controls which model and provider are used for vector embeddings.
/// When `base_url` is `None`, the provider implementation supplies its
/// own default URL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingConfig {
    /// Embedding provider.
    #[serde(default)]
    pub provider: ProviderKind,

    /// Embedding model name.
    #[serde(default = "default_model")]
    pub model: String,

    /// Vector dimensions (must match the model).
    #[serde(default = "default_dimensions")]
    pub dimensions: u32,

    /// Base URL for the provider API.
    ///
    /// When `None`, the provider supplies its own default.
    #[serde(default)]
    pub base_url: Option<String>,

    /// API key for cloud providers.
    ///
    /// Required when `provider` is `anthropic` or `openai`. Prefer
    /// setting via environment variable to avoid plaintext secrets in
    /// configuration files.
    #[serde(default)]
    pub api_key: Option<ApiKey>,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            provider: ProviderKind::default(),
            model: default_model(),
            dimensions: default_dimensions(),
            base_url: None,
            api_key: None,
        }
    }
}

// ---------------------------------------------------------------------------
// EnumerateFields
// ---------------------------------------------------------------------------

impl EnumerateFields for EmbeddingConfig {
    fn enumerate(prefix: &str, out: &mut Vec<ConfigPath>) {
        out.push(ConfigPath::child(prefix, "provider"));
        out.push(ConfigPath::child(prefix, "model"));
        out.push(ConfigPath::child(prefix, "dimensions"));
        out.push(ConfigPath::child(prefix, "base_url"));
        out.push(ConfigPath::child(prefix, "api_key"));
    }
}

#[cfg(test)]
#[allow(dead_code, clippy::let_underscore_untyped)]
fn _check_embedding_config_fields(c: &EmbeddingConfig) {
    let _ = &c.provider;
    let _ = &c.model;
    let _ = &c.dimensions;
    let _ = &c.base_url;
    let _ = &c.api_key;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = EmbeddingConfig::default();
        assert_eq!(config.provider, ProviderKind::Ollama);
        assert_eq!(config.model, DEFAULT_MODEL);
        assert_eq!(config.dimensions, DEFAULT_DIMENSIONS);
        assert_eq!(config.base_url, None);
        assert_eq!(config.api_key, None);
    }
}
