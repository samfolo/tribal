//! Embedding provider configuration.

use serde::{Deserialize, Serialize};
use tribal_domain::{ApiKey, ProviderKind};

use super::init::{DEFAULT_DIMENSIONS, DEFAULT_MODEL};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

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
