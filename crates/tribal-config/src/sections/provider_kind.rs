//! LLM provider selection.
//!
//! [`ProviderKind`] identifies which LLM provider is used for embedding
//! and inference stages.

use serde::{Deserialize, Serialize};

/// LLM provider.
///
/// Used as the type for embedding and per-stage inference configuration,
/// and as the key type for per-provider limits.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    /// Local Ollama instance.
    #[default]
    Ollama,

    /// Anthropic cloud API.
    Anthropic,

    /// OpenAI cloud API.
    #[serde(rename = "openai")]
    OpenAi,
}

impl ProviderKind {
    /// Returns `true` for cloud providers that require an API key.
    #[must_use]
    pub fn requires_api_key(self) -> bool {
        matches!(self, Self::Anthropic | Self::OpenAi)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enum_serde_tests;

    enum_serde_tests!(test_provider_kind_serde_roundtrip, ProviderKind {
        ProviderKind::Ollama => "ollama",
        ProviderKind::Anthropic => "anthropic",
        ProviderKind::OpenAi => "openai",
    });

    #[test]
    fn test_default_is_ollama() {
        assert_eq!(ProviderKind::default(), ProviderKind::Ollama);
    }

    #[test]
    fn test_requires_api_key() {
        assert!(!ProviderKind::Ollama.requires_api_key());
        assert!(ProviderKind::Anthropic.requires_api_key());
        assert!(ProviderKind::OpenAi.requires_api_key());
    }
}
