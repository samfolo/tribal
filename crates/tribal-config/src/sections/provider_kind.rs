//! LLM provider selection.
//!
//! [`ProviderKind`] identifies which LLM provider is used for embedding
//! and inference stages.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default base URL for local `Ollama` instances.
pub const DEFAULT_OLLAMA_BASE_URL: &str = "http://localhost:11434";

/// Default base URL for the `Anthropic` cloud API.
pub const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";

/// Default base URL for the `OpenAI` cloud API.
pub const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com";

// ---------------------------------------------------------------------------
// ProviderKind
// ---------------------------------------------------------------------------

/// LLM provider.
///
/// Used as the type for embedding and per-stage inference configuration,
/// and as the key type for per-provider limits.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    /// Local `Ollama` instance.
    #[default]
    Ollama,

    /// `Anthropic` cloud API.
    Anthropic,

    /// `OpenAI` cloud API.
    #[serde(rename = "openai")]
    OpenAi,
}

impl ProviderKind {
    /// Returns `true` for cloud providers that require an API key.
    #[must_use]
    pub fn requires_api_key(self) -> bool {
        matches!(self, Self::Anthropic | Self::OpenAi)
    }

    /// Returns the default base URL for this provider kind.
    ///
    /// Used when the configuration does not specify a `base_url` override.
    #[must_use]
    pub fn default_base_url(self) -> &'static str {
        match self {
            Self::Ollama => DEFAULT_OLLAMA_BASE_URL,
            Self::Anthropic => DEFAULT_ANTHROPIC_BASE_URL,
            Self::OpenAi => DEFAULT_OPENAI_BASE_URL,
        }
    }
}

impl std::fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ollama => write!(f, "ollama"),
            Self::Anthropic => write!(f, "anthropic"),
            Self::OpenAi => write!(f, "openai"),
        }
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
    fn test_display_matches_serde_names() {
        assert_eq!(ProviderKind::Ollama.to_string(), "ollama");
        assert_eq!(ProviderKind::Anthropic.to_string(), "anthropic");
        assert_eq!(ProviderKind::OpenAi.to_string(), "openai");
    }

    #[test]
    fn test_requires_api_key() {
        assert!(!ProviderKind::Ollama.requires_api_key());
        assert!(ProviderKind::Anthropic.requires_api_key());
        assert!(ProviderKind::OpenAi.requires_api_key());
    }

    #[test]
    fn test_default_base_url() {
        assert_eq!(
            ProviderKind::Ollama.default_base_url(),
            DEFAULT_OLLAMA_BASE_URL,
        );
        assert_eq!(
            ProviderKind::Anthropic.default_base_url(),
            DEFAULT_ANTHROPIC_BASE_URL,
        );
        assert_eq!(
            ProviderKind::OpenAi.default_base_url(),
            DEFAULT_OPENAI_BASE_URL,
        );
    }
}
