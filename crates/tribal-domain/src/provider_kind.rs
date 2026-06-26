//! LLM provider selection.
//!
//! [`ProviderKind`] identifies which LLM provider is used for embedding
//! and inference stages.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// LLM provider.
///
/// Used as the type for embedding and per-stage inference configuration,
/// as the key type for per-provider limits, and as the capability-resolution
/// key (with the model string) for request-field admissibility.
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
    /// All variants, in canonical order.
    ///
    /// Compile-time forced: every variant must be listed in [`Self::as_str`]
    /// to satisfy the exhaustive match, so any new variant added to the enum
    /// will fail the build until it is added to [`Self::as_str`].
    pub const ALL: [Self; 3] = [Self::Ollama, Self::Anthropic, Self::OpenAi];

    /// Default base URL for local `Ollama` instances.
    pub const DEFAULT_OLLAMA_BASE_URL: &str = "http://localhost:11434";

    /// Default base URL for the `Anthropic` cloud API.
    pub const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";

    /// Default base URL for the `OpenAI` cloud API.
    pub const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com";

    /// Canonical lowercase name.
    ///
    /// Matches the serde `rename_all = "lowercase"` form and is the single
    /// source of truth for both [`Display`](std::fmt::Display) and
    /// [`FromStr`](std::str::FromStr).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ollama => "ollama",
            Self::Anthropic => "anthropic",
            Self::OpenAi => "openai",
        }
    }

    /// Returns `true` for cloud providers that require an API key.
    #[must_use]
    pub fn requires_api_key(self) -> bool {
        match self {
            Self::Anthropic | Self::OpenAi => true,
            Self::Ollama => false,
        }
    }

    /// Returns `true` if this provider exposes an embedding API.
    #[must_use]
    pub fn supports_embedding(self) -> bool {
        match self {
            Self::Ollama | Self::OpenAi => true,
            Self::Anthropic => false,
        }
    }

    /// Returns the default base URL for this provider kind.
    ///
    /// Used when the configuration does not specify a `base_url` override.
    #[must_use]
    pub fn default_base_url(self) -> &'static str {
        match self {
            Self::Ollama => Self::DEFAULT_OLLAMA_BASE_URL,
            Self::Anthropic => Self::DEFAULT_ANTHROPIC_BASE_URL,
            Self::OpenAi => Self::DEFAULT_OPENAI_BASE_URL,
        }
    }

    /// Returns this provider's chat-completion request path.
    #[must_use]
    pub const fn request_path(self) -> &'static str {
        match self {
            Self::Ollama => "/api/chat",
            Self::Anthropic => "/v1/messages",
            Self::OpenAi => "/v1/chat/completions",
        }
    }
}

impl FromStr for ProviderKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.as_str() == s)
            .ok_or_else(|| {
                let expected = Self::ALL
                    .iter()
                    .map(|k| k.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("unknown provider: {s} (expected one of {expected})")
            })
    }
}

impl std::fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
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
    fn test_request_path() {
        assert_eq!(ProviderKind::Ollama.request_path(), "/api/chat");
        assert_eq!(ProviderKind::Anthropic.request_path(), "/v1/messages");
        assert_eq!(ProviderKind::OpenAi.request_path(), "/v1/chat/completions");
    }

    #[test]
    fn test_supports_embedding() {
        assert!(ProviderKind::Ollama.supports_embedding());
        assert!(!ProviderKind::Anthropic.supports_embedding());
        assert!(ProviderKind::OpenAi.supports_embedding());
    }

    #[test]
    fn test_default_base_url() {
        assert_eq!(
            ProviderKind::Ollama.default_base_url(),
            ProviderKind::DEFAULT_OLLAMA_BASE_URL,
        );
        assert_eq!(
            ProviderKind::Anthropic.default_base_url(),
            ProviderKind::DEFAULT_ANTHROPIC_BASE_URL,
        );
        assert_eq!(
            ProviderKind::OpenAi.default_base_url(),
            ProviderKind::DEFAULT_OPENAI_BASE_URL,
        );
    }

    #[test]
    fn test_from_str_valid() {
        for kind in ProviderKind::ALL {
            assert_eq!(kind.as_str().parse::<ProviderKind>().unwrap(), kind);
        }
    }

    #[test]
    fn test_from_str_invalid() {
        let err = "grpc".parse::<ProviderKind>().unwrap_err();
        assert_eq!(
            err,
            "unknown provider: grpc (expected one of ollama, anthropic, openai)",
        );
    }
}
