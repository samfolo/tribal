//! LLM provider selection.
//!
//! [`ProviderKind`] identifies which LLM provider is used for embedding
//! and inference stages.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::env::{ENV_ANTHROPIC_API_KEY, ENV_OPENAI_API_KEY};

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
    /// All variants, in canonical order.
    ///
    /// Compile-time forced: every variant must be listed in [`Self::as_str`]
    /// to satisfy the exhaustive match, so any new variant added to the enum
    /// will fail the build until it is added to [`Self::as_str`].
    pub const ALL: [Self; 3] = [Self::Ollama, Self::Anthropic, Self::OpenAi];

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

    /// Returns the standard environment variable name carrying this
    /// provider's API key, if one applies.
    ///
    /// Consulted as a final fallback by the config loader when no
    /// config-file `api_key` or `TRIBAL_*__API_KEY` env var is supplied.
    /// Returns `None` for providers that do not require an API key.
    #[must_use]
    pub fn standard_env_var_name(self) -> Option<&'static str> {
        match self {
            Self::Ollama => None,
            Self::Anthropic => Some(ENV_ANTHROPIC_API_KEY),
            Self::OpenAi => Some(ENV_OPENAI_API_KEY),
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

    #[test]
    fn test_standard_env_var_name() {
        assert_eq!(ProviderKind::Ollama.standard_env_var_name(), None);
        assert_eq!(
            ProviderKind::Anthropic.standard_env_var_name(),
            Some(ENV_ANTHROPIC_API_KEY),
        );
        assert_eq!(
            ProviderKind::OpenAi.standard_env_var_name(),
            Some(ENV_OPENAI_API_KEY),
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
