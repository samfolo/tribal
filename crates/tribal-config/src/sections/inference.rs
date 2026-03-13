//! Per-stage inference configuration.
//!
//! Each worker LLM stage (extraction, triage, relation) is independently
//! configurable with its own provider, model, and parameters.

use serde::{Deserialize, Serialize};

use super::provider_kind::ProviderKind;

// ---------------------------------------------------------------------------
// Constants — extraction
// ---------------------------------------------------------------------------

fn default_extraction_model() -> String {
    String::from("llama3.1:70b")
}

const fn default_extraction_temperature() -> f64 {
    0.2
}

const fn default_extraction_max_tokens() -> u32 {
    4096
}

// ---------------------------------------------------------------------------
// Constants — triage / relation (shared defaults)
// ---------------------------------------------------------------------------

fn default_small_model() -> String {
    String::from("llama3.1:8b")
}

const fn default_small_temperature() -> f64 {
    0.1
}

const fn default_small_max_tokens() -> u32 {
    2048
}

// ---------------------------------------------------------------------------
// StageInferenceConfig
// ---------------------------------------------------------------------------

/// Configuration for a single inference stage.
///
/// When `base_url` is `None`, the provider implementation supplies its
/// own default URL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageInferenceConfig {
    /// LLM provider.
    #[serde(default)]
    pub provider: ProviderKind,

    /// Model name.
    pub model: String,

    /// Base URL for the provider API.
    #[serde(default)]
    pub base_url: Option<String>,

    /// API key for cloud providers.
    #[serde(default)]
    pub api_key: Option<String>,

    /// Sampling temperature.
    pub temperature: f64,

    /// Maximum output tokens.
    pub max_tokens: u32,
}

// ---------------------------------------------------------------------------
// InferenceConfig
// ---------------------------------------------------------------------------

/// Per-stage inference configuration.
///
/// Each stage is independently configurable to allow cost/quality tuning
/// — e.g. a capable model for extraction and a fast/cheap model for
/// classification stages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InferenceConfig {
    /// Extraction stage configuration.
    #[serde(default = "default_extraction")]
    pub extraction: StageInferenceConfig,

    /// Triage stage configuration.
    #[serde(default = "default_triage")]
    pub triage: StageInferenceConfig,

    /// Relation classification stage configuration.
    #[serde(default = "default_relation")]
    pub relation: StageInferenceConfig,
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            extraction: default_extraction(),
            triage: default_triage(),
            relation: default_relation(),
        }
    }
}

// ---------------------------------------------------------------------------
// Stage defaults
// ---------------------------------------------------------------------------

fn default_extraction() -> StageInferenceConfig {
    StageInferenceConfig {
        provider: ProviderKind::default(),
        model: default_extraction_model(),
        base_url: None,
        api_key: None,
        temperature: default_extraction_temperature(),
        max_tokens: default_extraction_max_tokens(),
    }
}

fn default_triage() -> StageInferenceConfig {
    StageInferenceConfig {
        provider: ProviderKind::default(),
        model: default_small_model(),
        base_url: None,
        api_key: None,
        temperature: default_small_temperature(),
        max_tokens: default_small_max_tokens(),
    }
}

fn default_relation() -> StageInferenceConfig {
    StageInferenceConfig {
        provider: ProviderKind::default(),
        model: default_small_model(),
        base_url: None,
        api_key: None,
        temperature: default_small_temperature(),
        max_tokens: default_small_max_tokens(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extraction_defaults() {
        let config = default_extraction();
        assert_eq!(config.provider, ProviderKind::Ollama);
        assert_eq!(config.model, "llama3.1:70b");
        assert!((config.temperature - 0.2).abs() < f64::EPSILON);
        assert_eq!(config.max_tokens, 4096);
    }

    #[test]
    fn test_triage_defaults() {
        let config = default_triage();
        assert_eq!(config.model, "llama3.1:8b");
        assert!((config.temperature - 0.1).abs() < f64::EPSILON);
        assert_eq!(config.max_tokens, 2048);
    }

    #[test]
    fn test_relation_defaults() {
        let config = default_relation();
        assert_eq!(config.model, "llama3.1:8b");
        assert!((config.temperature - 0.1).abs() < f64::EPSILON);
        assert_eq!(config.max_tokens, 2048);
    }
}
