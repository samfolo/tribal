//! Per-stage inference configuration.
//!
//! Each worker LLM stage (extraction, triage, relation) is independently
//! configurable with its own provider, model, and parameters.

use serde::{Deserialize, Serialize};
use tribal_domain::ProviderConnectionName;

use super::provider_connections::default_provider_connection_name;

/// One configured inference stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferenceStage {
    /// Memory extraction.
    Extraction,
    /// Triage classification.
    Triage,
    /// Relation classification.
    Relation,
}

impl InferenceStage {
    /// Returns the stage's configuration section path.
    #[must_use]
    pub const fn config_path(self) -> &'static str {
        match self {
            Self::Extraction => "inference.extraction",
            Self::Triage => "inference.triage",
            Self::Relation => "inference.relation",
        }
    }
}

// ---------------------------------------------------------------------------
// Constants — extraction
// ---------------------------------------------------------------------------

/// Default extraction model name.
pub const DEFAULT_EXTRACTION_MODEL: &str = "llama3.1:70b";

fn default_extraction_model() -> String {
    String::from(DEFAULT_EXTRACTION_MODEL)
}

// ---------------------------------------------------------------------------
// Constants — triage / relation (shared defaults)
// ---------------------------------------------------------------------------

/// Default triage/relation model name.
pub const DEFAULT_SMALL_MODEL: &str = "llama3.1:8b";

fn default_small_model() -> String {
    String::from(DEFAULT_SMALL_MODEL)
}

// ---------------------------------------------------------------------------
// StageInferenceConfig
// ---------------------------------------------------------------------------

/// Configuration for a single inference stage.
///
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct StageInferenceConfig {
    /// Reusable provider connection.
    #[serde(default = "default_provider_connection_name")]
    pub connection: ProviderConnectionName,

    /// Model name.
    pub model: String,

    /// Sampling temperature. `None` uses the provider default.
    #[serde(default)]
    pub temperature: Option<f64>,

    /// Maximum output tokens. `None` uses the provider default.
    #[serde(default)]
    pub max_tokens: Option<u32>,
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
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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

impl InferenceConfig {
    /// Iterates all stages in pipeline order.
    pub fn stages(&self) -> impl Iterator<Item = (InferenceStage, &StageInferenceConfig)> {
        [
            (InferenceStage::Extraction, &self.extraction),
            (InferenceStage::Triage, &self.triage),
            (InferenceStage::Relation, &self.relation),
        ]
        .into_iter()
    }
}

// ---------------------------------------------------------------------------
// Stage defaults
// ---------------------------------------------------------------------------

fn default_extraction() -> StageInferenceConfig {
    StageInferenceConfig {
        connection: default_provider_connection_name(),
        model: default_extraction_model(),
        temperature: None,
        max_tokens: None,
    }
}

fn default_triage() -> StageInferenceConfig {
    StageInferenceConfig {
        connection: default_provider_connection_name(),
        model: default_small_model(),
        temperature: None,
        max_tokens: None,
    }
}

fn default_relation() -> StageInferenceConfig {
    StageInferenceConfig {
        connection: default_provider_connection_name(),
        model: default_small_model(),
        temperature: None,
        max_tokens: None,
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
        assert_eq!(config.connection.as_str(), "ollama_default");
        assert_eq!(config.model, DEFAULT_EXTRACTION_MODEL);
        assert_eq!(config.temperature, None);
        assert_eq!(config.max_tokens, None);
    }

    #[test]
    fn test_triage_defaults() {
        let config = default_triage();
        assert_eq!(config.model, DEFAULT_SMALL_MODEL);
        assert_eq!(config.temperature, None);
        assert_eq!(config.max_tokens, None);
    }

    #[test]
    fn test_relation_defaults() {
        let config = default_relation();
        assert_eq!(config.model, DEFAULT_SMALL_MODEL);
        assert_eq!(config.temperature, None);
        assert_eq!(config.max_tokens, None);
    }
}
