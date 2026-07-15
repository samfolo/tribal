//! Typed processing profiles for inference and agent execution.

use serde::{Deserialize, Serialize};
use tribal_domain::ProviderConnectionName;

/// A complete processing profile.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "profile", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProcessingProfile {
    /// One completion call per stage.
    Efficient {
        /// Shared preset model.
        model: PresetModelSettings,
    },
    /// The built-in agent loop and recommended budgets.
    HigherQuality {
        /// Shared preset model.
        model: PresetModelSettings,
    },
    /// Explicit settings for every stage.
    Custom {
        /// Per-stage processing settings.
        settings: Box<CustomProcessingSettings>,
    },
}

/// The model shared by a preset profile.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PresetModelSettings {
    /// Provider connection name.
    pub connection: ProviderConnectionName,
    /// Model identifier.
    pub model: String,
}

/// Model settings for one custom stage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct StageModelSettings {
    /// Provider connection name.
    pub connection: ProviderConnectionName,
    /// Model identifier.
    pub model: String,
    /// Sampling temperature.
    #[serde(default)]
    pub temperature: Option<f64>,
    /// Per-call output-token ceiling.
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
}

/// Custom settings for every processing stage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CustomProcessingSettings {
    /// Extraction settings.
    pub extraction: ExtractionStageSettings,
    /// Triage settings.
    pub triage: VerifiedStageSettings,
    /// Relation-classification settings.
    pub relation: VerifiedStageSettings,
}

/// Extraction processing settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ExtractionStageSettings {
    /// Model settings.
    pub model: StageModelSettings,
    /// Execution settings.
    pub execution: StageExecutionSettings,
}

/// Processing settings for a stage that can verify its output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct VerifiedStageSettings {
    /// Model settings.
    pub model: StageModelSettings,
    /// Execution and verification settings.
    pub execution: VerifiedStageExecutionSettings,
}

/// Extraction execution settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum StageExecutionSettings {
    /// One completion call.
    Efficient {
        /// Total run-token budget.
        #[serde(default)]
        max_total_tokens: Option<u64>,
    },
    /// Agent-loop execution.
    HigherQuality {
        /// Total run-token budget.
        #[serde(default)]
        max_total_tokens: Option<u64>,
        /// Turn limit.
        #[serde(default)]
        max_turns: Option<u32>,
        /// Wall-clock execution limit.
        #[serde(default)]
        execution_deadline_seconds: Option<u32>,
    },
}

/// Execution settings for a stage that can verify its output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum VerifiedStageExecutionSettings {
    /// One completion call.
    Efficient {
        /// Total run-token budget.
        #[serde(default)]
        max_total_tokens: Option<u64>,
    },
    /// Agent-loop execution.
    HigherQuality {
        /// Whether to verify the stage output.
        #[serde(default)]
        verifier: Option<bool>,
        /// Total run-token budget.
        #[serde(default)]
        max_total_tokens: Option<u64>,
        /// Turn limit.
        #[serde(default)]
        max_turns: Option<u32>,
        /// Wall-clock execution limit.
        #[serde(default)]
        execution_deadline_seconds: Option<u32>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extraction_cannot_deserialise_a_verifier() {
        let value = serde_json::json!({
            "mode": "higher_quality",
            "verifier": true,
            "max_total_tokens": 1000,
        });

        assert!(serde_json::from_value::<StageExecutionSettings>(value).is_err());
    }

    #[test]
    fn test_efficient_execution_cannot_deserialise_loop_budgets() {
        let value = serde_json::json!({
            "mode": "efficient",
            "max_total_tokens": 1000,
            "max_turns": 4,
        });

        assert!(serde_json::from_value::<VerifiedStageExecutionSettings>(value).is_err());
    }

    #[test]
    fn test_profiles_round_trip_without_hidden_variant_state() {
        let profile = ProcessingProfile::HigherQuality {
            model: PresetModelSettings {
                connection: ProviderConnectionName::parse("openai_default")
                    .expect("fixture name is valid"),
                model: "gpt-5".to_owned(),
            },
        };

        let value = serde_json::to_value(&profile).expect("profile serialises");
        assert_eq!(
            serde_json::from_value::<ProcessingProfile>(value).expect("profile deserialises"),
            profile
        );
    }
}
