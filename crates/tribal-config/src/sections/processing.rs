//! Typed processing profiles for inference and agent execution.

use serde::{Deserialize, Serialize};
use tribal_domain::ProviderConnectionName;

use super::{
    AgentsConfig, ExecutorChoice, InferenceConfig, StageAgentConfig, StageInferenceConfig,
    TribalConfig,
};

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

impl TribalConfig {
    /// Projects the effective inference and agent settings into one complete profile.
    #[must_use]
    pub fn processing_profile(&self) -> ProcessingProfile {
        if let Some(model) = preset_model(&self.inference, &self.agents, ExecutorChoice::OneShot) {
            return ProcessingProfile::Efficient { model };
        }
        if let Some(model) = preset_model(&self.inference, &self.agents, ExecutorChoice::Loop) {
            return ProcessingProfile::HigherQuality { model };
        }
        ProcessingProfile::Custom {
            settings: Box::new(CustomProcessingSettings {
                extraction: ExtractionStageSettings {
                    model: stage_model(&self.inference.extraction),
                    execution: extraction_execution(&self.agents.extraction),
                },
                triage: VerifiedStageSettings {
                    model: stage_model(&self.inference.triage),
                    execution: verified_execution(&self.agents.triage),
                },
                relation: VerifiedStageSettings {
                    model: stage_model(&self.inference.relation),
                    execution: verified_execution(&self.agents.relation),
                },
            }),
        }
    }

    /// Replaces every processing-owned field with the supplied profile.
    pub fn apply_processing_profile(&mut self, profile: ProcessingProfile) {
        match profile {
            ProcessingProfile::Efficient { model } => {
                self.inference = preset_inference(&model);
                self.agents = preset_agents(ExecutorChoice::OneShot);
            }
            ProcessingProfile::HigherQuality { model } => {
                self.inference = preset_inference(&model);
                self.agents = preset_agents(ExecutorChoice::Loop);
            }
            ProcessingProfile::Custom { settings } => {
                let settings = *settings;
                self.inference = InferenceConfig {
                    extraction: raw_stage_model(settings.extraction.model),
                    triage: raw_stage_model(settings.triage.model),
                    relation: raw_stage_model(settings.relation.model),
                };
                self.agents = AgentsConfig {
                    extraction: raw_extraction_execution(&settings.extraction.execution),
                    triage: raw_verified_execution(&settings.triage.execution),
                    relation: raw_verified_execution(&settings.relation.execution),
                };
            }
        }
    }
}

fn preset_model(
    inference: &InferenceConfig,
    agents: &AgentsConfig,
    executor: ExecutorChoice,
) -> Option<PresetModelSettings> {
    let stages = [
        &inference.extraction,
        &inference.triage,
        &inference.relation,
    ];
    let first = stages[0];
    let models_match = stages.iter().all(|stage| {
        stage.connection == first.connection
            && stage.model == first.model
            && stage.temperature.is_none()
            && stage.max_tokens.is_none()
    });
    let agents_match = [&agents.extraction, &agents.triage, &agents.relation]
        .iter()
        .all(|stage| {
            stage.executor == executor
                && stage.verifier.is_none()
                && stage.max_turns.is_none()
                && stage.max_total_tokens.is_none()
                && stage.execution_deadline_seconds.is_none()
        });
    (models_match && agents_match).then(|| PresetModelSettings {
        connection: first.connection.clone(),
        model: first.model.clone(),
    })
}

fn preset_inference(model: &PresetModelSettings) -> InferenceConfig {
    let stage = || StageInferenceConfig {
        connection: model.connection.clone(),
        model: model.model.clone(),
        temperature: None,
        max_tokens: None,
    };
    InferenceConfig {
        extraction: stage(),
        triage: stage(),
        relation: stage(),
    }
}

fn preset_agents(executor: ExecutorChoice) -> AgentsConfig {
    let stage = || StageAgentConfig {
        executor,
        ..StageAgentConfig::default()
    };
    AgentsConfig {
        extraction: stage(),
        triage: stage(),
        relation: stage(),
    }
}

fn stage_model(stage: &StageInferenceConfig) -> StageModelSettings {
    StageModelSettings {
        connection: stage.connection.clone(),
        model: stage.model.clone(),
        temperature: stage.temperature,
        max_output_tokens: stage.max_tokens,
    }
}

fn raw_stage_model(stage: StageModelSettings) -> StageInferenceConfig {
    StageInferenceConfig {
        connection: stage.connection,
        model: stage.model,
        temperature: stage.temperature,
        max_tokens: stage.max_output_tokens,
    }
}

fn extraction_execution(stage: &StageAgentConfig) -> StageExecutionSettings {
    match stage.executor {
        ExecutorChoice::OneShot => StageExecutionSettings::Efficient {
            max_total_tokens: stage.max_total_tokens,
        },
        ExecutorChoice::Loop => StageExecutionSettings::HigherQuality {
            max_total_tokens: stage.max_total_tokens,
            max_turns: stage.max_turns,
            execution_deadline_seconds: stage.execution_deadline_seconds,
        },
    }
}

fn verified_execution(stage: &StageAgentConfig) -> VerifiedStageExecutionSettings {
    match stage.executor {
        ExecutorChoice::OneShot => VerifiedStageExecutionSettings::Efficient {
            max_total_tokens: stage.max_total_tokens,
        },
        ExecutorChoice::Loop => VerifiedStageExecutionSettings::HigherQuality {
            verifier: stage.verifier,
            max_total_tokens: stage.max_total_tokens,
            max_turns: stage.max_turns,
            execution_deadline_seconds: stage.execution_deadline_seconds,
        },
    }
}

fn raw_extraction_execution(stage: &StageExecutionSettings) -> StageAgentConfig {
    match stage {
        StageExecutionSettings::Efficient { max_total_tokens } => StageAgentConfig {
            max_total_tokens: *max_total_tokens,
            ..StageAgentConfig::default()
        },
        StageExecutionSettings::HigherQuality {
            max_total_tokens,
            max_turns,
            execution_deadline_seconds,
        } => StageAgentConfig {
            executor: ExecutorChoice::Loop,
            max_total_tokens: *max_total_tokens,
            max_turns: *max_turns,
            execution_deadline_seconds: *execution_deadline_seconds,
            ..StageAgentConfig::default()
        },
    }
}

fn raw_verified_execution(stage: &VerifiedStageExecutionSettings) -> StageAgentConfig {
    match stage {
        VerifiedStageExecutionSettings::Efficient { max_total_tokens } => StageAgentConfig {
            max_total_tokens: *max_total_tokens,
            ..StageAgentConfig::default()
        },
        VerifiedStageExecutionSettings::HigherQuality {
            verifier,
            max_total_tokens,
            max_turns,
            execution_deadline_seconds,
        } => StageAgentConfig {
            executor: ExecutorChoice::Loop,
            verifier: *verifier,
            max_total_tokens: *max_total_tokens,
            max_turns: *max_turns,
            execution_deadline_seconds: *execution_deadline_seconds,
        },
    }
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

    #[test]
    fn test_processing_presets_clear_every_hidden_override() {
        let mut config = TribalConfig::default();
        config.inference.triage.temperature = Some(0.7);
        config.agents.relation.max_turns = Some(9);

        config.apply_processing_profile(ProcessingProfile::HigherQuality {
            model: PresetModelSettings {
                connection: ProviderConnectionName::parse("ollama_default").unwrap(),
                model: "qwen3:8b".to_owned(),
            },
        });

        assert!(matches!(
            config.processing_profile(),
            ProcessingProfile::HigherQuality { .. }
        ));
        assert_eq!(config.inference.triage.temperature, None);
        assert_eq!(config.agents.relation.max_turns, None);
        assert_eq!(config.agents.triage.verifier, None);
    }

    #[test]
    fn test_custom_processing_round_trips_exactly() {
        let mut config = TribalConfig::default();
        config.inference.extraction.temperature = Some(0.2);
        config.agents.triage.executor = ExecutorChoice::Loop;
        config.agents.triage.verifier = Some(false);
        config.agents.triage.max_turns = Some(4);
        let expected = config.processing_profile();

        let mut rebuilt = TribalConfig::default();
        rebuilt.apply_processing_profile(expected.clone());

        assert_eq!(rebuilt.processing_profile(), expected);
    }
}
