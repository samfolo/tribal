//! Stage definition derivation: the single constructor both lockstep
//! sites use.
//!
//! The worker's claim path and the ingest-time fingerprint must arrive
//! at byte-identical definitions, or the recorded composite stops naming
//! the binding execution resolves. Both call here: the agentic
//! configuration selects the executor, the budgets, the tool surface,
//! and which prompt pair the definition hashes — everything else
//! reproduces the launched one-shot shape exactly.

use tribal_config::{
    AgentsConfig, DEFAULT_AGENTIC_EXECUTION_DEADLINE_SECONDS, DEFAULT_AGENTIC_MAX_TOTAL_TOKENS,
    DEFAULT_AGENTIC_MAX_TURNS, DEFAULT_AGENTIC_VERIFY_ROUNDS, ExecutorChoice, StageAgentConfig,
};
use tribal_domain::{AgentDefinition, ExecutionBudgets, StageExecutorKind, TaskType};
use tribal_inference::CompletionStageSpec;

use crate::tools::triage_tool_descriptors;

/// A stage's prompt content hashes: the launched pair always, the loop
/// pair when the agentic executor needs it.
#[derive(Debug, Clone)]
pub struct StagePromptHashes {
    /// The launched system prompt's content hash.
    pub system: String,
    /// The launched user prompt's content hash.
    pub user: String,
    /// The loop pair's content hashes, `(system, user)`, when the
    /// active set carries them.
    pub loop_pair: Option<(String, String)>,
}

/// The derivation refused: the configuration selects a shape the
/// supplied inputs cannot produce.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DefinitionError {
    /// The loop executor is configured but no loop prompt hashes were
    /// supplied — a wiring fault at the caller, not a configuration the
    /// system can run.
    #[error("the {stage} loop executor needs loop prompt hashes, and none were supplied")]
    MissingLoopPrompts {
        /// The stage whose derivation failed.
        stage: TaskType,
    },
}

/// Derives one stage's definition from its endpoint spec, prompt
/// hashes, and the agentic configuration.
///
/// Absent agentic configuration reproduces the launched one-shot
/// definition byte for byte. Budget overrides apply to either executor
/// (a budgeted one-shot is legal); the finite agentic defaults apply
/// only under the loop.
///
/// # Errors
///
/// Returns [`DefinitionError::MissingLoopPrompts`] when the loop
/// executor is selected without its prompt hashes.
pub fn derive_stage_definition(
    stage: TaskType,
    spec: &CompletionStageSpec,
    prompts: &StagePromptHashes,
    agents: &AgentsConfig,
) -> Result<AgentDefinition, DefinitionError> {
    match stage_agent_config(stage, agents) {
        Some(stage_config) if stage_config.executor == ExecutorChoice::Loop => {
            let Some((loop_system, loop_user)) = prompts.loop_pair.clone() else {
                return Err(DefinitionError::MissingLoopPrompts { stage });
            };
            Ok(AgentDefinition {
                pipeline_stage: stage,
                executor: StageExecutorKind::BuiltInLoop,
                provider: spec.provider,
                model: spec.model.clone(),
                base_url: spec.base_url.clone(),
                parameters: spec.parameters.clone(),
                prompt_hashes: vec![loop_system, loop_user],
                budgets: loop_budgets(stage_config),
                tools: triage_tool_descriptors(),
            })
        }
        stage_config => {
            let mut definition = AgentDefinition::one_shot(
                stage,
                spec.provider,
                spec.model.clone(),
                spec.base_url.clone(),
                spec.parameters.clone(),
                prompts.system.clone(),
                prompts.user.clone(),
            );
            if let Some(stage_config) = stage_config {
                definition.budgets = one_shot_budgets(stage_config);
            }
            Ok(definition)
        }
    }
}

/// The stage's agentic configuration, where one exists. Only triage is
/// configurable in this release; the config section makes any other
/// stage key an unknown-field error at load.
fn stage_agent_config(stage: TaskType, agents: &AgentsConfig) -> Option<&StageAgentConfig> {
    match stage {
        TaskType::Triage => Some(&agents.triage),
        TaskType::Extraction | TaskType::Relation => None,
    }
}

/// A one-shot's budgets: overrides only, absent caps stay absent — the
/// launched no-limit behaviour, and the launched binding hash with it.
fn one_shot_budgets(stage_config: &StageAgentConfig) -> ExecutionBudgets {
    ExecutionBudgets {
        max_total_tokens: stage_config.max_total_tokens,
        max_turns: stage_config.max_turns,
        max_child_launches: None,
        execution_deadline_seconds: stage_config.execution_deadline_seconds,
    }
}

/// The loop's budgets: every cap finite, overrides over the named
/// defaults.
fn loop_budgets(stage_config: &StageAgentConfig) -> ExecutionBudgets {
    ExecutionBudgets {
        max_total_tokens: Some(
            stage_config
                .max_total_tokens
                .unwrap_or(DEFAULT_AGENTIC_MAX_TOTAL_TOKENS),
        ),
        max_turns: Some(stage_config.max_turns.unwrap_or(DEFAULT_AGENTIC_MAX_TURNS)),
        max_child_launches: Some(DEFAULT_AGENTIC_VERIFY_ROUNDS),
        execution_deadline_seconds: Some(
            stage_config
                .execution_deadline_seconds
                .unwrap_or(DEFAULT_AGENTIC_EXECUTION_DEADLINE_SECONDS),
        ),
    }
}

#[cfg(test)]
mod tests {
    use tribal_domain::{ProviderKind, StageParameters};

    use super::*;

    fn a_spec() -> CompletionStageSpec {
        CompletionStageSpec {
            provider: ProviderKind::Ollama,
            model: "llama3".to_owned(),
            base_url: "http://localhost:11434".to_owned(),
            api_key: String::new(),
            parameters: StageParameters::default(),
        }
    }

    fn hashes(loop_pair: Option<(String, String)>) -> StagePromptHashes {
        StagePromptHashes {
            system: "a".repeat(64),
            user: "b".repeat(64),
            loop_pair,
        }
    }

    #[test]
    fn test_absent_configuration_reproduces_the_launched_definition() {
        let derived = derive_stage_definition(
            TaskType::Triage,
            &a_spec(),
            &hashes(None),
            &AgentsConfig::default(),
        )
        .expect("derives");
        let launched = AgentDefinition::one_shot(
            TaskType::Triage,
            ProviderKind::Ollama,
            "llama3".to_owned(),
            "http://localhost:11434".to_owned(),
            StageParameters::default(),
            "a".repeat(64),
            "b".repeat(64),
        );
        assert_eq!(
            derived.canonical_json().expect("serialises"),
            launched.canonical_json().expect("serialises"),
            "the default path's binding hash must not move",
        );
    }

    #[test]
    fn test_the_loop_executor_reshapes_the_triage_definition() {
        let mut agents = AgentsConfig::default();
        agents.triage.executor = ExecutorChoice::Loop;
        agents.triage.max_turns = Some(4);

        let derived = derive_stage_definition(
            TaskType::Triage,
            &a_spec(),
            &hashes(Some(("c".repeat(64), "d".repeat(64)))),
            &agents,
        )
        .expect("derives");

        assert_eq!(derived.executor, StageExecutorKind::BuiltInLoop);
        assert_eq!(derived.prompt_hashes, vec!["c".repeat(64), "d".repeat(64)]);
        assert_eq!(derived.budgets.max_turns, Some(4));
        assert_eq!(
            derived.budgets.max_total_tokens,
            Some(DEFAULT_AGENTIC_MAX_TOTAL_TOKENS),
            "unset caps take the finite agentic defaults",
        );
        assert!(
            !derived.tools.is_empty(),
            "the loop binding hashes its tool surface",
        );
        assert!(
            derived
                .tools
                .iter()
                .any(|tool| tool.name == tribal_agent_runtime::SUBMIT_RESULT_TOOL),
            "the completion tool is part of the surface",
        );
    }

    #[test]
    fn test_the_loop_executor_without_loop_prompts_is_refused() {
        let mut agents = AgentsConfig::default();
        agents.triage.executor = ExecutorChoice::Loop;
        let err = derive_stage_definition(TaskType::Triage, &a_spec(), &hashes(None), &agents)
            .expect_err("no loop hashes, no loop definition");
        assert_eq!(
            err,
            DefinitionError::MissingLoopPrompts {
                stage: TaskType::Triage,
            },
        );
    }

    #[test]
    fn test_budget_overrides_apply_to_a_one_shot_without_moving_its_defaults() {
        let mut agents = AgentsConfig::default();
        agents.triage.max_total_tokens = Some(50_000);

        let derived = derive_stage_definition(TaskType::Triage, &a_spec(), &hashes(None), &agents)
            .expect("derives");
        assert_eq!(derived.executor, StageExecutorKind::OneShot);
        assert_eq!(derived.budgets.max_total_tokens, Some(50_000));
        assert_eq!(
            derived.budgets.max_turns, None,
            "unset one-shot caps stay absent — never the agentic defaults",
        );
    }

    #[test]
    fn test_non_triage_stages_never_gain_agentic_shape() {
        let mut agents = AgentsConfig::default();
        agents.triage.executor = ExecutorChoice::Loop;
        agents.triage.max_total_tokens = Some(1);

        let derived =
            derive_stage_definition(TaskType::Extraction, &a_spec(), &hashes(None), &agents)
                .expect("derives");
        assert_eq!(derived.executor, StageExecutorKind::OneShot);
        assert_eq!(derived.budgets, ExecutionBudgets::default());
    }
}
