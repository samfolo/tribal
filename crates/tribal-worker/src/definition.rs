//! Stage definition derivation: the single constructor both lockstep
//! sites use.
//!
//! The worker's claim path and the ingest-time fingerprint must arrive
//! at byte-identical definitions, or the recorded composite stops naming
//! the binding execution resolves. Both call here: the agentic
//! configuration selects the executor, the budgets, the tool surface,
//! and which prompt pair the definition hashes. Everything else
//! reproduces the launched one-shot shape exactly.

use tribal_config::{
    AgentsConfig, DEFAULT_AGENTIC_EXECUTION_DEADLINE_SECONDS, DEFAULT_AGENTIC_MAX_TOTAL_TOKENS,
    DEFAULT_AGENTIC_MAX_TURNS, DEFAULT_AGENTIC_VERIFY_ROUNDS, ExecutorChoice, StageAgentConfig,
};
use tribal_domain::{AgentDefinition, ExecutionBudgets, StageExecutorKind, TaskType};
use tribal_inference::CompletionStageSpec;

use crate::tools::stage_tool_bindings;

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
    /// supplied: a wiring fault at the caller, not a configuration the
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
    let stage_config = stage_agent_config(stage, agents);
    if stage_config.executor == ExecutorChoice::Loop {
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
            tools: stage_tool_bindings(stage),
        })
    } else {
        let mut definition = AgentDefinition::one_shot(
            stage,
            spec.provider,
            spec.model.clone(),
            spec.base_url.clone(),
            spec.parameters.clone(),
            prompts.system.clone(),
            prompts.user.clone(),
        );
        definition.budgets = one_shot_budgets(stage_config);
        Ok(definition)
    }
}

/// The verifier child's binding definition: a one-shot on the parent's
/// model and endpoint, carrying the verifier prompts.
///
/// Flat by construction. A one-shot has no tools and runs no submission
/// pipeline, so it can launch no verifier of its own. The delegation
/// chain terminates by rule, not by configuration discipline, and a
/// verifier-of-a-verifier is unrepresentable rather than merely refused.
pub(crate) fn verifier_definition(
    parent: &AgentDefinition,
    system_prompt_hash: String,
    user_prompt_hash: String,
) -> AgentDefinition {
    AgentDefinition::one_shot(
        parent.pipeline_stage,
        parent.provider,
        parent.model.clone(),
        parent.base_url.clone(),
        parent.parameters.clone(),
        system_prompt_hash,
        user_prompt_hash,
    )
}

/// The budgets the admission check enforces for a stage right now.
///
/// Budgets re-resolve from the current configuration at every claim
/// (never from the thread's recorded binding), so headroom can genuinely
/// return through a configuration change while the binding stays the
/// recorded truth of what ran. The executor kind is the recorded one:
/// a loop thread keeps the finite-default discipline whatever the
/// configuration now selects.
pub(crate) fn current_stage_budgets(
    stage: TaskType,
    executor: StageExecutorKind,
    agents: &AgentsConfig,
) -> ExecutionBudgets {
    let stage_config = stage_agent_config(stage, agents);
    if executor == StageExecutorKind::BuiltInLoop {
        loop_budgets(stage_config)
    } else {
        one_shot_budgets(stage_config)
    }
}

/// The stage's agentic configuration. Every stage is configurable; a
/// one-shot configuration with no overrides reproduces the launched
/// definition's budgets exactly.
fn stage_agent_config(stage: TaskType, agents: &AgentsConfig) -> &StageAgentConfig {
    match stage {
        TaskType::Extraction => &agents.extraction,
        TaskType::Triage => &agents.triage,
        TaskType::Relation => &agents.relation,
    }
}

/// A one-shot's budgets: the token cap it enforces, nothing more. The turn
/// and deadline caps bound a turn loop, so a one-shot binding does not record
/// a contract it would not honour. Absent caps stay absent, reproducing the
/// launched binding hash.
fn one_shot_budgets(stage_config: &StageAgentConfig) -> ExecutionBudgets {
    ExecutionBudgets {
        max_total_tokens: stage_config.max_total_tokens,
        max_turns: None,
        max_child_launches: None,
        execution_deadline_seconds: None,
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
        assert_eq!(
            derived.budgets.max_child_launches,
            Some(DEFAULT_AGENTIC_VERIFY_ROUNDS),
            "the loop binds the verify-round budget, the runtime's launch cap",
        );
        assert!(
            !derived.tools.is_empty(),
            "the loop binding hashes its tool surface",
        );
        assert!(
            derived
                .tools
                .iter()
                .any(|tool| tool.descriptor.name == tribal_agent_runtime::SUBMIT_RESULT_TOOL),
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
            "unset one-shot caps stay absent, never the agentic defaults",
        );
    }

    #[test]
    fn test_a_one_shot_drops_the_turn_and_deadline_caps_it_cannot_honour() {
        // An operator sets the loop-only caps on a one-shot stage; the
        // one-shot bracket enforces neither, so the binding records neither
        // rather than a contract it would not honour. Only the token cap,
        // which the one-shot does enforce, survives.
        let mut agents = AgentsConfig::default();
        agents.triage.max_total_tokens = Some(50_000);
        agents.triage.max_turns = Some(8);
        agents.triage.execution_deadline_seconds = Some(120);

        let derived = derive_stage_definition(TaskType::Triage, &a_spec(), &hashes(None), &agents)
            .expect("derives");
        assert_eq!(derived.executor, StageExecutorKind::OneShot);
        assert_eq!(derived.budgets.max_total_tokens, Some(50_000));
        assert_eq!(
            derived.budgets.max_turns, None,
            "a one-shot records no turn cap it would not enforce",
        );
        assert_eq!(
            derived.budgets.execution_deadline_seconds, None,
            "a one-shot records no deadline it would not enforce",
        );
    }

    #[test]
    fn test_the_verifier_binding_is_flat_by_construction() {
        let mut agents = AgentsConfig::default();
        agents.triage.executor = ExecutorChoice::Loop;
        let parent = derive_stage_definition(
            TaskType::Triage,
            &a_spec(),
            &hashes(Some(("c".repeat(64), "d".repeat(64)))),
            &agents,
        )
        .expect("derives the loop parent");

        let verifier = verifier_definition(&parent, "e".repeat(64), "f".repeat(64));

        // A one-shot with no tools cannot run a submission pipeline, so it
        // launches no verifier: the chain terminates by rule.
        assert_eq!(verifier.executor, StageExecutorKind::OneShot);
        assert!(
            verifier.tools.is_empty(),
            "a verifier binding declares no tools, so it can launch no child",
        );
        assert_eq!(verifier.pipeline_stage, TaskType::Triage);
        assert_eq!(verifier.prompt_hashes, vec!["e".repeat(64), "f".repeat(64)]);
        assert_eq!(
            verifier.model, parent.model,
            "the verifier runs on the parent's model",
        );
    }

    #[test]
    fn test_a_stage_follows_its_own_executor_not_a_sibling_stage() {
        // Extraction left at its default stays one-shot even when triage
        // and relation are fully agentic: a stage's shape is its own
        // configuration's, never a sibling's.
        let mut agents = AgentsConfig::default();
        agents.triage.executor = ExecutorChoice::Loop;
        agents.triage.max_total_tokens = Some(1);
        agents.relation.executor = ExecutorChoice::Loop;

        let derived =
            derive_stage_definition(TaskType::Extraction, &a_spec(), &hashes(None), &agents)
                .expect("derives");
        assert_eq!(derived.executor, StageExecutorKind::OneShot);
        assert_eq!(derived.budgets, ExecutionBudgets::default());
    }

    #[test]
    fn test_the_loop_executor_reshapes_the_extraction_definition() {
        let mut agents = AgentsConfig::default();
        agents.extraction.executor = ExecutorChoice::Loop;

        let derived = derive_stage_definition(
            TaskType::Extraction,
            &a_spec(),
            &hashes(Some(("c".repeat(64), "d".repeat(64)))),
            &agents,
        )
        .expect("derives");

        assert_eq!(derived.executor, StageExecutorKind::BuiltInLoop);
        assert_eq!(derived.prompt_hashes, vec!["c".repeat(64), "d".repeat(64)]);
        assert!(
            derived
                .tools
                .iter()
                .any(|tool| tool.descriptor.name == tribal_agent_runtime::SUBMIT_RESULT_TOOL),
            "the extraction loop binds its submission contract",
        );
    }

    #[test]
    fn test_the_loop_executor_reshapes_the_relation_definition() {
        let mut agents = AgentsConfig::default();
        agents.relation.executor = ExecutorChoice::Loop;

        let derived = derive_stage_definition(
            TaskType::Relation,
            &a_spec(),
            &hashes(Some(("c".repeat(64), "d".repeat(64)))),
            &agents,
        )
        .expect("derives");

        assert_eq!(derived.executor, StageExecutorKind::BuiltInLoop);
        assert_eq!(derived.prompt_hashes, vec!["c".repeat(64), "d".repeat(64)]);
        assert!(
            derived
                .tools
                .iter()
                .any(|tool| tool.descriptor.name == tribal_agent_runtime::SUBMIT_RESULT_TOOL),
            "the relation loop binds its own tool surface",
        );
        assert!(
            derived
                .tools
                .iter()
                .all(|tool| tool.project_scope == tribal_domain::ProjectScope::CrossProject),
            "every relation tool reaches across projects",
        );
    }
}
