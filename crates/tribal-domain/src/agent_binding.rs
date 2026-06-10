//! Agent bindings: what a thread runs, content-addressed and versioned.
//!
//! A binding pins a stage's executor kind, model identity, prompt
//! identity, budgets, and tool descriptors. The version hash covers the
//! serialised tool descriptors as well as the definition, so a tool-surface
//! change is a new version even when the definition text is unchanged.
//! Threads record the version they were admitted under; budgets at resume
//! are re-resolved from the binding current at that moment without
//! touching the thread's original version. Hash computation lives with
//! binding resolution in the runtime; these are the pure shapes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{AgentBindingVersionId, PipelineStage, ProviderKind};

// ---------------------------------------------------------------------------
// Executor kinds
// ---------------------------------------------------------------------------

/// How a stage's binding executes its turns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageExecutorKind {
    /// Render prompt, single structured-output inference call, parse,
    /// reconcile — the degenerate case: zero tools, maximum one turn.
    OneShot,
    /// The in-process turn loop over stage-scoped tools.
    BuiltInLoop,
    /// An external agent bound over ACP.
    ExternalAgent,
}

enum_text_conversions!(StageExecutorKind {
    StageExecutorKind::OneShot => "one_shot",
    StageExecutorKind::BuiltInLoop => "built_in_loop",
    StageExecutorKind::ExternalAgent => "external_agent",
});

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

/// How dangerous re-executing a tool is; part of the tool's contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSafetyTier {
    /// Side effect and tool-result record commit in one transaction:
    /// genuine exactly-once.
    InternalTransactional,
    /// Fires beyond the database; guarded by an intent row and
    /// verify-then-reconcile, never a blind re-fire.
    ExternalWithIntent,
    /// No side effects; freely re-executable.
    Pure,
}

enum_text_conversions!(ToolSafetyTier {
    ToolSafetyTier::InternalTransactional => "internal_transactional",
    ToolSafetyTier::ExternalWithIntent => "external_with_intent",
    ToolSafetyTier::Pure => "pure",
});

/// When a tool's result arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionMode {
    /// The result returns within the turn.
    Immediate,
    /// The result arrives later through a suspension's resolution.
    Deferred,
}

enum_text_conversions!(ToolExecutionMode {
    ToolExecutionMode::Immediate => "immediate",
    ToolExecutionMode::Deferred => "deferred",
});

/// One tool as declared to the model and to the runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct ToolDescriptor {
    /// The tool name the model calls.
    pub name: String,
    /// The model-facing description.
    pub description: String,
    /// The JSON Schema for the tool's arguments.
    pub input_schema: serde_json::Value,
    /// The declared upper bound on the result's serialised size, in bytes.
    pub response_size_bound: u32,
    /// How dangerous re-execution is.
    pub safety_tier: ToolSafetyTier,
    /// When the result arrives.
    pub execution_mode: ToolExecutionMode,
}

// ---------------------------------------------------------------------------
// Budgets and spend
// ---------------------------------------------------------------------------

/// Caps a binding imposes on one thread's execution.
///
/// Absent caps reproduce current behaviour: the default binding derived
/// from existing config carries no limits. Admission checks run against
/// the ledger-side number, which counts every request actually made.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, TypedBuilder)]
pub struct ExecutionBudgets {
    /// Cap on total tokens across the thread, all classes counted.
    #[builder(default)]
    pub max_total_tokens: Option<u64>,
    /// Cap on the number of turns.
    #[builder(default)]
    pub max_turns: Option<u32>,
}

/// The committed-record projection of one thread's spend.
///
/// Distinct from the `token_usage` ledger, which records every request the
/// gateway actually makes including requests whose records never commit;
/// this is what the thread's log accounts for, cache classes counted
/// separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ExecutionSpend {
    /// Input tokens across committed records.
    pub input_tokens: u64,
    /// Output tokens across committed records.
    pub output_tokens: u64,
    /// Cache-read input tokens across committed records.
    pub cache_read_tokens: u64,
    /// Cache-write input tokens across committed records.
    pub cache_write_tokens: u64,
    /// Turns whose terminal record committed.
    pub turns: u32,
}

// ---------------------------------------------------------------------------
// Definition, binding, and version
// ---------------------------------------------------------------------------

/// Everything a binding pins about how a stage runs.
///
/// The content address hashes this structure's canonical serialisation,
/// tool descriptors included.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct AgentDefinition {
    /// The stage this definition serves.
    pub pipeline_stage: PipelineStage,
    /// How turns execute.
    pub executor: StageExecutorKind,
    /// The provider the stage's calls bind to.
    pub provider: ProviderKind,
    /// The model the stage's calls bind to.
    pub model: String,
    /// The provider endpoint the stage's calls bind to.
    pub base_url: String,
    /// Content hashes of the stage's system and user prompts, in role
    /// order, so a prompt edit is a new binding version.
    pub prompt_hashes: Vec<String>,
    /// The execution caps, absent by default.
    pub budgets: ExecutionBudgets,
    /// The tools exposed to the model; empty for one-shot.
    pub tools: Vec<ToolDescriptor>,
}

/// One stored, content-addressed binding version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct AgentBinding {
    /// Unique identifier with `abv_` prefix.
    id: AgentBindingVersionId,
    /// The content address over the canonically serialised definition.
    hash: String,
    /// The stage this binding serves.
    pipeline_stage: PipelineStage,
    /// The pinned definition.
    definition: AgentDefinition,
    /// When this version was first recorded.
    created_at: DateTime<Utc>,
}

impl AgentBinding {
    /// Returns the binding-version identifier.
    pub fn id(&self) -> AgentBindingVersionId {
        self.id
    }

    /// Returns the content address.
    pub fn hash(&self) -> &str {
        &self.hash
    }

    /// Returns the stage this binding serves.
    pub fn pipeline_stage(&self) -> PipelineStage {
        self.pipeline_stage
    }

    /// Returns the pinned definition.
    pub fn definition(&self) -> &AgentDefinition {
        &self.definition
    }

    /// Returns when this version was first recorded.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_executor_kind_serde_roundtrip, StageExecutorKind {
        StageExecutorKind::OneShot => "one_shot",
        StageExecutorKind::BuiltInLoop => "built_in_loop",
        StageExecutorKind::ExternalAgent => "external_agent",
    });

    enum_text_tests!(test_executor_kind_text_roundtrip, StageExecutorKind {
        StageExecutorKind::OneShot => "one_shot",
        StageExecutorKind::BuiltInLoop => "built_in_loop",
        StageExecutorKind::ExternalAgent => "external_agent",
    });

    enum_serde_tests!(test_safety_tier_serde_roundtrip, ToolSafetyTier {
        ToolSafetyTier::InternalTransactional => "internal_transactional",
        ToolSafetyTier::ExternalWithIntent => "external_with_intent",
        ToolSafetyTier::Pure => "pure",
    });

    enum_text_tests!(test_safety_tier_text_roundtrip, ToolSafetyTier {
        ToolSafetyTier::InternalTransactional => "internal_transactional",
        ToolSafetyTier::ExternalWithIntent => "external_with_intent",
        ToolSafetyTier::Pure => "pure",
    });

    enum_serde_tests!(test_execution_mode_serde_roundtrip, ToolExecutionMode {
        ToolExecutionMode::Immediate => "immediate",
        ToolExecutionMode::Deferred => "deferred",
    });

    enum_text_tests!(test_execution_mode_text_roundtrip, ToolExecutionMode {
        ToolExecutionMode::Immediate => "immediate",
        ToolExecutionMode::Deferred => "deferred",
    });

    #[test]
    fn test_default_budgets_impose_no_caps() {
        let budgets = ExecutionBudgets::default();
        assert!(matches!(
            budgets,
            ExecutionBudgets {
                max_total_tokens: None,
                max_turns: None,
            }
        ));
    }

    #[test]
    fn test_definition_serialisation_is_field_stable() {
        // The content address hashes this serialisation: field renames or
        // reorderings are version-breaking, which this snapshot of the
        // key set pins.
        let definition = AgentDefinition::builder()
            .pipeline_stage(PipelineStage::Extraction)
            .executor(StageExecutorKind::OneShot)
            .provider(ProviderKind::Ollama)
            .model("llama3".to_owned())
            .base_url("http://localhost:11434".to_owned())
            .prompt_hashes(vec!["a".repeat(64)])
            .budgets(ExecutionBudgets::default())
            .tools(vec![])
            .build();

        let json = serde_json::to_value(&definition).expect("definition serialises");
        let keys: Vec<&str> = json
            .as_object()
            .expect("definition is an object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "base_url",
                "budgets",
                "executor",
                "model",
                "pipeline_stage",
                "prompt_hashes",
                "provider",
                "tools",
            ],
        );
    }
}
