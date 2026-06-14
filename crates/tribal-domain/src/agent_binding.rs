//! Agent bindings: what a thread runs, content-addressed and versioned.
//!
//! A binding pins a stage's executor kind, model identity, prompt
//! identity, budgets, and tool descriptors. The version hash covers the
//! serialised tool descriptors as well as the definition, so a tool-surface
//! change is a new version even when the definition text is unchanged.
//! Threads record the version they were admitted under, and the
//! pre-call admission check enforces the budgets for both executors:
//! the token cap against the ledger, the turn cap and deadline at the
//! loop's boundaries. Hash derivation is
//! [`AgentDefinition::canonical_json`]; binding resolution lives in the
//! runtime.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{AgentBindingVersionId, ProviderKind, StageParameters, TaskType};

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
    /// Cap on a thread's token spend: input, output, and cache-write
    /// tokens (cache-read is a subset of input, so it is not counted again).
    #[builder(default)]
    pub max_total_tokens: Option<u64>,
    /// Cap on the number of turns.
    #[builder(default)]
    pub max_turns: Option<u32>,
    /// Cap on child executions launched by the thread.
    #[builder(default)]
    pub max_child_launches: Option<u32>,
    /// Wall-clock bound on the thread's whole execution, in seconds,
    /// measured from its creation.
    #[builder(default)]
    pub execution_deadline_seconds: Option<u32>,
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
    pub pipeline_stage: TaskType,
    /// How turns execute.
    pub executor: StageExecutorKind,
    /// The provider the stage's calls bind to.
    pub provider: ProviderKind,
    /// The model the stage's calls bind to.
    pub model: String,
    /// The provider endpoint the stage's calls bind to.
    pub base_url: String,
    /// The effective (post-reconcile) sampling parameters the stage's
    /// calls carry, so a temperature edit is a new binding version.
    /// Defaulted on read so rows written before the field existed
    /// deserialise; the canonical serialisation always emits it.
    #[serde(default)]
    pub parameters: StageParameters,
    /// Content hashes of the stage's system and user prompts, in role
    /// order, so a prompt edit is a new binding version.
    pub prompt_hashes: Vec<String>,
    /// The execution caps, absent by default.
    pub budgets: ExecutionBudgets,
    /// The tools exposed to the model; empty for one-shot.
    pub tools: Vec<ToolDescriptor>,
}

impl AgentDefinition {
    /// Builds the default one-shot definition for a stage: no tools and
    /// no budget caps, reproducing launched behaviour exactly. The single
    /// constructor every deriver uses, so the worker and the fingerprint
    /// computation arrive at identical binding versions.
    #[must_use]
    pub fn one_shot(
        pipeline_stage: TaskType,
        provider: ProviderKind,
        model: String,
        base_url: String,
        parameters: StageParameters,
        system_prompt_hash: String,
        user_prompt_hash: String,
    ) -> Self {
        Self {
            pipeline_stage,
            executor: StageExecutorKind::OneShot,
            provider,
            model,
            base_url,
            parameters,
            prompt_hashes: vec![system_prompt_hash, user_prompt_hash],
            budgets: ExecutionBudgets::default(),
            tools: Vec::new(),
        }
    }

    /// Canonically serialises the definition: compact JSON with fields
    /// in declaration order (guaranteed by serde for structs), the form
    /// the content address hashes. Renaming or reordering a field, or
    /// adding an always-emitted one, changes every binding hash.
    ///
    /// # Errors
    ///
    /// Returns the underlying serde error when serialisation fails.
    pub fn canonical_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

/// One stored, content-addressed binding version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct AgentBinding {
    /// Unique identifier with `abv_` prefix.
    id: AgentBindingVersionId,
    /// The content address over the canonically serialised definition.
    hash: String,
    /// The stage this binding serves.
    pipeline_stage: TaskType,
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
    pub fn pipeline_stage(&self) -> TaskType {
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
                max_child_launches: None,
                execution_deadline_seconds: None,
            }
        ));
    }

    #[test]
    fn test_definition_canonical_serialisation_is_byte_stable() {
        // The content address hashes this exact byte sequence: renaming,
        // reordering, or adding a field is version-breaking, which this
        // snapshot pins byte-for-byte. (`serde_json::to_value` would sort
        // keys and hide a reorder; the canonical string does not.)
        let definition = AgentDefinition::one_shot(
            TaskType::Extraction,
            ProviderKind::Ollama,
            "llama3".to_owned(),
            "http://localhost:11434".to_owned(),
            StageParameters {
                temperature: Some(0.2),
                max_tokens: Some(4096),
            },
            "a".repeat(64),
            "b".repeat(64),
        );

        let expected = format!(
            concat!(
                "{{\"pipeline_stage\":\"extraction\",",
                "\"executor\":\"one_shot\",",
                "\"provider\":\"ollama\",",
                "\"model\":\"llama3\",",
                "\"base_url\":\"http://localhost:11434\",",
                "\"parameters\":{{\"temperature\":0.2,\"max_tokens\":4096}},",
                "\"prompt_hashes\":[\"{a}\",\"{b}\"],",
                "\"budgets\":{{\"max_total_tokens\":null,\"max_turns\":null,\"max_child_launches\":null,\"execution_deadline_seconds\":null}},",
                "\"tools\":[]}}"
            ),
            a = "a".repeat(64),
            b = "b".repeat(64),
        );
        assert_eq!(definition.canonical_json().expect("serialises"), expected,);
    }

    #[test]
    fn test_a_parameter_edit_changes_the_canonical_form() {
        let base = AgentDefinition::one_shot(
            TaskType::Triage,
            ProviderKind::Ollama,
            "llama3".to_owned(),
            "http://localhost:11434".to_owned(),
            StageParameters {
                temperature: Some(0.1),
                max_tokens: Some(2048),
            },
            "a".repeat(64),
            "b".repeat(64),
        );
        let mut edited = base.clone();
        edited.parameters.temperature = Some(0.9);

        assert_ne!(
            base.canonical_json().expect("serialises"),
            edited.canonical_json().expect("serialises"),
        );
    }
}
