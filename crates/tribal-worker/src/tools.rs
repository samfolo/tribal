//! The stage tool implementations the worker registers with the runtime.
//!
//! Graph semantics live here, behind the runtime's
//! [`StageTool`](tribal_agent_runtime::StageTool) contract: the registry
//! owns name lookup, response trimming, and the wire projection, while
//! these implementations own what the operations mean. Scope is
//! structural — every tool captures its project, job, or thread at
//! construction, so no model-supplied argument can widen what it
//! reaches. Argument faults and misses on named reads return as
//! recoverable failures the model can act on; database and
//! serialisation faults are system failures the conversation never
//! sees.

mod common;
mod triage;

use std::sync::Arc;

pub(crate) use common::{ReadJobContextTool, ReadSiblingThreadsTool};
use serde::{Serialize, de::DeserializeOwned};
pub(crate) use triage::{
    ListTagRegistryTool, ReadItemNeighbourhoodTool, ReadKnowledgeItemTool, SearchSimilarItemsTool,
    submit_result_descriptor,
};
use tribal_agent_runtime::{StageTool, ToolOutcome, ToolRegistry, ToolRegistryError};
use tribal_domain::{
    AgentThreadId, EmbeddingProfile, JobId, ProjectId, ProjectScope, RecoverableToolFailure,
    TaskType, ToolBinding, ToolDescriptor, ToolFailure,
};
use tribal_inference::{InferenceGateway, UsageAttribution};

/// The tool bindings a stage's agentic loop exposes, in wire order: the
/// read inventory name-sorted (the registry's projection), then the
/// distinguished completion tool. A binding-hash input, reach included:
/// reorder the surface, or change a tool's reach, and every binding
/// version for the stage moves. Stage-keyed so a relation loop hashes its
/// own cross-project reach rather than triage's fence.
pub(crate) fn stage_tool_bindings(stage: TaskType) -> Vec<ToolBinding> {
    match stage {
        TaskType::Triage => triage_tool_bindings(),
        // Relation and extraction run one-shot until their loops land; a
        // one-shot binding carries no tools.
        TaskType::Relation | TaskType::Extraction => Vec::new(),
    }
}

/// The triage surface: every tool fenced to the candidate's project, so
/// dedup stays project-local by construction rather than by prompt.
fn triage_tool_bindings() -> Vec<ToolBinding> {
    let mut reads = vec![
        SearchSimilarItemsTool::describe(),
        ReadKnowledgeItemTool::describe(),
        ReadItemNeighbourhoodTool::describe(),
        ListTagRegistryTool::describe(),
        ReadJobContextTool::describe(),
        ReadSiblingThreadsTool::describe(),
    ];
    reads.sort_by(|a, b| a.name.cmp(&b.name));
    reads.push(submit_result_descriptor());
    reads.into_iter().map(fenced).collect()
}

/// Grants a descriptor the project-local fence.
fn fenced(descriptor: ToolDescriptor) -> ToolBinding {
    ToolBinding {
        descriptor,
        project_scope: ProjectScope::Fenced,
    }
}

/// The runtime context a triage tool registry is built against: the
/// project the reads fence to, the embedding identity the search embeds
/// under, and the ids that scope the job-context and sibling reads.
pub(crate) struct TriageToolset {
    /// The project the reads are fenced to.
    pub project_id: ProjectId,
    /// The active embedding profile the candidate search embeds under.
    pub profile: EmbeddingProfile,
    /// The gateway the search tool embeds through.
    pub gateway: Arc<InferenceGateway>,
    /// The attribution the search's embedding calls meter to.
    pub attribution: UsageAttribution,
    /// The candidate-result cap the search tool returns.
    pub search_limit: u32,
    /// The deadline the search tool's embedding call honours.
    pub deadline: tokio::time::Instant,
    /// The job whose context and siblings the reads scope to.
    pub job_id: JobId,
    /// The candidate's batch index within the job.
    pub batch_index: u32,
    /// The thread the sibling read excludes (its own).
    pub thread_id: AgentThreadId,
}

/// Builds the triage stage's live tool registry: the same surface
/// [`stage_tool_bindings`] hashes, instantiated with the runtime scope
/// each tool captures at construction. The lockstep test pins the two
/// against drift; this is the only constructor the loop uses.
///
/// # Errors
///
/// Returns [`ToolRegistryError`] when a tool declares an inadmissible
/// tier or mode, or two tools contest a name.
pub(crate) fn build_triage_registry(
    toolset: &TriageToolset,
) -> Result<ToolRegistry, ToolRegistryError> {
    let mut registry = ToolRegistry::new();
    let reads: [Arc<dyn StageTool>; 6] = [
        Arc::new(SearchSimilarItemsTool::new(
            toolset.project_id,
            toolset.profile.clone(),
            Arc::clone(&toolset.gateway),
            toolset.attribution.clone(),
            toolset.search_limit,
            toolset.deadline,
        )),
        Arc::new(ReadKnowledgeItemTool::new(toolset.project_id)),
        Arc::new(ReadItemNeighbourhoodTool::new(toolset.project_id)),
        Arc::new(ListTagRegistryTool::new()),
        Arc::new(ReadJobContextTool::new(toolset.job_id, toolset.batch_index)),
        Arc::new(ReadSiblingThreadsTool::new(
            toolset.job_id,
            toolset.thread_id,
        )),
    ];
    for tool in reads {
        registry.register(tool)?;
    }
    Ok(registry)
}

/// Parses a tool's model-supplied arguments, treating an absent
/// arguments object as empty and rendering the expected shape into the
/// recoverable diagnostic on failure.
fn parse_arguments<T: DeserializeOwned>(
    tool: &str,
    arguments: &serde_json::Value,
    expected: &str,
) -> Result<T, ToolFailure> {
    let arguments = match arguments {
        serde_json::Value::Null => serde_json::Value::Object(serde_json::Map::new()),
        other => other.clone(),
    };
    serde_json::from_value(arguments).map_err(|source| {
        ToolFailure::Recoverable(RecoverableToolFailure::InvalidArguments {
            tool: tool.to_owned(),
            detail: format!("{source}; expected {expected}"),
        })
    })
}

/// Serialises a tool's result payload into the outcome's content.
fn serialise_outcome<T: Serialize>(context: &str, payload: &T) -> Result<ToolOutcome, ToolFailure> {
    serde_json::to_string(payload)
        .map(|content| ToolOutcome { content })
        .map_err(|source| ToolFailure::System {
            context: format!("{context}: {source}"),
        })
}

/// Maps a repository failure into the system class: it routes to the
/// stage-error path, never the conversation.
fn db_failure(context: &str, source: &tribal_db::DbError) -> ToolFailure {
    ToolFailure::System {
        context: format!("{context}: {source}"),
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use serde::Deserialize;
    use tribal_domain::UsageOwner;
    use tribal_inference::{
        InjectedProviders, NoopLedgerSink, ProviderKey, ProviderLimits, ProviderRegistry,
        RequestClass,
    };
    use tribal_test_utils::{MockInferenceProvider, an_embedding_profile};

    use super::*;

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct NoArguments {}

    #[test]
    fn test_parse_arguments_treats_null_as_an_empty_object() {
        // Providers deliver a no-argument call as null or {}; both must
        // satisfy a tool that takes nothing.
        let parsed: Result<NoArguments, _> =
            parse_arguments("a_tool", &serde_json::Value::Null, "no arguments");
        assert!(parsed.is_ok());

        let err = parse_arguments::<NoArguments>(
            "a_tool",
            &serde_json::json!({"unexpected": 1}),
            "no arguments",
        )
        .expect_err("unknown fields are argument faults");
        assert!(matches!(
            err,
            ToolFailure::Recoverable(RecoverableToolFailure::InvalidArguments { ref tool, .. })
                if tool == "a_tool"
        ));
    }

    /// A triage toolset over a mock gateway, the fixtures both the
    /// clean-registration and lockstep tests build their registry from.
    fn a_triage_toolset() -> TriageToolset {
        let inference_key =
            ProviderKey::new("mock", "http://localhost:9999", RequestClass::Inference)
                .expect("valid key");
        let registry = ProviderRegistry::new(vec![(
            inference_key.clone(),
            ProviderLimits {
                max_in_flight: 1,
                request_timeout: Duration::from_secs(1),
            },
        )])
        .expect("valid registry");
        let gateway = Arc::new(InferenceGateway::with_providers(
            InjectedProviders::uniform(
                registry,
                Arc::new(MockInferenceProvider::builder().build()),
                inference_key,
                vec![],
                Arc::new(NoopLedgerSink),
            ),
        ));
        TriageToolset {
            project_id: ProjectId::new(),
            profile: an_embedding_profile().build(),
            gateway,
            attribution: UsageAttribution {
                owner: UsageOwner::Unowned,
                system_prompt_version_id: None,
                user_prompt_version_id: None,
                trace_id: None,
            },
            search_limit: 10,
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            job_id: JobId::new(),
            batch_index: 0,
            thread_id: AgentThreadId::new(),
        }
    }

    #[test]
    fn test_the_triage_inventory_registers_cleanly() {
        // One registration pass over the whole read inventory pins that
        // every descriptor declares an admissible tier and mode and that
        // no two tools contest a name.
        let registry = build_triage_registry(&a_triage_toolset()).expect("the inventory registers");
        let names: Vec<String> = registry
            .descriptors()
            .iter()
            .map(|descriptor| descriptor.name.clone())
            .collect();
        assert_eq!(
            names,
            vec![
                "list_tag_registry",
                "read_item_neighbourhood",
                "read_job_context",
                "read_knowledge_item",
                "read_sibling_threads",
                "search_similar_items",
            ],
        );
    }

    #[test]
    fn test_aggregator_and_registry_descriptor_sets_match() {
        // The hashed surface and the live registry are two hand-maintained
        // lists; a tool added to one and not the other silently breaks
        // attribution (an unhashed tool) or registration (an unbuilt one).
        // The submit descriptor is the binding's completion contract, not a
        // registered read, so the registry omits it by construction.
        let aggregated: Vec<ToolDescriptor> = stage_tool_bindings(TaskType::Triage)
            .into_iter()
            .map(|binding| binding.descriptor)
            .filter(|descriptor| descriptor.name != submit_result_descriptor().name)
            .collect();
        let built = build_triage_registry(&a_triage_toolset()).expect("the inventory registers");
        assert_eq!(
            aggregated,
            built.descriptors(),
            "the hashed surface and the live registry must agree, same order",
        );
    }

    #[test]
    fn test_every_triage_tool_is_fenced() {
        // Triage reads never cross projects: the fence is the dedup
        // invariant, hashed so a future cross-project slip moves the
        // binding version rather than passing silently.
        assert!(
            stage_tool_bindings(TaskType::Triage)
                .iter()
                .all(|binding| binding.project_scope == tribal_domain::ProjectScope::Fenced),
        );
    }
}
