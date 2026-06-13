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

pub(crate) use common::{ReadJobContextTool, ReadSiblingThreadsTool};
use serde::{Serialize, de::DeserializeOwned};
pub(crate) use triage::{
    ListTagRegistryTool, ReadItemNeighbourhoodTool, ReadKnowledgeItemTool, SearchSimilarItemsTool,
    submit_result_descriptor,
};
use tribal_agent_runtime::ToolOutcome;
use tribal_domain::{RecoverableToolFailure, ToolDescriptor, ToolFailure};

/// The triage stage's declared tool surface, in the wire's order: the
/// read inventory name-sorted (the registry's projection), then the
/// distinguished completion tool. A binding-hash input — reorder it and
/// every triage binding version moves.
pub(crate) fn triage_tool_descriptors() -> Vec<ToolDescriptor> {
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
    reads
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
    use tribal_agent_runtime::ToolRegistry;
    use tribal_domain::{JobId, ProjectId, UsageOwner};
    use tribal_inference::{
        InferenceGateway, InjectedProviders, NoopLedgerSink, ProviderKey, ProviderLimits,
        ProviderRegistry, RequestClass, UsageAttribution,
    };
    use tribal_test_utils::{MockInferenceProvider, an_embedding_profile};

    use super::{
        common::{ReadJobContextTool, ReadSiblingThreadsTool},
        triage::{
            ListTagRegistryTool, ReadItemNeighbourhoodTool, ReadKnowledgeItemTool,
            SearchSimilarItemsTool,
        },
        *,
    };

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

    #[test]
    fn test_the_triage_inventory_registers_cleanly() {
        // One registration pass over the whole read inventory pins that
        // every descriptor declares an admissible tier and mode and that
        // no two tools contest a name.
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
        let profile = an_embedding_profile().build();
        let attribution = UsageAttribution {
            owner: UsageOwner::Unowned,
            system_prompt_version_id: None,
            user_prompt_version_id: None,
            trace_id: None,
        };
        let project_id = ProjectId::new();
        let job_id = JobId::new();
        let thread_id = tribal_domain::AgentThreadId::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);

        let mut tools = ToolRegistry::new();
        tools
            .register(Arc::new(SearchSimilarItemsTool::new(
                project_id,
                profile,
                gateway,
                attribution,
                10,
                deadline,
            )))
            .expect("search registers");
        tools
            .register(Arc::new(ReadKnowledgeItemTool::new(project_id)))
            .expect("item read registers");
        tools
            .register(Arc::new(ReadItemNeighbourhoodTool::new(project_id)))
            .expect("neighbourhood registers");
        tools
            .register(Arc::new(ListTagRegistryTool::new()))
            .expect("tag registry registers");
        tools
            .register(Arc::new(ReadJobContextTool::new(job_id, 0)))
            .expect("job context registers");
        tools
            .register(Arc::new(ReadSiblingThreadsTool::new(job_id, thread_id)))
            .expect("sibling reader registers");

        let names: Vec<String> = tools
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
}
