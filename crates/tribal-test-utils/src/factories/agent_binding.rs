use chrono::Utc;
use tribal_domain::{
    AgentBinding, AgentBindingVersionId, AgentDefinition, ExecutionBudgets, PipelineStage,
    ProviderKind, StageExecutorKind, ToolDescriptor,
};

/// A well-formed placeholder content address (64 hex characters).
const PLACEHOLDER_HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

define_factory! {
    /// Factory for [`AgentDefinition`] instances.
    pub struct AgentDefinitionFactory for AgentDefinition {
        pipeline_stage: PipelineStage = PipelineStage::Extraction,
        executor: StageExecutorKind = StageExecutorKind::OneShot,
        provider: ProviderKind = ProviderKind::Ollama,
        model: String = "llama3".to_owned(),
        base_url: String = "http://localhost:11434".to_owned(),
        prompt_hashes: Vec<String> = vec![PLACEHOLDER_HASH.to_owned()],
        budgets: ExecutionBudgets = ExecutionBudgets::default(),
        tools: Vec<ToolDescriptor> = Vec::new(),
    }
}

/// Returns an [`AgentDefinitionFactory`] with sensible defaults.
pub fn an_agent_definition() -> AgentDefinitionFactory {
    AgentDefinitionFactory::new()
}

define_factory! {
    /// Factory for [`AgentBinding`] instances.
    pub struct AgentBindingFactory for AgentBinding {
        id: AgentBindingVersionId = AgentBindingVersionId::new(),
        hash: String = PLACEHOLDER_HASH.to_owned(),
        pipeline_stage: PipelineStage = PipelineStage::Extraction,
        definition: AgentDefinition = an_agent_definition().build(),
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns an [`AgentBindingFactory`] with sensible defaults.
pub fn an_agent_binding() -> AgentBindingFactory {
    AgentBindingFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let binding = an_agent_binding().build();
        assert_eq!(binding.pipeline_stage(), PipelineStage::Extraction);
        assert_eq!(binding.hash().len(), 64);
        assert!(matches!(
            binding.definition().executor,
            StageExecutorKind::OneShot
        ));
    }
}
