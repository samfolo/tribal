use chrono::Utc;
use tribal_domain::{JobId, PipelineStage, PromptVersionId, TaskId, TokenUsage, TokenUsageId};

define_factory! {
    /// Factory for [`TokenUsage`] instances.
    pub struct TokenUsageFactory for TokenUsage {
        id: TokenUsageId = TokenUsageId::new(),
        job_id: Option<JobId> = None,
        task_id: Option<TaskId> = None,
        attempt: i32 = 0,
        stage: PipelineStage = PipelineStage::Extraction,
        purpose: Option<tribal_domain::EmbeddingPurpose> = None,
        provider: String = "test-provider".to_owned(),
        model: String = "test-model".to_owned(),
        tokens_input: i32 = 100,
        tokens_output: i32 = 50,
        tokens_cache_read: i32 = 0,
        tokens_cache_write: i32 = 0,
        tokens_total: i32 = 150,
        latency_ms: i32 = 200,
        prompt_version_id: Option<PromptVersionId> = None,
        trace_id: Option<String> = None,
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`TokenUsageFactory`] with sensible defaults.
pub fn a_token_usage() -> TokenUsageFactory {
    TokenUsageFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let tu = a_token_usage().build();
        assert_eq!(tu.stage(), PipelineStage::Extraction);
        assert_eq!(tu.tokens_total(), 150);
    }
}
