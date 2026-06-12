use chrono::Utc;
use tribal_db::NewTokenUsage;
use tribal_domain::{
    AgentThreadId, AgentThreadRecordId, JobId, PipelineStage, PromptVersionId, ReindexRunId,
    TaskId, TokenUsage, TokenUsageId, TokenUsageStage,
};

define_factory! {
    /// Factory for [`TokenUsage`] instances.
    pub struct TokenUsageFactory for TokenUsage {
        id: TokenUsageId = TokenUsageId::new(),
        job_id: Option<JobId> = None,
        task_id: Option<TaskId> = None,
        reindex_run_id: Option<ReindexRunId> = None,
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
        system_prompt_version_id: Option<PromptVersionId> = None,
        user_prompt_version_id: Option<PromptVersionId> = None,
        trace_id: Option<String> = None,
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`TokenUsageFactory`] with sensible defaults.
pub fn a_token_usage() -> TokenUsageFactory {
    TokenUsageFactory::new()
}

define_factory! {
    /// Factory for [`NewTokenUsage`] instances used in repository
    /// insert operations.
    pub struct NewTokenUsageFactory for NewTokenUsage {
        job_id: Option<JobId> = None,
        task_id: Option<TaskId> = None,
        reindex_run_id: Option<ReindexRunId> = None,
        agent_thread_id: Option<AgentThreadId> = None,
        agent_thread_record_id: Option<AgentThreadRecordId> = None,
        attempt: i32 = 0,
        stage: TokenUsageStage = TokenUsageStage::Extraction,
        provider: String = "test-provider".to_owned(),
        model: String = "test-model".to_owned(),
        tokens_input: i32 = 100,
        tokens_output: i32 = 50,
        tokens_cache_read: i32 = 0,
        tokens_cache_write: i32 = 0,
        latency_ms: i32 = 200,
        system_prompt_version_id: Option<PromptVersionId> = None,
        user_prompt_version_id: Option<PromptVersionId> = None,
        trace_id: Option<String> = None,
    }
}

/// Returns a [`NewTokenUsageFactory`] with sensible defaults.
pub fn a_new_token_usage() -> NewTokenUsageFactory {
    NewTokenUsageFactory::new()
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

    #[test]
    fn test_new_builds_with_defaults() {
        let new = a_new_token_usage().build();
        assert!(new.job_id.is_none());
        assert!(new.task_id.is_none());
        assert_eq!(new.attempt, 0);
        assert_eq!(new.provider, "test-provider");
        assert_eq!(new.model, "test-model");
        assert_eq!(new.tokens_input, 100);
        assert_eq!(new.tokens_output, 50);
        assert_eq!(new.tokens_cache_read, 0);
        assert_eq!(new.tokens_cache_write, 0);
        assert_eq!(new.latency_ms, 200);
        assert!(new.system_prompt_version_id.is_none());
        assert!(new.user_prompt_version_id.is_none());
        assert!(new.trace_id.is_none());
    }
}
