//! The Postgres-backed ledger sink.
//!
//! Implements the gateway's [`LedgerSink`] port: one `token_usage` row and
//! the `GenAI` client metrics per request, from the same data, so the
//! ledger and the telemetry cannot disagree. Writes are best-effort on a
//! freshly acquired connection, never inside a domain commit, and never
//! failing the request being recorded.

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::PgPool;
use tribal_common::clamp_to_i32;
use tribal_db::{NewTokenUsage, PgTokenUsageRepository, TokenUsageRepository};
use tribal_domain::{TokenUsageStage, Usage, gen_ai};
use tribal_inference::{LedgerSink, UsageAttribution};
use tribal_telemetry::{InferenceOperationRecord, MetricsRecorder, current_trace_id};

/// Records gateway usage into the `token_usage` ledger and the `GenAI`
/// client metric instruments.
pub struct PgLedgerSink {
    pool: PgPool,
    metrics: Arc<dyn MetricsRecorder>,
}

impl PgLedgerSink {
    /// Creates a sink over the given pool and metric recorder.
    #[must_use]
    pub fn new(pool: PgPool, metrics: Arc<dyn MetricsRecorder>) -> Self {
        Self { pool, metrics }
    }

    /// Builds the ledger row for one usage record.
    ///
    /// Prompt versions attach to completion rows only; an embedding call
    /// has no prompt. The trace falls back to the live span's identifier
    /// for calls whose attribution carries none.
    fn new_token_usage(
        usage: &Usage,
        stage: TokenUsageStage,
        attribution: &UsageAttribution,
    ) -> NewTokenUsage {
        let trace_id = attribution.trace_id.clone().or_else(current_trace_id);

        let owner = &attribution.owner;
        match usage {
            Usage::Completion { usage: cu } => NewTokenUsage::builder()
                .job_id(owner.job_id())
                .task_id(owner.task_id())
                .reindex_run_id(owner.reindex_run_id())
                .agent_thread_id(owner.agent_thread_id())
                .agent_thread_record_id(owner.agent_thread_record_id())
                .attempt(owner.attempt())
                .principal_id(attribution.principal_id)
                .stage(stage)
                .provider(cu.provider.clone())
                .model(cu.model.clone())
                .tokens_input(clamp_to_i32(cu.input_tokens))
                .tokens_output(clamp_to_i32(cu.output_tokens))
                .tokens_cache_read(clamp_to_i32(cu.cache_read_tokens))
                .tokens_cache_write(clamp_to_i32(cu.cache_write_tokens))
                .latency_ms(clamp_to_i32(cu.latency.as_millis()))
                .system_prompt_version_id(attribution.system_prompt_version_id)
                .user_prompt_version_id(attribution.user_prompt_version_id)
                .trace_id(trace_id)
                .build(),
            Usage::Embedding { usage: eu, .. } => NewTokenUsage::builder()
                .job_id(owner.job_id())
                .task_id(owner.task_id())
                .reindex_run_id(owner.reindex_run_id())
                .agent_thread_id(owner.agent_thread_id())
                .agent_thread_record_id(owner.agent_thread_record_id())
                .attempt(owner.attempt())
                .principal_id(attribution.principal_id)
                .stage(stage)
                .provider(eu.provider.clone())
                .model(eu.model.clone())
                .tokens_input(clamp_to_i32(eu.total_tokens))
                .tokens_output(0)
                .latency_ms(clamp_to_i32(eu.latency.as_millis()))
                .trace_id(trace_id)
                .build(),
        }
    }

    /// Builds the metric record for one usage record.
    fn operation_record(usage: &Usage, stage: TokenUsageStage) -> InferenceOperationRecord<'_> {
        match usage {
            Usage::Completion { usage: cu } => InferenceOperationRecord {
                operation: gen_ai::OPERATION_CHAT,
                provider: &cu.provider,
                model: &cu.model,
                stage: stage.pipeline_stage().as_str(),
                purpose: None,
                duration: cu.latency,
                input_tokens: u64::from(cu.input_tokens),
                output_tokens: u64::from(cu.output_tokens),
            },
            Usage::Embedding { usage: eu, purpose } => InferenceOperationRecord {
                operation: gen_ai::OPERATION_EMBEDDINGS,
                provider: &eu.provider,
                model: &eu.model,
                stage: stage.pipeline_stage().as_str(),
                purpose: Some(purpose.as_str()),
                duration: eu.latency,
                input_tokens: u64::from(eu.total_tokens),
                output_tokens: 0,
            },
        }
    }
}

#[async_trait]
impl LedgerSink for PgLedgerSink {
    async fn record_usage(
        &self,
        usage: &Usage,
        stage: TokenUsageStage,
        attribution: &UsageAttribution,
    ) {
        self.metrics
            .record_inference_operation(&Self::operation_record(usage, stage));

        let mut conn = match self.pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to acquire connection for token usage recording",
                );
                return;
            }
        };

        let new = Self::new_token_usage(usage, stage, attribution);
        match PgTokenUsageRepository.insert(&mut conn, &new).await {
            Ok(recorded) => {
                tracing::debug!(
                    stage = %stage.pipeline_stage(),
                    tokens_total = recorded.tokens_total(),
                    latency_ms = recorded.latency_ms(),
                    "token usage recorded",
                );
            }
            Err(e) => {
                tracing::warn!(
                    stage = %stage.pipeline_stage(),
                    error = %e,
                    "failed to record token usage",
                );
            }
        }
    }

    fn record_semaphore_wait(&self, provider_key: &str, wait: std::time::Duration) {
        self.metrics.record_semaphore_acquire(provider_key, wait);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tribal_domain::{
        AgentThreadId, AgentThreadRecordId, CompletionUsage, EmbeddingPurpose, EmbeddingUsage,
        JobId, PrincipalId, ReindexRunId, TaskId, UsageOwner,
    };

    use super::*;

    fn a_pipeline_owner(attempt: i32) -> UsageOwner {
        UsageOwner::Pipeline {
            job_id: JobId::new(),
            task_id: TaskId::new(),
            thread_id: AgentThreadId::new(),
            record_id: Some(AgentThreadRecordId::new()),
            attempt,
        }
    }

    fn a_completion_usage() -> Usage {
        Usage::Completion {
            usage: CompletionUsage {
                provider: "ollama".to_owned(),
                model: "llama3".to_owned(),
                input_tokens: 100,
                output_tokens: 50,
                cache_read_tokens: 10,
                cache_write_tokens: 20,
                total_tokens: 150,
                latency: Duration::from_millis(500),
            },
        }
    }

    fn an_embedding_usage() -> Usage {
        Usage::Embedding {
            usage: EmbeddingUsage {
                provider: "ollama".to_owned(),
                model: "nomic-embed-text:v1.5".to_owned(),
                total_tokens: 25,
                latency: Duration::from_millis(80),
            },
            purpose: EmbeddingPurpose::Query,
        }
    }

    #[test]
    fn test_completion_row_carries_full_attribution() {
        let owner = a_pipeline_owner(3);
        let principal_id = PrincipalId::new();
        let attribution = UsageAttribution {
            owner,
            principal_id: Some(principal_id),
            trace_id: Some("trace-9".to_owned()),
            ..UsageAttribution::default()
        };

        let row = PgLedgerSink::new_token_usage(
            &a_completion_usage(),
            TokenUsageStage::Extraction,
            &attribution,
        );

        assert_eq!(row.principal_id, Some(principal_id));
        assert_eq!(row.job_id, owner.job_id());
        assert_eq!(row.task_id, owner.task_id());
        assert_eq!(row.agent_thread_id, owner.agent_thread_id());
        assert_eq!(row.agent_thread_record_id, owner.agent_thread_record_id());
        assert!(row.agent_thread_record_id.is_some());
        assert_eq!(row.attempt, 3);
        assert_eq!(row.tokens_input, 100);
        assert_eq!(row.tokens_output, 50);
        assert_eq!(row.tokens_cache_read, 10);
        assert_eq!(row.tokens_cache_write, 20);
        assert_eq!(row.latency_ms, 500);
        assert_eq!(row.trace_id.as_deref(), Some("trace-9"));
    }

    #[test]
    fn test_reindex_row_carries_the_run_column_alone() {
        let run_id = ReindexRunId::new();
        let attribution = UsageAttribution {
            owner: UsageOwner::Reindex { run_id },
            ..UsageAttribution::default()
        };

        let row = PgLedgerSink::new_token_usage(
            &an_embedding_usage(),
            TokenUsageStage::Embedding {
                purpose: EmbeddingPurpose::Candidate,
            },
            &attribution,
        );

        assert_eq!(row.reindex_run_id, Some(run_id));
        assert_eq!(row.job_id, None);
        assert_eq!(row.task_id, None);
        assert_eq!(row.agent_thread_id, None);
        assert_eq!(row.agent_thread_record_id, None);
        assert_eq!(row.attempt, 0);
    }

    #[test]
    fn test_embedding_row_never_carries_prompt_versions() {
        // An embedding call has no prompt, so even an attribution that
        // names prompt versions (a stage attribution shared across the
        // stage's calls) must not attach them to an embedding row.
        let attribution = UsageAttribution {
            owner: a_pipeline_owner(0),
            system_prompt_version_id: Some(tribal_domain::PromptVersionId::new()),
            user_prompt_version_id: Some(tribal_domain::PromptVersionId::new()),
            ..UsageAttribution::default()
        };

        let row = PgLedgerSink::new_token_usage(
            &an_embedding_usage(),
            TokenUsageStage::Embedding {
                purpose: EmbeddingPurpose::Query,
            },
            &attribution,
        );

        assert_eq!(row.system_prompt_version_id, None);
        assert_eq!(row.user_prompt_version_id, None);
        assert_eq!(row.tokens_input, 25);
        assert_eq!(row.tokens_output, 0);
    }

    #[test]
    fn test_metric_record_carries_purpose_for_embeddings() {
        let usage = an_embedding_usage();
        let record = PgLedgerSink::operation_record(
            &usage,
            TokenUsageStage::Embedding {
                purpose: EmbeddingPurpose::Query,
            },
        );

        assert_eq!(record.operation, gen_ai::OPERATION_EMBEDDINGS);
        assert_eq!(record.stage, "embedding");
        assert_eq!(record.purpose, Some("query"));
        assert_eq!(record.input_tokens, 25);
        assert_eq!(record.output_tokens, 0);
    }

    #[test]
    fn test_metric_record_for_completions_has_no_purpose() {
        let usage = a_completion_usage();
        let record = PgLedgerSink::operation_record(&usage, TokenUsageStage::Probe);

        assert_eq!(record.operation, gen_ai::OPERATION_CHAT);
        assert_eq!(record.stage, "probe");
        assert_eq!(record.purpose, None);
        assert_eq!(record.input_tokens, 100);
        assert_eq!(record.output_tokens, 50);
    }
}
