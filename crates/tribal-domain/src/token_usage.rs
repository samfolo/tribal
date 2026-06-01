//! Token usage entity — persistent accounting for LLM and embedding calls.
//!
//! Written immediately after each call, regardless of task outcome.
//! `job_id` is null for read-path embedding calls (e.g. discovery queries).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{
    EmbeddingPurpose, JobId, PipelineStage, PromptVersionId, ReindexRunId, TaskId, TaskType,
    TokenUsageId,
};

/// A token usage record for a single LLM or embedding call.
///
/// Records input/output/cache token counts, latency, and links to the
/// originating job, task, and prompt version for cost analysis and
/// performance monitoring.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
pub struct TokenUsage {
    /// Unique identifier with `tu_` prefix.
    id: TokenUsageId,
    /// The job this usage belongs to (null for read-path calls).
    #[builder(default)]
    job_id: Option<JobId>,
    /// The task this usage belongs to.
    #[builder(default)]
    task_id: Option<TaskId>,
    /// The reindex run this usage belongs to (set only for reindex backfill
    /// and catch-up embedding, null otherwise).
    #[builder(default)]
    reindex_run_id: Option<ReindexRunId>,
    /// The attempt number within the task (starts at 0).
    attempt: i32,
    /// Which pipeline stage produced this usage record.
    stage: PipelineStage,
    /// Purpose qualifier for embedding stage. `None` for non-embedding stages.
    #[builder(default)]
    purpose: Option<EmbeddingPurpose>,
    /// The inference provider name.
    provider: String,
    /// The model identifier.
    model: String,
    /// Number of input tokens.
    tokens_input: i32,
    /// Number of output tokens.
    tokens_output: i32,
    /// Number of cache-read tokens (subset of input).
    tokens_cache_read: i32,
    /// Number of cache-write tokens.
    tokens_cache_write: i32,
    /// Total tokens (must equal `tokens_input + tokens_output`).
    tokens_total: i32,
    /// Call latency in milliseconds.
    latency_ms: i32,
    /// The system prompt version used for this call.
    #[builder(default)]
    system_prompt_version_id: Option<PromptVersionId>,
    /// The user prompt version used for this call.
    #[builder(default)]
    user_prompt_version_id: Option<PromptVersionId>,
    /// OpenTelemetry trace identifier.
    #[builder(default)]
    trace_id: Option<String>,
    /// When this usage was recorded.
    created_at: DateTime<Utc>,
}

impl TokenUsage {
    /// Returns the token usage identifier.
    pub fn id(&self) -> TokenUsageId {
        self.id
    }

    /// Returns the job identifier, if applicable.
    pub fn job_id(&self) -> Option<JobId> {
        self.job_id
    }

    /// Returns the task identifier, if applicable.
    pub fn task_id(&self) -> Option<TaskId> {
        self.task_id
    }

    /// Returns the reindex run identifier, if applicable.
    pub fn reindex_run_id(&self) -> Option<ReindexRunId> {
        self.reindex_run_id
    }

    /// Returns the attempt number.
    pub fn attempt(&self) -> i32 {
        self.attempt
    }

    /// Returns the pipeline stage.
    pub fn stage(&self) -> PipelineStage {
        self.stage
    }

    /// Returns the embedding purpose, if applicable.
    pub fn purpose(&self) -> Option<EmbeddingPurpose> {
        self.purpose
    }

    /// Returns the inference provider name.
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Returns the model identifier.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns the number of input tokens.
    pub fn tokens_input(&self) -> i32 {
        self.tokens_input
    }

    /// Returns the number of output tokens.
    pub fn tokens_output(&self) -> i32 {
        self.tokens_output
    }

    /// Returns the number of cache-read tokens.
    pub fn tokens_cache_read(&self) -> i32 {
        self.tokens_cache_read
    }

    /// Returns the number of cache-write tokens.
    pub fn tokens_cache_write(&self) -> i32 {
        self.tokens_cache_write
    }

    /// Returns the total token count.
    pub fn tokens_total(&self) -> i32 {
        self.tokens_total
    }

    /// Returns the call latency in milliseconds.
    pub fn latency_ms(&self) -> i32 {
        self.latency_ms
    }

    /// Returns the system prompt version identifier, if applicable.
    pub fn system_prompt_version_id(&self) -> Option<PromptVersionId> {
        self.system_prompt_version_id
    }

    /// Returns the user prompt version identifier, if applicable.
    pub fn user_prompt_version_id(&self) -> Option<PromptVersionId> {
        self.user_prompt_version_id
    }

    /// Returns the trace identifier, if applicable.
    pub fn trace_id(&self) -> Option<&str> {
        self.trace_id.as_deref()
    }

    /// Returns when this usage was recorded.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

// ---------------------------------------------------------------------------
// Token usage stage
// ---------------------------------------------------------------------------

/// Encodes the `purpose_stage_check` constraint at the type level.
///
/// Embedding usage requires a purpose qualifier; non-embedding stages
/// forbid it.  This enum makes the invalid state unrepresentable.
#[derive(Debug, Clone, Copy)]
pub enum TokenUsageStage {
    /// The extraction pipeline stage.
    Extraction,
    /// The triage pipeline stage.
    Triage,
    /// The relation pipeline stage.
    Relation,
    /// The embedding pipeline stage with a required purpose.
    Embedding {
        /// Whether the embedding was for indexing, querying, or tag resolution.
        purpose: EmbeddingPurpose,
    },
}

impl TokenUsageStage {
    /// Returns the pipeline stage.
    #[must_use]
    pub fn pipeline_stage(&self) -> PipelineStage {
        match self {
            Self::Extraction => PipelineStage::Extraction,
            Self::Triage => PipelineStage::Triage,
            Self::Relation => PipelineStage::Relation,
            Self::Embedding { .. } => PipelineStage::Embedding,
        }
    }

    /// Returns the embedding purpose, if applicable.
    #[must_use]
    pub fn purpose(&self) -> Option<EmbeddingPurpose> {
        match self {
            Self::Embedding { purpose } => Some(*purpose),
            _ => None,
        }
    }
}

impl From<TaskType> for TokenUsageStage {
    fn from(task_type: TaskType) -> Self {
        match task_type {
            TaskType::Extraction => Self::Extraction,
            TaskType::Triage => Self::Triage,
            TaskType::Relation => Self::Relation,
        }
    }
}
