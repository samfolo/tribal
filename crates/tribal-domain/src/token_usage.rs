//! Token usage entity — persistent accounting for LLM and embedding calls.
//!
//! Written immediately after each call, regardless of task outcome.
//! `job_id` is null for read-path embedding calls (e.g. discovery queries).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{JobId, PromptVersionId, TaskId, TokenUsageId};

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
    /// The attempt number within the task (starts at 0).
    attempt: i32,
    /// Pipeline stage: `"extraction"`, `"triage"`, `"relation"`, or `"embedding"`.
    stage: String,
    /// Purpose qualifier for embedding stage: `"candidate"` or `"query"`.
    /// Null for non-embedding stages.
    #[builder(default)]
    purpose: Option<String>,
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
    /// The prompt version used for this call.
    #[builder(default)]
    prompt_version_id: Option<PromptVersionId>,
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

    /// Returns the attempt number.
    pub fn attempt(&self) -> i32 {
        self.attempt
    }

    /// Returns the pipeline stage.
    pub fn stage(&self) -> &str {
        &self.stage
    }

    /// Returns the purpose qualifier, if applicable.
    pub fn purpose(&self) -> Option<&str> {
        self.purpose.as_deref()
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

    /// Returns the prompt version identifier, if applicable.
    pub fn prompt_version_id(&self) -> Option<PromptVersionId> {
        self.prompt_version_id
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
