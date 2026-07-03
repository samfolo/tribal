//! Token usage entity — persistent accounting for LLM and embedding calls.
//!
//! Written immediately after each call, regardless of task outcome.
//! `job_id` is null for read-path embedding calls (e.g. discovery queries).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{
    AgentThreadId, AgentThreadRecordId, EmbeddingPurpose, ExecutionLocus, JobId, PipelineStage,
    PrincipalId, PromptVersionId, ReindexRunId, TaskId, TaskType, TokenUsageId,
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
    /// The contributing principal, when the meter attributed the spend to one.
    /// `None` for system work and calls made before a principal binds.
    #[builder(default)]
    principal_id: Option<PrincipalId>,
    /// Where the metered work ran — the edge runtime locally, or the managed
    /// runtime elsewhere.
    #[builder(default)]
    execution_locus: ExecutionLocus,
    /// The reindex run this usage belongs to (set only for reindex backfill
    /// and catch-up embedding, null otherwise).
    #[builder(default)]
    reindex_run_id: Option<ReindexRunId>,
    /// The agent thread the request ran under, when thread-attributed.
    #[builder(default)]
    agent_thread_id: Option<AgentThreadId>,
    /// The committed record the request produced, when one committed.
    #[builder(default)]
    agent_thread_record_id: Option<AgentThreadRecordId>,
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

    /// Returns the contributing principal, if the spend was attributed.
    pub fn principal_id(&self) -> Option<PrincipalId> {
        self.principal_id
    }

    /// Returns where the metered work ran.
    pub fn execution_locus(&self) -> ExecutionLocus {
        self.execution_locus
    }

    /// Returns the reindex run identifier, if applicable.
    pub fn reindex_run_id(&self) -> Option<ReindexRunId> {
        self.reindex_run_id
    }

    /// Returns the agent thread the request ran under, when
    /// thread-attributed.
    pub fn agent_thread_id(&self) -> Option<AgentThreadId> {
        self.agent_thread_id
    }

    /// Returns the committed record the request produced, when one
    /// committed.
    pub fn agent_thread_record_id(&self) -> Option<AgentThreadRecordId> {
        self.agent_thread_record_id
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
// Usage totals
// ---------------------------------------------------------------------------

/// Summed token spend for one attribution scope at one execution locus.
///
/// Per-user and per-account aggregation groups by [`ExecutionLocus`], so a
/// scope's edge spend and managed spend stay separable — the ledger keeps what
/// the user ran locally distinct from the managed enrichment run for them. The
/// summed counts are `i64` because Postgres `SUM`/`COUNT` widen to `bigint`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageTotals {
    /// Where the summed work ran.
    pub execution_locus: ExecutionLocus,
    /// Number of usage records in this scope-and-locus group.
    pub records: i64,
    /// Summed input tokens.
    pub tokens_input: i64,
    /// Summed output tokens.
    pub tokens_output: i64,
    /// Summed cache-read tokens.
    pub tokens_cache_read: i64,
    /// Summed cache-write tokens.
    pub tokens_cache_write: i64,
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
        /// What the embedding served.
        purpose: EmbeddingPurpose,
    },
    /// An inference provider probe — a real but minimal completion call
    /// made to verify reachability. Embedding probes ride
    /// [`Self::Embedding`] with [`EmbeddingPurpose::Probe`].
    Probe,
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
            Self::Probe => PipelineStage::Probe,
        }
    }

    /// Returns the embedding purpose, if applicable.
    #[must_use]
    pub fn purpose(&self) -> Option<EmbeddingPurpose> {
        match self {
            Self::Embedding { purpose } => Some(*purpose),
            Self::Extraction | Self::Triage | Self::Relation | Self::Probe => None,
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

// ---------------------------------------------------------------------------
// Usage owner
// ---------------------------------------------------------------------------

/// The owner a usage row attributes spend to.
///
/// One variant per legitimate identifier combination, so a row claiming
/// two owners at once (a job and a reindex run, say) is unrepresentable
/// at this level. The flat ledger columns derive from the accessors, the
/// single mapping point between owners and columns.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum UsageOwner {
    /// No owner: spend with no job, run, or thread to attribute to, such
    /// as read-path queries, provider probes, and startup backfills.
    #[default]
    Unowned,
    /// A pipeline stage execution: the task it runs, that task's job, and
    /// the durable thread the execution runs under.
    Pipeline {
        /// The job the stage execution belongs to.
        job_id: JobId,
        /// The task driving the stage execution.
        task_id: TaskId,
        /// The thread the execution runs under.
        thread_id: AgentThreadId,
        /// The committed record the request produced, attributed after
        /// the fact by sinks that learn it; callers issuing the request
        /// leave it unset.
        record_id: Option<AgentThreadRecordId>,
        /// The attempt number within the task (starts at 0).
        attempt: i32,
    },
    /// A reindex run's backfill or catch-up embedding.
    Reindex {
        /// The run the embedding belongs to.
        run_id: ReindexRunId,
    },
    /// A driver-driven thread's execution (a child run): no stage task to
    /// reference, so the thread carries the attribution, with its job
    /// captured at launch so child spend meters to the job without a
    /// lineage walk.
    Thread {
        /// The job the child's run belongs to, captured at launch; absent
        /// for a thread launched outside a pipeline job.
        job_id: Option<JobId>,
        /// The thread the execution runs under.
        thread_id: AgentThreadId,
        /// The committed record the request produced, attributed after
        /// the fact by sinks that learn it; callers issuing the request
        /// leave it unset.
        record_id: Option<AgentThreadRecordId>,
        /// The driving driver task's attempt number (starts at 0); scopes
        /// after-the-fact record links so a reclaimed attempt's orphaned
        /// rows stay thread-attributed.
        attempt: i32,
    },
}

impl UsageOwner {
    /// Returns the job column value for this owner.
    #[must_use]
    pub fn job_id(&self) -> Option<JobId> {
        match self {
            Self::Pipeline { job_id, .. } => Some(*job_id),
            Self::Thread { job_id, .. } => *job_id,
            Self::Unowned | Self::Reindex { .. } => None,
        }
    }

    /// Returns the task column value for this owner.
    #[must_use]
    pub fn task_id(&self) -> Option<TaskId> {
        match self {
            Self::Pipeline { task_id, .. } => Some(*task_id),
            Self::Unowned | Self::Reindex { .. } | Self::Thread { .. } => None,
        }
    }

    /// Returns the reindex-run column value for this owner.
    #[must_use]
    pub fn reindex_run_id(&self) -> Option<ReindexRunId> {
        match self {
            Self::Reindex { run_id } => Some(*run_id),
            Self::Unowned | Self::Pipeline { .. } | Self::Thread { .. } => None,
        }
    }

    /// Returns the thread column value for this owner.
    #[must_use]
    pub fn agent_thread_id(&self) -> Option<AgentThreadId> {
        match self {
            Self::Pipeline { thread_id, .. } | Self::Thread { thread_id, .. } => Some(*thread_id),
            Self::Unowned | Self::Reindex { .. } => None,
        }
    }

    /// Returns the record column value for this owner.
    #[must_use]
    pub fn agent_thread_record_id(&self) -> Option<AgentThreadRecordId> {
        match self {
            Self::Pipeline { record_id, .. } | Self::Thread { record_id, .. } => *record_id,
            Self::Unowned | Self::Reindex { .. } => None,
        }
    }

    /// Returns the attempt column value for this owner (0 outside a task).
    #[must_use]
    pub fn attempt(&self) -> i32 {
        match self {
            Self::Pipeline { attempt, .. } | Self::Thread { attempt, .. } => *attempt,
            Self::Unowned | Self::Reindex { .. } => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_owner_maps_thread_columns_only() {
        let thread_id = AgentThreadId::new();
        let owner = UsageOwner::Thread {
            job_id: None,
            thread_id,
            record_id: None,
            attempt: 2,
        };

        assert_eq!(owner.job_id(), None);
        assert_eq!(owner.task_id(), None);
        assert_eq!(owner.reindex_run_id(), None);
        assert_eq!(owner.agent_thread_id(), Some(thread_id));
        assert_eq!(owner.agent_thread_record_id(), None);
        assert_eq!(owner.attempt(), 2);
    }

    #[test]
    fn test_thread_owner_meters_its_captured_job() {
        // A child run captures its parent's job at launch, so its spend
        // meters to the job without referencing a stage task.
        let job_id = JobId::new();
        let owner = UsageOwner::Thread {
            job_id: Some(job_id),
            thread_id: AgentThreadId::new(),
            record_id: None,
            attempt: 0,
        };
        assert_eq!(owner.job_id(), Some(job_id));
        assert_eq!(owner.task_id(), None, "a child has no stage task");
    }

    #[test]
    fn test_unowned_maps_to_no_columns() {
        let owner = UsageOwner::Unowned;

        assert_eq!(owner.job_id(), None);
        assert_eq!(owner.task_id(), None);
        assert_eq!(owner.reindex_run_id(), None);
        assert_eq!(owner.agent_thread_id(), None);
        assert_eq!(owner.agent_thread_record_id(), None);
        assert_eq!(owner.attempt(), 0);
    }

    #[test]
    fn test_pipeline_owner_maps_to_job_task_and_thread_columns() {
        let job_id = JobId::new();
        let task_id = TaskId::new();
        let thread_id = AgentThreadId::new();
        let record_id = AgentThreadRecordId::new();
        let owner = UsageOwner::Pipeline {
            job_id,
            task_id,
            thread_id,
            record_id: Some(record_id),
            attempt: 3,
        };

        assert_eq!(owner.job_id(), Some(job_id));
        assert_eq!(owner.task_id(), Some(task_id));
        assert_eq!(owner.agent_thread_id(), Some(thread_id));
        assert_eq!(owner.agent_thread_record_id(), Some(record_id));
        assert_eq!(owner.attempt(), 3);
        assert_eq!(owner.reindex_run_id(), None);
    }

    #[test]
    fn test_reindex_owner_maps_to_the_run_column_alone() {
        let run_id = ReindexRunId::new();
        let owner = UsageOwner::Reindex { run_id };

        assert_eq!(owner.reindex_run_id(), Some(run_id));
        assert_eq!(owner.job_id(), None);
        assert_eq!(owner.task_id(), None);
        assert_eq!(owner.agent_thread_id(), None);
        assert_eq!(owner.agent_thread_record_id(), None);
        assert_eq!(owner.attempt(), 0);
    }

    #[test]
    fn test_default_owner_is_unowned() {
        assert_eq!(UsageOwner::default(), UsageOwner::Unowned);
    }
}
