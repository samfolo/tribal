//! The ledger-sink port: where the gateway reports every request it makes.
//!
//! The gateway calls the sink for each request that produced usage, from the
//! same code path that returns the response, so the usage ledger and any
//! telemetry the implementation emits cannot disagree on source data. The
//! trait lives beside the gateway and is implemented above this crate, which
//! keeps `tribal-inference` persistence-free.

use std::time::Duration;

use async_trait::async_trait;
use tribal_domain::{JobId, PromptVersionId, ReindexRunId, TaskId, TokenUsageStage, Usage};

/// Caller-supplied attribution for one inference or embedding request.
///
/// Every field is optional: a pipeline call attributes its job and task, a
/// reindex call its run, and an unowned call (a query embed, a probe)
/// nothing at all. The stage identity is not carried here — the gateway
/// derives it from the operation itself, so a caller cannot mislabel one.
/// New owner kinds extend this struct without changing the
/// [`LedgerSink`] signature.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageAttribution {
    /// The job the request belongs to.
    pub job_id: Option<JobId>,
    /// The task the request belongs to.
    pub task_id: Option<TaskId>,
    /// The reindex run the request belongs to.
    pub reindex_run_id: Option<ReindexRunId>,
    /// The attempt number within the task (0 when unowned).
    pub attempt: i32,
    /// The system prompt version used, for completion calls.
    pub system_prompt_version_id: Option<PromptVersionId>,
    /// The user prompt version used, for completion calls.
    pub user_prompt_version_id: Option<PromptVersionId>,
    /// The OpenTelemetry trace identifier to record against the row.
    pub trace_id: Option<String>,
}

/// The accounting port the gateway reports through.
///
/// Implementations must be best-effort: a failed write is logged and
/// swallowed, never surfaced to the calling request, and never performed
/// inside a caller's database transaction.
#[async_trait]
pub trait LedgerSink: Send + Sync {
    /// Records one request's usage.
    ///
    /// `stage` is derived by the gateway from the operation that produced
    /// `usage`; for embedding usage its purpose equals the purpose inside
    /// `usage` by construction.
    async fn record_usage(
        &self,
        usage: &Usage,
        stage: TokenUsageStage,
        attribution: &UsageAttribution,
    );

    /// Reports how long a request waited for its concurrency permit.
    fn record_semaphore_wait(&self, provider_key: &str, wait: Duration);
}

/// A sink that records nothing.
///
/// For contexts with no ledger to write to, such as diagnostic commands
/// running without a database.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopLedgerSink;

#[async_trait]
impl LedgerSink for NoopLedgerSink {
    async fn record_usage(
        &self,
        _usage: &Usage,
        _stage: TokenUsageStage,
        _attribution: &UsageAttribution,
    ) {
    }

    fn record_semaphore_wait(&self, _provider_key: &str, _wait: Duration) {}
}
