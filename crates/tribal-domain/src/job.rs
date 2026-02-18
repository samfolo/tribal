//! Job lifecycle types and the job entity.
//!
//! A job represents a single ingest pipeline run. Status and outcome are
//! separated — `outcome` is `None` until `status` reaches a terminal state.
//! Terminal states (`Completed`, `Failed`) enforce outcome constraints via
//! database CHECK constraints.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{EpisodeId, JobId, PrincipalId, ProjectId, PromptVersionId, RelationBatchId};

/// The lifecycle status of a job in the ingest pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Waiting for extraction.
    Queued,
    /// Extraction task running.
    Extracting,
    /// Triage tasks running (set when extraction completes).
    Triaging,
    /// Relation task running (set when all triage tasks are terminal).
    Relating,
    /// Terminal; pipeline reached conclusion (check [`JobOutcome`] for details).
    Completed,
    /// Terminal; pipeline could not complete.
    Failed,
}

/// The outcome of a completed or failed job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobOutcome {
    /// All candidates triaged successfully and relations committed.
    Success,
    /// Job completed but some triage tasks dead-lettered; relation task ran
    /// on a subset.
    Partial,
    /// Pipeline completed but produced no new knowledge items — zero
    /// candidates from extraction, or all candidates classified as
    /// duplicates.
    Empty,
    /// Pipeline could not complete; extraction or relation task
    /// dead-lettered.
    Failure,
}

enum_text_conversions!(JobStatus {
    JobStatus::Queued => "queued",
    JobStatus::Extracting => "extracting",
    JobStatus::Triaging => "triaging",
    JobStatus::Relating => "relating",
    JobStatus::Completed => "completed",
    JobStatus::Failed => "failed",
});

enum_text_conversions!(JobOutcome {
    JobOutcome::Success => "success",
    JobOutcome::Partial => "partial",
    JobOutcome::Empty => "empty",
    JobOutcome::Failure => "failure",
});

/// An ingest pipeline job.
///
/// # Invariants
///
/// - `outcome` is `None` until `status` is terminal (`Completed` or `Failed`).
/// - `Completed` permits outcomes `Success`, `Partial`, or `Empty`.
/// - `Failed` permits only `Failure`.
/// - `Failed` requires `error_message` to be set.
/// - `Completed` requires `error_message` to be `None`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct Job {
    /// Unique identifier with `job_` prefix.
    id: JobId,
    /// Episode grouping (correlation key).
    #[builder(default)]
    correlation_id: Option<EpisodeId>,
    /// The project this job belongs to.
    project_id: ProjectId,
    /// The ultimate principal who initiated this job.
    principal_id: PrincipalId,
    /// The immediate caller if operating on behalf of another principal.
    #[builder(default)]
    actor_id: Option<PrincipalId>,
    /// Current lifecycle status.
    status: JobStatus,
    /// Outcome — set only when status is terminal.
    #[builder(default)]
    outcome: Option<JobOutcome>,
    /// Capped candidate count from extraction.
    #[builder(default)]
    batch_size: Option<u32>,
    /// Active relation batch identifier.
    #[builder(default)]
    committed_batch_id: Option<RelationBatchId>,
    /// Source context (opaque JSONB — no control flow on contents).
    source_context: serde_json::Value,
    /// Pre-cap candidate count from extraction.
    #[builder(default)]
    extraction_original_count: Option<u32>,
    /// Error message — set only when status is `Failed`.
    #[builder(default)]
    error_message: Option<String>,
    /// Extraction prompt version at job creation time.
    extraction_prompt_version_id: PromptVersionId,
    /// Triage prompt version at job creation time.
    triage_prompt_version_id: PromptVersionId,
    /// Relation prompt version at job creation time.
    relation_prompt_version_id: PromptVersionId,
    /// W3C traceparent for distributed tracing.
    #[builder(default)]
    trace_context: Option<String>,
    /// When the job reached a terminal state.
    #[builder(default)]
    completed_at: Option<DateTime<Utc>>,
    /// When the job was created.
    created_at: DateTime<Utc>,
    /// When the job was last updated.
    updated_at: DateTime<Utc>,
}

impl Job {
    /// Returns the job identifier.
    pub fn id(&self) -> JobId {
        self.id
    }

    /// Returns the correlation (episode) identifier, if set.
    pub fn correlation_id(&self) -> Option<EpisodeId> {
        self.correlation_id
    }

    /// Returns the project identifier.
    pub fn project_id(&self) -> ProjectId {
        self.project_id
    }

    /// Returns the principal who initiated this job.
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Returns the actor identifier, if operating on behalf of another.
    pub fn actor_id(&self) -> Option<PrincipalId> {
        self.actor_id
    }

    /// Returns the current lifecycle status.
    pub fn status(&self) -> JobStatus {
        self.status
    }

    /// Returns the outcome, if the job has reached a terminal state.
    pub fn outcome(&self) -> Option<JobOutcome> {
        self.outcome
    }

    /// Returns the capped batch size.
    pub fn batch_size(&self) -> Option<u32> {
        self.batch_size
    }

    /// Returns the committed relation batch identifier.
    pub fn committed_batch_id(&self) -> Option<RelationBatchId> {
        self.committed_batch_id
    }

    /// Returns the source context (opaque JSONB).
    pub fn source_context(&self) -> &serde_json::Value {
        &self.source_context
    }

    /// Returns the pre-cap extraction candidate count.
    pub fn extraction_original_count(&self) -> Option<u32> {
        self.extraction_original_count
    }

    /// Returns the error message, if the job failed.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the extraction prompt version identifier.
    pub fn extraction_prompt_version_id(&self) -> PromptVersionId {
        self.extraction_prompt_version_id
    }

    /// Returns the triage prompt version identifier.
    pub fn triage_prompt_version_id(&self) -> PromptVersionId {
        self.triage_prompt_version_id
    }

    /// Returns the relation prompt version identifier.
    pub fn relation_prompt_version_id(&self) -> PromptVersionId {
        self.relation_prompt_version_id
    }

    /// Returns the W3C traceparent, if set.
    pub fn trace_context(&self) -> Option<&str> {
        self.trace_context.as_deref()
    }

    /// Returns when the job reached a terminal state, if applicable.
    pub fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }

    /// Returns when the job was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns when the job was last updated.
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_job_status_serde_roundtrip, JobStatus {
        JobStatus::Queued => "queued",
        JobStatus::Extracting => "extracting",
        JobStatus::Triaging => "triaging",
        JobStatus::Relating => "relating",
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
    });

    enum_serde_tests!(test_job_outcome_serde_roundtrip, JobOutcome {
        JobOutcome::Success => "success",
        JobOutcome::Partial => "partial",
        JobOutcome::Empty => "empty",
        JobOutcome::Failure => "failure",
    });

    enum_text_tests!(test_job_status_text_roundtrip, JobStatus {
        JobStatus::Queued => "queued",
        JobStatus::Extracting => "extracting",
        JobStatus::Triaging => "triaging",
        JobStatus::Relating => "relating",
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
    });

    enum_text_tests!(test_job_outcome_text_roundtrip, JobOutcome {
        JobOutcome::Success => "success",
        JobOutcome::Partial => "partial",
        JobOutcome::Empty => "empty",
        JobOutcome::Failure => "failure",
    });
}
