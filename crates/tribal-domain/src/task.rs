//! Task types and the task entity.
//!
//! Tasks are the unit of work in the ingest pipeline. Each job spawns
//! one extraction task, zero or more triage tasks, and one relation task.
//! Claiming uses atomic CTE with `FOR UPDATE SKIP LOCKED`. Retry uses
//! exponential backoff capped at 60 seconds.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{JobId, TaskId};

/// The type of a task in the ingest pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskType {
    /// Extract candidate knowledge items from raw input.
    Extraction,
    /// Classify and process a single candidate.
    Triage,
    /// Create relations across all triaged items in the batch.
    Relation,
}

/// The lifecycle status of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Waiting to be claimed by a worker.
    Queued,
    /// A worker owns this task and is actively processing it.
    Claimed,
    /// Task finished successfully.
    Completed,
    /// Task exhausted its retry budget and has been permanently shelved.
    DeadLetter,
}

/// Structured classification of a task failure.
///
/// Stored alongside `error_message` to enable queryable metrics without
/// string-prefix parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskErrorKind {
    /// LLM/embedding provider returned an error or timed out.
    ProviderError,
    /// Could not acquire provider semaphore within task timeout.
    SemaphoreTimeout,
    /// LLM output could not be parsed into expected schema.
    ParseError,
    /// Reclaimed by periodic sweep after heartbeat lapsed.
    HeartbeatExpired,
    /// Reclaimed by startup sweep after process crash.
    StartupReclaim,
    /// Commit rejected because another worker reclaimed the task.
    OwnershipLost,
    /// Task exceeded `task_timeout_seconds`.
    Timeout,
    /// Database error during stage execution.
    DatabaseError,
}

enum_text_conversions!(TaskType {
    TaskType::Extraction => "extraction",
    TaskType::Triage => "triage",
    TaskType::Relation => "relation",
});

enum_text_conversions!(TaskStatus {
    TaskStatus::Queued => "queued",
    TaskStatus::Claimed => "claimed",
    TaskStatus::Completed => "completed",
    TaskStatus::DeadLetter => "dead_letter",
});

enum_text_conversions!(TaskErrorKind {
    TaskErrorKind::ProviderError => "provider_error",
    TaskErrorKind::SemaphoreTimeout => "semaphore_timeout",
    TaskErrorKind::ParseError => "parse_error",
    TaskErrorKind::HeartbeatExpired => "heartbeat_expired",
    TaskErrorKind::StartupReclaim => "startup_reclaim",
    TaskErrorKind::OwnershipLost => "ownership_lost",
    TaskErrorKind::Timeout => "timeout",
    TaskErrorKind::DatabaseError => "database_error",
});

/// A task in the ingest pipeline.
///
/// # Invariants
///
/// - `batch_index` is set only for `Triage` tasks.
/// - `claim_token` is set only when `status` is `Claimed`.
/// - `retry_count` starts at 0 and is incremented on failure.
/// - Dead-letter condition: `retry_count > task_max_retries` after increment.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
#[allow(clippy::struct_field_names)]
pub struct Task {
    /// Unique identifier with `task_` prefix.
    id: TaskId,
    /// The job this task belongs to.
    job_id: JobId,
    /// The type of work this task performs.
    task_type: TaskType,
    /// Current lifecycle status.
    status: TaskStatus,
    /// Triage task index within the extraction batch (null for
    /// extraction and relation tasks).
    #[builder(default)]
    batch_index: Option<u32>,
    /// Ownership guard — set on claim, verified on commit.
    #[builder(default)]
    claim_token: Option<uuid::Uuid>,
    /// Earliest time this task can be claimed (for retry backoff).
    available_at: DateTime<Utc>,
    /// Worker identity that claimed this task.
    #[builder(default)]
    claimed_by: Option<String>,
    /// When this task was claimed.
    #[builder(default)]
    claimed_at: Option<DateTime<Utc>>,
    /// Most recent heartbeat from the claiming worker.
    #[builder(default)]
    heartbeat_at: Option<DateTime<Utc>>,
    /// Number of failed attempts completed so far (starts at 0).
    #[builder(default)]
    retry_count: u32,
    /// Structured failure classification.
    #[builder(default)]
    error_kind: Option<TaskErrorKind>,
    /// Failure details.
    #[builder(default)]
    error_message: Option<String>,
    /// When this task was created.
    created_at: DateTime<Utc>,
    /// When this task was last updated.
    updated_at: DateTime<Utc>,
}

impl Task {
    /// Returns the task identifier.
    pub fn id(&self) -> TaskId {
        self.id
    }

    /// Returns the job identifier.
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Returns the task type.
    pub fn task_type(&self) -> TaskType {
        self.task_type
    }

    /// Returns the current lifecycle status.
    pub fn status(&self) -> TaskStatus {
        self.status
    }

    /// Returns the batch index (triage tasks only).
    pub fn batch_index(&self) -> Option<u32> {
        self.batch_index
    }

    /// Returns the ownership claim token.
    pub fn claim_token(&self) -> Option<uuid::Uuid> {
        self.claim_token
    }

    /// Returns the earliest claimable time.
    pub fn available_at(&self) -> DateTime<Utc> {
        self.available_at
    }

    /// Returns the claiming worker identity.
    pub fn claimed_by(&self) -> Option<&str> {
        self.claimed_by.as_deref()
    }

    /// Returns when this task was claimed.
    pub fn claimed_at(&self) -> Option<DateTime<Utc>> {
        self.claimed_at
    }

    /// Returns the most recent heartbeat time.
    pub fn heartbeat_at(&self) -> Option<DateTime<Utc>> {
        self.heartbeat_at
    }

    /// Returns the number of failed attempts so far.
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Returns the structured failure classification, if failed.
    pub fn error_kind(&self) -> Option<TaskErrorKind> {
        self.error_kind
    }

    /// Returns the failure details, if failed.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns when this task was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns when this task was last updated.
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_task_type_serde_roundtrip, TaskType {
        TaskType::Extraction => "extraction",
        TaskType::Triage => "triage",
        TaskType::Relation => "relation",
    });

    enum_serde_tests!(test_task_status_serde_roundtrip, TaskStatus {
        TaskStatus::Queued => "queued",
        TaskStatus::Claimed => "claimed",
        TaskStatus::Completed => "completed",
        TaskStatus::DeadLetter => "dead_letter",
    });

    enum_serde_tests!(test_task_error_kind_serde_roundtrip, TaskErrorKind {
        TaskErrorKind::ProviderError => "provider_error",
        TaskErrorKind::SemaphoreTimeout => "semaphore_timeout",
        TaskErrorKind::ParseError => "parse_error",
        TaskErrorKind::HeartbeatExpired => "heartbeat_expired",
        TaskErrorKind::StartupReclaim => "startup_reclaim",
        TaskErrorKind::OwnershipLost => "ownership_lost",
        TaskErrorKind::Timeout => "timeout",
        TaskErrorKind::DatabaseError => "database_error",
    });

    enum_text_tests!(test_task_type_text_roundtrip, TaskType {
        TaskType::Extraction => "extraction",
        TaskType::Triage => "triage",
        TaskType::Relation => "relation",
    });

    enum_text_tests!(test_task_status_text_roundtrip, TaskStatus {
        TaskStatus::Queued => "queued",
        TaskStatus::Claimed => "claimed",
        TaskStatus::Completed => "completed",
        TaskStatus::DeadLetter => "dead_letter",
    });

    enum_text_tests!(test_task_error_kind_text_roundtrip, TaskErrorKind {
        TaskErrorKind::ProviderError => "provider_error",
        TaskErrorKind::SemaphoreTimeout => "semaphore_timeout",
        TaskErrorKind::ParseError => "parse_error",
        TaskErrorKind::HeartbeatExpired => "heartbeat_expired",
        TaskErrorKind::StartupReclaim => "startup_reclaim",
        TaskErrorKind::OwnershipLost => "ownership_lost",
        TaskErrorKind::Timeout => "timeout",
        TaskErrorKind::DatabaseError => "database_error",
    });
}
