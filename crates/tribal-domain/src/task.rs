use serde::{Deserialize, Serialize};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enum_serde_tests;

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
}
