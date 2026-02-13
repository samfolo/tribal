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

    #[test]
    fn test_task_type_serde_roundtrip() {
        let variants = [
            (TaskType::Extraction, "\"extraction\""),
            (TaskType::Triage, "\"triage\""),
            (TaskType::Relation, "\"relation\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: TaskType = serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_task_status_serde_roundtrip() {
        let variants = [
            (TaskStatus::Queued, "\"queued\""),
            (TaskStatus::Claimed, "\"claimed\""),
            (TaskStatus::Completed, "\"completed\""),
            (TaskStatus::DeadLetter, "\"dead_letter\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: TaskStatus = serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }
}
