use serde::{Deserialize, Serialize};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_status_serde_roundtrip() {
        let variants = [
            (JobStatus::Queued, "\"queued\""),
            (JobStatus::Extracting, "\"extracting\""),
            (JobStatus::Triaging, "\"triaging\""),
            (JobStatus::Relating, "\"relating\""),
            (JobStatus::Completed, "\"completed\""),
            (JobStatus::Failed, "\"failed\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: JobStatus =
                serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_job_outcome_serde_roundtrip() {
        let variants = [
            (JobOutcome::Success, "\"success\""),
            (JobOutcome::Partial, "\"partial\""),
            (JobOutcome::Empty, "\"empty\""),
            (JobOutcome::Failure, "\"failure\""),
        ];
        for (variant, expected_json) in variants {
            let json = serde_json::to_string(&variant).expect("should serialise");
            assert_eq!(json, expected_json, "serialised form of {variant:?}");
            let parsed: JobOutcome =
                serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(parsed, variant);
        }
    }
}
