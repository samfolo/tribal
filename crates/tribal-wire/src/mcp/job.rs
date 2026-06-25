//! MCP request and response types for `tribal_ingest` and `tribal_job_status`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tribal_domain::{Job, JobId, JobOutcome, JobStatus};

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_ingest` input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpIngestRequest {
    pub content: String,
    pub project_id: Option<String>,
}

/// Response for `tribal_ingest`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpIngestResponse {
    pub job_id: String,
}

impl From<JobId> for McpIngestResponse {
    fn from(id: JobId) -> Self {
        Self {
            job_id: id.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Job status
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_job_status` input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpJobStatusRequest {
    pub job_id: String,
    pub wait_seconds: Option<u32>,
}

/// Response for `tribal_job_status`.
///
/// Constructed via `from_domain` because the `Job` domain type does not
/// carry aggregate task counts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpJobStatusResponse {
    pub job_id: String,
    pub status: JobStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<JobOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,
    pub tasks_completed: u32,
    pub tasks_failed: u32,
    pub items_created: u32,
    pub observations_created: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl McpJobStatusResponse {
    /// Builds a response from a domain `Job` and aggregate task counts.
    #[must_use]
    pub fn from_domain(
        job: &Job,
        tasks_completed: u32,
        tasks_failed: u32,
        items_created: u32,
        observations_created: u32,
    ) -> Self {
        Self {
            job_id: job.id().to_string(),
            status: job.status(),
            outcome: job.outcome(),
            batch_size: job.batch_size(),
            tasks_completed,
            tasks_failed,
            items_created,
            observations_created,
            created_at: job.created_at(),
            updated_at: job.updated_at(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::{PrincipalId, ProjectId, PromptVersionId};

    use super::*;

    fn sample_job() -> Job {
        Job::builder()
            .id(JobId::new())
            .project_id(ProjectId::new())
            .principal_id(PrincipalId::new())
            .status(JobStatus::Completed)
            .outcome(Some(JobOutcome::Success))
            .batch_size(Some(3))
            .source_context(serde_json::json!({}))
            .raw_input("test input".to_owned())
            .extraction_system_prompt_version_id(PromptVersionId::new())
            .extraction_user_prompt_version_id(PromptVersionId::new())
            .triage_system_prompt_version_id(PromptVersionId::new())
            .triage_user_prompt_version_id(PromptVersionId::new())
            .relation_system_prompt_version_id(PromptVersionId::new())
            .relation_user_prompt_version_id(PromptVersionId::new())
            .system_fingerprint_hash("a".repeat(64))
            .created_at(chrono::Utc::now())
            .updated_at(chrono::Utc::now())
            .build()
    }

    // -- Ingest -----------------------------------------------------------

    #[test]
    fn test_ingest_request_deserialises() {
        let json = serde_json::json!({"content": "learned something"});
        let req: McpIngestRequest = serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.content, "learned something");
        assert!(req.project_id.is_none());
    }

    // -- Job status -------------------------------------------------------

    #[test]
    fn test_job_status_request_deserialises() {
        let json = serde_json::json!({"job_id": "job_abc", "wait_seconds": 5});
        let req: McpJobStatusRequest = serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.job_id, "job_abc");
        assert_eq!(req.wait_seconds, Some(5));
    }

    #[test]
    fn test_job_status_response_serialises_non_required_absent() {
        let job = sample_job();
        let resp = McpJobStatusResponse::from_domain(&job, 2, 1, 3, 0);
        let json = serde_json::to_value(&resp).expect("serialises");

        // outcome and batch_size present when Some
        assert!(json.get("outcome").is_some());
        assert!(json.get("batch_size").is_some());

        // Verify non-required absent scenario
        let mut resp_no_outcome = resp;
        resp_no_outcome.outcome = None;
        resp_no_outcome.batch_size = None;
        let json2 = serde_json::to_value(&resp_no_outcome).expect("serialises");
        assert!(json2.get("outcome").is_none());
        assert!(json2.get("batch_size").is_none());
    }

    #[test]
    fn test_job_status_response_from_domain() {
        let job = sample_job();
        let resp = McpJobStatusResponse::from_domain(&job, 5, 1, 3, 2);
        assert!(resp.job_id.starts_with("job_"));
        assert_eq!(resp.status, JobStatus::Completed);
        assert_eq!(resp.outcome, Some(JobOutcome::Success));
        assert_eq!(resp.tasks_completed, 5);
        assert_eq!(resp.tasks_failed, 1);
        assert_eq!(resp.items_created, 3);
        assert_eq!(resp.observations_created, 2);
    }
}
