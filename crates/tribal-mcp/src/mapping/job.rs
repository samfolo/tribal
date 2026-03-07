//! MCP request and response types for `tribal_ingest`, `tribal_feedback`,
//! and `tribal_job_status`.

use chrono::{DateTime, Utc};
use rmcp::model::{CallToolResult, Content};
use serde::{Deserialize, Serialize};
use tribal_domain::{FeedbackRating, Job, JobId, JobOutcome, JobStatus, RetrievalFeedbackId};

use crate::error::IntoCallToolResult;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERIALISE_INGEST_RESPONSE: &str =
    "McpIngestResponse should always serialise successfully";
const SERIALISE_FEEDBACK_RESPONSE: &str =
    "McpFeedbackResponse should always serialise successfully";
const SERIALISE_JOB_STATUS_RESPONSE: &str =
    "McpJobStatusResponse should always serialise successfully";

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_ingest` input.
#[derive(Debug, Deserialize)]
pub(crate) struct McpIngestRequest {
    pub(crate) content: String,
    pub(crate) project_id: Option<String>,
}

/// Response for `tribal_ingest`.
#[derive(Debug, Serialize)]
pub(crate) struct McpIngestResponse {
    pub(crate) job_id: String,
}

impl From<JobId> for McpIngestResponse {
    fn from(id: JobId) -> Self {
        Self {
            job_id: id.to_string(),
        }
    }
}

impl IntoCallToolResult for McpIngestResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let text = format!(
            "Ingest job created: {}. Use tribal_job_status to track progress.",
            self.job_id,
        );
        let structured =
            serde_json::to_value(&self).expect(SERIALISE_INGEST_RESPONSE);
        let mut result = CallToolResult::success(vec![Content::text(text)]);
        result.structured_content = Some(structured);
        result
    }
}

// ---------------------------------------------------------------------------
// Feedback
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_feedback` input.
#[derive(Debug, Deserialize)]
pub(crate) struct McpFeedbackRequest {
    pub(crate) trace_id: String,
    pub(crate) query_text: String,
    pub(crate) returned_item_ids: Vec<String>,
    pub(crate) explored_anchor_ids: Option<Vec<String>>,
    pub(crate) rating: FeedbackRating,
    pub(crate) notes: Option<String>,
}

/// Response for `tribal_feedback`.
///
/// The `rating` field is `#[serde(skip)]` — it is not part of the output
/// schema but is needed for the human-readable text summary.
#[derive(Debug, Serialize)]
pub(crate) struct McpFeedbackResponse {
    pub(crate) feedback_id: String,
    #[serde(skip)]
    pub(crate) rating: FeedbackRating,
}

impl McpFeedbackResponse {
    /// Creates a new feedback response.
    pub(crate) fn new(id: RetrievalFeedbackId, rating: FeedbackRating) -> Self {
        Self {
            feedback_id: id.to_string(),
            rating,
        }
    }
}

impl IntoCallToolResult for McpFeedbackResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let text = format!(
            "Feedback recorded: {} (rating: {})",
            self.feedback_id, self.rating,
        );
        let structured =
            serde_json::to_value(&self).expect(SERIALISE_FEEDBACK_RESPONSE);
        let mut result = CallToolResult::success(vec![Content::text(text)]);
        result.structured_content = Some(structured);
        result
    }
}

// ---------------------------------------------------------------------------
// Job status
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_job_status` input.
#[derive(Debug, Deserialize)]
pub(crate) struct McpJobStatusRequest {
    pub(crate) job_id: String,
    pub(crate) wait_seconds: Option<u32>,
}

/// Response for `tribal_job_status`.
///
/// Constructed via `from_domain` because the `Job` domain type does not
/// carry aggregate task counts.
#[derive(Debug, Serialize)]
pub(crate) struct McpJobStatusResponse {
    pub(crate) job_id: String,
    pub(crate) status: JobStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) outcome: Option<JobOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) batch_size: Option<u32>,
    pub(crate) tasks_completed: u32,
    pub(crate) tasks_failed: u32,
    pub(crate) items_created: u32,
    pub(crate) observations_created: u32,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

impl McpJobStatusResponse {
    /// Builds a response from a domain `Job` and aggregate task counts.
    pub(crate) fn from_domain(
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

impl IntoCallToolResult for McpJobStatusResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let mut text = format!("Job {}: {}", self.job_id, self.status);
        if let Some(outcome) = &self.outcome {
            text.push_str(&format!(" ({outcome})"));
        }

        let structured =
            serde_json::to_value(&self).expect(SERIALISE_JOB_STATUS_RESPONSE);
        let mut result = CallToolResult::success(vec![Content::text(text)]);
        result.structured_content = Some(structured);
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::{JobBuilder, ProjectId, PrincipalId, PromptVersionId};

    use super::*;

    fn sample_job() -> Job {
        JobBuilder::default()
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
            .created_at(chrono::Utc::now())
            .updated_at(chrono::Utc::now())
            .build()
    }

    // -- Ingest -----------------------------------------------------------

    #[test]
    fn test_ingest_request_deserialises() {
        let json = serde_json::json!({"content": "learned something"});
        let req: McpIngestRequest =
            serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.content, "learned something");
        assert!(req.project_id.is_none());
    }

    #[test]
    fn test_ingest_response_into_call_tool_result() {
        let resp = McpIngestResponse::from(JobId::new());
        let result = resp.into_call_tool_result();
        assert!(result.is_error.is_none());
        assert!(result.structured_content.is_some());

        let Content::Text(text) = &result.content[0] else {
            panic!("expected text content");
        };
        assert!(text.text.contains("Ingest job created"));
        assert!(text.text.contains("tribal_job_status"));
    }

    // -- Feedback ---------------------------------------------------------

    #[test]
    fn test_feedback_request_deserialises() {
        let json = serde_json::json!({
            "trace_id": "t1",
            "query_text": "auth patterns",
            "returned_item_ids": ["ki_abc"],
            "rating": "positive",
        });
        let req: McpFeedbackRequest =
            serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.rating, FeedbackRating::Positive);
        assert!(req.explored_anchor_ids.is_none());
    }

    #[test]
    fn test_feedback_response_into_call_tool_result() {
        let resp = McpFeedbackResponse::new(
            RetrievalFeedbackId::new(),
            FeedbackRating::Positive,
        );

        // rating must not appear in serialised JSON
        let json = serde_json::to_value(&resp).expect("serialises");
        assert!(json.get("rating").is_none());

        let result = resp.into_call_tool_result();
        assert!(result.is_error.is_none());

        let Content::Text(text) = &result.content[0] else {
            panic!("expected text content");
        };
        assert!(text.text.contains("positive"));
    }

    // -- Job status -------------------------------------------------------

    #[test]
    fn test_job_status_request_deserialises() {
        let json = serde_json::json!({"job_id": "job_abc", "wait_seconds": 5});
        let req: McpJobStatusRequest =
            serde_json::from_value(json).expect("deserialises");
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

    #[test]
    fn test_job_status_response_into_call_tool_result() {
        let job = sample_job();
        let resp = McpJobStatusResponse::from_domain(&job, 0, 0, 0, 0);
        let result = resp.into_call_tool_result();
        assert!(result.is_error.is_none());

        let Content::Text(text) = &result.content[0] else {
            panic!("expected text content");
        };
        assert!(text.text.contains("completed"));
        assert!(text.text.contains("success"));
    }
}
