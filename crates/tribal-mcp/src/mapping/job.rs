//! MCP request and response types for `tribal_ingest` and `tribal_job_status`.

use std::fmt::Write;

use rmcp::model::{CallToolResult, Content};
use tribal_wire::{McpIngestResponse, McpJobStatusResponse};

use crate::error::IntoCallToolResult;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERIALISE_INGEST_RESPONSE: &str = "McpIngestResponse should always serialise successfully";
const SERIALISE_JOB_STATUS_RESPONSE: &str =
    "McpJobStatusResponse should always serialise successfully";

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

impl IntoCallToolResult for McpIngestResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let text = format!(
            "Ingest job created: {}. Use tribal_job_status to track progress.",
            self.job_id,
        );
        let structured = serde_json::to_value(&self).expect(SERIALISE_INGEST_RESPONSE);
        let mut result = CallToolResult::success(vec![Content::text(text)]);
        result.structured_content = Some(structured);
        result
    }
}

// ---------------------------------------------------------------------------
// Job status
// ---------------------------------------------------------------------------

impl IntoCallToolResult for McpJobStatusResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let mut text = format!("Job {}: {}", self.job_id, self.status);
        if let Some(outcome) = &self.outcome {
            let _ = write!(text, " ({outcome})");
        }

        let structured = serde_json::to_value(&self).expect(SERIALISE_JOB_STATUS_RESPONSE);
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
    use rmcp::model::RawContent;
    use tribal_domain::{
        Job, JobId, JobOutcome, JobStatus, PrincipalId, ProjectId, PromptVersionId,
    };

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
    fn test_ingest_response_into_call_tool_result() {
        let resp = McpIngestResponse::from(JobId::new());
        let result = resp.into_call_tool_result();
        assert_eq!(result.is_error, Some(false));
        assert!(result.structured_content.is_some());

        let RawContent::Text(text) = &result.content[0].raw else {
            panic!("expected text content");
        };
        assert!(text.text.contains("Ingest job created"));
        assert!(text.text.contains("tribal_job_status"));
    }

    // -- Job status -------------------------------------------------------

    #[test]
    fn test_job_status_response_into_call_tool_result() {
        let job = sample_job();
        let resp = McpJobStatusResponse::from_domain(&job, 0, 0, 0, 0);
        let result = resp.into_call_tool_result();
        assert_eq!(result.is_error, Some(false));

        let RawContent::Text(text) = &result.content[0].raw else {
            panic!("expected text content");
        };
        assert!(text.text.contains("completed"));
        assert!(text.text.contains("success"));
    }
}
