use chrono::Utc;
use tribal_domain::{
    Job, JobId, JobStatus, PrincipalId, ProjectId, PromptVersionId, RelationBatchId,
};

define_factory! {
    /// Factory for [`Job`] instances.
    ///
    /// Defaults to `Queued` status with `None` outcome, respecting the
    /// status/outcome invariant.
    pub struct JobFactory for Job {
        id: JobId = JobId::new(),
        correlation_id: Option<tribal_domain::EpisodeId> = None,
        project_id: ProjectId = ProjectId::new(),
        principal_id: PrincipalId = PrincipalId::new(),
        actor_id: Option<PrincipalId> = None,
        status: JobStatus = JobStatus::Queued,
        outcome: Option<tribal_domain::JobOutcome> = None,
        batch_size: Option<u32> = None,
        committed_batch_id: Option<RelationBatchId> = None,
        source_context: serde_json::Value = serde_json::json!({}),
        extraction_original_count: Option<u32> = None,
        error_message: Option<String> = None,
        extraction_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        triage_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        relation_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        trace_context: Option<String> = None,
        completed_at: Option<chrono::DateTime<Utc>> = None,
        created_at: chrono::DateTime<Utc> = Utc::now(),
        updated_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`JobFactory`] with sensible defaults.
pub fn a_job() -> JobFactory {
    JobFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let job = a_job().build();
        assert_eq!(job.status(), JobStatus::Queued);
        assert!(job.outcome().is_none());
    }
}
