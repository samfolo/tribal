use chrono::Utc;
use tribal_db::{JobStatusTransition, NewJob};
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

define_factory! {
    /// Factory for [`NewJob`] instances used in repository insert operations.
    pub struct NewJobFactory for NewJob {
        correlation_id: Option<tribal_domain::EpisodeId> = None,
        project_id: ProjectId = ProjectId::new(),
        principal_id: PrincipalId = PrincipalId::new(),
        actor_id: Option<PrincipalId> = None,
        source_context: serde_json::Value = serde_json::json!({}),
        extraction_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        triage_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        relation_prompt_version_id: PromptVersionId = PromptVersionId::new(),
        trace_context: Option<String> = None,
    }
}

/// Returns a [`NewJobFactory`] with sensible defaults.
pub fn a_new_job() -> NewJobFactory {
    NewJobFactory::new()
}

define_factory! {
    /// Factory for [`JobStatusTransition`] instances.
    pub struct JobStatusTransitionFactory for JobStatusTransition {
        status: JobStatus = JobStatus::Extracting,
        outcome: Option<tribal_domain::JobOutcome> = None,
        error_message: Option<String> = None,
        completed_at: Option<chrono::DateTime<Utc>> = None,
    }
}

/// Returns a [`JobStatusTransitionFactory`] with sensible defaults.
pub fn a_job_status_transition() -> JobStatusTransitionFactory {
    JobStatusTransitionFactory::new()
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

    #[test]
    fn test_new_job_builds_with_defaults() {
        let new = a_new_job().build();
        assert_eq!(new.source_context, serde_json::json!({}));
        assert!(new.actor_id.is_none());
    }

    #[test]
    fn test_job_status_transition_builds_with_defaults() {
        let t = a_job_status_transition().build();
        assert_eq!(t.status.as_str(), "extracting");
        assert!(t.outcome.is_none());
    }
}
