//! Factory functions for domain entity construction in tests.
//!
//! Each factory returns a fully constructed instance pre-populated with
//! sensible defaults. These cover the FK-parent types that almost every
//! repository or integration test needs as row prerequisites. Tests that
//! need to customise a field use the builder directly instead.
//!
//! Additional factories should be added here only when a concrete test
//! demonstrates the need.

use chrono::Utc;
use tribal_domain::{
    Confidence, Job, JobId, JobStatus, KnowledgeItem, KnowledgeItemId, KnowledgeKind, Principal,
    PrincipalId, Project, ProjectId, PromptVersionId,
};

/// Returns a [`Project`] with sensible defaults.
pub fn a_project() -> Project {
    Project::builder()
        .id(ProjectId::new())
        .git_remote("git@github.com:test/test-project.git".to_owned())
        .name("test-project".to_owned())
        .default_branch("main".to_owned())
        .schema_version(1)
        .settings(serde_json::json!({}))
        .created_at(Utc::now())
        .updated_at(Utc::now())
        .build()
}

/// Returns a [`Principal`] with sensible defaults.
pub fn a_principal() -> Principal {
    Principal::new(PrincipalId::new(), "user:test".to_owned(), None, Utc::now())
}

/// Returns a [`KnowledgeItem`] with sensible defaults.
pub fn a_knowledge_item() -> KnowledgeItem {
    KnowledgeItem::builder()
        .id(KnowledgeItemId::new())
        .project_id(ProjectId::new())
        .principal_id(PrincipalId::new())
        .kind(KnowledgeKind::Heuristic)
        .content("test knowledge content".to_owned())
        .confidence(Confidence::Inferred)
        .source_context(serde_json::json!({}))
        .created_at(Utc::now())
        .build()
}

/// Returns a [`Job`] with sensible defaults.
///
/// Defaults to `Queued` status with `None` outcome, respecting the
/// status/outcome invariant.
pub fn a_job() -> Job {
    Job::builder()
        .id(JobId::new())
        .project_id(ProjectId::new())
        .principal_id(PrincipalId::new())
        .status(JobStatus::Queued)
        .source_context(serde_json::json!({}))
        .extraction_prompt_version_id(PromptVersionId::new())
        .triage_prompt_version_id(PromptVersionId::new())
        .relation_prompt_version_id(PromptVersionId::new())
        .created_at(Utc::now())
        .updated_at(Utc::now())
        .build()
}
