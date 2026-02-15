//! Factory functions for domain entity construction in tests.
//!
//! Each factory returns a builder (or a constructed instance for small types)
//! pre-populated with sensible defaults. Tests override only the fields
//! relevant to their scenario.

use chrono::{TimeDelta, Utc};
use tribal_domain::{
    AuthToken, AuthTokenId, Confidence, Embedding, EmbeddingId, EmbeddingPurpose, FeedbackRating,
    ItemObservation, ItemObservationId, Job, JobId, JobStatus, KnowledgeItem, KnowledgeItemId,
    KnowledgeItemRelation, KnowledgeKind, PipelineStage, Principal, PrincipalId, Project,
    ProjectId, PromptVersion, PromptVersionId, Reference, ReferenceId, ReferenceKind,
    RelationBatchId, RelationId, RelationKind, RelationSuggestion, RetrievalFeedback,
    RetrievalFeedbackId, SimilarItem, SourceType, Standing, TagRegistryEntry, Task, TaskId,
    TaskStatus, TaskType, TokenUsage, TokenUsageId, TriageOutcome, TriageResult, TriageResultId,
    TriageSimilarItemDecision, TriageSimilarItemDecisionId,
};

/// Returns a [`ProjectBuilder`] with sensible defaults.
pub fn a_project() -> ProjectBuilder {
    Project::builder()
        .id(ProjectId::new())
        .git_remote("git@github.com:test/test-project.git".to_owned())
        .name("test-project".to_owned())
        .default_branch("main".to_owned())
        .schema_version(1)
        .settings(serde_json::json!({}))
        .created_at(Utc::now())
        .updated_at(Utc::now())
}

/// Returns a [`KnowledgeItemBuilder`] with sensible defaults.
pub fn a_knowledge_item() -> KnowledgeItemBuilder {
    KnowledgeItem::builder()
        .id(KnowledgeItemId::new())
        .project_id(ProjectId::new())
        .principal_id(PrincipalId::new())
        .kind(KnowledgeKind::Heuristic)
        .content("test knowledge content".to_owned())
        .confidence(Confidence::Inferred)
        .source_context(serde_json::json!({}))
        .created_at(Utc::now())
}

/// Returns a [`ReferenceBuilder`] with sensible defaults.
pub fn a_reference() -> ReferenceBuilder {
    Reference::builder()
        .id(ReferenceId::new())
        .knowledge_item_id(KnowledgeItemId::new())
        .kind(ReferenceKind::FilePath)
        .value("//src/test.rs".to_owned())
        .project_id(ProjectId::new())
}

/// Returns an [`EmbeddingBuilder`] with sensible defaults.
pub fn a_embedding() -> EmbeddingBuilder {
    Embedding::builder()
        .id(EmbeddingId::new())
        .knowledge_item_id(KnowledgeItemId::new())
        .model("test-model".to_owned())
        .dimensions(3)
        .embedding(vec![0.0; 3])
        .created_at(Utc::now())
}

/// Returns a [`KnowledgeItemRelationBuilder`] with sensible defaults.
pub fn a_knowledge_item_relation() -> KnowledgeItemRelationBuilder {
    KnowledgeItemRelation::builder()
        .id(RelationId::new())
        .relation_batch_id(RelationBatchId::new())
        .source_id(KnowledgeItemId::new())
        .target_id(KnowledgeItemId::new())
        .relation_type(RelationKind::Supports)
        .principal_id(PrincipalId::new())
        .created_at(Utc::now())
}

/// Returns an [`ItemObservationBuilder`] with sensible defaults.
pub fn a_item_observation() -> ItemObservationBuilder {
    ItemObservation::builder()
        .id(ItemObservationId::new())
        .knowledge_item_id(KnowledgeItemId::new())
        .principal_id(PrincipalId::new())
        .source_type(SourceType::AgentMediated)
        .observed_at(Utc::now())
}

/// Returns a default [`TagRegistryEntry`].
pub fn a_tag_registry_entry() -> TagRegistryEntry {
    TagRegistryEntry::new("test-tag".to_owned(), Utc::now())
}

/// Returns a [`TriageSimilarItemDecisionBuilder`] with sensible defaults.
pub fn a_triage_similar_item_decision() -> TriageSimilarItemDecisionBuilder {
    TriageSimilarItemDecision::builder()
        .id(TriageSimilarItemDecisionId::new())
        .job_id(JobId::new())
        .batch_index(0)
        .matched_item_id(KnowledgeItemId::new())
        .similarity_score(0.85)
        .suggested_relation(RelationSuggestion::Supports)
        .justification_text("test justification".to_owned())
        .created_at(Utc::now())
}

/// Returns a [`TriageResultBuilder`] with a `Created` outcome.
pub fn a_triage_result_created() -> TriageResultBuilder {
    TriageResult::builder()
        .id(TriageResultId::new())
        .job_id(JobId::new())
        .batch_index(0)
        .created_at(Utc::now())
        .outcome(TriageOutcome::Created {
            item_id: KnowledgeItemId::new(),
        })
}

/// Returns a [`TriageResultBuilder`] with a `Duplicate` outcome.
pub fn a_triage_result_duplicate() -> TriageResultBuilder {
    TriageResult::builder()
        .id(TriageResultId::new())
        .job_id(JobId::new())
        .batch_index(0)
        .created_at(Utc::now())
        .outcome(TriageOutcome::Duplicate {
            observation_id: ItemObservationId::new(),
            matched_item_id: KnowledgeItemId::new(),
        })
}

/// Returns a [`TriageResultBuilder`] with a `Failed` outcome.
pub fn a_triage_result_failed() -> TriageResultBuilder {
    TriageResult::builder()
        .id(TriageResultId::new())
        .job_id(JobId::new())
        .batch_index(0)
        .created_at(Utc::now())
        .outcome(TriageOutcome::Failed {
            error_message: "test error".to_owned(),
            retryable: false,
        })
}

/// Returns a default [`SimilarItem`].
pub fn a_similar_item() -> SimilarItem {
    SimilarItem::new(
        KnowledgeItemId::new(),
        0.85,
        RelationSuggestion::Supports,
    )
}

/// Returns a [`RetrievalFeedbackBuilder`] with sensible defaults.
pub fn a_retrieval_feedback() -> RetrievalFeedbackBuilder {
    RetrievalFeedback::builder()
        .id(RetrievalFeedbackId::new())
        .trace_id("test-trace-id".to_owned())
        .query_text("test query".to_owned())
        .embedding_model("test-model".to_owned())
        .principal_id(PrincipalId::new())
        .rating(FeedbackRating::Positive)
        .created_at(Utc::now())
}

/// Returns a default [`Principal`].
pub fn a_principal() -> Principal {
    Principal::new(
        PrincipalId::new(),
        "user:test".to_owned(),
        None,
        Utc::now(),
    )
}

/// Returns an [`AuthTokenBuilder`] with sensible defaults.
pub fn a_auth_token() -> AuthTokenBuilder {
    AuthToken::builder()
        .id(AuthTokenId::new())
        .token_hash("a".repeat(64))
        .principal_id(PrincipalId::new())
        .expires_at(Utc::now() + TimeDelta::hours(24))
        .created_at(Utc::now())
}

/// Returns a [`PromptVersionBuilder`] with sensible defaults.
pub fn a_prompt_version() -> PromptVersionBuilder {
    PromptVersion::builder()
        .id(PromptVersionId::new())
        .stage(PipelineStage::Extraction)
        .content_hash("b".repeat(64))
        .content("test prompt content".to_owned())
        .created_at(Utc::now())
}

/// Returns a [`TokenUsageBuilder`] with sensible defaults.
pub fn a_token_usage() -> TokenUsageBuilder {
    TokenUsage::builder()
        .id(TokenUsageId::new())
        .attempt(0)
        .stage(PipelineStage::Extraction)
        .provider("test-provider".to_owned())
        .model("test-model".to_owned())
        .tokens_input(0)
        .tokens_output(0)
        .tokens_cache_read(0)
        .tokens_cache_write(0)
        .tokens_total(0)
        .latency_ms(100)
        .created_at(Utc::now())
}

/// Returns a [`JobBuilder`] with sensible defaults.
///
/// Defaults to `Queued` status with `None` outcome, respecting the
/// status/outcome invariant.
pub fn a_job() -> JobBuilder {
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
}

/// Returns a [`TaskBuilder`] with sensible defaults.
pub fn a_task() -> TaskBuilder {
    Task::builder()
        .id(TaskId::new())
        .job_id(JobId::new())
        .task_type(TaskType::Extraction)
        .status(TaskStatus::Queued)
        .available_at(Utc::now())
        .created_at(Utc::now())
        .updated_at(Utc::now())
}

/// Returns a [`StandingBuilder`] with all counts at zero.
pub fn a_standing() -> StandingBuilder {
    Standing::builder()
        .supporting_count(0)
        .contradicting_count(0)
        .observation_count(0)
        .supporting_episode_count(0)
        .supporting_project_count(0)
}

// Re-export builder types so consumers can name them in function signatures.
// typed-builder generates these as `<Struct>Builder` type aliases.
pub type ProjectBuilder = tribal_domain::ProjectBuilder;
pub type KnowledgeItemBuilder = tribal_domain::KnowledgeItemBuilder;
pub type ReferenceBuilder = tribal_domain::ReferenceBuilder;
pub type EmbeddingBuilder = tribal_domain::EmbeddingBuilder;
pub type KnowledgeItemRelationBuilder = tribal_domain::KnowledgeItemRelationBuilder;
pub type ItemObservationBuilder = tribal_domain::ItemObservationBuilder;
pub type TriageSimilarItemDecisionBuilder = tribal_domain::TriageSimilarItemDecisionBuilder;
pub type TriageResultBuilder = tribal_domain::TriageResultBuilder;
pub type RetrievalFeedbackBuilder = tribal_domain::RetrievalFeedbackBuilder;
pub type AuthTokenBuilder = tribal_domain::AuthTokenBuilder;
pub type PromptVersionBuilder = tribal_domain::PromptVersionBuilder;
pub type TokenUsageBuilder = tribal_domain::TokenUsageBuilder;
pub type JobBuilder = tribal_domain::JobBuilder;
pub type TaskBuilder = tribal_domain::TaskBuilder;
pub type StandingBuilder = tribal_domain::StandingBuilder;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_project_builds() {
        let project = a_project().build();
        assert_eq!(project.name(), "test-project");
        assert_eq!(project.default_branch(), "main");
        assert_eq!(project.schema_version(), 1);
    }

    #[test]
    fn test_a_knowledge_item_builds() {
        let item = a_knowledge_item().build();
        assert_eq!(item.kind(), KnowledgeKind::Heuristic);
        assert_eq!(item.confidence(), Confidence::Inferred);
        assert_eq!(item.content(), "test knowledge content");
    }

    #[test]
    fn test_a_reference_builds() {
        let reference = a_reference().build();
        assert_eq!(reference.kind(), ReferenceKind::FilePath);
        assert_eq!(reference.value(), "//src/test.rs");
    }

    #[test]
    fn test_a_embedding_builds() {
        let embedding = a_embedding().build();
        assert_eq!(embedding.model(), "test-model");
        assert_eq!(embedding.dimensions(), 3);
        assert_eq!(embedding.embedding().len(), 3);
    }

    #[test]
    fn test_a_knowledge_item_relation_builds() {
        let relation = a_knowledge_item_relation().build();
        assert_eq!(relation.relation_type(), RelationKind::Supports);
    }

    #[test]
    fn test_a_item_observation_builds() {
        let observation = a_item_observation().build();
        assert_eq!(observation.source_type(), SourceType::AgentMediated);
    }

    #[test]
    fn test_a_tag_registry_entry_builds() {
        let entry = a_tag_registry_entry();
        assert_eq!(entry.tag(), "test-tag");
    }

    #[test]
    fn test_a_triage_similar_item_decision_builds() {
        let decision = a_triage_similar_item_decision().build();
        assert_eq!(decision.suggested_relation(), RelationSuggestion::Supports);
        assert!((decision.similarity_score() - 0.85).abs() < f32::EPSILON);
    }

    #[test]
    fn test_a_triage_result_created_builds() {
        let result = a_triage_result_created().build();
        assert!(matches!(result.outcome(), TriageOutcome::Created { .. }));
    }

    #[test]
    fn test_a_triage_result_duplicate_builds() {
        let result = a_triage_result_duplicate().build();
        assert!(matches!(result.outcome(), TriageOutcome::Duplicate { .. }));
    }

    #[test]
    fn test_a_triage_result_failed_builds() {
        let result = a_triage_result_failed().build();
        assert!(matches!(result.outcome(), TriageOutcome::Failed { .. }));
    }

    #[test]
    fn test_a_similar_item_builds() {
        let item = a_similar_item();
        assert_eq!(item.suggested_relation(), RelationSuggestion::Supports);
        assert!((item.similarity_score() - 0.85).abs() < f32::EPSILON);
    }

    #[test]
    fn test_a_retrieval_feedback_builds() {
        let feedback = a_retrieval_feedback().build();
        assert_eq!(feedback.rating(), FeedbackRating::Positive);
        assert_eq!(feedback.query_text(), "test query");
    }

    #[test]
    fn test_a_principal_builds() {
        let principal = a_principal();
        assert_eq!(principal.principal_key(), "user:test");
    }

    #[test]
    fn test_a_auth_token_builds() {
        let token = a_auth_token().build();
        assert_eq!(token.token_hash().len(), 64);
    }

    #[test]
    fn test_a_prompt_version_builds() {
        let version = a_prompt_version().build();
        assert_eq!(version.stage(), PipelineStage::Extraction);
        assert_eq!(version.content(), "test prompt content");
    }

    #[test]
    fn test_a_token_usage_builds() {
        let usage = a_token_usage().build();
        assert_eq!(usage.stage(), PipelineStage::Extraction);
        assert_eq!(usage.tokens_total(), 0);
        assert_eq!(usage.latency_ms(), 100);
    }

    #[test]
    fn test_a_job_builds_with_queued_status() {
        let job = a_job().build();
        assert_eq!(job.status(), JobStatus::Queued);
        assert_eq!(job.outcome(), None);
    }

    #[test]
    fn test_a_task_builds_with_queued_status() {
        let task = a_task().build();
        assert_eq!(task.status(), TaskStatus::Queued);
        assert_eq!(task.task_type(), TaskType::Extraction);
        assert_eq!(task.retry_count(), 0);
    }

    #[test]
    fn test_a_standing_builds_with_zero_counts() {
        let standing = a_standing().build();
        assert_eq!(standing.supporting_count(), 0);
        assert_eq!(standing.contradicting_count(), 0);
        assert_eq!(standing.superseded_by(), None);
        assert_eq!(standing.observation_count(), 0);
        assert_eq!(standing.newest_supporting_id(), None);
        assert_eq!(standing.newest_contradicting_id(), None);
        assert_eq!(standing.supporting_episode_count(), 0);
        assert_eq!(standing.supporting_project_count(), 0);
    }
}
