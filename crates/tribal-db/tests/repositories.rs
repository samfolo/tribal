//! Integration tests for repository implementations.
//!
//! All tests in this binary share a single testcontainers Postgres
//! instance via [`tribal_test_utils::test_context`].  Each test uses
//! [`TestTransaction`](tribal_test_utils::TestTransaction) for isolation
//! via transaction rollback.

#[path = "repositories/advisory_lock.rs"]
mod advisory_lock;

#[path = "repositories/auth_token.rs"]
mod auth_token;

#[path = "repositories/embedding_profile.rs"]
mod embedding_profile;

#[path = "repositories/embedding.rs"]
mod embedding;

#[path = "repositories/extraction_result.rs"]
mod extraction_result;

#[path = "repositories/item_observation.rs"]
mod item_observation;

#[path = "repositories/job.rs"]
mod job;

#[path = "repositories/knowledge_item.rs"]
mod knowledge_item;

#[path = "repositories/migration.rs"]
mod migration;

#[path = "repositories/principal.rs"]
mod principal;

#[path = "repositories/project.rs"]
mod project;

#[path = "repositories/prompt_version.rs"]
mod prompt_version;

#[path = "repositories/reference.rs"]
mod reference;

#[path = "repositories/reindex.rs"]
mod reindex;

#[path = "repositories/relation.rs"]
mod relation;

#[path = "repositories/retrieval_feedback.rs"]
mod retrieval_feedback;

#[path = "repositories/standing.rs"]
mod standing;

#[path = "repositories/system_fingerprint.rs"]
mod system_fingerprint;

#[path = "repositories/tag_embedding.rs"]
mod tag_embedding;

#[path = "repositories/tag_registry.rs"]
mod tag_registry;

#[path = "repositories/task.rs"]
mod task;

#[path = "repositories/token_usage.rs"]
mod token_usage;

#[path = "repositories/triage_result.rs"]
mod triage_result;

#[path = "repositories/triage_similar_item_decision.rs"]
mod triage_similar_item_decision;
