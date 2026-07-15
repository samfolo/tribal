//! Integration tests for repository implementations.
//!
//! Each test owns an isolated database via
//! [`tribal_test_utils::TestDb`] — one [`tribal_test_utils::TestDb::new`]
//! call per test — so tests are isolated at the database level rather than
//! sharing a single instance.

#[path = "repositories/advisory_lock.rs"]
mod advisory_lock;

#[path = "repositories/agent_binding_version.rs"]
mod agent_binding_version;

#[path = "repositories/agent_thread.rs"]
mod agent_thread;

#[path = "repositories/auth_token.rs"]
mod auth_token;

#[path = "repositories/embedding_profile.rs"]
mod embedding_profile;

#[path = "repositories/embedding.rs"]
mod embedding;

#[path = "repositories/embedding_index.rs"]
mod embedding_index;

#[path = "repositories/extraction_result.rs"]
mod extraction_result;

#[path = "repositories/item_observation.rs"]
mod item_observation;

#[path = "repositories/job.rs"]
mod job;

#[path = "repositories/knowledge_item.rs"]
mod knowledge_item;
#[path = "repositories/local_default_credential.rs"]
mod local_default_credential;

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
