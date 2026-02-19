//! Integration tests for repository implementations.
//!
//! All tests in this binary share a single testcontainers Postgres
//! instance via [`tribal_test_utils::test_context`].  Each test uses
//! [`TestTransaction`](tribal_test_utils::TestTransaction) for isolation
//! via transaction rollback.

#[path = "repositories/embedding.rs"]
mod embedding;

#[path = "repositories/item_observation.rs"]
mod item_observation;

#[path = "repositories/job.rs"]
mod job;

#[path = "repositories/knowledge_item.rs"]
mod knowledge_item;

#[path = "repositories/principal.rs"]
mod principal;

#[path = "repositories/project.rs"]
mod project;

#[path = "repositories/reference.rs"]
mod reference;

#[path = "repositories/relation.rs"]
mod relation;

#[path = "repositories/standing.rs"]
mod standing;

#[path = "repositories/tag_registry.rs"]
mod tag_registry;

#[path = "repositories/task.rs"]
mod task;

#[path = "repositories/triage_result.rs"]
mod triage_result;

#[path = "repositories/triage_similar_item_decision.rs"]
mod triage_similar_item_decision;
