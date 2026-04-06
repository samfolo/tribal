//! Application table names for the Tribal database schema.
//!
//! The `APPLICATION_TABLES` constant enumerates every user-data table
//! (excluding the `_sqlx_migrations` housekeeping table). It is used by
//! test infrastructure for truncation and validated against the live
//! schema by `tribal-test-utils/tests/tables_validation.rs`.

/// All application tables in the Tribal database, sorted alphabetically.
///
/// Excludes internal tables (e.g. `_sqlx_migrations`).  Add new tables
/// here when creating migrations — the schema validation test will
/// catch omissions.
pub const APPLICATION_TABLES: &[&str] = &[
    "auth_tokens",
    "embeddings",
    "extraction_results",
    "item_external_references",
    "item_observations",
    "jobs",
    "knowledge_item_relations",
    "knowledge_items",
    "principals",
    "projects",
    "prompt_versions",
    "retrieval_feedback",
    "system_fingerprints",
    "tag_embeddings",
    "tag_registry",
    "tasks",
    "token_usage",
    "triage_results",
    "triage_similar_item_decisions",
];
