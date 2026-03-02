//! Tera context variable names used across prompt templates.

/// Tera context variable: the candidate object.
pub(crate) const VAR_CANDIDATE: &str = "candidate";

/// Tera context variable: similar items from semantic search.
pub(crate) const VAR_SIMILAR_ITEMS: &str = "similar_items";

/// Tera context variable: the tag registry as a list of strings.
pub(crate) const VAR_TAGS: &str = "tags";

/// Tera context variable: the JSON Schema for the expected output.
pub(crate) const VAR_SCHEMA: &str = "schema";

/// Tera context variable: candidates with triage outcomes.
pub(crate) const VAR_CANDIDATES: &str = "candidates";

/// Tera context variable: intra-batch relation hints from extraction.
pub(crate) const VAR_RELATION_HINTS: &str = "relation_hints";

/// Tera context variable: similar item decisions from triage.
pub(crate) const VAR_SIMILAR_ITEM_DECISIONS: &str = "similar_item_decisions";
