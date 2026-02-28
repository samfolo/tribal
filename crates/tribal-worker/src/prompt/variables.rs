//! Tera context variable names used across prompt templates.

/// Tera context variable: the candidate object.
pub(crate) const VAR_CANDIDATE: &str = "candidate";

/// Tera context variable: similar items from semantic search.
pub(crate) const VAR_SIMILAR_ITEMS: &str = "similar_items";

/// Tera context variable: the tag registry as a list of strings.
pub(crate) const VAR_TAGS: &str = "tags";

/// Tera context variable: the JSON Schema for the expected output.
pub(crate) const VAR_SCHEMA: &str = "schema";
