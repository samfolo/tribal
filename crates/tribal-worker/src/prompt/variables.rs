//! Tera context variable names and system context builders.

use super::legends::{relation_suggestion_legend, similarity_score_legend};

// ---------------------------------------------------------------------------
// Variable names
// ---------------------------------------------------------------------------

/// Tera context variable: the candidate object.
pub(crate) const VAR_CANDIDATE: &str = "candidate";

/// Tera context variable: similar items from semantic search.
pub(crate) const VAR_SIMILAR_ITEMS: &str = "similar_items";

/// Tera context variable: the tag registry as a list of strings.
pub(crate) const VAR_TAGS: &str = "tags";

/// Tera context variable: the verbatim raw input text.
pub(crate) const VAR_RAW_INPUT: &str = "raw_input";

/// Tera context variable: candidates with triage outcomes.
pub(crate) const VAR_CANDIDATES: &str = "candidates";

/// Tera context variable: intra-batch relation hints from extraction.
pub(crate) const VAR_RELATION_HINTS: &str = "relation_hints";

/// Tera context variable: similar item decisions from triage.
pub(crate) const VAR_SIMILAR_ITEM_DECISIONS: &str = "similar_item_decisions";

/// Tera context variable: per-request nonce for content boundary fencing.
pub(crate) const VAR_NONCE: &str = "nonce";

/// Tera context variable: formatted similarity score band legend.
pub(crate) const VAR_SIMILARITY_SCORE_LEGEND: &str = "similarity_score_legend";

/// Tera context variable: formatted relation suggestion value descriptions.
pub(crate) const VAR_RELATION_SUGGESTION_LEGEND: &str = "relation_suggestion_legend";

// ---------------------------------------------------------------------------
// System context builders
// ---------------------------------------------------------------------------

/// System prompt context for the extraction stage.
pub(crate) fn extraction_system_context() -> tera::Context {
    tera::Context::new()
}

/// System prompt context for the triage stage.
pub(crate) fn triage_system_context() -> tera::Context {
    let mut ctx = tera::Context::new();
    ctx.insert(VAR_SIMILARITY_SCORE_LEGEND, &similarity_score_legend());
    ctx.insert(
        VAR_RELATION_SUGGESTION_LEGEND,
        &relation_suggestion_legend(),
    );
    ctx
}

/// System prompt context for the relation stage.
pub(crate) fn relation_system_context() -> tera::Context {
    let mut ctx = tera::Context::new();
    ctx.insert(VAR_SIMILARITY_SCORE_LEGEND, &similarity_score_legend());
    ctx
}
