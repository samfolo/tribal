//! Tera context variable names, reserved variable definitions, and
//! system context builders.

use strum::IntoEnumIterator;

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
// Reserved variables
// ---------------------------------------------------------------------------

/// Template variables managed exclusively by the renderer.
///
/// Adding a new variant automatically propagates to the reserved-key
/// check in `PromptRenderer::render()`, validation default injection,
/// and the coverage test — all driven by `strum::IntoEnumIterator`.
#[derive(Debug, Clone, Copy, strum::EnumIter)]
pub(crate) enum ReservedVariable {
    Nonce,
}

impl ReservedVariable {
    /// The Tera context key for this variable.
    pub(crate) fn key(self) -> &'static str {
        match self {
            Self::Nonce => VAR_NONCE,
        }
    }

    /// The fixed value used in validation and test contexts.
    pub(crate) fn validation_default(self) -> &'static str {
        match self {
            Self::Nonce => "validation00",
        }
    }
}

/// Injects validation-safe values for all reserved variables.
///
/// Used by `synthetic_validation_context` to ensure the server's
/// hot-reload validator sees the same variable set that production
/// rendering provides.
pub(crate) fn inject_validation_defaults(ctx: &mut tera::Context) {
    for var in ReservedVariable::iter() {
        ctx.insert(var.key(), var.validation_default());
    }
}

/// Returns the set of reserved variable keys.
///
/// The server's hot-reload validator uses this to exclude reserved
/// keys from the "must be referenced" check — reserved variables
/// are available in all prompts but only referenced by user templates.
pub fn reserved_keys() -> Vec<&'static str> {
    ReservedVariable::iter()
        .map(ReservedVariable::key)
        .collect()
}

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_defaults_cover_all_reserved_variables() {
        let mut ctx = tera::Context::new();
        inject_validation_defaults(&mut ctx);
        for var in ReservedVariable::iter() {
            assert!(
                ctx.get(var.key()).is_some(),
                "reserved variable '{var:?}' missing from validation defaults",
            );
        }
    }
}
