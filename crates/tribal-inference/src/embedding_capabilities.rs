//! Per-model operational capabilities for embedding providers.
//!
//! [`resolve_embedding`] maps a `(ProviderKind, model)` target to the
//! [`EmbeddingCapabilities`] describing operational properties the reindex
//! planner and the dimension resolver read: the model's native output
//! dimensionality, the largest batch the endpoint accepts, a courtesy
//! request-rate ceiling, and the published per-token cost.
//!
//! Unlike [`crate::ModelCapabilities`], embedding models do not share a
//! profile: native dimensionality differs per model, so each entry is a full
//! row rather than a reference to a shared named const. Matching is exact on
//! both provider kind and model id, never by prefix, for the same asymmetric
//! reason the inference table is verified-only: a wrong row silently produces
//! the wrong dimension or a misleading cost estimate, whereas a missing row
//! resolves to [`EmbeddingCapabilities::UNKNOWN`] and falls back to the
//! caller-supplied dimension and a default-send posture.

use tribal_domain::ProviderKind;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of inputs the `OpenAI` embeddings endpoint accepts in a
/// single request, shared by every `text-embedding-*` model.
const OPENAI_EMBED_BATCH_MAX: usize = 2048;

// ---------------------------------------------------------------------------
// EmbeddingCapabilities
// ---------------------------------------------------------------------------

/// Operational capabilities for a resolved embedding target.
///
/// Every field is optional: absence means "unknown, fall back to the
/// caller-supplied value or a default-send posture", never a hard-coded
/// guess. Carries an `f32` cost, so it is `PartialEq` but not `Eq`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmbeddingCapabilities {
    /// The model's native output dimensionality, when fixed and known.
    pub native_dimensions: Option<u32>,

    /// The largest number of inputs the endpoint accepts in one request.
    pub batch_max_size: Option<usize>,

    /// A courtesy client-side request-rate ceiling, in requests per minute.
    /// Absent where the limit is account-tier-dependent (cloud) or
    /// effectively unbounded (local); the provider 429 is the backstop.
    pub requests_per_minute: Option<u32>,

    /// Published cost per million input tokens in US dollars, when known.
    pub cost_per_million_tokens: Option<f32>,
}

impl EmbeddingCapabilities {
    /// The all-absent posture for an unrecognised target.
    pub const UNKNOWN: Self = Self {
        native_dimensions: None,
        batch_max_size: None,
        requests_per_minute: None,
        cost_per_million_tokens: None,
    };
}

// ---------------------------------------------------------------------------
// Capability table
// ---------------------------------------------------------------------------

/// Verified `(provider, model, capabilities)` rows. Each model id is an exact
/// string the corresponding provider serves; native dimensionalities are the
/// models' default (full) output sizes, and the `OpenAI` costs are the
/// published per-million-token prices. The default Ollama embedding model is
/// `nomic-embed-text:v1.5` at 768 dimensions; the cloud models support a
/// `dimensions` parameter to truncate below these natives.
const EMBEDDING_CAPABILITIES: &[(ProviderKind, &str, EmbeddingCapabilities)] = &[
    (
        ProviderKind::Ollama,
        "nomic-embed-text:v1.5",
        EmbeddingCapabilities {
            native_dimensions: Some(768),
            batch_max_size: None,
            requests_per_minute: None,
            cost_per_million_tokens: None,
        },
    ),
    (
        ProviderKind::OpenAi,
        "text-embedding-3-small",
        EmbeddingCapabilities {
            native_dimensions: Some(1536),
            batch_max_size: Some(OPENAI_EMBED_BATCH_MAX),
            requests_per_minute: None,
            cost_per_million_tokens: Some(0.02),
        },
    ),
    (
        ProviderKind::OpenAi,
        "text-embedding-3-large",
        EmbeddingCapabilities {
            native_dimensions: Some(3072),
            batch_max_size: Some(OPENAI_EMBED_BATCH_MAX),
            requests_per_minute: None,
            cost_per_million_tokens: Some(0.13),
        },
    ),
    (
        ProviderKind::OpenAi,
        "text-embedding-ada-002",
        EmbeddingCapabilities {
            native_dimensions: Some(1536),
            batch_max_size: Some(OPENAI_EMBED_BATCH_MAX),
            requests_per_minute: None,
            cost_per_million_tokens: Some(0.10),
        },
    ),
];

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/// Resolves operational capabilities for a `(provider, model)` embedding
/// target.
///
/// The match is exact on both axes: a recognised model id served through a
/// different provider kind, or an unrecognised id, resolves to
/// [`EmbeddingCapabilities::UNKNOWN`].
#[must_use]
pub fn resolve_embedding(provider: ProviderKind, model: &str) -> EmbeddingCapabilities {
    EMBEDDING_CAPABILITIES
        .iter()
        .find(|(table_provider, table_model, _)| {
            *table_provider == provider && *table_model == model
        })
        .map_or(EmbeddingCapabilities::UNKNOWN, |(_, _, capabilities)| {
            *capabilities
        })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_every_row_resolves_to_itself() {
        for &(provider, model, expected) in EMBEDDING_CAPABILITIES {
            assert_eq!(
                resolve_embedding(provider, model),
                expected,
                "{provider} {model} should resolve to its own row",
            );
        }
    }

    #[test]
    fn test_default_ollama_model_has_native_768() {
        let caps = resolve_embedding(ProviderKind::Ollama, "nomic-embed-text:v1.5");
        assert_eq!(caps.native_dimensions, Some(768));
    }

    #[test]
    fn test_openai_natives() {
        assert_eq!(
            resolve_embedding(ProviderKind::OpenAi, "text-embedding-3-small").native_dimensions,
            Some(1536),
        );
        assert_eq!(
            resolve_embedding(ProviderKind::OpenAi, "text-embedding-3-large").native_dimensions,
            Some(3072),
        );
        assert_eq!(
            resolve_embedding(ProviderKind::OpenAi, "text-embedding-ada-002").native_dimensions,
            Some(1536),
        );
    }

    #[test]
    fn test_unrecognised_model_resolves_unknown() {
        assert_eq!(
            resolve_embedding(ProviderKind::OpenAi, "text-embedding-9-imaginary"),
            EmbeddingCapabilities::UNKNOWN,
        );
    }

    #[test]
    fn test_match_is_exact_not_prefix() {
        // A prefix of a catalogued id must not match.
        assert_eq!(
            resolve_embedding(ProviderKind::OpenAi, "text-embedding-3"),
            EmbeddingCapabilities::UNKNOWN,
        );
    }

    #[test]
    fn test_right_model_wrong_provider_resolves_unknown() {
        // Capabilities are endpoint-scoped: the OpenAI model id served through
        // Ollama, and the Ollama id served through OpenAI, both miss.
        assert_eq!(
            resolve_embedding(ProviderKind::Ollama, "text-embedding-3-small"),
            EmbeddingCapabilities::UNKNOWN,
        );
        assert_eq!(
            resolve_embedding(ProviderKind::OpenAi, "nomic-embed-text:v1.5"),
            EmbeddingCapabilities::UNKNOWN,
        );
    }
}
