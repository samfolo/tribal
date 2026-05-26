//! Per-model request-field admissibility for inference providers.
//!
//! [`resolve`] maps a `(ProviderKind, model)` target to the
//! [`ModelCapabilities`] describing how the endpoint handles two request
//! axes that vary by model: whether it honours caller-supplied sampling
//! parameters, and which field carries the maximum-output-token cap. It is
//! the single source of capability truth: each provider's request builder
//! consults it at the wire boundary (covering both ingest and the readiness
//! probe, which share that path), and the system fingerprint derives its
//! effective sampling parameters from it, so probe and ingest stay
//! consistent by construction.
//!
//! The layer governs only what varies by model class. Provider-protocol-fixed
//! shaping (a provider's always-required or fixed-name fields) stays in each
//! provider's request builder and is not represented here.

use tribal_domain::ProviderKind;

// ---------------------------------------------------------------------------
// Capability table
// ---------------------------------------------------------------------------

/// `OpenAI` model IDs served as reasoning models over the Chat Completions
/// API, carrying the [`ModelCapabilities::OPENAI_REASONING`] profile.
///
/// Exact-match, never prefix matching: an unlisted ID resolves to
/// [`ModelCapabilities::SEND_ALL`], so a brand-new or ambiguous model
/// default-sends and a cloud probe surfaces any mismatch, whereas a prefix
/// rule would over-claim and silently mis-shape a request that would
/// otherwise work. Models the Chat Completions client cannot address, and
/// variants whose sampling support differs across vendor surfaces, are
/// therefore simply absent. Curated against the provider contract; extend
/// (including dated snapshots) as the supported lineup changes.
const OPENAI_REASONING_MODELS: &[&str] = &[
    "o1",
    "o1-mini",
    "o3",
    "o3-mini",
    "o4-mini",
    "gpt-5",
    "gpt-5-mini",
    "gpt-5-nano",
    "gpt-5.1",
    "gpt-5.2",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.4-nano",
    "gpt-5.5",
];

/// `Anthropic` model IDs that sample adaptively, carrying the
/// [`ModelCapabilities::ANTHROPIC_ADAPTIVE`] profile. Exact-match for the
/// same reasons as [`OPENAI_REASONING_MODELS`].
const ANTHROPIC_ADAPTIVE_MODELS: &[&str] = &["claude-opus-4-7"];

// ---------------------------------------------------------------------------
// Capability axes
// ---------------------------------------------------------------------------

/// How a target handles caller-supplied sampling parameters.
///
/// Adaptive-sampling models reject the parameters as a family rather than one
/// at a time, so this names the behaviour rather than a single field. Tribal
/// currently sends only `temperature`, which [`SamplingControl::Adaptive`]
/// causes to be dropped. `#[non_exhaustive]` leaves room for finer future
/// modes without a breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SamplingControl {
    /// The endpoint honours caller-supplied sampling parameters
    /// (`temperature`, `top_p`, ...).
    Configurable,
    /// The endpoint samples adaptively and rejects caller-supplied sampling
    /// parameters as a family; such parameters must be dropped before the
    /// wire.
    Adaptive,
}

impl SamplingControl {
    /// Whether the endpoint honours caller-supplied sampling parameters.
    ///
    /// `false` means such parameters must be dropped before the request is
    /// sent and recorded as unset in the fingerprint. The single source for
    /// this mapping, so request reconciliation and fingerprint derivation
    /// agree without re-deriving it.
    #[must_use]
    pub fn accepts_overrides(self) -> bool {
        match self {
            Self::Configurable => true,
            Self::Adaptive => false,
        }
    }
}

/// Which request field carries the maximum-output-token cap.
///
/// Models on the same provider can differ here. `#[non_exhaustive]` leaves
/// room for future model-class-driven field names. Cap field names that do
/// not vary by model class stay owned by their provider's request builder
/// and are not represented here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MaxOutputTokensParam {
    /// The conventional `max_tokens` field.
    MaxTokens,
    /// The `max_completion_tokens` field required by `OpenAI` reasoning models.
    MaxCompletionTokens,
}

// ---------------------------------------------------------------------------
// ModelCapabilities
// ---------------------------------------------------------------------------

/// Request-field admissibility for a resolved inference target.
///
/// Describes only what varies by model class; provider-protocol-fixed facts
/// are handled in each provider's request builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelCapabilities {
    /// How the target handles caller-supplied sampling parameters.
    pub sampling: SamplingControl,

    /// Which field name carries the output-token cap.
    pub max_output_tokens_param: MaxOutputTokensParam,
}

impl ModelCapabilities {
    /// The default-send posture for an unrecognised target: honour caller
    /// sampling parameters and use the conventional `max_tokens` field name.
    pub const SEND_ALL: Self = Self {
        sampling: SamplingControl::Configurable,
        max_output_tokens_param: MaxOutputTokensParam::MaxTokens,
    };

    /// Reasoning models served over the Chat Completions API: adaptive
    /// sampling, output-token cap as `max_completion_tokens`.
    const OPENAI_REASONING: Self = Self {
        sampling: SamplingControl::Adaptive,
        max_output_tokens_param: MaxOutputTokensParam::MaxCompletionTokens,
    };

    /// Adaptive-sampling models that retain the conventional `max_tokens`
    /// cap field (always required by the Anthropic protocol).
    const ANTHROPIC_ADAPTIVE: Self = Self {
        sampling: SamplingControl::Adaptive,
        max_output_tokens_param: MaxOutputTokensParam::MaxTokens,
    };
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/// Resolves request-field admissibility for a `(provider, model)` target.
///
/// Restriction entries are scoped to the vendor-cloud [`ProviderKind`] plus a
/// verified model identifier: the same model served by a permissive local
/// backend (addressed through a different provider kind) accepts parameters
/// the vendor cloud rejects, so admissibility is a property of the serving
/// endpoint, not the model weights. An unrecognised target resolves to
/// [`ModelCapabilities::SEND_ALL`].
#[must_use]
pub fn resolve(provider: ProviderKind, model: &str) -> ModelCapabilities {
    match provider {
        ProviderKind::OpenAi if OPENAI_REASONING_MODELS.contains(&model) => {
            ModelCapabilities::OPENAI_REASONING
        }
        ProviderKind::Anthropic if ANTHROPIC_ADAPTIVE_MODELS.contains(&model) => {
            ModelCapabilities::ANTHROPIC_ADAPTIVE
        }
        ProviderKind::Ollama | ProviderKind::Anthropic | ProviderKind::OpenAi => {
            ModelCapabilities::SEND_ALL
        }
    }
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

/// Logged when a configured `temperature` is dropped because the resolved
/// target samples adaptively. Names the field so the dropped value (attached
/// as a structured field) is actionable in the logs.
const TEMPERATURE_DROPPED: &str = "dropping configured temperature: target model samples adaptively and rejects caller sampling parameters";

/// Reconciles a requested `temperature` against the target's sampling control,
/// dropping it (with a warning naming the discarded value) when the target
/// samples adaptively.
///
/// `None` in yields `None` out, so the default (unset) path is silent — only
/// a configured value that the target rejects triggers the warning. Shared by
/// the cloud providers' request builders; Ollama accepts sampling parameters
/// and does not call this.
pub(crate) fn reconcile_temperature(
    sampling: SamplingControl,
    requested: Option<f32>,
    model: &str,
) -> Option<f32> {
    if sampling.accepts_overrides() {
        requested
    } else {
        if let Some(temperature) = requested {
            tracing::warn!(model, temperature, "{TEMPERATURE_DROPPED}");
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_openai_reasoning_models_sample_adaptively_and_rename_cap() {
        for &model in OPENAI_REASONING_MODELS {
            let caps = resolve(ProviderKind::OpenAi, model);
            assert_eq!(
                caps.sampling,
                SamplingControl::Adaptive,
                "{model} should reject caller sampling parameters",
            );
            assert_eq!(
                caps.max_output_tokens_param,
                MaxOutputTokensParam::MaxCompletionTokens,
                "{model} should use max_completion_tokens",
            );
        }
    }

    #[test]
    fn test_ordinary_openai_model_sends_all() {
        let caps = resolve(ProviderKind::OpenAi, "gpt-4o-mini");
        assert_eq!(caps, ModelCapabilities::SEND_ALL);
    }

    #[test]
    fn test_openai_chat_variant_sends_all() {
        // The GPT-5 `-chat` variants are intentionally absent from the
        // reasoning table, so they default-send (the probe arbitrates if a
        // vendor surface disagrees).
        assert_eq!(
            resolve(ProviderKind::OpenAi, "gpt-5-chat"),
            ModelCapabilities::SEND_ALL,
        );
    }

    #[test]
    fn test_anthropic_adaptive_models_reject_sampling_keep_max_tokens() {
        for &model in ANTHROPIC_ADAPTIVE_MODELS {
            let caps = resolve(ProviderKind::Anthropic, model);
            assert_eq!(
                caps.sampling,
                SamplingControl::Adaptive,
                "{model} should reject caller sampling parameters",
            );
            assert_eq!(
                caps.max_output_tokens_param,
                MaxOutputTokensParam::MaxTokens,
                "{model} keeps the max_tokens field name",
            );
        }
    }

    #[test]
    fn test_ordinary_anthropic_model_sends_all() {
        let caps = resolve(ProviderKind::Anthropic, "claude-haiku-4-5-20251001");
        assert_eq!(caps, ModelCapabilities::SEND_ALL);
    }

    #[test]
    fn test_ollama_always_sends_all() {
        // Admissibility is endpoint-scoped: a reasoning identifier served
        // through Ollama (a permissive local backend) still sends all
        // parameters, unlike the same string addressed as OpenAI cloud.
        assert_eq!(
            resolve(ProviderKind::Ollama, "o3"),
            ModelCapabilities::SEND_ALL,
        );
        assert_eq!(
            resolve(ProviderKind::Ollama, "llama3.1:70b"),
            ModelCapabilities::SEND_ALL,
        );
    }

    #[test]
    fn test_reasoning_identifier_on_wrong_provider_sends_all() {
        // "o3" is restricted only under the OpenAi provider kind, never as a
        // bare substring on another vendor.
        assert_eq!(
            resolve(ProviderKind::Anthropic, "o3"),
            ModelCapabilities::SEND_ALL,
        );
    }

    #[test]
    fn test_sampling_control_accepts_overrides() {
        assert!(SamplingControl::Configurable.accepts_overrides());
        assert!(!SamplingControl::Adaptive.accepts_overrides());
    }

    #[test]
    fn test_reconcile_temperature_passes_through_when_configurable() {
        assert_eq!(
            reconcile_temperature(SamplingControl::Configurable, Some(0.7), "m"),
            Some(0.7),
        );
        assert_eq!(
            reconcile_temperature(SamplingControl::Configurable, None, "m"),
            None,
        );
    }

    #[test]
    fn test_reconcile_temperature_drops_when_adaptive() {
        assert_eq!(
            reconcile_temperature(SamplingControl::Adaptive, Some(0.7), "m"),
            None,
        );
        assert_eq!(
            reconcile_temperature(SamplingControl::Adaptive, None, "m"),
            None,
        );
    }
}
