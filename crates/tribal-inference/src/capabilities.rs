//! Per-model request-field admissibility for inference providers.
//!
//! [`resolve`] maps a `(ProviderKind, model)` target to the
//! [`ModelCapabilities`] describing how the endpoint handles three request
//! axes that vary by model: whether it honours caller-supplied sampling
//! parameters, which field carries the maximum-output-token cap, and how it
//! enforces a caller-supplied JSON Schema on its output. It is the single
//! source of capability truth — every site that shapes a request for the wire
//! or records its effective shape reads this one resolver, so they agree by
//! construction (ingest and the readiness probe included).
//!
//! The schema-dialect transform that rewrites a schema into a provider's
//! accepted shape is a separate mechanism, keyed on the transport rather than
//! the model class, and is not represented here.
//!
//! The layer governs only what varies by model class. Provider-protocol-fixed
//! shaping (a provider's always-required or fixed-name fields) stays in each
//! provider's request builder and is not represented here.

use tribal_domain::{ProviderKind, StageParameters};

// ---------------------------------------------------------------------------
// Capability table
// ---------------------------------------------------------------------------

/// `OpenAI` model IDs served as reasoning models over the Chat Completions
/// API — rejecting the caller sampling family and requiring
/// `max_completion_tokens` — carrying the [`ModelCapabilities::OPENAI_REASONING`]
/// profile.
///
/// Exact-match, never prefix matching, and verified-only: confirm each ID
/// against the provider contract before adding it, and never extrapolate from
/// the version pattern. The failure modes are asymmetric — a *missing*
/// reasoning model default-sends and fails visibly at the cloud probe (the
/// rejected `temperature` returns an error), whereas a *wrongly included* chat
/// model silently drops a parameter it would have honoured and the probe still
/// passes. Absence is therefore the safe default: chat/general variants and
/// Responses-API-only models (the `-pro` and `-codex` lines) are simply not
/// listed, and gaps in the version sequence are intentional, not omissions to
/// fill in.
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
    /// sent and recorded as unset. The single source for the adaptive→drop
    /// mapping, so every site that applies it agrees without re-deriving it.
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

/// How a target enforces a caller-supplied JSON Schema on its output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuredOutputMode {
    /// Decoding is constrained to the schema; the request marks it `strict`.
    Strict,
    /// The schema is advisory guidance; the request does not mark it `strict`.
    Advisory,
}

impl StructuredOutputMode {
    /// Whether a schema sent to this target must be marked `strict`.
    ///
    /// The single source for the mode→strict mapping, so every site that
    /// applies it agrees without re-deriving it.
    #[must_use]
    pub fn requires_strict(self) -> bool {
        match self {
            Self::Strict => true,
            Self::Advisory => false,
        }
    }
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

    /// How the target enforces a caller-supplied JSON Schema on its output.
    pub structured_output_mode: StructuredOutputMode,
}

impl ModelCapabilities {
    /// The default-send posture for an unrecognised target: honour caller
    /// sampling parameters, use the conventional `max_tokens` field name, and
    /// request strict schema enforcement.
    pub const SEND_ALL: Self = Self {
        sampling: SamplingControl::Configurable,
        max_output_tokens_param: MaxOutputTokensParam::MaxTokens,
        structured_output_mode: StructuredOutputMode::Strict,
    };

    /// Reasoning models served over the Chat Completions API: adaptive
    /// sampling, output-token cap as `max_completion_tokens`.
    const OPENAI_REASONING: Self = Self {
        sampling: SamplingControl::Adaptive,
        max_output_tokens_param: MaxOutputTokensParam::MaxCompletionTokens,
        structured_output_mode: StructuredOutputMode::Strict,
    };

    /// Adaptive-sampling models that retain the conventional `max_tokens`
    /// cap field (always required by the Anthropic protocol).
    const ANTHROPIC_ADAPTIVE: Self = Self {
        sampling: SamplingControl::Adaptive,
        max_output_tokens_param: MaxOutputTokensParam::MaxTokens,
        structured_output_mode: StructuredOutputMode::Strict,
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
        // The platform gateway enforces the upstream model's admissibility
        // server-side, so the client sends every field and lets it adapt.
        ProviderKind::Ollama
        | ProviderKind::Anthropic
        | ProviderKind::OpenAi
        | ProviderKind::Platform => ModelCapabilities::SEND_ALL,
    }
}

/// Logged when a configured `temperature` is ignored because the resolved
/// model samples adaptively. The drop happens at this config projection
/// rather than at the wire, so this is where the operator is told their
/// value took no effect.
const CONFIGURED_TEMPERATURE_IGNORED: &str = "ignoring configured temperature: stage model samples adaptively and rejects sampling parameters";

/// Derives a stage's effective [`StageParameters`] by resolving its
/// `(provider, model)` through the capability layer.
///
/// `temperature` is recorded as unset when the resolved target samples
/// adaptively; a configured value thereby ignored is logged once, naming
/// the stage, since this projection — not the wire layer — is where it is
/// dropped. `max_tokens` is unaffected (only its wire field name varies,
/// which does not change the recorded value).
#[must_use]
pub fn effective_stage_parameters(
    stage: &str,
    provider: ProviderKind,
    model: &str,
    temperature: Option<f64>,
    max_tokens: Option<u32>,
) -> StageParameters {
    let temperature = if resolve(provider, model).sampling.accepts_overrides() {
        temperature
    } else {
        if let Some(temperature) = temperature {
            tracing::warn!(
                stage,
                model,
                temperature,
                "{CONFIGURED_TEMPERATURE_IGNORED}"
            );
        }
        None
    };

    StageParameters {
        temperature,
        max_tokens,
    }
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

/// Logged at debug when a `temperature` is dropped because the resolved target
/// samples adaptively. This is a mechanical wire-boundary step; the operator
/// notice that a configured value will not take effect is surfaced separately,
/// before the request is built.
const TEMPERATURE_DROPPED: &str =
    "dropping temperature: target model samples adaptively and rejects caller sampling parameters";

/// Reconciles a requested `temperature` against the target's sampling control,
/// dropping it (logging the discarded value at debug) when the target samples
/// adaptively.
///
/// `None` in yields `None` out, so the unset path is silent — only a present
/// value that the target rejects emits a log line. Shared by the cloud
/// providers' request builders; Ollama accepts sampling parameters and does
/// not call this.
pub(crate) fn reconcile_temperature(
    sampling: SamplingControl,
    requested: Option<f32>,
    model: &str,
) -> Option<f32> {
    if sampling.accepts_overrides() {
        requested
    } else {
        if let Some(temperature) = requested {
            tracing::debug!(model, temperature, "{TEMPERATURE_DROPPED}");
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
            assert_eq!(
                caps.structured_output_mode,
                StructuredOutputMode::Strict,
                "{model} should request strict schema enforcement",
            );
        }
    }

    #[test]
    fn test_ordinary_openai_model_sends_all() {
        let caps = resolve(ProviderKind::OpenAi, "gpt-4o-mini");
        assert_eq!(caps, ModelCapabilities::SEND_ALL);
    }

    #[test]
    fn test_unrecognised_openai_target_requests_strict() {
        // An unrecognised OpenAI target resolves strict-by-default: a 400 from a
        // surface that cannot honour it surfaces at the probe, which is
        // preferable to silent advisory drift.
        assert!(
            resolve(ProviderKind::OpenAi, "gpt-4o-mini")
                .structured_output_mode
                .requires_strict()
        );
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
    fn test_structured_output_mode_requires_strict() {
        assert!(StructuredOutputMode::Strict.requires_strict());
        assert!(!StructuredOutputMode::Advisory.requires_strict());
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
