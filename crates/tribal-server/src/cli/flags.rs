//! Persistable CLI flag identity.
//!
//! The bootstrap renderer's conditional persistence step pairs each
//! flag the user passed with the matching `TRIBAL_*` env var. Both
//! sides need to agree on the flag's literal name, its config path,
//! and whether it was set on a given run.
//!
//! [`PersistableFlag`] is the single source of truth: clap arg
//! attributes reference each variant's [`PersistableFlag::flag_name`]
//! (a `const fn`, so the attribute parses at compile time), and the
//! renderer reads the same variant for its display strings.

use tribal_config::{CliOverrides, env_var_for_path};

/// CLI flag whose value the bootstrap renderer may persist into the
/// user's config file on first run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistableFlag {
    EmbeddingProvider,
    EmbeddingModel,
    InferenceExtractionProvider,
    InferenceExtractionModel,
    InferenceTriageProvider,
    InferenceTriageModel,
    InferenceRelationProvider,
    InferenceRelationModel,
    TelemetryOtlpEndpoint,
}

impl PersistableFlag {
    /// Long-flag name as accepted by clap (no leading `--`).
    #[must_use]
    pub const fn flag_name(self) -> &'static str {
        match self {
            Self::EmbeddingProvider => "embedding-provider",
            Self::EmbeddingModel => "embedding-model",
            Self::InferenceExtractionProvider => "inference-extraction-provider",
            Self::InferenceExtractionModel => "inference-extraction-model",
            Self::InferenceTriageProvider => "inference-triage-provider",
            Self::InferenceTriageModel => "inference-triage-model",
            Self::InferenceRelationProvider => "inference-relation-provider",
            Self::InferenceRelationModel => "inference-relation-model",
            Self::TelemetryOtlpEndpoint => "telemetry-otlp-endpoint",
        }
    }

    /// Dot-separated config path the flag resolves into through the
    /// figment cascade. Distinct from [`Self::flag_name`] because the
    /// underlying serde fields may use `snake_case` segments (e.g.
    /// `otlp_endpoint`) that the kebab-cased flag flattens.
    #[must_use]
    pub const fn config_path(self) -> &'static str {
        match self {
            Self::EmbeddingProvider => "embedding.provider",
            Self::EmbeddingModel => "embedding.model",
            Self::InferenceExtractionProvider => "inference.extraction.provider",
            Self::InferenceExtractionModel => "inference.extraction.model",
            Self::InferenceTriageProvider => "inference.triage.provider",
            Self::InferenceTriageModel => "inference.triage.model",
            Self::InferenceRelationProvider => "inference.relation.provider",
            Self::InferenceRelationModel => "inference.relation.model",
            Self::TelemetryOtlpEndpoint => "telemetry.otlp_endpoint",
        }
    }

    /// `TRIBAL_*` env var name the user can export to make this
    /// flag's value durable without editing the config file.
    #[must_use]
    pub fn env_var(self) -> String {
        env_var_for_path(self.config_path())
    }

    /// Returns the persistable flags the user explicitly set, in the
    /// canonical declaration order of the enum. Use [`Self::resolve_overrides`]
    /// when the caller needs each flag's resolved value too.
    #[must_use]
    pub fn from_cli_overrides(overrides: &CliOverrides) -> Vec<Self> {
        Self::resolve_overrides(overrides)
            .into_iter()
            .map(|(flag, _)| flag)
            .collect()
    }

    /// Returns the persistable flags the user explicitly set in
    /// `overrides`, paired with each flag's resolved value, in the
    /// canonical declaration order of the enum.
    #[must_use]
    pub fn resolve_overrides(overrides: &CliOverrides) -> Vec<(Self, String)> {
        let mut out = Vec::new();
        if let Some(embedding) = &overrides.embedding {
            if let Some(provider) = &embedding.provider {
                out.push((Self::EmbeddingProvider, provider.to_string()));
            }
            if let Some(model) = &embedding.model {
                out.push((Self::EmbeddingModel, model.clone()));
            }
        }
        if let Some(inference) = &overrides.inference {
            if let Some(stage) = &inference.extraction {
                if let Some(provider) = &stage.provider {
                    out.push((Self::InferenceExtractionProvider, provider.to_string()));
                }
                if let Some(model) = &stage.model {
                    out.push((Self::InferenceExtractionModel, model.clone()));
                }
            }
            if let Some(stage) = &inference.triage {
                if let Some(provider) = &stage.provider {
                    out.push((Self::InferenceTriageProvider, provider.to_string()));
                }
                if let Some(model) = &stage.model {
                    out.push((Self::InferenceTriageModel, model.clone()));
                }
            }
            if let Some(stage) = &inference.relation {
                if let Some(provider) = &stage.provider {
                    out.push((Self::InferenceRelationProvider, provider.to_string()));
                }
                if let Some(model) = &stage.model {
                    out.push((Self::InferenceRelationModel, model.clone()));
                }
            }
        }
        if let Some(telemetry) = &overrides.telemetry
            && let Some(endpoint) = &telemetry.otlp_endpoint
        {
            out.push((Self::TelemetryOtlpEndpoint, endpoint.clone()));
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_config::{
        CliOverrides, EmbeddingCliOverrides, InferenceCliOverrides, InferenceStageCliOverrides,
        ProviderKind, TelemetryCliOverrides,
    };

    use super::*;

    #[test]
    fn test_config_path_uses_dots_between_segments() {
        assert_eq!(
            PersistableFlag::InferenceExtractionProvider.config_path(),
            "inference.extraction.provider",
        );
    }

    #[test]
    fn test_config_path_preserves_snake_case_within_segments() {
        // The CLI flag is `--telemetry-otlp-endpoint` (kebab-case)
        // but the underlying config field is `telemetry.otlp_endpoint`
        // (segment uses snake_case). A mechanical `-` → `.` swap
        // would land on the wrong path; the explicit match keeps the
        // mapping load-bearing.
        assert_eq!(
            PersistableFlag::TelemetryOtlpEndpoint.config_path(),
            "telemetry.otlp_endpoint",
        );
    }

    #[test]
    fn test_env_var_routes_through_loader_convention() {
        assert_eq!(
            PersistableFlag::EmbeddingProvider.env_var(),
            "TRIBAL_EMBEDDING__PROVIDER",
        );
    }

    #[test]
    fn test_env_var_preserves_snake_case_segment() {
        // `otlp_endpoint` keeps its underscore through the env var
        // derivation; `__` is the segment separator the loader splits
        // on, `_` inside a segment is part of the field name.
        assert_eq!(
            PersistableFlag::TelemetryOtlpEndpoint.env_var(),
            "TRIBAL_TELEMETRY__OTLP_ENDPOINT",
        );
    }

    #[test]
    fn test_from_cli_overrides_returns_only_set_flags() {
        let overrides = CliOverrides {
            embedding: Some(EmbeddingCliOverrides {
                provider: Some(ProviderKind::OpenAi),
                model: None,
            }),
            inference: Some(InferenceCliOverrides {
                extraction: Some(InferenceStageCliOverrides {
                    provider: None,
                    model: Some("gpt-4o-mini".into()),
                }),
                triage: None,
                relation: None,
            }),
            telemetry: Some(TelemetryCliOverrides {
                otlp_endpoint: Some("http://localhost:4317".into()),
            }),
            ..CliOverrides::default()
        };
        let flags = PersistableFlag::from_cli_overrides(&overrides);
        assert_eq!(
            flags,
            vec![
                PersistableFlag::EmbeddingProvider,
                PersistableFlag::InferenceExtractionModel,
                PersistableFlag::TelemetryOtlpEndpoint,
            ],
        );
    }

    #[test]
    fn test_from_cli_overrides_empty_when_nothing_set() {
        let flags = PersistableFlag::from_cli_overrides(&CliOverrides::default());
        assert!(flags.is_empty());
    }
}
