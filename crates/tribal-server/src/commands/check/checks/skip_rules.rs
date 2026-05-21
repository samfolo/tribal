//! One-pass classification of phase-2 validation diagnostics into the
//! phase-3 skip decisions they imply, plus the cascade-skip constructor
//! used by phase-1 fan-out.

use tribal_config::{ProviderStage, ValidationError};

use super::types::{CheckDetail, CheckName, CheckOutcome, SkipReason};

impl CheckOutcome {
    /// Constructs a `Skip` outcome for `name`, attributing the skip to
    /// `reason`.
    pub(in crate::commands::check) fn dependency_skipped(
        name: CheckName,
        reason: SkipReason,
    ) -> Self {
        Self::Skip {
            detail: CheckDetail::DependencySkipped { name, reason },
        }
    }
}

/// Decisions about which phase-3 checks must be skipped because of a
/// phase-2 validation failure.  Built in a single pass over the
/// diagnostics; queried per call site at orchestration time.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(in crate::commands::check) struct SkipMask {
    bits: u8,
}

/// Bit positions inside [`SkipMask::bits`].  Private — callers query
/// the mask through the named methods below.
mod flag {
    pub(super) const ADVERTISED_URL: u8 = 1 << 0;
    pub(super) const PROVIDER_EMBEDDING: u8 = 1 << 1;
    pub(super) const PROVIDER_EXTRACTION: u8 = 1 << 2;
    pub(super) const PROVIDER_TRIAGE: u8 = 1 << 3;
    pub(super) const PROVIDER_RELATION: u8 = 1 << 4;
}

impl SkipMask {
    pub(in crate::commands::check) fn from_validation_errors(
        diagnostics: &[ValidationError],
    ) -> Self {
        let mut mask = Self::default();
        for diagnostic in diagnostics {
            mask.classify(diagnostic);
        }
        mask
    }

    /// Sets the bit(s) implied by a single diagnostic.  The match is
    /// exhaustive: a new [`ValidationError`] variant fails the build
    /// here until its skip semantics are decided.
    fn classify(&mut self, diagnostic: &ValidationError) {
        match diagnostic {
            ValidationError::BindAddressStdioConflict
            | ValidationError::BindAddressMalformed { .. } => {
                self.bits |= flag::ADVERTISED_URL;
            }
            ValidationError::MissingApiKey { stage, .. } => match stage {
                ProviderStage::Embedding => self.bits |= flag::PROVIDER_EMBEDDING,
                ProviderStage::Extraction => self.bits |= flag::PROVIDER_EXTRACTION,
                ProviderStage::Triage => self.bits |= flag::PROVIDER_TRIAGE,
                ProviderStage::Relation => self.bits |= flag::PROVIDER_RELATION,
            },
            ValidationError::Empty { .. }
            | ValidationError::BelowMin { .. }
            | ValidationError::AboveMax { .. }
            | ValidationError::OutOfRange { .. }
            | ValidationError::FieldOrdering { .. }
            | ValidationError::DerivedFloor { .. }
            | ValidationError::EmbeddingProviderUnsupported { .. }
            | ValidationError::TelemetryFileExportRequiresEnabled => {
                // No downstream skip implied.
            }
        }
    }

    pub(in crate::commands::check) fn skip_advertised_url(self) -> bool {
        self.bits & flag::ADVERTISED_URL != 0
    }

    pub(in crate::commands::check) fn skip_provider_probe(self, target: ProviderStage) -> bool {
        let bit = match target {
            ProviderStage::Embedding => flag::PROVIDER_EMBEDDING,
            ProviderStage::Extraction => flag::PROVIDER_EXTRACTION,
            ProviderStage::Triage => flag::PROVIDER_TRIAGE,
            ProviderStage::Relation => flag::PROVIDER_RELATION,
        };
        self.bits & bit != 0
    }
}

#[cfg(test)]
mod tests {
    use tribal_config::{ConfigPath, ProviderKind};

    use super::*;

    #[test]
    fn test_dependency_skipped_carries_name_and_reason() {
        let outcome = CheckOutcome::dependency_skipped(
            CheckName::AdvertisedUrlReachable,
            SkipReason::ConfigValidateFailedTransportBind,
        );
        assert!(matches!(
            &outcome,
            CheckOutcome::Skip {
                detail: CheckDetail::DependencySkipped {
                    name: CheckName::AdvertisedUrlReachable,
                    reason: SkipReason::ConfigValidateFailedTransportBind,
                },
            },
        ));
    }

    #[test]
    fn test_skip_mask_empty_diagnostics_yields_no_skips() {
        let mask = SkipMask::from_validation_errors(&[]);
        assert!(!mask.skip_advertised_url());
        for target in [
            ProviderStage::Embedding,
            ProviderStage::Extraction,
            ProviderStage::Triage,
            ProviderStage::Relation,
        ] {
            assert!(!mask.skip_provider_probe(target));
        }
    }

    #[test]
    fn test_skip_mask_sets_advertised_url_for_stdio_bind_conflict() {
        let mask = SkipMask::from_validation_errors(&[ValidationError::BindAddressStdioConflict]);
        assert!(mask.skip_advertised_url());
    }

    #[test]
    fn test_skip_mask_sets_advertised_url_for_malformed_bind_address() {
        let mask = SkipMask::from_validation_errors(&[ValidationError::BindAddressMalformed {
            value: "not-an-address".into(),
        }]);
        assert!(mask.skip_advertised_url());
    }

    #[test]
    fn test_skip_mask_structural_errors_set_nothing() {
        let mask = SkipMask::from_validation_errors(&[
            ValidationError::Empty {
                field: ConfigPath::from_static("database.url"),
            },
            ValidationError::BelowMin {
                field: ConfigPath::from_static("auth.token_ttl_hours"),
                value: 0,
                min: 1,
            },
        ]);
        assert_eq!(mask, SkipMask::default());
    }

    #[test]
    fn test_skip_mask_classifies_each_stage_to_its_own_flag() {
        let all = [
            ProviderStage::Embedding,
            ProviderStage::Extraction,
            ProviderStage::Triage,
            ProviderStage::Relation,
        ];
        for stage in all {
            let mask = SkipMask::from_validation_errors(&[ValidationError::MissingApiKey {
                stage,
                provider: ProviderKind::OpenAi,
            }]);
            assert!(
                mask.skip_provider_probe(stage),
                "{stage:?} did not match its own diagnostic",
            );
            for other in all {
                if other != stage {
                    assert!(
                        !mask.skip_provider_probe(other),
                        "{other:?} matched a {stage:?}-specific diagnostic",
                    );
                }
            }
        }
    }

    #[test]
    fn test_skip_mask_aggregates_mixed_diagnostics_into_one_pass() {
        let mask = SkipMask::from_validation_errors(&[
            ValidationError::Empty {
                field: ConfigPath::from_static("database.url"),
            },
            ValidationError::BindAddressStdioConflict,
            ValidationError::MissingApiKey {
                stage: ProviderStage::Embedding,
                provider: ProviderKind::OpenAi,
            },
            ValidationError::MissingApiKey {
                stage: ProviderStage::Triage,
                provider: ProviderKind::OpenAi,
            },
        ]);
        assert!(mask.skip_advertised_url());
        assert!(mask.skip_provider_probe(ProviderStage::Embedding));
        assert!(mask.skip_provider_probe(ProviderStage::Triage));
        assert!(!mask.skip_provider_probe(ProviderStage::Extraction));
        assert!(!mask.skip_provider_probe(ProviderStage::Relation));
    }
}
