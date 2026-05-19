//! One-pass classification of phase-2 validation errors into the
//! phase-3 skip decisions they imply, plus the cascade-skip constructor
//! used by phase-1 fan-out.

use tribal_config::{
    EMBEDDING_API_KEY_REQUIRED_PREFIX, EXTRACTION_API_KEY_REQUIRED_PREFIX,
    RELATION_API_KEY_REQUIRED_PREFIX, SERVER_BIND_ADDRESS_ERROR_PREFIX,
    TRIAGE_API_KEY_REQUIRED_PREFIX,
};

use super::types::{CheckDetail, CheckName, CheckOutcome, ProviderProbeTarget, SkipReason};

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
/// phase-2 validation failure.  Built in a single pass over the error
/// list; queried per call site at orchestration time.
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
    pub(in crate::commands::check) fn from_validation_errors(errors: &[String]) -> Self {
        let mut mask = Self::default();
        for error in errors {
            // Each error matches at most one prefix; first hit wins.
            if error.starts_with(SERVER_BIND_ADDRESS_ERROR_PREFIX) {
                mask.bits |= flag::ADVERTISED_URL;
            } else if error.starts_with(EMBEDDING_API_KEY_REQUIRED_PREFIX) {
                mask.bits |= flag::PROVIDER_EMBEDDING;
            } else if error.starts_with(EXTRACTION_API_KEY_REQUIRED_PREFIX) {
                mask.bits |= flag::PROVIDER_EXTRACTION;
            } else if error.starts_with(TRIAGE_API_KEY_REQUIRED_PREFIX) {
                mask.bits |= flag::PROVIDER_TRIAGE;
            } else if error.starts_with(RELATION_API_KEY_REQUIRED_PREFIX) {
                mask.bits |= flag::PROVIDER_RELATION;
            }
        }
        mask
    }

    pub(in crate::commands::check) fn skip_advertised_url(self) -> bool {
        self.bits & flag::ADVERTISED_URL != 0
    }

    pub(in crate::commands::check) fn skip_provider_probe(
        self,
        target: ProviderProbeTarget,
    ) -> bool {
        let bit = match target {
            ProviderProbeTarget::Embedding => flag::PROVIDER_EMBEDDING,
            ProviderProbeTarget::Extraction => flag::PROVIDER_EXTRACTION,
            ProviderProbeTarget::Triage => flag::PROVIDER_TRIAGE,
            ProviderProbeTarget::Relation => flag::PROVIDER_RELATION,
        };
        self.bits & bit != 0
    }
}

#[cfg(test)]
mod tests {
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
    fn test_skip_mask_empty_errors_yields_no_skips() {
        let mask = SkipMask::from_validation_errors(&[]);
        assert!(!mask.skip_advertised_url());
        for target in [
            ProviderProbeTarget::Embedding,
            ProviderProbeTarget::Extraction,
            ProviderProbeTarget::Triage,
            ProviderProbeTarget::Relation,
        ] {
            assert!(!mask.skip_provider_probe(target));
        }
    }

    #[test]
    fn test_skip_mask_sets_advertised_url_for_stdio_bind_conflict() {
        let errors =
            vec!["server.bind_address cannot be set when server.transport is stdio".to_owned()];
        let mask = SkipMask::from_validation_errors(&errors);
        assert!(mask.skip_advertised_url());
    }

    #[test]
    fn test_skip_mask_sets_advertised_url_for_malformed_bind_address() {
        let errors =
            vec!["server.bind_address is not a valid socket address: not-an-address".to_owned()];
        let mask = SkipMask::from_validation_errors(&errors);
        assert!(mask.skip_advertised_url());
    }

    #[test]
    fn test_skip_mask_unrelated_errors_set_nothing() {
        let errors = vec![
            "database.url must not be empty".to_owned(),
            "auth.token_ttl_hours must be greater than zero".to_owned(),
        ];
        let mask = SkipMask::from_validation_errors(&errors);
        assert_eq!(mask, SkipMask::default());
    }

    #[test]
    fn test_skip_mask_classifies_each_target_to_its_prefix() {
        for (target, error) in [
            (
                ProviderProbeTarget::Embedding,
                "embedding.api_key is required when embedding.provider is openai",
            ),
            (
                ProviderProbeTarget::Extraction,
                "inference.extraction.api_key is required when \
                 inference.extraction.provider is openai",
            ),
            (
                ProviderProbeTarget::Triage,
                "inference.triage.api_key is required when inference.triage.provider is openai",
            ),
            (
                ProviderProbeTarget::Relation,
                "inference.relation.api_key is required when \
                 inference.relation.provider is openai",
            ),
        ] {
            let mask = SkipMask::from_validation_errors(&[error.to_owned()]);
            assert!(
                mask.skip_provider_probe(target),
                "{target:?} did not match {error:?}"
            );
            for other in [
                ProviderProbeTarget::Embedding,
                ProviderProbeTarget::Extraction,
                ProviderProbeTarget::Triage,
                ProviderProbeTarget::Relation,
            ] {
                if other != target {
                    assert!(
                        !mask.skip_provider_probe(other),
                        "{other:?} matched a {target:?}-specific error"
                    );
                }
            }
        }
    }

    #[test]
    fn test_skip_mask_aggregates_mixed_errors_into_one_pass() {
        let errors = vec![
            "database.url must not be empty".to_owned(),
            "server.bind_address cannot be set when server.transport is stdio".to_owned(),
            "embedding.api_key is required when embedding.provider is openai".to_owned(),
            "inference.triage.api_key is required when inference.triage.provider is openai"
                .to_owned(),
        ];
        let mask = SkipMask::from_validation_errors(&errors);
        assert!(mask.skip_advertised_url());
        assert!(mask.skip_provider_probe(ProviderProbeTarget::Embedding));
        assert!(mask.skip_provider_probe(ProviderProbeTarget::Triage));
        assert!(!mask.skip_provider_probe(ProviderProbeTarget::Extraction));
        assert!(!mask.skip_provider_probe(ProviderProbeTarget::Relation));
    }
}
