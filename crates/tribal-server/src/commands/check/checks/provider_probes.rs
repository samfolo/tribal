//! Outcome constructors and action for the four provider probe
//! checks (`provider_embedding`, `provider_extraction`,
//! `provider_triage`, `provider_relation`).
//!
//! Each invocation calls the matching `probe_*_provider` helper against
//! the configured slice on [`CheckState::config`], identified by the
//! [`ProviderStage`] dispatched in by the step pipeline.

use tribal_config::{ProviderStage, TribalConfig};
use tribal_domain::ProviderKind;

use super::{
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation},
};
use crate::startup::{probe_embedding_provider, probe_inference_provider};

impl CheckOutcome {
    pub(in crate::commands::check) fn provider_probe_passed(
        target: ProviderStage,
        provider: ProviderKind,
    ) -> Self {
        Self::Pass {
            detail: CheckDetail::ProviderProbePassed { target, provider },
        }
    }

    pub(in crate::commands::check) fn provider_probe_failed(
        target: ProviderStage,
        provider: ProviderKind,
        error: String,
    ) -> Self {
        Self::Fail {
            detail: CheckDetail::ProviderProbeFailed {
                target,
                provider,
                error,
            },
            remediation: CheckRemediation::FixProviderConfig { target, provider },
        }
    }
}

/// Probes the configured provider for `target`.  The `target` is the
/// step dispatcher's identification of which config block to read.
pub(in crate::commands::check) async fn act(
    state: &mut CheckState,
    target: ProviderStage,
) -> CheckOutcome {
    let config = state
        .config
        .as_ref()
        .expect("preflight ensures state.config is populated");
    let provider = provider_kind(config, target);
    let result = match target {
        ProviderStage::Embedding => {
            probe_embedding_provider(state.http_client.clone(), &config.embedding).await
        }
        ProviderStage::Extraction => {
            probe_inference_provider(state.http_client.clone(), &config.inference.extraction).await
        }
        ProviderStage::Triage => {
            probe_inference_provider(state.http_client.clone(), &config.inference.triage).await
        }
        ProviderStage::Relation => {
            probe_inference_provider(state.http_client.clone(), &config.inference.relation).await
        }
    };
    match result {
        Ok(()) => CheckOutcome::provider_probe_passed(target, provider),
        Err(err) => CheckOutcome::provider_probe_failed(target, provider, err.to_string()),
    }
}

fn provider_kind(config: &TribalConfig, target: ProviderStage) -> ProviderKind {
    match target {
        ProviderStage::Embedding => config.embedding.provider,
        ProviderStage::Extraction => config.inference.extraction.provider,
        ProviderStage::Triage => config.inference.triage.provider,
        ProviderStage::Relation => config.inference.relation.provider,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_probe_passed_is_pass() {
        let outcome =
            CheckOutcome::provider_probe_passed(ProviderStage::Embedding, ProviderKind::OpenAi);
        assert!(matches!(
            &outcome,
            CheckOutcome::Pass {
                detail: CheckDetail::ProviderProbePassed {
                    target: ProviderStage::Embedding,
                    provider: ProviderKind::OpenAi,
                },
            },
        ));
    }

    #[test]
    fn test_provider_probe_failed_carries_target_provider_and_remediation() {
        let outcome = CheckOutcome::provider_probe_failed(
            ProviderStage::Triage,
            ProviderKind::Anthropic,
            "rate limit exceeded".into(),
        );
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::ProviderProbeFailed {
                    target: ProviderStage::Triage,
                    provider: ProviderKind::Anthropic,
                    error,
                },
                remediation: CheckRemediation::FixProviderConfig {
                    target: ProviderStage::Triage,
                    provider: ProviderKind::Anthropic,
                },
            } if error == "rate limit exceeded",
        ));
    }
}
