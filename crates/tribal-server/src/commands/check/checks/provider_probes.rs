//! Outcome constructors and action for the four provider probe
//! checks (`provider_embedding`, `provider_extraction`,
//! `provider_triage`, `provider_relation`).
//!
//! Each invocation calls the matching `probe_*_provider` helper against
//! the configured slice on [`CheckState::config`], identified by the
//! [`ProviderProbeTarget`] dispatched in by the step pipeline.

use tribal_config::{ProviderKind, TribalConfig};

use super::{
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation, ProviderProbeTarget},
};
use crate::startup::{probe_embedding_provider, probe_inference_provider};

impl CheckOutcome {
    pub(in crate::commands::check) fn provider_probe_passed(
        target: ProviderProbeTarget,
        provider: ProviderKind,
    ) -> Self {
        Self::Pass {
            detail: CheckDetail::ProviderProbePassed { target, provider },
        }
    }

    pub(in crate::commands::check) fn provider_probe_failed(
        target: ProviderProbeTarget,
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
    target: ProviderProbeTarget,
) -> CheckOutcome {
    let config = state
        .config
        .as_ref()
        .expect("preflight ensures state.config is populated");
    let provider = provider_kind(config, target);
    let result = match target {
        ProviderProbeTarget::Embedding => {
            probe_embedding_provider(state.http_client.clone(), &config.embedding).await
        }
        ProviderProbeTarget::Extraction => {
            probe_inference_provider(state.http_client.clone(), &config.inference.extraction).await
        }
        ProviderProbeTarget::Triage => {
            probe_inference_provider(state.http_client.clone(), &config.inference.triage).await
        }
        ProviderProbeTarget::Relation => {
            probe_inference_provider(state.http_client.clone(), &config.inference.relation).await
        }
    };
    match result {
        Ok(()) => CheckOutcome::provider_probe_passed(target, provider),
        Err(err) => CheckOutcome::provider_probe_failed(target, provider, err.to_string()),
    }
}

fn provider_kind(config: &TribalConfig, target: ProviderProbeTarget) -> ProviderKind {
    match target {
        ProviderProbeTarget::Embedding => config.embedding.provider,
        ProviderProbeTarget::Extraction => config.inference.extraction.provider,
        ProviderProbeTarget::Triage => config.inference.triage.provider,
        ProviderProbeTarget::Relation => config.inference.relation.provider,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_probe_passed_is_pass() {
        let outcome = CheckOutcome::provider_probe_passed(
            ProviderProbeTarget::Embedding,
            ProviderKind::OpenAi,
        );
        assert!(matches!(
            &outcome,
            CheckOutcome::Pass {
                detail: CheckDetail::ProviderProbePassed {
                    target: ProviderProbeTarget::Embedding,
                    provider: ProviderKind::OpenAi,
                },
            },
        ));
    }

    #[test]
    fn test_provider_probe_failed_carries_target_provider_and_remediation() {
        let outcome = CheckOutcome::provider_probe_failed(
            ProviderProbeTarget::Triage,
            ProviderKind::Anthropic,
            "rate limit exceeded".into(),
        );
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::ProviderProbeFailed {
                    target: ProviderProbeTarget::Triage,
                    provider: ProviderKind::Anthropic,
                    error,
                },
                remediation: CheckRemediation::FixProviderConfig {
                    target: ProviderProbeTarget::Triage,
                    provider: ProviderKind::Anthropic,
                },
            } if error == "rate limit exceeded",
        ));
    }
}
