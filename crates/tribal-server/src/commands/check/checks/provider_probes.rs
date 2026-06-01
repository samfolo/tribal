//! Outcome constructors and action for the four provider probe
//! checks (`provider_embedding`, `provider_extraction`,
//! `provider_triage`, `provider_relation`).
//!
//! The inference probes read their stage's slice on [`CheckState::config`],
//! identified by the [`ProviderStage`] dispatched in by the step pipeline. The
//! embedding probe follows the live identity instead: it exercises the active
//! profile's endpoint when a corpus exists, falling back to the genesis seed
//! only before the first profile is provisioned.

use sqlx::PgPool;
use tribal_config::{ProviderStage, StageInferenceConfig, TribalConfig};
use tribal_db::{EmbeddingProfileRepository, PgEmbeddingProfileRepository};
use tribal_domain::{EmbeddingProfile, ProviderKind, normalise_endpoint_url};
use tribal_inference::resolve_dimensions;

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
        // The embedding credential resolves through the catalogue, not an
        // `init.embedding.api_key` field, so its probe failure routes to a
        // remediation naming `init.embedding.base_url` and the catalogue
        // credential rather than the removed `embedding.*` paths.
        let remediation = match target {
            ProviderStage::Embedding => CheckRemediation::FixEmbeddingProviderConfig { provider },
            ProviderStage::Extraction | ProviderStage::Triage | ProviderStage::Relation => {
                CheckRemediation::FixProviderConfig { target, provider }
            }
        };
        Self::Fail {
            detail: CheckDetail::ProviderProbeFailed {
                target,
                provider,
                error,
            },
            remediation,
        }
    }
}

/// Probes the configured provider for `target`.  The `target` is the
/// step dispatcher's identification of which config block to read.
pub(in crate::commands::check) async fn act(
    state: &mut CheckState,
    target: ProviderStage,
) -> CheckOutcome {
    // The embedding probe follows the live identity: once a corpus exists, the
    // active profile (§5.7) is the endpoint every read and write uses, so it is
    // the one a reachability check must exercise. The genesis seed is the
    // fallback only before the first profile exists (or when the database is
    // unreachable), the same identity first-boot provisioning will consume. The
    // inference stages read their own config block directly.
    let (provider, result) = match target {
        ProviderStage::Embedding => probe_live_embedding(state).await,
        ProviderStage::Extraction => {
            probe_inference_stage(state, &config(state).inference.extraction).await
        }
        ProviderStage::Triage => {
            probe_inference_stage(state, &config(state).inference.triage).await
        }
        ProviderStage::Relation => {
            probe_inference_stage(state, &config(state).inference.relation).await
        }
    };
    match result {
        Ok(()) => CheckOutcome::provider_probe_passed(target, provider),
        Err(error) => CheckOutcome::provider_probe_failed(target, provider, error),
    }
}

/// Probes a single inference stage's configured provider, returning the provider
/// it probed.
async fn probe_inference_stage(
    state: &CheckState,
    inference: &StageInferenceConfig,
) -> (ProviderKind, Result<(), String>) {
    let result = probe_inference_provider(state.http_client.clone(), inference)
        .await
        .map_err(|e| e.to_string());
    (inference.provider, result)
}

/// Borrows the parsed config, which the preflight guarantees is present.
fn config(state: &CheckState) -> &TribalConfig {
    state
        .config
        .as_ref()
        .expect("preflight ensures state.config is populated")
}

/// Probes the live embedding endpoint, returning the provider it probed so the
/// outcome names the endpoint that was actually exercised.
///
/// Probes the active profile when one exists, falling back to the genesis seed
/// when the corpus has no profile yet or the database is unreachable.
async fn probe_live_embedding(state: &CheckState) -> (ProviderKind, Result<(), String>) {
    let config = state
        .config
        .as_ref()
        .expect("preflight ensures state.config is populated");

    if let Some(pool) = state.pool.as_ref()
        && let Some(active) = read_active_profile(pool).await
    {
        return (
            active.provider_kind(),
            probe_active_embedding(state.http_client.clone(), config, &active).await,
        );
    }

    (
        config.init.embedding.provider,
        probe_genesis_embedding(state.http_client.clone(), config).await,
    )
}

/// Reads the active embedding profile, treating any query failure as "no active
/// profile": the embedding-profile check already grades a database fault, so the
/// probe degrades to the genesis seed rather than double-reporting it.
async fn read_active_profile(pool: &PgPool) -> Option<EmbeddingProfile> {
    let mut conn = pool.acquire().await.ok()?;
    PgEmbeddingProfileRepository
        .find_active(&mut conn)
        .await
        .ok()
        .flatten()
}

/// Probes the active profile's endpoint for reachability, resolving its
/// credential through the catalogue the same way the server boot does.
async fn probe_active_embedding(
    client: reqwest::Client,
    config: &TribalConfig,
    active: &EmbeddingProfile,
) -> Result<(), String> {
    let provider = active.provider_kind();
    let normalised_base_url = active.normalised_base_url();
    let api_key = config
        .credentials
        .resolve_api_key(provider, normalised_base_url)
        .map_err(|e| e.to_string())?;

    probe_embedding_provider(
        client,
        provider,
        active.model(),
        active.dimensions(),
        normalised_base_url,
        api_key,
    )
    .await
    .map_err(|e| e.to_string())
}

/// Probes the genesis embedding seed (`init.embedding`) for reachability,
/// resolving its dimension through the capability chain and its credential
/// through the catalogue exactly as first-boot provisioning does.
async fn probe_genesis_embedding(
    client: reqwest::Client,
    config: &TribalConfig,
) -> Result<(), String> {
    let init = &config.init.embedding;
    let provider = init.provider;
    let base_url = init
        .base_url
        .as_deref()
        .unwrap_or_else(|| provider.default_base_url());
    let dimensions =
        resolve_dimensions(provider, &init.model, init.dimensions).map_err(|e| e.to_string())?;
    let normalised_base_url = normalise_endpoint_url(base_url).map_err(|e| e.to_string())?;
    let api_key = config
        .credentials
        .resolve_api_key(provider, &normalised_base_url)
        .map_err(|e| e.to_string())?;

    probe_embedding_provider(client, provider, &init.model, dimensions, base_url, api_key)
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use tribal_test_utils::an_embedding_profile;

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

    #[test]
    fn test_embedding_probe_failure_routes_to_the_catalogue_remediation() {
        let outcome = CheckOutcome::provider_probe_failed(
            ProviderStage::Embedding,
            ProviderKind::OpenAi,
            "connection refused".into(),
        );
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::ProviderProbeFailed {
                    target: ProviderStage::Embedding,
                    provider: ProviderKind::OpenAi,
                    error,
                },
                remediation: CheckRemediation::FixEmbeddingProviderConfig {
                    provider: ProviderKind::OpenAi,
                },
            } if error == "connection refused",
        ));
    }

    #[tokio::test]
    async fn test_active_embedding_probe_resolves_against_the_active_endpoint() {
        // A post-migration corpus: the active profile points at an endpoint that
        // differs from the genesis seed. The probe must resolve the credential
        // against the active endpoint, so its failure names that endpoint, not
        // the genesis one.
        let config = TribalConfig::default();
        let active = an_embedding_profile()
            .provider_kind(ProviderKind::Anthropic)
            .normalised_base_url("https://migrated-host:443".to_owned())
            .build();

        let error = probe_active_embedding(reqwest::Client::new(), &config, &active)
            .await
            .expect_err("an unresolved active credential must fail the probe");
        assert!(
            error.contains("https://migrated-host:443"),
            "the probe targets the active endpoint: {error}"
        );
    }
}
