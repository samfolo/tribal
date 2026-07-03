//! Outcome constructors and action for the four provider probe
//! checks (`provider_embedding`, `provider_extraction`,
//! `provider_triage`, `provider_relation`).
//!
//! Probes route through a check-local inference gateway, so a probe is a
//! real, minimal, billable call exercising exactly the path the server
//! uses. Probe usage ledgers when the database step produced a pool and
//! is recorded nowhere when the database is unreachable, keeping the
//! probes themselves decoupled from database connectivity.
//!
//! The inference probes read their stage's slice on [`CheckState::config`],
//! identified by the [`ProviderStage`] dispatched in by the step pipeline. The
//! embedding probe follows the live identity instead: it exercises the active
//! profile's endpoint when a corpus exists, falling back to the genesis seed
//! only before the first profile is provisioned.

use std::{sync::Arc, time::Duration};

use sqlx::PgPool;
use tribal_agent_runtime::PgLedgerSink;
use tribal_config::{ProviderStage, TribalConfig};
use tribal_db::{EmbeddingProfileRepository, PgEmbeddingProfileRepository};
use tribal_domain::{EmbeddingProfile, ProviderKind, TaskType, normalise_endpoint_url};
use tribal_inference::{
    EmbeddingTarget, InferenceGateway, LedgerSink, NoopLedgerSink, UsageAttribution,
    resolve_dimensions,
};
use tribal_telemetry::noop_recorder;

use super::{
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation},
};
use crate::{
    error::AppError,
    startup::{CatalogueCredentialResolver, build_command_registry, completion_stage_specs},
};

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
    let gateway = match ensure_gateway(state) {
        Ok(gateway) => gateway,
        Err(error) => {
            return CheckOutcome::provider_probe_failed(
                target,
                configured_provider(config(state), target),
                error,
            );
        }
    };

    // The embedding probe follows the live identity: once a corpus exists, the
    // active profile is the endpoint every read and write uses, so it is the
    // one a reachability check must exercise. The genesis seed is the fallback
    // only before the first profile exists (or when the database is
    // unreachable), the same identity first-boot provisioning will consume.
    // The inference stages read their own config block directly.
    let (provider, result) = match target {
        ProviderStage::Embedding => probe_live_embedding(state, &gateway).await,
        ProviderStage::Extraction => (
            config(state).inference.extraction.provider,
            probe_inference_stage(&gateway, TaskType::Extraction).await,
        ),
        ProviderStage::Triage => (
            config(state).inference.triage.provider,
            probe_inference_stage(&gateway, TaskType::Triage).await,
        ),
        ProviderStage::Relation => (
            config(state).inference.relation.provider,
            probe_inference_stage(&gateway, TaskType::Relation).await,
        ),
    };
    match result {
        Ok(()) => CheckOutcome::provider_probe_passed(target, provider),
        Err(error) => CheckOutcome::provider_probe_failed(target, provider, error),
    }
}

/// Returns the gateway for this run, building it on the first probe.
fn ensure_gateway(state: &mut CheckState) -> Result<Arc<InferenceGateway>, String> {
    if let Some(gateway) = &state.gateway {
        return Ok(Arc::clone(gateway));
    }
    let gateway =
        build_check_gateway(config(state), state.pool.clone()).map_err(|e| e.to_string())?;
    state.gateway = Some(Arc::clone(&gateway));
    Ok(gateway)
}

/// Builds the check-local gateway: a command registry from the inference
/// configuration, the catalogue credential resolver, and a ledger sink
/// matching what the run has to write to.
fn build_check_gateway(
    config: &TribalConfig,
    pool: Option<PgPool>,
) -> Result<Arc<InferenceGateway>, AppError> {
    let registry = build_command_registry(config)?;
    let sink: Arc<dyn LedgerSink> = match pool {
        Some(pool) => Arc::new(PgLedgerSink::new(pool, noop_recorder())),
        None => Arc::new(NoopLedgerSink),
    };
    let gateway = InferenceGateway::new(
        registry,
        &completion_stage_specs(config),
        Arc::new(CatalogueCredentialResolver::new(config.credentials.clone())),
        sink,
    )
    .map_err(|e| AppError::ProviderSetup {
        context: e.to_string(),
    })?;
    Ok(Arc::new(gateway))
}

/// Names the provider a probe would have exercised, for failures that
/// happen before any probe runs (the gateway itself failed to build).
fn configured_provider(config: &TribalConfig, target: ProviderStage) -> ProviderKind {
    match target {
        ProviderStage::Embedding => config.init.embedding.provider,
        ProviderStage::Extraction => config.inference.extraction.provider,
        ProviderStage::Triage => config.inference.triage.provider,
        ProviderStage::Relation => config.inference.relation.provider,
    }
}

/// 10s bounds a blackholed provider without choking slow cloud probes: the
/// gateway's per-provider request timeouts are sized for real inference, far
/// too generous for a reachability check. The bound cancels the whole probe
/// future, so in a rare window a probe that billed may go unledgered —
/// acceptable for a diagnostic command.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Names the bound in the failure detail so the outcome explains itself.
fn probe_timed_out() -> String {
    format!("no response within {}s", PROBE_TIMEOUT.as_secs())
}

/// Probes a single inference stage's configured endpoint.
async fn probe_inference_stage(gateway: &InferenceGateway, stage: TaskType) -> Result<(), String> {
    let attribution = UsageAttribution::default();
    let probe = gateway.probe_completion(stage, &attribution);
    match tokio::time::timeout(PROBE_TIMEOUT, probe).await {
        Ok(result) => result.map_err(|e| e.to_string()),
        Err(_) => Err(probe_timed_out()),
    }
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
async fn probe_live_embedding(
    state: &CheckState,
    gateway: &InferenceGateway,
) -> (ProviderKind, Result<(), String>) {
    let target = match active_embedding_target(state).await {
        Some(target) => target,
        None => match genesis_embedding_target(config(state)) {
            Ok(target) => target,
            Err(error) => return (config(state).init.embedding.provider, Err(error)),
        },
    };

    let provider = target.provider;
    let attribution = UsageAttribution::default();
    let probe = gateway.probe_embedding(&target, &attribution);
    let result = match tokio::time::timeout(PROBE_TIMEOUT, probe).await {
        Ok(result) => result.map(|_| ()).map_err(|e| e.to_string()),
        Err(_) => Err(probe_timed_out()),
    };
    (provider, result)
}

/// Resolves the active profile into an embedding target, when a pool
/// exists and a profile is active.
async fn active_embedding_target(state: &CheckState) -> Option<EmbeddingTarget> {
    let pool = state.pool.as_ref()?;
    let active = read_active_profile(pool).await?;
    Some(EmbeddingTarget::from(&active))
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

/// Builds the genesis embedding target (`init.embedding`), resolving its
/// dimension through the capability chain exactly as first-boot
/// provisioning does.
fn genesis_embedding_target(config: &TribalConfig) -> Result<EmbeddingTarget, String> {
    let init = &config.init.embedding;
    let provider = init.provider;
    let base_url = init
        .base_url
        .as_deref()
        .or_else(|| provider.default_base_url())
        .ok_or_else(|| format!("{provider} requires a base URL and has no default"))?;
    let dimensions =
        resolve_dimensions(provider, &init.model, init.dimensions).map_err(|e| e.to_string())?;
    let normalised_base_url = normalise_endpoint_url(base_url).map_err(|e| e.to_string())?;

    Ok(EmbeddingTarget {
        provider,
        model: init.model.clone(),
        dimensions,
        base_url: normalised_base_url,
        profile_id: None,
    })
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
        let gateway = build_check_gateway(&config, None).expect("a check gateway");
        let active = an_embedding_profile()
            .provider_kind(ProviderKind::OpenAi)
            .normalised_base_url("https://migrated-host:443".to_owned())
            .build();

        let error = gateway
            .probe_embedding(
                &EmbeddingTarget::from(&active),
                &UsageAttribution::default(),
            )
            .await
            .expect_err("an unresolved active credential must fail the probe")
            .to_string();
        assert!(
            error.contains("https://migrated-host:443"),
            "the probe targets the active endpoint: {error}"
        );
    }
}
