//! Outcome constructors and action for the `embedding_profile` check.
//!
//! Reports the live embedding identity against the genesis seed and the
//! catalogue: the active provider's credential being unresolvable is a fail
//! (the boot fail-closed precondition), a live reindex run or quarantined
//! items are a warning, and a genesis seed that no longer matches the live
//! profile is informational state, never a warning, because the seed is stale
//! by design once a corpus exists.

use tribal_config::MissingApiKeyKind;
use tribal_db::{
    EmbeddingProfileRepository, PgEmbeddingProfileRepository, PgReindexQuarantineRepository,
    PgReindexRunRepository, ReindexQuarantineRepository, ReindexRunRepository,
};
use tribal_domain::{ProviderKind, ReindexRunState};

use super::{
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation},
};

impl CheckOutcome {
    pub(in crate::commands::check) fn embedding_profile_pending() -> Self {
        Self::Pass {
            detail: CheckDetail::EmbeddingProfilePending,
        }
    }

    pub(in crate::commands::check) fn embedding_profile_live(
        provider: ProviderKind,
        model: String,
        dimensions: u32,
        genesis_drift: Option<String>,
    ) -> Self {
        Self::Pass {
            detail: CheckDetail::EmbeddingProfileLive {
                provider,
                model,
                dimensions,
                genesis_drift,
            },
        }
    }

    pub(in crate::commands::check) fn embedding_credential_unresolved(
        provider: ProviderKind,
        base_url: String,
        kind: MissingApiKeyKind,
    ) -> Self {
        // The detail names the endpoint either way; the remediation differs by
        // whether an entry exists to fill: add the connection, or set the key
        // on the one that is already there.
        let remediation = match kind {
            MissingApiKeyKind::NoMatchingEntry => {
                CheckRemediation::AddEmbeddingCredential { provider }
            }
            MissingApiKeyKind::EmptyKey => CheckRemediation::SetEmbeddingCredentialKey { provider },
        };
        Self::Fail {
            detail: CheckDetail::EmbeddingCredentialUnresolved {
                provider,
                base_url,
                kind,
            },
            remediation,
        }
    }

    pub(in crate::commands::check) fn embedding_reindex_attention(
        live_run: Option<(String, ReindexRunState)>,
        quarantined: u64,
    ) -> Self {
        Self::Warn {
            detail: CheckDetail::EmbeddingReindexAttention {
                live_run,
                quarantined,
            },
            remediation: CheckRemediation::InspectReindexRun,
        }
    }

    pub(in crate::commands::check) fn embedding_profile_query_failed(error: String) -> Self {
        Self::Fail {
            detail: CheckDetail::EmbeddingProfileQueryFailed { error },
            remediation: CheckRemediation::ConsultUnderlyingError,
        }
    }
}

/// Reports the active embedding profile against the genesis seed and the
/// credential catalogue.
pub(in crate::commands::check) async fn act(state: &mut CheckState) -> CheckOutcome {
    let pool = state
        .pool
        .as_ref()
        .expect("preflight ensures state.pool is populated");
    let config = state
        .config
        .as_ref()
        .expect("preflight ensures state.config is populated");

    let mut conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(err) => return CheckOutcome::embedding_profile_query_failed(err.to_string()),
    };

    let active = match PgEmbeddingProfileRepository.find_active(&mut conn).await {
        Ok(Some(profile)) => profile,
        Ok(None) => return CheckOutcome::embedding_profile_pending(),
        Err(err) => return CheckOutcome::embedding_profile_query_failed(err.to_string()),
    };

    // The active provider's credential must resolve through the catalogue, or
    // the next server boot fails closed.
    if let Err(missing) = config
        .credentials
        .resolve_api_key(active.provider_kind(), active.normalised_base_url())
    {
        return CheckOutcome::embedding_credential_unresolved(
            missing.provider,
            missing.base_url,
            missing.kind,
        );
    }

    let live_run = match PgReindexRunRepository.find_live(&mut conn).await {
        Ok(Some(run)) => Some((run.id().to_string(), run.state())),
        Ok(None) => None,
        Err(err) => return CheckOutcome::embedding_profile_query_failed(err.to_string()),
    };
    let quarantined = match PgReindexQuarantineRepository
        .count_for_profile(&mut conn, active.id())
        .await
    {
        Ok(count) => u64::try_from(count).unwrap_or(0),
        Err(err) => return CheckOutcome::embedding_profile_query_failed(err.to_string()),
    };

    if live_run.is_some() || quarantined > 0 {
        return CheckOutcome::embedding_reindex_attention(live_run, quarantined);
    }

    // Genesis-vs-active divergence is informational state, never a warning.
    let genesis = &config.init.embedding;
    let genesis_drift = (genesis.provider != active.provider_kind()
        || genesis.model != active.model())
    .then(|| format!("{}/{}", genesis.provider, genesis.model));

    CheckOutcome::embedding_profile_live(
        active.provider_kind(),
        active.model().to_owned(),
        active.dimensions(),
        genesis_drift,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_is_a_pass() {
        assert!(matches!(
            CheckOutcome::embedding_profile_pending(),
            CheckOutcome::Pass {
                detail: CheckDetail::EmbeddingProfilePending,
            },
        ));
    }

    #[test]
    fn test_live_without_drift_renders_the_identity() {
        let outcome = CheckOutcome::embedding_profile_live(
            ProviderKind::Ollama,
            "nomic-embed-text:v1.5".into(),
            768,
            None,
        );
        let CheckOutcome::Pass { detail } = outcome else {
            panic!("expected a pass");
        };
        assert_eq!(
            detail.render(),
            "active embedding profile: ollama/nomic-embed-text:v1.5 (768d)",
        );
    }

    #[test]
    fn test_live_with_drift_reports_genesis_as_informational() {
        let outcome = CheckOutcome::embedding_profile_live(
            ProviderKind::OpenAi,
            "text-embedding-3-small".into(),
            1536,
            Some("ollama/nomic-embed-text:v1.5".into()),
        );
        let CheckOutcome::Pass { detail } = outcome else {
            panic!("expected a pass");
        };
        let rendered = detail.render();
        assert!(
            rendered.contains("openai/text-embedding-3-small (1536d)"),
            "{rendered}"
        );
        assert!(
            rendered.contains("genesis seed init.embedding (ollama/nomic-embed-text:v1.5)"),
            "{rendered}"
        );
        assert!(rendered.contains("informational"), "{rendered}");
    }

    #[test]
    fn test_no_matching_entry_is_a_fail_telling_the_operator_to_add_the_connection() {
        let outcome = CheckOutcome::embedding_credential_unresolved(
            ProviderKind::OpenAi,
            "https://api.openai.com:443".into(),
            MissingApiKeyKind::NoMatchingEntry,
        );
        let (detail, remediation) = match outcome {
            CheckOutcome::Fail {
                detail,
                remediation,
            } => (detail, remediation),
            other => panic!("expected a fail, got {other:?}"),
        };
        assert!(detail.render().contains("https://api.openai.com:443"));
        assert!(detail.render().contains("no catalogue entry matches"));
        assert!(matches!(
            remediation,
            CheckRemediation::AddEmbeddingCredential {
                provider: ProviderKind::OpenAi,
            },
        ));
        assert!(remediation.render().contains("openai_default"));
    }

    #[test]
    fn test_empty_key_is_a_fail_telling_the_operator_to_set_the_existing_key() {
        let outcome = CheckOutcome::embedding_credential_unresolved(
            ProviderKind::OpenAi,
            "https://api.openai.com:443".into(),
            MissingApiKeyKind::EmptyKey,
        );
        let (detail, remediation) = match outcome {
            CheckOutcome::Fail {
                detail,
                remediation,
            } => (detail, remediation),
            other => panic!("expected a fail, got {other:?}"),
        };
        assert!(detail.render().contains("empty api_key"));
        assert!(matches!(
            remediation,
            CheckRemediation::SetEmbeddingCredentialKey {
                provider: ProviderKind::OpenAi,
            },
        ));
        let rendered = remediation.render();
        assert!(
            rendered.contains("exists but has no key"),
            "should point at the existing entry: {rendered}",
        );
        assert!(
            rendered.contains("credentials.openai_default.api_key"),
            "{rendered}"
        );
    }

    #[test]
    fn test_reindex_attention_reports_both_a_live_run_and_quarantine() {
        let outcome = CheckOutcome::embedding_reindex_attention(
            Some(("reindex_run_1".into(), ReindexRunState::Running)),
            3,
        );
        let CheckOutcome::Warn { detail, .. } = outcome else {
            panic!("expected a warn");
        };
        let rendered = detail.render();
        assert!(
            rendered.contains("reindex run reindex_run_1 is live (running)"),
            "{rendered}"
        );
        assert!(rendered.contains("3 item(s) quarantined"), "{rendered}");
    }
}
