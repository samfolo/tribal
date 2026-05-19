//! Centralised ordered list of check steps with `preflight` / `act`
//! dispatch.
//!
//! Each variant of [`CheckStep`] names one check, in execution order.
//! `preflight` consults [`CheckState`] to decide whether the step is
//! applicable; `act` runs the underlying probe and may mutate state for
//! downstream steps to consume.

use strum::EnumIter;

use super::{
    advertised_url_reachable, binary_uniqueness, config_parse, config_validate, database_reachable,
    migrations_current, project_resolution, provider_probes,
    state::CheckState,
    types::{CheckName, CheckOutcome, ProviderProbeTarget, SkipReason},
    valid_token_exists,
};

/// What `preflight` decides about a step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) enum Preflight {
    /// State satisfies the step's prerequisites; the action will run.
    Run,
    /// The step is applicable but cannot run for the given reason; a
    /// `Skip` row is emitted with that reason.
    Skip(SkipReason),
    /// The step is not applicable to this invocation (e.g. provider
    /// probes without `--providers`); no row is emitted.
    Omit,
}

/// One step in the check pipeline.  Variant order is execution order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
pub(in crate::commands::check) enum CheckStep {
    ConfigParse,
    ConfigValidate,
    DatabaseReachable,
    MigrationsCurrent,
    ProjectResolution,
    ValidTokenExists,
    AdvertisedUrlReachable,
    BinaryUniqueness,
    ProviderEmbedding,
    ProviderExtraction,
    ProviderTriage,
    ProviderRelation,
}

impl CheckStep {
    pub(in crate::commands::check) fn name(self) -> CheckName {
        match self {
            Self::ConfigParse => CheckName::ConfigParse,
            Self::ConfigValidate => CheckName::ConfigValidate,
            Self::DatabaseReachable => CheckName::DatabaseReachable,
            Self::MigrationsCurrent => CheckName::MigrationsCurrent,
            Self::ProjectResolution => CheckName::ProjectResolution,
            Self::ValidTokenExists => CheckName::ValidTokenExists,
            Self::AdvertisedUrlReachable => CheckName::AdvertisedUrlReachable,
            Self::BinaryUniqueness => CheckName::BinaryUniqueness,
            Self::ProviderEmbedding => CheckName::ProviderEmbedding,
            Self::ProviderExtraction => CheckName::ProviderExtraction,
            Self::ProviderTriage => CheckName::ProviderTriage,
            Self::ProviderRelation => CheckName::ProviderRelation,
        }
    }

    pub(in crate::commands::check) fn preflight(self, state: &CheckState) -> Preflight {
        match self {
            Self::ConfigParse => Preflight::Run,
            Self::ConfigValidate | Self::DatabaseReachable | Self::BinaryUniqueness => {
                require_config(state)
            }
            Self::MigrationsCurrent | Self::ProjectResolution | Self::ValidTokenExists => {
                require_pool(state)
            }
            Self::AdvertisedUrlReachable => require_advertised_url(state),
            Self::ProviderEmbedding => require_provider(state, ProviderProbeTarget::Embedding),
            Self::ProviderExtraction => require_provider(state, ProviderProbeTarget::Extraction),
            Self::ProviderTriage => require_provider(state, ProviderProbeTarget::Triage),
            Self::ProviderRelation => require_provider(state, ProviderProbeTarget::Relation),
        }
    }

    pub(in crate::commands::check) async fn act(self, state: &mut CheckState) -> CheckOutcome {
        match self {
            Self::ConfigParse => config_parse::act(state).await,
            Self::ConfigValidate => config_validate::act(state).await,
            Self::DatabaseReachable => database_reachable::act(state).await,
            Self::MigrationsCurrent => migrations_current::act(state).await,
            Self::ProjectResolution => project_resolution::act(state).await,
            Self::ValidTokenExists => valid_token_exists::act(state).await,
            Self::AdvertisedUrlReachable => advertised_url_reachable::act(state).await,
            Self::BinaryUniqueness => binary_uniqueness::act(state).await,
            Self::ProviderEmbedding => {
                provider_probes::act(state, ProviderProbeTarget::Embedding).await
            }
            Self::ProviderExtraction => {
                provider_probes::act(state, ProviderProbeTarget::Extraction).await
            }
            Self::ProviderTriage => provider_probes::act(state, ProviderProbeTarget::Triage).await,
            Self::ProviderRelation => {
                provider_probes::act(state, ProviderProbeTarget::Relation).await
            }
        }
    }
}

fn require_config(state: &CheckState) -> Preflight {
    if state.config.is_none() {
        Preflight::Skip(SkipReason::ConfigParseFailed)
    } else {
        Preflight::Run
    }
}

fn require_pool(state: &CheckState) -> Preflight {
    if state.config.is_none() {
        return Preflight::Skip(SkipReason::ConfigParseFailed);
    }
    if state.pool.is_none() {
        return Preflight::Skip(SkipReason::DatabaseUnreachable);
    }
    Preflight::Run
}

fn require_advertised_url(state: &CheckState) -> Preflight {
    if state.config.is_none() {
        return Preflight::Skip(SkipReason::ConfigParseFailed);
    }
    if state.skip_mask.skip_advertised_url() {
        return Preflight::Skip(SkipReason::ConfigValidateFailedTransportBind);
    }
    Preflight::Run
}

fn require_provider(state: &CheckState, target: ProviderProbeTarget) -> Preflight {
    if !state.providers {
        return Preflight::Omit;
    }
    if state.config.is_none() {
        return Preflight::Skip(SkipReason::ConfigParseFailed);
    }
    if state.skip_mask.skip_provider_probe(target) {
        return Preflight::Skip(SkipReason::ConfigValidateFailedApiKey { target });
    }
    Preflight::Run
}
