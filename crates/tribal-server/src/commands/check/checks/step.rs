//! Centralised ordered list of check steps with `preflight` / `act`
//! dispatch.
//!
//! Each variant of [`CheckStep`] names one check, in execution order.
//! `preflight` consults [`CheckState`] to decide whether the step is
//! applicable; `act` runs the underlying probe and may mutate state for
//! downstream steps to consume.

use strum::EnumIter;
use tribal_config::{ProviderStage, TransportKind};

use super::{
    advertised_url_reachable, binary_uniqueness, config_parse, config_validate, database_reachable,
    embedding_profile, migrations_current, project_resolution, provider_probes,
    state::CheckState,
    types::{CheckName, CheckOutcome, SkipReason},
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
    EmbeddingProfile,
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
            Self::EmbeddingProfile => CheckName::EmbeddingProfile,
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
            Self::MigrationsCurrent | Self::ProjectResolution | Self::EmbeddingProfile => {
                require_pool(state)
            }
            Self::ValidTokenExists => require_token_resolution(state),
            Self::AdvertisedUrlReachable => require_advertised_url(state),
            Self::ProviderEmbedding => require_provider(state, ProviderStage::Embedding),
            Self::ProviderExtraction => require_provider(state, ProviderStage::Extraction),
            Self::ProviderTriage => require_provider(state, ProviderStage::Triage),
            Self::ProviderRelation => require_provider(state, ProviderStage::Relation),
        }
    }

    pub(in crate::commands::check) async fn act(self, state: &mut CheckState) -> CheckOutcome {
        match self {
            Self::ConfigParse => config_parse::act(state).await,
            Self::ConfigValidate => config_validate::act(state).await,
            Self::DatabaseReachable => database_reachable::act(state).await,
            Self::MigrationsCurrent => migrations_current::act(state).await,
            Self::ProjectResolution => project_resolution::act(state).await,
            Self::EmbeddingProfile => embedding_profile::act(state).await,
            Self::ValidTokenExists => valid_token_exists::act(state).await,
            Self::AdvertisedUrlReachable => advertised_url_reachable::act(state).await,
            Self::BinaryUniqueness => binary_uniqueness::act(state).await,
            Self::ProviderEmbedding => provider_probes::act(state, ProviderStage::Embedding).await,
            Self::ProviderExtraction => {
                provider_probes::act(state, ProviderStage::Extraction).await
            }
            Self::ProviderTriage => provider_probes::act(state, ProviderStage::Triage).await,
            Self::ProviderRelation => provider_probes::act(state, ProviderStage::Relation).await,
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

/// Token resolution under stdio without `--token` always lands on a
/// clean `TokenSkippedStdio` outcome, which consults neither the pool
/// nor the database.  Letting the step run in that case surfaces the
/// stdio rationale instead of the less informative
/// `DatabaseUnreachable` skip the pool gate would emit.
fn require_token_resolution(state: &CheckState) -> Preflight {
    let Some(config) = state.config.as_ref() else {
        return Preflight::Skip(SkipReason::ConfigParseFailed);
    };
    if config.server.transport == TransportKind::Stdio && state.token_override.is_none() {
        return Preflight::Run;
    }
    require_pool(state)
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

fn require_provider(state: &CheckState, target: ProviderStage) -> Preflight {
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
