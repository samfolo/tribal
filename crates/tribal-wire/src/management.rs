//! Public same-machine management contract for operator clients.
//!
//! This façade owns the lifecycle and readiness DTOs exposed to operator clients.

mod configuration;
mod envelope;
mod launch;
mod lifecycle;
mod panic_correlation;
mod readiness;
mod wire_id;

pub use configuration::{
    ConfigChangeEvent, ConfigChangeSource, ConfigDocument, ConfigFieldOutcome, ConfigGetRequest,
    ConfigLiteral, ConfigPatchChange, ConfigPatchOutcome, ConfigPatchRefusal, ConfigPatchRequest,
    ConfigPersistenceObservation, ConfigPersistencePhase, ConfigSetRequest, ConfigValue,
    ConfigWriteEffect, ConfigWriteOutcome, CredentialCapabilityInvalidReason, CredentialInput,
    CredentialRequirement, CredentialSource, CredentialSourceKind, CredentialSources,
    CredentialSourcesRequest, CredentialUse, CredentialUseCapabilities, EmbeddingProfileSummary,
    EmbeddingReuseAvailability, EmbeddingReuseUnavailableReason, EndpointRequirement,
    EndpointSelection, EndpointTransitionRefusal, GenesisConfigurationRequest,
    GenesisConvergenceRequest, GenesisDimensionsConstraint, GenesisEmbeddingInput,
    GenesisModelConstraint, GenesisOptions, GenesisPolicyRefusal, GenesisProviderAvailability,
    GenesisProviderOption, GenesisUnavailableReason, GraphEmbeddingProfile, InferenceStage,
    InvalidStageSetReason, KnownModelEntry, ManagementError, ManagementResponseError, ModelAccess,
    ModelAvailability, ModelSelectionRequest, ModelSettingsCapability, ModelUnavailableReason,
    ModelsCatalogue, SecretLiteral, SecretLiteralError,
};
pub use envelope::{
    BootstrapShutdownRefusal, ManagementBootstrapRequest, ManagementBootstrapResponse,
    ManagementClientHello, ManagementServerHello,
};
pub use launch::{
    AuthorityUnavailableReason, ConflictingRuntimeIdentity, ManagerAnnouncement,
    ManagerLaunchDisposition, ManagerLaunchFailure, ManagerLaunchRecord, ManagerStartupFailure,
};
pub use lifecycle::{
    CleanNoRuntimeLifecycleSnapshot, CleanNoRuntimePhase,
    CleanReadinessUnavailableLifecycleSnapshot, CleanReadinessUnavailablePhase, CleanStoppedState,
    CleanUnconfiguredLifecycleSnapshot, CleanUnconfiguredPhase, CustodyLossTerminationRuntime,
    DegradedReason, EarlyChildTerminationEvidence, EarlyChildTerminationOperation,
    FailedNoRuntimeLifecycleSnapshot, FailedNoRuntimePhase, FailedStoppedState,
    FailurePresentation, LifecyclePhase, LifecycleSnapshot, LifecycleSnapshotHeader,
    ManagerShutdownOperation, ManagerShutdownResult, ManagerTerminatingLifecycleSnapshot,
    ManagerTerminatingPhase, ManagerTermination, ManagerTerminationRuntime,
    NoRuntimeLifecycleSnapshot, NoRuntimePhase, NoStoppedProcessFailure,
    ReadinessUnavailableLifecycleSnapshot, ReadinessUnavailablePhase,
    ReadinessUnavailableStoppedState, RestartOperationInProgress, RestartRuntimeOperation,
    RestartRuntimeUnresponsiveLifecycleSnapshot, RestartRuntimeUnresponsivePhase,
    RestartSuperseder, RunningLifecycleSnapshot, RunningPhase,
    RuntimeControlUnavailableLifecycleSnapshot, RuntimeControlUnavailablePhase,
    RuntimeControlUnavailableReason, RuntimeExitFailure, RuntimeIdentity, RuntimeOperation,
    RuntimeRestartResult, RuntimeStartResult, RuntimeStopResult, RuntimeStopTimedOutFailure,
    RuntimeUnresponsiveLifecycleSnapshot, RuntimeUnresponsivePhase,
    ShutdownInProgressLifecycleSnapshot, ShutdownInProgressPhase,
    ShutdownRuntimeUnresponsiveLifecycleSnapshot, ShutdownRuntimeUnresponsivePhase,
    StartOperationInProgress, StartSuperseder, StartingLifecycleSnapshot, StartingPhase,
    StopEarlyChildCancellationLifecycleSnapshot, StopEarlyChildCancellationPhase,
    StopRuntimeOperation, StopRuntimeUnresponsiveLifecycleSnapshot, StopRuntimeUnresponsivePhase,
    StoppedProcessFailure, StoppedState, StoppingLifecycleSnapshot, StoppingPhase,
    UnconfiguredLifecycleSnapshot, UnconfiguredPhase,
};
pub use panic_correlation::{PanicCorrelationId, PanicCorrelationIdParseError};
pub use readiness::{
    CheckObservation, CheckSubject, ConfigDiagnosticLocation, ConfigFilePath,
    CredentialEntryMember, HealthDegradedReadinessReport, HealthDegradedVerdict, HealthVerdict,
    ReadinessRefinementError, ReadinessReport, ReadinessScope, StartBlockedReadinessReport,
    StartBlockedVerdict, StartClearReadinessReport, StartClearVerdict, StartVerdict,
};
pub use tribal_domain::{ConfigFieldPath, ProviderKind};
pub use wire_id::{
    ConfigDigest, ConfigRevision, CredentialSourceId, EmbeddingProfileRevision, KnownModelId,
    WireIdError,
};

pub use crate::operator_check::{CheckName, CheckResult};

/// The version of the public local-management contract.
pub const MANAGEMENT_CONTRACT_VERSION: u16 = 1;
