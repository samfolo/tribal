//! Public same-machine management contract for operator clients.
//!
//! This façade owns the lifecycle and readiness DTOs exposed to operator clients.

mod administration;
mod bootstrap;
mod config_schema;
mod configuration;
mod envelope;
mod event;
mod integration;
mod launch;
mod lifecycle;
mod maintenance;
mod method;
mod readiness;
mod runtime;
mod wire_id;

pub use administration::{
    AbsoluteDirectoryPath, AbsoluteDirectoryPathError, CredentialPersistenceResult,
    DatabaseInitialiseOutcome, DatabaseInitialiseRequest, DatabaseInitialiseResult,
    IssuedBearerToken, PageCursor, PageCursorError, PageRequest, PageSize, PageSizeError,
    ProjectList, ProjectListRequest, ProjectPage, ProjectRegisterInput, ProjectRegisterOutcome,
    ProjectRegisterRequest, ProjectRegisterResult, ProjectRegistrationSource, ProjectSummary,
    Revisioned, TokenCreateOutcome, TokenCreateRequest, TokenCreateResult, TokenInventory,
    TokenListRequest, TokenPage, TokenRevokeAllOutcome, TokenRevokeAllRequest,
    TokenRevokeAllResult, TokenRevokeOutcome, TokenRevokeRequest, TokenRevokeResult, TokenState,
    TokenSummary,
};
pub use bootstrap::{
    BootstrapGenesisCredential, BootstrapGenesisInput, BootstrapHandoff, BootstrapOutcome,
    BootstrapPublicCredential, BootstrapRequest, BootstrapResult, BootstrapStorage,
    BootstrapTelemetryInput, BootstrapTokenPolicy, CredentialOrigin, ModelSelectionInput,
    OtlpEndpoint, OtlpEndpointError,
};
pub use config_schema::{AudienceTier, ConfigFieldMeta, ConfigSchema, ReloadClass};
pub use configuration::{
    AdministrationFailure, ConfigChangeEvent, ConfigChangeSource, ConfigDocument,
    ConfigFieldOutcome, ConfigGetRequest, ConfigLiteral, ConfigPatchChange, ConfigPatchOutcome,
    ConfigPatchRefusal, ConfigPatchRequest, ConfigPersistenceObservation, ConfigPersistencePhase,
    ConfigSetRequest, ConfigValidateRequest, ConfigValidation, ConfigValue, ConfigViolation,
    ConfigWriteEffect, ConfigWriteOutcome, CredentialCapabilityInvalidReason, CredentialInput,
    CredentialRequirement, CredentialSource, CredentialSourceKind, CredentialSources,
    CredentialSourcesRequest, CredentialUse, CredentialUseCapabilities, EmbeddingProfileSummary,
    EmbeddingReuseAvailability, EmbeddingReuseUnavailableReason, EndpointRequirement,
    EndpointSelection, EndpointTransitionRefusal, GenesisConfigurationRequest,
    GenesisConvergenceRequest, GenesisDimensionsConstraint, GenesisEmbeddingInput,
    GenesisModelConstraint, GenesisOptions, GenesisPolicyRefusal, GenesisProviderAvailability,
    GenesisProviderOption, GenesisUnavailableReason, GraphEmbeddingProfile, InferenceStage,
    InvalidStageSetReason, InventoryItemRef, KnownModelEntry, ManagementError,
    ManagementResponseError, ModelAccess, ModelAvailability, ModelSelectionRequest,
    ModelSettingsCapability, ModelUnavailableReason, ModelsCatalogue, SecretLiteral,
    SecretLiteralError,
};
pub use envelope::{
    BootstrapShutdownRefusal, ManagementBootstrapRequest, ManagementBootstrapResponse,
    ManagementClientHello, ManagementServerHello,
};
pub use event::{ManagementEvent, ManagementLogLoss};
pub use integration::{
    ConfiguredMcpTarget, McpConfigEntry, McpConfigRequest, McpConfigResult, McpTarget,
    McpTargetSelection, NetworkIntegrationAuth, ProjectSelector, PublicMcpConfigDocument,
    PublicMcpServerEntry, SensitiveMcpConfigDocument, StdioProjectContext,
};
pub use launch::{
    AuthorityUnavailableReason, ConflictingRuntimeIdentity, ManagerAnnouncement,
    ManagerLaunchDisposition, ManagerLaunchFailure, ManagerLaunchRecord, ManagerStartupFailure,
};
pub use lifecycle::{
    CleanNoRuntimeLifecycleSnapshot, CleanNoRuntimePhase,
    CleanReadinessUnavailableLifecycleSnapshot, CleanReadinessUnavailablePhase,
    CleanReadinessUnavailableStoppedState, CleanStoppedState, CleanUnconfiguredLifecycleSnapshot,
    CleanUnconfiguredPhase, CustodyLossTerminationRuntime, DegradedReason,
    EarlyChildTerminationEvidence, EarlyChildTerminationOperation,
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
pub use maintenance::{
    MutationMode, ReindexApplyResolution, ReindexCancelOutcome, ReindexCancelRequest,
    ReindexCancelResult, ReindexPlan, ReindexPruneCounts, ReindexPruneOutcome, ReindexPruneRequest,
    ReindexPruneResult, ReindexRunOutcome, ReindexRunRequest, ReindexRunResult, RetentionDays,
    RetentionDaysError, ThreadPruneApplied, ThreadPruneOutcome, ThreadPrunePlan,
    ThreadPruneRequest, ThreadPruneResult,
};
pub use method::{
    CheckReportCall, ConfigGetAllCall, ConfigGetCall, ConfigPatchCall, ConfigPathCall,
    ConfigSchemaCall, ConfigSetCall, ConfigValidateCall, CredentialProbeCall,
    CredentialSourcesCall, DatabaseInitialiseCall, DatabaseProbeCall, GraphConfigureGenesisCall,
    GraphConvergeGenesisCall, GraphEmbeddingProfileCall, GraphGenesisOptionsCall, LogsTailCall,
    ManagementCall, ManagementMethod, ManagerShutdownCall, ManagerSnapshotCall,
    ModelsCatalogueCall, ModelsSelectCall, RuntimeRestartCall, RuntimeStartCall, RuntimeStopCall,
    ServerStatusCall, TokenListCall,
};
#[cfg(feature = "schema")]
pub use method::{ManagementCallSchema, management_call_schemas};
pub use readiness::{
    CheckObservation, CheckSubject, ConfigDiagnosticLocation, ConfigFilePath,
    CredentialEntryMember, HealthDegradedReadinessReport, HealthDegradedVerdict, HealthVerdict,
    ProbeOutcome, ProbeReceipt, ProbeReceiptFreshness, ProbeSubject, ProviderProbeCapability,
    ReadinessRefinementError, ReadinessReport, ReadinessScope, StartBlockedReadinessReport,
    StartBlockedVerdict, StartClearReadinessReport, StartClearVerdict, StartVerdict,
};
pub use runtime::{
    ManagedRuntimeStatus, ManagedRuntimeStatusResult, RuntimeLogsTailRequest,
    RuntimeLogsTailResult, RuntimeReadUnavailable, RuntimeTokenListResult,
};
pub use tribal_domain::{ConfigFieldPath, ProviderKind, TransportKind};
pub use wire_id::{
    ConfigDigest, ConfigRevision, CredentialSourceId, EmbeddingProfileRevision, KnownModelId,
    PanicCorrelationId, PanicCorrelationIdParseError, WireIdError,
};

pub use crate::{
    operator_check::{CheckName, CheckResult},
    token::{TokenInfo, TokenList},
};

/// The version of the public local-management contract.
pub const MANAGEMENT_CONTRACT_VERSION: u16 = 3;
