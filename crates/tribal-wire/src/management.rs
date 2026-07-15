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
mod settings;
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
    BootstrapGenesisInput, BootstrapHandoff, BootstrapOutcome, BootstrapProviderConnectionInput,
    BootstrapPublicCredential, BootstrapRequest, BootstrapResult, BootstrapStorage,
    BootstrapTelemetryInput, BootstrapTokenPolicy, CredentialOrigin, OtlpEndpoint,
    OtlpEndpointError,
};
pub use config_schema::{AudienceTier, ConfigFieldMeta, ConfigSchema, ReloadClass};
pub use configuration::{
    AdministrationFailure, ConfigChangeEvent, ConfigChangeSource, ConfigDocument,
    ConfigFieldOutcome, ConfigGetRequest, ConfigLiteral, ConfigPatchChange, ConfigPatchOutcome,
    ConfigPatchRefusal, ConfigPatchRequest, ConfigPersistenceObservation, ConfigPersistencePhase,
    ConfigSetRequest, ConfigValue, ConfigWriteEffect, ConfigWriteOutcome, CredentialRequirement,
    EmbeddingDimensions, EmbeddingModelOption, EmbeddingProfileSummary, EndpointRequirement,
    GenesisConfigurationRequest, GenesisConnectionOption, GenesisConvergenceRequest,
    GenesisEmbeddingInput, GenesisModelConstraint, GenesisModelOptions, GenesisOptions,
    GenesisPolicyRefusal, GenesisProviderAvailability, GenesisUnavailableReason,
    GraphEmbeddingProfile, InferenceStage, InventoryItemRef, KnownModelEntry, ManagementError,
    ManagementResponseError, ModelAccess, ModelAvailability, ModelSettingsCapability,
    ModelUnavailableReason, ModelsCatalogue, SecretLiteral, SecretLiteralError,
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
    AuthenticationSettingsCall, BootstrapRunCall, CheckReportCall, ConfigGetAllCall, ConfigGetCall,
    ConfigPatchCall, ConfigPathCall, ConfigSchemaCall, ConfigSetCall, ConfigValidatePatchCall,
    DatabaseConnectionCall, DatabaseInitialiseCall, DatabaseProbeCall, GraphConfigureGenesisCall,
    GraphConvergeGenesisCall, GraphEmbeddingProfileCall, GraphGenesisOptionsCall,
    IntegrationMcpConfigCall, LogsTailCall, ManagementCall, ManagementMethod, ManagerShutdownCall,
    ManagerSnapshotCall, ModelsCatalogueCall, ProcessingProfileCall, ProcessingProfileSetCall,
    ProjectListCall, ProjectRegisterCall, ProviderConnectionRemoveCall,
    ProviderConnectionUpsertCall, ProviderConnectionsCall, ProviderProbeCall, ReindexCancelCall,
    ReindexPruneCall, ReindexRunCall, RuntimeRestartCall, RuntimeStartCall, RuntimeStopCall,
    ServerStatusCall, SettingsResetPreviewCall, ThreadsPruneCall, TokenCreateCall, TokenListCall,
    TokenRevokeAllCall, TokenRevokeCall,
};
#[cfg(feature = "schema")]
pub use method::{ManagementCallSchema, management_call_schemas};
pub use readiness::{
    CheckObservation, CheckSubject, ConfigDiagnosticLocation, ConfigFilePath,
    HealthDegradedReadinessReport, HealthDegradedVerdict, HealthVerdict, ProbeOutcome,
    ProbeReceipt, ProbeReceiptFreshness, ProbeSubject, ProviderProbeCapability,
    ReadinessRefinementError, ReadinessReport, ReadinessScope, StartBlockedReadinessReport,
    StartBlockedVerdict, StartClearReadinessReport, StartClearVerdict, StartVerdict,
};
pub use runtime::{
    ManagedRuntimeStatus, ManagedRuntimeStatusResult, RuntimeLogsTailRequest,
    RuntimeLogsTailResult, RuntimeReadUnavailable,
};
pub use settings::{
    AuthenticationSettingsSnapshot, CandidateProviderProbeObservation,
    ClientRegistrationAvailability, ClientRegistrationUnavailableReason, ConfigPatchValidation,
    ConfigValidatePatchRequest, CredentialMutation, CredentialMutationRefusal,
    CustomProcessingSettings, DatabaseConnectionSummary, DatabaseEndpointSummary,
    DatabaseProbeRequest, DatabaseProbeTarget, ExtractionStageSettings, ManagerSnapshot,
    PatchConfigViolation, PresetModelSettings, ProcessingProfile, ProcessingProfileSetRequest,
    ProcessingProfileSnapshot, ProviderConnectionAvailability, ProviderConnectionCapability,
    ProviderConnectionInput, ProviderConnectionMutationReceipt, ProviderConnectionProbeReceipt,
    ProviderConnectionRemovalRefusal, ProviderConnectionRemoveRequest, ProviderConnectionSummary,
    ProviderConnectionUnavailableReason, ProviderConnectionUpsertRequest, ProviderConnectionUse,
    ProviderConnectionsCatalogue, ProviderCredentialSummary, ProviderEndpointSummary,
    ProviderProbeRequest, ProviderProbeResponse, ProviderProbeTarget, SecretPresence,
    SettingsFocusDestination, SettingsPane, SettingsResetPreview, SettingsResetPreviewRequest,
    SettingsResetScope, SettingsSetupFocus, SettingsSetupReason, SettingsSetupSnapshot,
    SettingsSetupStep, SettingsSetupStepKind, SettingsSetupStepState, StageExecutionSettings,
    StageModelSettings, VerifiedStageExecutionSettings, VerifiedStageSettings,
};
pub use tribal_domain::{ConfigFieldPath, ProviderConnectionName, ProviderKind, TransportKind};
pub use wire_id::{
    ConfigDigest, ConfigRevision, EmbeddingProfileRevision, KnownModelId, PanicCorrelationId,
    PanicCorrelationIdParseError, WireIdError,
};

pub use crate::{
    operator_check::{CheckName, CheckResult},
    token::{TokenInfo, TokenList},
};

include!(concat!(env!("OUT_DIR"), "/management_contract_metadata.rs"));

/// Deadline for flushing one framed management response or event.
pub const MANAGEMENT_FRAME_WRITE_TIMEOUT_SECONDS: u64 = 5;
