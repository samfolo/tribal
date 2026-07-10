//! Public same-machine management contract for operator clients.
//!
//! This façade owns lifecycle and readiness DTOs while re-exporting the existing
//! operator crossings during their migration from the legacy control namespace.

mod lifecycle;
mod readiness;

pub use lifecycle::{
    CleanNoRuntimeLifecycleSnapshot, CleanNoRuntimePhase,
    CleanReadinessUnavailableLifecycleSnapshot, CleanReadinessUnavailablePhase, CleanStoppedState,
    CleanUnconfiguredLifecycleSnapshot, CleanUnconfiguredPhase, CustodyLossTerminationRuntime,
    DegradedReason, EarlyChildTerminationEvidence, EarlyChildTerminationOperation,
    FailedNoRuntimeLifecycleSnapshot, FailedNoRuntimePhase, FailedStoppedState,
    FailurePresentation, LifecyclePhase, LifecycleSnapshot, LifecycleSnapshotHeader,
    ManagerShutdownOperation, ManagerShutdownResult, ManagerTerminatingLifecycleSnapshot,
    ManagerTerminatingPhase, ManagerTermination, ManagerTerminationRuntime,
    NoRuntimeLifecycleSnapshot, NoRuntimePhase, NoStoppedProcessFailure, PanicCorrelationId,
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
pub use readiness::{
    CheckObservation, CheckSubject, ConfigDiagnosticLocation, ConfigFilePath,
    CredentialEntryMember, HealthDegradedReadinessReport, HealthDegradedVerdict, HealthVerdict,
    ReadinessRefinementError, ReadinessReport, ReadinessScope, StartBlockedReadinessReport,
    StartBlockedVerdict, StartClearReadinessReport, StartClearVerdict, StartVerdict,
};
pub use tribal_domain::ConfigFieldPath;

pub use crate::control::{
    AudienceTier, CheckName, CheckReport, CheckReportRequest, CheckResult, ConfigDocument,
    ConfigFieldMeta, ConfigGetRequest, ConfigPath, ConfigSchema, ConfigSetRequest,
    ConfigValidateRequest, ConfigValidation, ConfigValue, ConfigViolation, ConfigWriteOutcome,
    ControlEvent, ControlNotification, ControlRequest, ControlResponse, CredentialProbe,
    CredentialProbeRequest, DatabaseProbe, DatabaseProbeRequest, GraphEmbeddingProfile,
    JsonRpcVersion, KnownModelEntry, LogLevel, LogLine, LogLines, LogsTailRequest, ModelsCatalogue,
    ProjectSummary, RequestId, ResponseError, ResponseResult, ServerStatus, TokenInfo, TokenList,
    WorkerStatus,
};

/// The version of the public local-management contract.
pub const MANAGEMENT_CONTRACT_VERSION: u16 = 1;
