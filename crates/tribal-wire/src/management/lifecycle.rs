//! Lifecycle snapshots and method-specific operation results.

use serde::{Deserialize, Serialize};
use tribal_domain::{ConfigFieldPath, StorageTransitionId};

use super::{
    PanicCorrelationId,
    readiness::{
        ConfigFilePath, HealthDegradedReadinessReport, ReadinessReport,
        StartBlockedReadinessReport, StartClearReadinessReport,
    },
};

/// Stable identity for one managed runtime process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RuntimeIdentity {
    pub instance_id: String,
    pub pid: u32,
    pub binary_version: String,
    pub config_path: ConfigFilePath,
}

/// A lifecycle operation that may be waiting for runtime termination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RuntimeOperation {
    Stop,
    Restart,
    ManagerShutdown,
}

/// Redacted failure copy suitable for operator presentation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FailurePresentation {
    pub message: String,
    pub remediation: Option<String>,
}

/// Presentation of an unexpected runtime exit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RuntimeExitFailure {
    pub presentation: FailurePresentation,
}

/// Exact process failure retained while no runtime is serving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "code", content = "data", rename_all = "snake_case")]
pub enum StoppedProcessFailure {
    SpawnFailed { presentation: FailurePresentation },
    RuntimeAnnouncementFailed { presentation: FailurePresentation },
    RuntimeCustodyCommitFailed { presentation: FailurePresentation },
    RuntimeHandshakeFailed { presentation: FailurePresentation },
    RuntimeExited { failure: RuntimeExitFailure },
}

/// The legal states of an absent runtime whose configuration is parseable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum StoppedState {
    Ready {
        readiness: StartClearReadinessReport,
        failure: Option<StoppedProcessFailure>,
    },
    ReadinessUnavailable {
        last_report: Option<ReadinessReport>,
        presentation: FailurePresentation,
        process_failure: Option<StoppedProcessFailure>,
    },
}

/// The reason a serving runtime is degraded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "code", content = "data", rename_all = "snake_case")]
pub enum DegradedReason {
    Readiness {
        report: HealthDegradedReadinessReport,
    },
    RuntimeControlLost {
        report: ReadinessReport,
        presentation: FailurePresentation,
    },
}

/// A runtime did not stop before its bounded deadline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RuntimeStopTimedOutFailure {
    pub presentation: FailurePresentation,
}

/// Runtime evidence legal after a configuration-worker panic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum ManagerTerminationRuntime {
    Absent,
    Recoverable { runtime: RuntimeIdentity },
    CommitOutcomeUnknown { runtime: RuntimeIdentity },
}

/// Runtime evidence legal after lifetime custody is lost.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum CustodyLossTerminationRuntime {
    Absent,
    Recoverable { runtime: RuntimeIdentity },
}

/// Intent responsible for terminating a child before normal attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum EarlyChildTerminationOperation {
    Stop,
    ManagerShutdown,
}

/// Best process-ownership evidence available during early-child termination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum EarlyChildTerminationEvidence {
    PreCommit {
        pid: u32,
        config_path: ConfigFilePath,
    },
    CommitOutcomeUnknown {
        runtime: RuntimeIdentity,
    },
    Recoverable {
        runtime: RuntimeIdentity,
    },
}

/// Fatal reason the manager stopped accepting full-protocol commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "code", content = "data", rename_all = "snake_case")]
pub enum ManagerTermination {
    ConfigWorkerPanicked {
        correlation: Option<PanicCorrelationId>,
        runtime: ManagerTerminationRuntime,
    },
    CustodyLost {
        presentation: FailurePresentation,
        runtime: CustodyLossTerminationRuntime,
    },
    ChildReapTimedOut {
        operation: EarlyChildTerminationOperation,
        evidence: EarlyChildTerminationEvidence,
        presentation: FailurePresentation,
    },
}

/// Revision identity shared by every lifecycle projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LifecycleSnapshotHeader {
    pub manager_instance_id: String,
    pub revision: u64,
    pub manager_version: String,
}

/// An uninhabited process failure; an option of this type can encode only none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum NoStoppedProcessFailure {}

/// Stopped state narrowed to the absence of a process failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum CleanStoppedState {
    Ready {
        readiness: StartClearReadinessReport,
        failure: Option<NoStoppedProcessFailure>,
    },
    ReadinessUnavailable {
        last_report: Option<ReadinessReport>,
        presentation: FailurePresentation,
        process_failure: Option<NoStoppedProcessFailure>,
    },
}

/// Stopped state narrowed to a required process failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum FailedStoppedState {
    Ready {
        readiness: StartClearReadinessReport,
        failure: StoppedProcessFailure,
    },
    ReadinessUnavailable {
        last_report: Option<ReadinessReport>,
        presentation: FailurePresentation,
        process_failure: StoppedProcessFailure,
    },
}

/// The single degraded reason caused by losing runtime control.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "code", content = "data", rename_all = "snake_case")]
pub enum RuntimeControlUnavailableReason {
    RuntimeControlLost {
        report: ReadinessReport,
        presentation: FailurePresentation,
    },
}

/// The single runtime operation legal in a stop-specific result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum StopRuntimeOperation {
    Stop,
}

/// The single runtime operation legal in a restart-specific result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RestartRuntimeOperation {
    Restart,
}

/// The single runtime operation legal in a shutdown-specific result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ManagerShutdownOperation {
    ManagerShutdown,
}

macro_rules! lifecycle_catalogue {
    (
        $full_phase:ident => $full_snapshot:ident {
            $(
                $full_variant:ident $( { $($full_field:ident : $full_type:ty),+ $(,)? } )?
            ),+ $(,)?
        }
        projections {
            $(
                $phase:ident => $snapshot:ident {
                    $(
                        $variant:ident $( { $($field:ident : $type:ty),+ $(,)? } )?
                            => $into_full:expr
                    ),+ $(,)?
                }
            )+
        }
    ) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
        #[serde(tag = "phase", content = "data", rename_all = "snake_case")]
        pub enum $full_phase {
            $(
                $full_variant $( { $($full_field: $full_type),+ } )?,
            )+
        }

        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
        pub struct $full_snapshot {
            #[serde(flatten)]
            pub header: LifecycleSnapshotHeader,
            pub phase: $full_phase,
        }

        $(
            #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
            #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
            #[serde(tag = "phase", content = "data", rename_all = "snake_case")]
            pub enum $phase {
                $(
                    $variant $( { $($field: $type),+ } )?,
                )+
            }

            #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
            #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
            pub struct $snapshot {
                #[serde(flatten)]
                pub header: LifecycleSnapshotHeader,
                pub phase: $phase,
            }

            impl From<$snapshot> for $full_snapshot {
                fn from(snapshot: $snapshot) -> Self {
                    let phase = match snapshot.phase {
                        $(
                            $phase::$variant $( { $($field),+ } )? => $into_full,
                        )+
                    };
                    Self {
                        header: snapshot.header,
                        phase,
                    }
                }
            }
        )+
    };
}

lifecycle_catalogue! {
    LifecyclePhase => LifecycleSnapshot {
        Unconfigured {
            readiness: StartBlockedReadinessReport,
            focus: Option<ConfigFieldPath>,
            failure: Option<StoppedProcessFailure>,
        },
        Stopped { state: StoppedState },
        Starting,
        Healthy { runtime: RuntimeIdentity, restart_pending: bool },
        Degraded { runtime: RuntimeIdentity, reason: DegradedReason, restart_pending: bool },
        VersionMismatch { runtime: RuntimeIdentity, manager_version: String, runtime_version: String },
        Stopping { runtime: RuntimeIdentity },
        CancellingEarlyChild {
            operation: EarlyChildTerminationOperation,
            evidence: EarlyChildTerminationEvidence,
        },
        RuntimeUnresponsive {
            runtime: RuntimeIdentity,
            operation: RuntimeOperation,
            failure: RuntimeStopTimedOutFailure,
        },
        ManagerTerminating { termination: ManagerTermination },
    }
    projections {
        NoRuntimePhase => NoRuntimeLifecycleSnapshot {
            Unconfigured {
                readiness: StartBlockedReadinessReport,
                focus: Option<ConfigFieldPath>,
                failure: Option<StoppedProcessFailure>,
            } => LifecyclePhase::Unconfigured { readiness, focus, failure },
            Stopped { state: StoppedState } => LifecyclePhase::Stopped { state },
        }
        CleanNoRuntimePhase => CleanNoRuntimeLifecycleSnapshot {
            Unconfigured {
                readiness: StartBlockedReadinessReport,
                focus: Option<ConfigFieldPath>,
                failure: Option<NoStoppedProcessFailure>,
            } => LifecyclePhase::Unconfigured { readiness, focus, failure: failure.map(absurd) },
            Stopped { state: CleanStoppedState } => LifecyclePhase::Stopped { state: state.into() },
        }
        FailedNoRuntimePhase => FailedNoRuntimeLifecycleSnapshot {
            Unconfigured {
                readiness: StartBlockedReadinessReport,
                focus: Option<ConfigFieldPath>,
                failure: StoppedProcessFailure,
            } => LifecyclePhase::Unconfigured { readiness, focus, failure: Some(failure) },
            Stopped { state: FailedStoppedState } => LifecyclePhase::Stopped { state: state.into() },
        }
        UnconfiguredPhase => UnconfiguredLifecycleSnapshot {
            Unconfigured {
                readiness: StartBlockedReadinessReport,
                focus: Option<ConfigFieldPath>,
                failure: Option<StoppedProcessFailure>,
            } => LifecyclePhase::Unconfigured { readiness, focus, failure },
        }
        ReadinessUnavailablePhase => ReadinessUnavailableLifecycleSnapshot {
            Stopped { state: ReadinessUnavailableStoppedState } => LifecyclePhase::Stopped { state: state.into() },
        }
        CleanUnconfiguredPhase => CleanUnconfiguredLifecycleSnapshot {
            Unconfigured {
                readiness: StartBlockedReadinessReport,
                focus: Option<ConfigFieldPath>,
                failure: Option<NoStoppedProcessFailure>,
            } => LifecyclePhase::Unconfigured { readiness, focus, failure: failure.map(absurd) },
        }
        CleanReadinessUnavailablePhase => CleanReadinessUnavailableLifecycleSnapshot {
            Stopped { state: CleanReadinessUnavailableStoppedState } => LifecyclePhase::Stopped { state: state.into() },
        }
        RunningPhase => RunningLifecycleSnapshot {
            Healthy { runtime: RuntimeIdentity, restart_pending: bool } => LifecyclePhase::Healthy { runtime, restart_pending },
            Degraded { runtime: RuntimeIdentity, reason: DegradedReason, restart_pending: bool } => LifecyclePhase::Degraded { runtime, reason, restart_pending },
            VersionMismatch { runtime: RuntimeIdentity, manager_version: String, runtime_version: String } => LifecyclePhase::VersionMismatch { runtime, manager_version, runtime_version },
        }
        StartingPhase => StartingLifecycleSnapshot {
            Starting => LifecyclePhase::Starting,
        }
        StoppingPhase => StoppingLifecycleSnapshot {
            Stopping { runtime: RuntimeIdentity } => LifecyclePhase::Stopping { runtime },
        }
        StopEarlyChildCancellationPhase => StopEarlyChildCancellationLifecycleSnapshot {
            CancellingEarlyChild {
                operation: StopRuntimeOperation,
                evidence: EarlyChildTerminationEvidence,
            } => LifecyclePhase::CancellingEarlyChild { operation: operation.into(), evidence },
        }
        ShutdownInProgressPhase => ShutdownInProgressLifecycleSnapshot {
            Stopping { runtime: RuntimeIdentity } => LifecyclePhase::Stopping { runtime },
            CancellingEarlyChild {
                operation: ManagerShutdownOperation,
                evidence: EarlyChildTerminationEvidence,
            } => LifecyclePhase::CancellingEarlyChild { operation: operation.into(), evidence },
        }
        RuntimeControlUnavailablePhase => RuntimeControlUnavailableLifecycleSnapshot {
            Degraded {
                runtime: RuntimeIdentity,
                reason: RuntimeControlUnavailableReason,
                restart_pending: bool,
            } => LifecyclePhase::Degraded { runtime, reason: reason.into(), restart_pending },
        }
        RuntimeUnresponsivePhase => RuntimeUnresponsiveLifecycleSnapshot {
            RuntimeUnresponsive {
                runtime: RuntimeIdentity,
                operation: RuntimeOperation,
                failure: RuntimeStopTimedOutFailure,
            } => LifecyclePhase::RuntimeUnresponsive { runtime, operation, failure },
        }
        StopRuntimeUnresponsivePhase => StopRuntimeUnresponsiveLifecycleSnapshot {
            RuntimeUnresponsive {
                runtime: RuntimeIdentity,
                operation: StopRuntimeOperation,
                failure: RuntimeStopTimedOutFailure,
            } => LifecyclePhase::RuntimeUnresponsive { runtime, operation: operation.into(), failure },
        }
        RestartRuntimeUnresponsivePhase => RestartRuntimeUnresponsiveLifecycleSnapshot {
            RuntimeUnresponsive {
                runtime: RuntimeIdentity,
                operation: RestartRuntimeOperation,
                failure: RuntimeStopTimedOutFailure,
            } => LifecyclePhase::RuntimeUnresponsive { runtime, operation: operation.into(), failure },
        }
        ShutdownRuntimeUnresponsivePhase => ShutdownRuntimeUnresponsiveLifecycleSnapshot {
            RuntimeUnresponsive {
                runtime: RuntimeIdentity,
                operation: ManagerShutdownOperation,
                failure: RuntimeStopTimedOutFailure,
            } => LifecyclePhase::RuntimeUnresponsive { runtime, operation: operation.into(), failure },
        }
        ManagerTerminatingPhase => ManagerTerminatingLifecycleSnapshot {
            ManagerTerminating { termination: ManagerTermination } => LifecyclePhase::ManagerTerminating { termination },
        }
    }
}

/// Stopped-unavailable state used where readiness failure is the result itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum ReadinessUnavailableStoppedState {
    ReadinessUnavailable {
        last_report: Option<ReadinessReport>,
        presentation: FailurePresentation,
        process_failure: Option<StoppedProcessFailure>,
    },
}

/// Stopped-unavailable state narrowed to no process failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum CleanReadinessUnavailableStoppedState {
    ReadinessUnavailable {
        last_report: Option<ReadinessReport>,
        presentation: FailurePresentation,
        process_failure: Option<NoStoppedProcessFailure>,
    },
}

impl From<CleanStoppedState> for StoppedState {
    fn from(state: CleanStoppedState) -> Self {
        match state {
            CleanStoppedState::Ready { readiness, failure } => Self::Ready {
                readiness,
                failure: failure.map(absurd),
            },
            CleanStoppedState::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure,
            } => Self::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure: process_failure.map(absurd),
            },
        }
    }
}

impl From<FailedStoppedState> for StoppedState {
    fn from(state: FailedStoppedState) -> Self {
        match state {
            FailedStoppedState::Ready { readiness, failure } => Self::Ready {
                readiness,
                failure: Some(failure),
            },
            FailedStoppedState::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure,
            } => Self::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure: Some(process_failure),
            },
        }
    }
}

impl From<ReadinessUnavailableStoppedState> for StoppedState {
    fn from(state: ReadinessUnavailableStoppedState) -> Self {
        match state {
            ReadinessUnavailableStoppedState::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure,
            } => Self::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure,
            },
        }
    }
}

impl From<CleanReadinessUnavailableStoppedState> for StoppedState {
    fn from(state: CleanReadinessUnavailableStoppedState) -> Self {
        match state {
            CleanReadinessUnavailableStoppedState::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure,
            } => Self::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure: process_failure.map(absurd),
            },
        }
    }
}

impl From<RuntimeControlUnavailableReason> for DegradedReason {
    fn from(reason: RuntimeControlUnavailableReason) -> Self {
        match reason {
            RuntimeControlUnavailableReason::RuntimeControlLost {
                report,
                presentation,
            } => Self::RuntimeControlLost {
                report,
                presentation,
            },
        }
    }
}

impl From<StopRuntimeOperation> for RuntimeOperation {
    fn from(operation: StopRuntimeOperation) -> Self {
        match operation {
            StopRuntimeOperation::Stop => Self::Stop,
        }
    }
}

impl From<RestartRuntimeOperation> for RuntimeOperation {
    fn from(operation: RestartRuntimeOperation) -> Self {
        match operation {
            RestartRuntimeOperation::Restart => Self::Restart,
        }
    }
}

impl From<ManagerShutdownOperation> for RuntimeOperation {
    fn from(operation: ManagerShutdownOperation) -> Self {
        match operation {
            ManagerShutdownOperation::ManagerShutdown => Self::ManagerShutdown,
        }
    }
}

impl From<StopRuntimeOperation> for EarlyChildTerminationOperation {
    fn from(operation: StopRuntimeOperation) -> Self {
        match operation {
            StopRuntimeOperation::Stop => Self::Stop,
        }
    }
}

impl From<ManagerShutdownOperation> for EarlyChildTerminationOperation {
    fn from(operation: ManagerShutdownOperation) -> Self {
        match operation {
            ManagerShutdownOperation::ManagerShutdown => Self::ManagerShutdown,
        }
    }
}

fn absurd(value: NoStoppedProcessFailure) -> StoppedProcessFailure {
    match value {}
}

/// Operation occupying the lifecycle when start is requested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "operation", content = "data", rename_all = "snake_case")]
pub enum StartOperationInProgress {
    /// A graph transition owns the local barrier; no command was sent.
    GraphTransition {
        transition_id: StorageTransitionId,
    },
    StoppingForStop {
        snapshot: StoppingLifecycleSnapshot,
    },
    StoppingForRestart {
        snapshot: StoppingLifecycleSnapshot,
    },
    CancellingEarlyChildForStop {
        snapshot: StopEarlyChildCancellationLifecycleSnapshot,
    },
}

/// Operation occupying the lifecycle when restart is requested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "operation", content = "data", rename_all = "snake_case")]
pub enum RestartOperationInProgress {
    /// A graph transition owns the local barrier; no command was sent.
    GraphTransition {
        transition_id: StorageTransitionId,
    },
    CheckingStart {
        snapshot: NoRuntimeLifecycleSnapshot,
    },
    Launching {
        snapshot: StartingLifecycleSnapshot,
    },
    CommittingCustody {
        snapshot: StartingLifecycleSnapshot,
    },
    Handshaking {
        snapshot: StartingLifecycleSnapshot,
    },
    StoppingForStop {
        snapshot: StoppingLifecycleSnapshot,
    },
    CancellingEarlyChildForStop {
        snapshot: StopEarlyChildCancellationLifecycleSnapshot,
    },
}

/// Operation occupying the lifecycle when stop is requested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "operation", content = "data", rename_all = "snake_case")]
pub enum StopOperationInProgress {
    /// A graph transition owns the local barrier; no command was sent.
    GraphTransition { transition_id: StorageTransitionId },
}

/// Intent that can supersede a start request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum StartSuperseder {
    Stop,
    ManagerShutdown,
}

/// Intent that can supersede a restart request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RestartSuperseder {
    Stop,
    ManagerShutdown,
}

/// Result of `runtime.start`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum RuntimeStartResult {
    Started {
        snapshot: RunningLifecycleSnapshot,
    },
    AlreadyRunning {
        snapshot: RunningLifecycleSnapshot,
    },
    Blocked {
        snapshot: UnconfiguredLifecycleSnapshot,
    },
    ReadinessUnavailable {
        snapshot: ReadinessUnavailableLifecycleSnapshot,
    },
    OperationInProgress {
        state: StartOperationInProgress,
    },
    RuntimeUnresponsive {
        snapshot: RuntimeUnresponsiveLifecycleSnapshot,
    },
    ShuttingDown {
        snapshot: ShutdownInProgressLifecycleSnapshot,
    },
    ManagerTerminating {
        snapshot: ManagerTerminatingLifecycleSnapshot,
    },
    Superseded {
        by: StartSuperseder,
    },
    Failed {
        snapshot: FailedNoRuntimeLifecycleSnapshot,
    },
}

/// Result of `runtime.stop`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum RuntimeStopResult {
    Stopped {
        snapshot: CleanNoRuntimeLifecycleSnapshot,
    },
    /// A graph transition owns the local barrier; no stop was begun.
    OperationInProgress {
        state: StopOperationInProgress,
    },
    AlreadyStopped {
        snapshot: NoRuntimeLifecycleSnapshot,
    },
    ShuttingDown {
        snapshot: ShutdownInProgressLifecycleSnapshot,
    },
    ManagerTerminating {
        snapshot: ManagerTerminatingLifecycleSnapshot,
    },
    SupersededByManagerShutdown,
    Unavailable {
        snapshot: RuntimeControlUnavailableLifecycleSnapshot,
    },
    RuntimeUnresponsive {
        snapshot: StopRuntimeUnresponsiveLifecycleSnapshot,
    },
}

/// Result of `runtime.restart`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum RuntimeRestartResult {
    Restarted {
        snapshot: RunningLifecycleSnapshot,
    },
    NotRunning {
        snapshot: NoRuntimeLifecycleSnapshot,
    },
    Blocked {
        snapshot: CleanUnconfiguredLifecycleSnapshot,
    },
    ReadinessUnavailable {
        snapshot: CleanReadinessUnavailableLifecycleSnapshot,
    },
    OperationInProgress {
        state: RestartOperationInProgress,
    },
    ShuttingDown {
        snapshot: ShutdownInProgressLifecycleSnapshot,
    },
    ManagerTerminating {
        snapshot: ManagerTerminatingLifecycleSnapshot,
    },
    Superseded {
        by: RestartSuperseder,
    },
    Unavailable {
        snapshot: RuntimeControlUnavailableLifecycleSnapshot,
    },
    RuntimeUnresponsive {
        snapshot: RestartRuntimeUnresponsiveLifecycleSnapshot,
    },
    Failed {
        snapshot: FailedNoRuntimeLifecycleSnapshot,
    },
}

/// Result of `manager.shutdown`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum ManagerShutdownResult {
    ShuttingDown {
        snapshot: NoRuntimeLifecycleSnapshot,
    },
    ManagerTerminating {
        snapshot: ManagerTerminatingLifecycleSnapshot,
    },
    Unavailable {
        snapshot: RuntimeControlUnavailableLifecycleSnapshot,
    },
    RuntimeUnresponsive {
        snapshot: ShutdownRuntimeUnresponsiveLifecycleSnapshot,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> LifecycleSnapshotHeader {
        LifecycleSnapshotHeader {
            manager_instance_id: "manager-1".to_owned(),
            revision: 7,
            manager_version: "0.1.0".to_owned(),
        }
    }

    #[test]
    fn test_a_restricted_running_snapshot_converts_to_the_canonical_snapshot() {
        let restricted = RunningLifecycleSnapshot {
            header: header(),
            phase: RunningPhase::Healthy {
                runtime: RuntimeIdentity {
                    instance_id: "runtime-1".to_owned(),
                    pid: 42,
                    binary_version: "0.1.0".to_owned(),
                    config_path: ConfigFilePath {
                        path: "/tmp/tribal.yaml".to_owned(),
                    },
                },
                restart_pending: false,
            },
        };

        let canonical = LifecycleSnapshot::from(restricted);
        assert!(matches!(canonical.phase, LifecyclePhase::Healthy { .. }));
    }

    #[test]
    fn test_a_clean_stopped_snapshot_serialises_a_null_failure() {
        let snapshot = CleanNoRuntimeLifecycleSnapshot {
            header: header(),
            phase: CleanNoRuntimePhase::Stopped {
                state: CleanStoppedState::Ready {
                    readiness: StartClearReadinessReport {
                        start: super::super::readiness::StartClearVerdict::Clear,
                        health: super::super::readiness::HealthVerdict::NotApplicable,
                        checks: Vec::new(),
                    },
                    failure: None,
                },
            },
        };

        let value = serde_json::to_value(snapshot).expect("snapshot serialises");
        assert_eq!(
            value["data"]["state"]["data"]["failure"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn test_custody_loss_cannot_deserialise_commit_unknown_evidence() {
        let value = serde_json::json!({
            "code": "custody_lost",
            "data": {
                "presentation": { "message": "lost", "remediation": null },
                "runtime": {
                    "state": "commit_outcome_unknown",
                    "data": {
                        "runtime": {
                            "instance_id": "runtime-1",
                            "pid": 42,
                            "binary_version": "0.1.0",
                            "config_path": { "path": "/tmp/tribal.yaml" }
                        }
                    }
                }
            }
        });
        assert!(serde_json::from_value::<ManagerTermination>(value).is_err());
    }
}
