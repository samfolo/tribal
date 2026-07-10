//! One-owner lifecycle reducer for managed runtime processes.

use std::{os::fd::AsRawFd as _, path::PathBuf, sync::Arc, time::Duration};

use tokio::{
    process::Child,
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tribal_wire::management::{
    CheckName, CheckResult, CheckSubject, CleanNoRuntimeLifecycleSnapshot, CleanNoRuntimePhase,
    CleanStoppedState, CleanUnconfiguredLifecycleSnapshot, CleanUnconfiguredPhase,
    ConfigDiagnosticLocation, ConfigDocument, ConfigFilePath, CustodyLossTerminationRuntime,
    DegradedReason, FailedNoRuntimeLifecycleSnapshot, FailedNoRuntimePhase,
    HealthDegradedReadinessReport, HealthVerdict, LifecyclePhase, LifecycleSnapshot,
    LifecycleSnapshotHeader, ManagedRuntimeStatusResult, ManagerShutdownOperation,
    ManagerShutdownResult, ManagerTerminatingLifecycleSnapshot, ManagerTerminatingPhase,
    ManagerTermination, ManagerTerminationRuntime, NoRuntimeLifecycleSnapshot, NoRuntimePhase,
    ReadinessReport, RestartOperationInProgress, RestartRuntimeOperation,
    RestartRuntimeUnresponsiveLifecycleSnapshot, RestartRuntimeUnresponsivePhase,
    RunningLifecycleSnapshot, RunningPhase, RuntimeExitFailure, RuntimeIdentity,
    RuntimeLogsTailResult, RuntimeOperation, RuntimeReadUnavailable, RuntimeRestartResult,
    RuntimeStartResult, RuntimeStopResult, RuntimeStopTimedOutFailure, RuntimeTokenListResult,
    RuntimeUnresponsiveLifecycleSnapshot, RuntimeUnresponsivePhase,
    ShutdownInProgressLifecycleSnapshot, ShutdownInProgressPhase,
    ShutdownRuntimeUnresponsiveLifecycleSnapshot, ShutdownRuntimeUnresponsivePhase,
    StartBlockedReadinessReport, StartBlockedVerdict, StartClearReadinessReport, StartClearVerdict,
    StartOperationInProgress, StartSuperseder, StartVerdict, StopRuntimeOperation,
    StopRuntimeUnresponsiveLifecycleSnapshot, StopRuntimeUnresponsivePhase, StoppedProcessFailure,
    StoppedState, StoppingLifecycleSnapshot, StoppingPhase,
};
use tribal_wire::runtime_control::RuntimeCustodyProof;
use tribal_wire::runtime_control::{RuntimeConfigApplyOutcome, RuntimeConfigChange};

use crate::commands::serve::MANAGED_AUTHORITY_FD;

use super::{
    authority::{AuthorityLease, AuthorityPaths},
    custody::{
        MANAGED_CUSTODY_PROOF, MANAGED_MANAGER_INSTANCE_ID, MANAGED_RUNTIME_INSTANCE_ID,
        ManagerCustody, generate_proof,
    },
    readiness,
    runtime_control::{RuntimeControlClient, RuntimeControlConnection, RuntimeControlError},
    worker::{ConfigWorkerClient, ConfigWorkerExit},
};

const COMMAND_CAPACITY: usize = 16;
const COMPLETION_CAPACITY: usize = 1;
const STOP_DEADLINE: Duration = Duration::from_secs(10);

/// Async command handle for the sole lifecycle owner task.
#[derive(Debug, Clone)]
pub(crate) struct LifecycleController {
    sender: mpsc::Sender<LifecycleCommand>,
    snapshots: watch::Receiver<LifecycleSnapshot>,
}

enum LifecycleCommand {
    Snapshot(oneshot::Sender<LifecycleSnapshot>),
    Start(oneshot::Sender<RuntimeStartResult>),
    Stop(oneshot::Sender<RuntimeStopResult>),
    Restart(oneshot::Sender<RuntimeRestartResult>),
    Shutdown(oneshot::Sender<ManagerShutdownResult>),
    ApplyConfig {
        revision: tribal_wire::management::ConfigRevision,
        changes: Vec<RuntimeConfigChange>,
        response: oneshot::Sender<RuntimeConfigApplyOutcome>,
    },
    RuntimeStatus(oneshot::Sender<ManagedRuntimeStatusResult>),
    RuntimeLogsTail {
        lines: u32,
        response: oneshot::Sender<RuntimeLogsTailResult>,
    },
    RuntimeTokenList(oneshot::Sender<RuntimeTokenListResult>),
    Refresh,
    ConfigChanged,
    Readiness(ReadinessReport),
}

enum ManagedProcess {
    Owned(Child),
    Recovered,
}

struct ManagedChild {
    process: ManagedProcess,
    identity: RuntimeIdentity,
    custody: ManagerCustody,
    control: RuntimeControlConnection,
}

struct PreparedChild {
    child: Child,
    attachment: PendingAttachment,
}

struct PendingAttachment {
    identity: RuntimeIdentity,
    paths: AuthorityPaths,
    manager_instance_id: String,
    custody_proof: RuntimeCustodyProof,
    control_proof: RuntimeCustodyProof,
}

struct EarlyChild {
    child: Child,
    identity: RuntimeIdentity,
    custody: Option<ManagerCustody>,
    control: Option<RuntimeControlConnection>,
    evidence: tribal_wire::management::EarlyChildTerminationEvidence,
}

struct CommittedCustody {
    custody: ManagerCustody,
    control_proof: RuntimeCustodyProof,
}

struct LaunchFailure {
    failure: StoppedProcessFailure,
    evidence: tribal_wire::management::EarlyChildTerminationEvidence,
}

/// Runtime recovered through an authenticated lifetime-custody handoff.
pub(crate) struct RecoveredRuntime {
    pub(crate) identity: RuntimeIdentity,
    pub(crate) custody: ManagerCustody,
    pub(crate) control_proof: RuntimeCustodyProof,
}

enum LifecycleState {
    NoRuntime(NoRuntimeLifecycleSnapshot),
    Running {
        snapshot: RunningLifecycleSnapshot,
        child: ManagedChild,
    },
    Operating(LifecycleOperation),
    Unresponsive {
        snapshot: RuntimeUnresponsiveLifecycleSnapshot,
        child: ManagedChild,
    },
    TerminatingOperation {
        snapshot: ManagerTerminatingLifecycleSnapshot,
        operation: LifecycleOperation,
    },
    TerminatingManaged {
        snapshot: ManagerTerminatingLifecycleSnapshot,
        child: ManagedChild,
    },
    Terminating(ManagerTerminatingLifecycleSnapshot),
}

enum LifecycleOperation {
    Launching(LaunchingOperation),
    Stopping(StoppingOperation),
    CancellingLaunch(CancellingLaunchOperation),
}

struct LaunchingOperation {
    token: u64,
    snapshot: tribal_wire::management::StartingLifecycleSnapshot,
    origin: NoRuntimeLifecycleSnapshot,
    child: EarlyChild,
    intent: LaunchIntent,
    attachment: AttachmentStage,
}

enum AttachmentStage {
    Committing(JoinHandle<()>),
    Handshaking(JoinHandle<()>),
    Settled,
}

enum LaunchIntent {
    Start {
        waiters: Vec<oneshot::Sender<RuntimeStartResult>>,
    },
    Restart {
        start_waiters: Vec<oneshot::Sender<RuntimeStartResult>>,
        restart_waiters: Vec<oneshot::Sender<RuntimeRestartResult>>,
    },
}

struct CancellingLaunchOperation {
    token: u64,
    header: LifecycleSnapshotHeader,
    child: EarlyChild,
    origin: NoRuntimeLifecycleSnapshot,
    intent: CancellationIntent,
    attachment: AttachmentStage,
    termination: EarlyTerminationStage,
}

enum EarlyTerminationStage {
    Graceful {
        task: GracefulStopTask,
        deadline: tokio::time::Instant,
    },
    ForcedReap {
        deadline: tokio::time::Instant,
    },
}

enum GracefulStopTask {
    AwaitingCapability,
    Requesting(JoinHandle<()>),
    Accepted,
}

enum CancellationIntent {
    Stop {
        waiters: Vec<oneshot::Sender<RuntimeStopResult>>,
    },
    Shutdown {
        waiters: Vec<oneshot::Sender<ManagerShutdownResult>>,
    },
}

struct StoppingOperation {
    token: u64,
    snapshot: StoppingLifecycleSnapshot,
    intent: StopIntent,
    child: Option<ManagedChild>,
    task: Option<JoinHandle<()>>,
    deadline: tokio::time::Instant,
    forced: bool,
}

enum StopIntent {
    Stop {
        waiters: Vec<oneshot::Sender<RuntimeStopResult>>,
    },
    Restart {
        start_waiters: Vec<oneshot::Sender<RuntimeStartResult>>,
        restart_waiters: Vec<oneshot::Sender<RuntimeRestartResult>>,
    },
    Shutdown {
        waiters: Vec<oneshot::Sender<ManagerShutdownResult>>,
    },
}

enum LifecycleCompletion {
    CustodyCommitted {
        token: u64,
        result: Result<CommittedCustody, LaunchFailure>,
    },
    RuntimeConnected {
        token: u64,
        result: Result<RuntimeControlConnection, RuntimeControlError>,
    },
    EarlyStopRequested {
        token: u64,
        accepted: bool,
    },
    Stopped {
        token: u64,
        result: StopCompletion,
    },
    Document {
        token: u64,
        document: Option<ConfigDocument>,
    },
}

enum StopCompletion {
    Stopped { document: Option<ConfigDocument> },
}

struct LifecycleOwner {
    receiver: mpsc::Receiver<LifecycleCommand>,
    completions: mpsc::Receiver<LifecycleCompletion>,
    completion_sender: mpsc::Sender<LifecycleCompletion>,
    observations: tokio::task::JoinSet<()>,
    publisher: watch::Sender<LifecycleSnapshot>,
    state: LifecycleState,
    next_token: u64,
    config_path: PathBuf,
    config: ConfigWorkerClient,
    authority: Arc<AuthorityLease>,
    shutdown: CancellationToken,
    shutdown_seen: bool,
    config_terminal: oneshot::Receiver<ConfigWorkerExit>,
    worker_exit: Option<ConfigWorkerExit>,
}

/// Failure creating the lifecycle owner.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LifecycleStartError {
    #[error("configuration worker is unavailable during lifecycle initialisation")]
    ConfigUnavailable,
    #[error("managed runtime control is unavailable during lifecycle initialisation: {source}")]
    RuntimeControl {
        #[source]
        source: RuntimeControlError,
    },
}

/// Reason the lifecycle owner stopped.
#[derive(Debug)]
pub(crate) enum LifecycleExit {
    Shutdown,
    ConfigWorkerTerminated(ConfigWorkerExit),
}

impl EarlyChild {
    fn mark_commit_outcome_unknown(&mut self) {
        self.evidence =
            tribal_wire::management::EarlyChildTerminationEvidence::CommitOutcomeUnknown {
                runtime: self.identity.clone(),
            };
    }
}

impl LifecycleState {
    fn snapshot(&self) -> LifecycleSnapshot {
        match self {
            Self::NoRuntime(snapshot) => snapshot.clone().into(),
            Self::Running { snapshot, .. } => snapshot.clone().into(),
            Self::Operating(operation) => operation.snapshot(),
            Self::Unresponsive { snapshot, .. } => snapshot.clone().into(),
            Self::TerminatingOperation { snapshot, .. }
            | Self::TerminatingManaged { snapshot, .. }
            | Self::Terminating(snapshot) => snapshot.clone().into(),
        }
    }
}

impl LifecycleOperation {
    fn snapshot(&self) -> LifecycleSnapshot {
        match self {
            Self::Launching(operation) => operation.snapshot.clone().into(),
            Self::Stopping(operation) => operation.snapshot.clone().into(),
            Self::CancellingLaunch(operation) => cancellation_snapshot(operation),
        }
    }
}

impl LifecycleController {
    /// Starts the reducer from the latest durable configuration observation.
    pub(crate) async fn spawn(
        manager_instance_id: String,
        config_path: PathBuf,
        config: ConfigWorkerClient,
        authority: Arc<AuthorityLease>,
        shutdown: CancellationToken,
        config_terminal: oneshot::Receiver<ConfigWorkerExit>,
        recovered: Option<RecoveredRuntime>,
    ) -> Result<(Self, JoinHandle<LifecycleExit>), LifecycleStartError> {
        let document = config
            .document()
            .await
            .map_err(|_| LifecycleStartError::ConfigUnavailable)?;
        let header = LifecycleSnapshotHeader {
            manager_instance_id,
            revision: 1,
            manager_version: env!("CARGO_PKG_VERSION").to_owned(),
        };
        let state = match recovered {
            Some(recovered) => {
                let control = RuntimeControlClient::connect(
                    &authority.paths().runtime_control_socket_path,
                    &header.manager_instance_id,
                    &recovered.identity,
                    recovered.control_proof,
                )
                .await
                .map_err(|source| LifecycleStartError::RuntimeControl { source })?;
                let phase = running_phase(&recovered.identity, &control, false);
                LifecycleState::Running {
                    snapshot: RunningLifecycleSnapshot { header, phase },
                    child: ManagedChild {
                        process: ManagedProcess::Recovered,
                        identity: recovered.identity,
                        custody: recovered.custody,
                        control,
                    },
                }
            }
            None => LifecycleState::NoRuntime(no_runtime_snapshot(header, &document, None)),
        };
        let (publisher, snapshots) = watch::channel(state.snapshot());
        let (sender, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let (completion_sender, completions) = mpsc::channel(COMPLETION_CAPACITY);
        let owner = LifecycleOwner {
            receiver,
            completions,
            completion_sender,
            observations: tokio::task::JoinSet::new(),
            publisher,
            state,
            next_token: 1,
            config_path,
            config,
            authority,
            shutdown,
            shutdown_seen: false,
            config_terminal,
            worker_exit: None,
        };
        let task = tokio::spawn(owner.run());
        Ok((Self { sender, snapshots }, task))
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<LifecycleSnapshot> {
        self.snapshots.clone()
    }

    pub(crate) async fn snapshot(&self) -> Option<LifecycleSnapshot> {
        request(&self.sender, LifecycleCommand::Snapshot).await
    }

    pub(crate) async fn start(&self) -> Option<RuntimeStartResult> {
        request(&self.sender, LifecycleCommand::Start).await
    }

    pub(crate) async fn stop(&self) -> Option<RuntimeStopResult> {
        request(&self.sender, LifecycleCommand::Stop).await
    }

    pub(crate) async fn restart(&self) -> Option<RuntimeRestartResult> {
        request(&self.sender, LifecycleCommand::Restart).await
    }

    pub(crate) async fn shutdown(&self) -> Option<ManagerShutdownResult> {
        request(&self.sender, LifecycleCommand::Shutdown).await
    }

    pub(crate) async fn apply_config(
        &self,
        revision: tribal_wire::management::ConfigRevision,
        changes: Vec<RuntimeConfigChange>,
    ) -> Option<RuntimeConfigApplyOutcome> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(LifecycleCommand::ApplyConfig {
                revision,
                changes,
                response,
            })
            .await
            .ok()?;
        receiver.await.ok()
    }

    pub(crate) async fn runtime_status(&self) -> Option<ManagedRuntimeStatusResult> {
        request(&self.sender, LifecycleCommand::RuntimeStatus).await
    }

    pub(crate) async fn runtime_logs_tail(&self, lines: u32) -> Option<RuntimeLogsTailResult> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(LifecycleCommand::RuntimeLogsTail { lines, response })
            .await
            .ok()?;
        receiver.await.ok()
    }

    pub(crate) async fn runtime_token_list(&self) -> Option<RuntimeTokenListResult> {
        request(&self.sender, LifecycleCommand::RuntimeTokenList).await
    }

    pub(crate) async fn refresh(&self) {
        let _ = self.sender.send(LifecycleCommand::Refresh).await;
    }

    pub(crate) async fn config_changed(&self) {
        let _ = self.sender.send(LifecycleCommand::ConfigChanged).await;
    }

    pub(crate) async fn update_readiness(&self, report: ReadinessReport) {
        let _ = self.sender.send(LifecycleCommand::Readiness(report)).await;
    }
}

async fn request<T>(
    sender: &mpsc::Sender<LifecycleCommand>,
    command: impl FnOnce(oneshot::Sender<T>) -> LifecycleCommand,
) -> Option<T> {
    let (response, receiver) = oneshot::channel();
    sender.send(command(response)).await.ok()?;
    receiver.await.ok()
}

impl LifecycleOwner {
    async fn run(mut self) -> LifecycleExit {
        let mut process_poll = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                biased;
                terminal = &mut self.config_terminal, if self.worker_exit.is_none() => {
                    let exit = terminal.unwrap_or(ConfigWorkerExit::InputClosed);
                    let correlation = match &exit {
                        ConfigWorkerExit::InputClosed => None,
                        ConfigWorkerExit::Panicked { correlation } => correlation.clone(),
                    };
                    self.worker_exit = Some(exit);
                    self.terminate_for_worker(correlation).await;
                }
                completion = self.completions.recv() => {
                    if let Some(completion) = completion {
                        self.handle_completion(completion).await;
                    }
                }
                _ = process_poll.tick(), if matches!(self.state, LifecycleState::Running { .. } | LifecycleState::Unresponsive { .. } | LifecycleState::Operating(LifecycleOperation::CancellingLaunch(_) | LifecycleOperation::Stopping(_))) => {
                    self.observe_exit().await;
                }
                command = self.receiver.recv() => match command {
                    Some(command) => self.handle(command),
                    None => break,
                },
                joined = self.observations.join_next(), if !self.observations.is_empty() => {
                    if let Some(Err(error)) = joined {
                        tracing::error!(%error, "lifecycle observation task failed");
                    }
                }
                () = self.shutdown.cancelled(), if !self.shutdown_seen => {
                    self.shutdown_seen = true;
                    if self.begin_external_shutdown() {
                        break;
                    }
                }
            }
            if self.shutdown_seen
                && matches!(
                    self.state,
                    LifecycleState::NoRuntime(_)
                        | LifecycleState::TerminatingOperation { .. }
                        | LifecycleState::TerminatingManaged { .. }
                        | LifecycleState::Terminating(_)
                )
            {
                break;
            }
        }
        while let Some(result) = self.observations.join_next().await {
            if let Err(error) = result {
                tracing::error!(%error, "lifecycle observation task failed during shutdown");
            }
        }
        self.worker_exit.map_or(
            LifecycleExit::Shutdown,
            LifecycleExit::ConfigWorkerTerminated,
        )
    }

    fn handle(&mut self, command: LifecycleCommand) {
        match command {
            LifecycleCommand::Snapshot(response) => {
                let _ = response.send(self.state.snapshot());
            }
            LifecycleCommand::Start(response) => self.admit_start(response),
            LifecycleCommand::Stop(response) => self.admit_stop(response),
            LifecycleCommand::Restart(response) => self.admit_restart(response),
            LifecycleCommand::Shutdown(response) => self.admit_shutdown(response),
            LifecycleCommand::ApplyConfig {
                revision,
                changes,
                response,
            } => self.apply_runtime_config(revision, changes, response),
            LifecycleCommand::RuntimeStatus(response) => self.runtime_status_read(response),
            LifecycleCommand::RuntimeLogsTail { lines, response } => {
                self.runtime_logs_read(lines, response);
            }
            LifecycleCommand::RuntimeTokenList(response) => self.runtime_tokens_read(response),
            LifecycleCommand::Refresh => self.request_document_refresh(),
            LifecycleCommand::ConfigChanged => self.apply_config_change(),
            LifecycleCommand::Readiness(report) => self.apply_readiness(report),
        }
    }

    fn apply_runtime_config(
        &mut self,
        revision: tribal_wire::management::ConfigRevision,
        changes: Vec<RuntimeConfigChange>,
        response: oneshot::Sender<RuntimeConfigApplyOutcome>,
    ) {
        let client = match &self.state {
            LifecycleState::Running { child, .. } => child.control.compatible(),
            LifecycleState::NoRuntime(_)
            | LifecycleState::Operating(_)
            | LifecycleState::Unresponsive { .. }
            | LifecycleState::TerminatingOperation { .. }
            | LifecycleState::TerminatingManaged { .. }
            | LifecycleState::Terminating(_) => None,
        };
        let Some(client) = client else {
            let _ = response.send(RuntimeConfigApplyOutcome::RestartRequired);
            return;
        };
        self.observations.spawn(async move {
            let outcome = client
                .apply_config(revision, changes)
                .await
                .unwrap_or(RuntimeConfigApplyOutcome::RestartRequired);
            let _ = response.send(outcome);
        });
    }

    fn runtime_status_read(&mut self, response: oneshot::Sender<ManagedRuntimeStatusResult>) {
        let client = self.runtime_read_client();
        let restart_pending = match &self.state {
            LifecycleState::Running { snapshot, .. } => match &snapshot.phase {
                RunningPhase::Healthy {
                    restart_pending, ..
                }
                | RunningPhase::Degraded {
                    restart_pending, ..
                } => *restart_pending,
                RunningPhase::VersionMismatch { .. } => false,
            },
            LifecycleState::NoRuntime(_)
            | LifecycleState::Operating(_)
            | LifecycleState::Unresponsive { .. }
            | LifecycleState::TerminatingOperation { .. }
            | LifecycleState::TerminatingManaged { .. }
            | LifecycleState::Terminating(_) => false,
        };
        self.observations.spawn(async move {
            let result = match client {
                Ok(client) => match client.status().await {
                    Ok(status) => ManagedRuntimeStatusResult::Available {
                        status: tribal_wire::management::ManagedRuntimeStatus {
                            runtime: status.runtime,
                            restart_pending,
                        },
                    },
                    Err(_) => ManagedRuntimeStatusResult::Unavailable {
                        reason: RuntimeReadUnavailable::RuntimeControlUnavailable,
                    },
                },
                Err(reason) => ManagedRuntimeStatusResult::Unavailable { reason },
            };
            let _ = response.send(result);
        });
    }

    fn runtime_logs_read(&mut self, lines: u32, response: oneshot::Sender<RuntimeLogsTailResult>) {
        let client = self.runtime_read_client();
        self.observations.spawn(async move {
            let result = match client {
                Ok(client) => match client.logs_tail(lines).await {
                    Ok(lines) => RuntimeLogsTailResult::Available { lines },
                    Err(_) => RuntimeLogsTailResult::Unavailable {
                        reason: RuntimeReadUnavailable::RuntimeControlUnavailable,
                    },
                },
                Err(reason) => RuntimeLogsTailResult::Unavailable { reason },
            };
            let _ = response.send(result);
        });
    }

    fn runtime_tokens_read(&mut self, response: oneshot::Sender<RuntimeTokenListResult>) {
        let client = self.runtime_read_client();
        self.observations.spawn(async move {
            let result = match client {
                Ok(client) => match client.token_list().await {
                    Ok(list) => RuntimeTokenListResult::Available { list },
                    Err(_) => RuntimeTokenListResult::Unavailable {
                        reason: RuntimeReadUnavailable::RuntimeControlUnavailable,
                    },
                },
                Err(reason) => RuntimeTokenListResult::Unavailable { reason },
            };
            let _ = response.send(result);
        });
    }

    fn runtime_read_client(&self) -> Result<RuntimeControlClient, RuntimeReadUnavailable> {
        match &self.state {
            LifecycleState::Running { child, .. } | LifecycleState::Unresponsive { child, .. } => {
                child
                    .control
                    .compatible()
                    .ok_or(RuntimeReadUnavailable::VersionMismatch)
            }
            LifecycleState::NoRuntime(_) => Err(RuntimeReadUnavailable::NoRuntime),
            LifecycleState::Operating(_) => Err(RuntimeReadUnavailable::OperationInProgress),
            LifecycleState::TerminatingOperation { .. }
            | LifecycleState::TerminatingManaged { .. }
            | LifecycleState::Terminating(_) => Err(RuntimeReadUnavailable::ManagerTerminating),
        }
    }

    fn admit_start(&mut self, response: oneshot::Sender<RuntimeStartResult>) {
        match &mut self.state {
            LifecycleState::NoRuntime(snapshot) => match &snapshot.phase {
                NoRuntimePhase::Unconfigured { .. } => {
                    let _ = response.send(RuntimeStartResult::Blocked {
                        snapshot: unconfigured(snapshot),
                    });
                }
                NoRuntimePhase::Stopped {
                    state: StoppedState::ReadinessUnavailable { .. },
                } => {
                    let _ = response.send(RuntimeStartResult::ReadinessUnavailable {
                        snapshot: readiness_unavailable(snapshot),
                    });
                }
                NoRuntimePhase::Stopped {
                    state: StoppedState::Ready { .. },
                } => {
                    let origin = snapshot.clone();
                    self.begin_launch(
                        LaunchIntent::Start {
                            waiters: vec![response],
                        },
                        origin,
                    );
                }
            },
            LifecycleState::Running { snapshot, .. } => {
                let _ = response.send(RuntimeStartResult::AlreadyRunning {
                    snapshot: snapshot.clone(),
                });
            }
            LifecycleState::Operating(LifecycleOperation::Launching(operation)) => {
                match &mut operation.intent {
                    LaunchIntent::Start { waiters }
                    | LaunchIntent::Restart {
                        start_waiters: waiters,
                        ..
                    } => waiters.push(response),
                }
            }
            LifecycleState::Operating(LifecycleOperation::Stopping(operation)) => {
                match &mut operation.intent {
                    StopIntent::Restart { start_waiters, .. } => start_waiters.push(response),
                    StopIntent::Stop { .. } => {
                        let _ = response.send(RuntimeStartResult::OperationInProgress {
                            state: StartOperationInProgress::StoppingForStop {
                                snapshot: operation.snapshot.clone(),
                            },
                        });
                    }
                    StopIntent::Shutdown { .. } => {
                        let _ = response.send(RuntimeStartResult::ShuttingDown {
                            snapshot: shutdown_stopping(&operation.snapshot),
                        });
                    }
                }
            }
            LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation)) => {
                let result = match operation.intent {
                    CancellationIntent::Stop { .. } => RuntimeStartResult::Superseded {
                        by: StartSuperseder::Stop,
                    },
                    CancellationIntent::Shutdown { .. } => RuntimeStartResult::Superseded {
                        by: StartSuperseder::ManagerShutdown,
                    },
                };
                let _ = response.send(result);
            }
            LifecycleState::Unresponsive { snapshot, .. } => {
                let _ = response.send(RuntimeStartResult::RuntimeUnresponsive {
                    snapshot: snapshot.clone(),
                });
            }
            LifecycleState::Terminating(snapshot)
            | LifecycleState::TerminatingOperation { snapshot, .. }
            | LifecycleState::TerminatingManaged { snapshot, .. } => {
                let _ = response.send(RuntimeStartResult::ManagerTerminating {
                    snapshot: snapshot.clone(),
                });
            }
        }
    }

    fn admit_stop(&mut self, response: oneshot::Sender<RuntimeStopResult>) {
        let state = std::mem::replace(&mut self.state, placeholder_state());
        self.state = match state {
            LifecycleState::NoRuntime(snapshot) => {
                let _ = response.send(RuntimeStopResult::AlreadyStopped {
                    snapshot: snapshot.clone(),
                });
                LifecycleState::NoRuntime(snapshot)
            }
            LifecycleState::Running { snapshot, child } => self.begin_stop_state(
                child,
                StopIntent::Stop {
                    waiters: vec![response],
                },
                &snapshot,
            ),
            LifecycleState::Operating(LifecycleOperation::Launching(mut operation)) => {
                supersede_launch(&mut operation.intent, StartSuperseder::Stop);
                LifecycleState::Operating(LifecycleOperation::CancellingLaunch(
                    CancellingLaunchOperation {
                        token: operation.token,
                        header: next_header(&operation.snapshot.header),
                        child: operation.child,
                        origin: operation.origin,
                        intent: CancellationIntent::Stop {
                            waiters: vec![response],
                        },
                        attachment: operation.attachment,
                        termination: EarlyTerminationStage::Graceful {
                            task: GracefulStopTask::AwaitingCapability,
                            deadline: tokio::time::Instant::now() + STOP_DEADLINE,
                        },
                    },
                ))
            }
            LifecycleState::Operating(LifecycleOperation::Stopping(mut operation)) => {
                match &mut operation.intent {
                    StopIntent::Stop { waiters } => waiters.push(response),
                    StopIntent::Restart {
                        restart_waiters, ..
                    } => {
                        for waiter in restart_waiters.drain(..) {
                            let _ = waiter.send(RuntimeRestartResult::Superseded {
                                by: tribal_wire::management::RestartSuperseder::Stop,
                            });
                        }
                        operation.intent = StopIntent::Stop {
                            waiters: vec![response],
                        };
                    }
                    StopIntent::Shutdown { .. } => {
                        let _ = response.send(RuntimeStopResult::ShuttingDown {
                            snapshot: shutdown_stopping(&operation.snapshot),
                        });
                    }
                }
                LifecycleState::Operating(LifecycleOperation::Stopping(operation))
            }
            LifecycleState::Operating(LifecycleOperation::CancellingLaunch(mut operation)) => {
                match &mut operation.intent {
                    CancellationIntent::Stop { waiters } => waiters.push(response),
                    CancellationIntent::Shutdown { .. } => {
                        let _ = response.send(RuntimeStopResult::ShuttingDown {
                            snapshot: shutdown_cancelling(&operation),
                        });
                    }
                }
                LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation))
            }
            LifecycleState::Unresponsive { snapshot, child } => self.begin_stop_state(
                child,
                StopIntent::Stop {
                    waiters: vec![response],
                },
                &running_from_unresponsive(&snapshot),
            ),
            LifecycleState::Terminating(snapshot) => {
                let _ = response.send(RuntimeStopResult::ManagerTerminating {
                    snapshot: snapshot.clone(),
                });
                LifecycleState::Terminating(snapshot)
            }
            LifecycleState::TerminatingOperation {
                snapshot,
                operation,
            } => {
                let _ = response.send(RuntimeStopResult::ManagerTerminating {
                    snapshot: snapshot.clone(),
                });
                LifecycleState::TerminatingOperation {
                    snapshot,
                    operation,
                }
            }
            LifecycleState::TerminatingManaged { snapshot, child } => {
                let _ = response.send(RuntimeStopResult::ManagerTerminating {
                    snapshot: snapshot.clone(),
                });
                LifecycleState::TerminatingManaged { snapshot, child }
            }
        };
        self.publish_current();
    }

    fn admit_restart(&mut self, response: oneshot::Sender<RuntimeRestartResult>) {
        let state = std::mem::replace(&mut self.state, placeholder_state());
        self.state = match state {
            LifecycleState::NoRuntime(snapshot) => {
                let result = match snapshot.phase {
                    NoRuntimePhase::Unconfigured { .. } => RuntimeRestartResult::Blocked {
                        snapshot: clean_unconfigured_from_no_runtime(&snapshot),
                    },
                    NoRuntimePhase::Stopped {
                        state: StoppedState::ReadinessUnavailable { .. },
                    } => RuntimeRestartResult::ReadinessUnavailable {
                        snapshot: clean_readiness_unavailable(&snapshot),
                    },
                    NoRuntimePhase::Stopped { .. } => RuntimeRestartResult::NotRunning {
                        snapshot: snapshot.clone(),
                    },
                };
                let _ = response.send(result);
                LifecycleState::NoRuntime(snapshot)
            }
            LifecycleState::Running { snapshot, child } => self.begin_stop_state(
                child,
                StopIntent::Restart {
                    start_waiters: Vec::new(),
                    restart_waiters: vec![response],
                },
                &snapshot,
            ),
            LifecycleState::Operating(LifecycleOperation::Launching(operation)) => {
                let _ = response.send(RuntimeRestartResult::OperationInProgress {
                    state: RestartOperationInProgress::Launching {
                        snapshot: operation.snapshot.clone(),
                    },
                });
                LifecycleState::Operating(LifecycleOperation::Launching(operation))
            }
            LifecycleState::Operating(LifecycleOperation::Stopping(mut operation)) => {
                match &mut operation.intent {
                    StopIntent::Restart {
                        restart_waiters, ..
                    } => restart_waiters.push(response),
                    StopIntent::Stop { .. } => {
                        let _ = response.send(RuntimeRestartResult::OperationInProgress {
                            state: RestartOperationInProgress::StoppingForStop {
                                snapshot: operation.snapshot.clone(),
                            },
                        });
                    }
                    StopIntent::Shutdown { .. } => {
                        let _ = response.send(RuntimeRestartResult::ShuttingDown {
                            snapshot: shutdown_stopping(&operation.snapshot),
                        });
                    }
                }
                LifecycleState::Operating(LifecycleOperation::Stopping(operation))
            }
            LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation)) => {
                let result = match operation.intent {
                    CancellationIntent::Stop { .. } => RuntimeRestartResult::Superseded {
                        by: tribal_wire::management::RestartSuperseder::Stop,
                    },
                    CancellationIntent::Shutdown { .. } => RuntimeRestartResult::Superseded {
                        by: tribal_wire::management::RestartSuperseder::ManagerShutdown,
                    },
                };
                let _ = response.send(result);
                LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation))
            }
            LifecycleState::Unresponsive { snapshot, child } => self.begin_stop_state(
                child,
                StopIntent::Restart {
                    start_waiters: Vec::new(),
                    restart_waiters: vec![response],
                },
                &running_from_unresponsive(&snapshot),
            ),
            LifecycleState::Terminating(snapshot) => {
                let _ = response.send(RuntimeRestartResult::ManagerTerminating {
                    snapshot: snapshot.clone(),
                });
                LifecycleState::Terminating(snapshot)
            }
            LifecycleState::TerminatingOperation {
                snapshot,
                operation,
            } => {
                let _ = response.send(RuntimeRestartResult::ManagerTerminating {
                    snapshot: snapshot.clone(),
                });
                LifecycleState::TerminatingOperation {
                    snapshot,
                    operation,
                }
            }
            LifecycleState::TerminatingManaged { snapshot, child } => {
                let _ = response.send(RuntimeRestartResult::ManagerTerminating {
                    snapshot: snapshot.clone(),
                });
                LifecycleState::TerminatingManaged { snapshot, child }
            }
        };
        self.publish_current();
    }

    fn admit_shutdown(&mut self, response: oneshot::Sender<ManagerShutdownResult>) {
        let state = std::mem::replace(&mut self.state, placeholder_state());
        self.state = match state {
            LifecycleState::NoRuntime(snapshot) => {
                let _ = response.send(ManagerShutdownResult::ShuttingDown {
                    snapshot: snapshot.clone(),
                });
                self.shutdown_seen = true;
                self.shutdown.cancel();
                LifecycleState::NoRuntime(snapshot)
            }
            LifecycleState::Running { snapshot, child } => self.begin_stop_state(
                child,
                StopIntent::Shutdown {
                    waiters: vec![response],
                },
                &snapshot,
            ),
            LifecycleState::Operating(LifecycleOperation::Launching(mut operation)) => {
                supersede_launch(&mut operation.intent, StartSuperseder::ManagerShutdown);
                LifecycleState::Operating(LifecycleOperation::CancellingLaunch(
                    CancellingLaunchOperation {
                        token: operation.token,
                        header: next_header(&operation.snapshot.header),
                        child: operation.child,
                        origin: operation.origin,
                        intent: CancellationIntent::Shutdown {
                            waiters: vec![response],
                        },
                        attachment: operation.attachment,
                        termination: EarlyTerminationStage::Graceful {
                            task: GracefulStopTask::AwaitingCapability,
                            deadline: tokio::time::Instant::now() + STOP_DEADLINE,
                        },
                    },
                ))
            }
            LifecycleState::Operating(LifecycleOperation::Stopping(mut operation)) => {
                match &mut operation.intent {
                    StopIntent::Shutdown { waiters } => waiters.push(response),
                    StopIntent::Stop { waiters } => {
                        for waiter in waiters.drain(..) {
                            let _ = waiter.send(RuntimeStopResult::SupersededByManagerShutdown);
                        }
                        operation.intent = StopIntent::Shutdown {
                            waiters: vec![response],
                        };
                    }
                    StopIntent::Restart {
                        restart_waiters, ..
                    } => {
                        for waiter in restart_waiters.drain(..) {
                            let _ = waiter.send(RuntimeRestartResult::Superseded {
                                by: tribal_wire::management::RestartSuperseder::ManagerShutdown,
                            });
                        }
                        operation.intent = StopIntent::Shutdown {
                            waiters: vec![response],
                        };
                    }
                }
                LifecycleState::Operating(LifecycleOperation::Stopping(operation))
            }
            LifecycleState::Operating(LifecycleOperation::CancellingLaunch(mut operation)) => {
                match &mut operation.intent {
                    CancellationIntent::Shutdown { waiters } => waiters.push(response),
                    CancellationIntent::Stop { waiters } => {
                        for waiter in waiters.drain(..) {
                            let _ = waiter.send(RuntimeStopResult::SupersededByManagerShutdown);
                        }
                        operation.intent = CancellationIntent::Shutdown {
                            waiters: vec![response],
                        };
                    }
                }
                LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation))
            }
            LifecycleState::Unresponsive { snapshot, child } => self.begin_stop_state(
                child,
                StopIntent::Shutdown {
                    waiters: vec![response],
                },
                &running_from_unresponsive(&snapshot),
            ),
            LifecycleState::Terminating(snapshot) => {
                let _ = response.send(ManagerShutdownResult::ManagerTerminating {
                    snapshot: snapshot.clone(),
                });
                LifecycleState::Terminating(snapshot)
            }
            LifecycleState::TerminatingOperation {
                snapshot,
                operation,
            } => {
                let _ = response.send(ManagerShutdownResult::ManagerTerminating {
                    snapshot: snapshot.clone(),
                });
                LifecycleState::TerminatingOperation {
                    snapshot,
                    operation,
                }
            }
            LifecycleState::TerminatingManaged { snapshot, child } => {
                let _ = response.send(ManagerShutdownResult::ManagerTerminating {
                    snapshot: snapshot.clone(),
                });
                LifecycleState::TerminatingManaged { snapshot, child }
            }
        };
        self.publish_current();
    }

    fn begin_launch(&mut self, intent: LaunchIntent, origin: NoRuntimeLifecycleSnapshot) {
        let header = next_header(&origin.header);
        let snapshot = tribal_wire::management::StartingLifecycleSnapshot {
            header,
            phase: tribal_wire::management::StartingPhase::Starting,
        };
        let token = self.take_token();
        match prepare_child(
            &self.config_path,
            &self.authority,
            &snapshot.header.manager_instance_id,
        ) {
            Ok(prepared) => {
                let PreparedChild { child, attachment } = prepared;
                let evidence = tribal_wire::management::EarlyChildTerminationEvidence::PreCommit {
                    pid: attachment.identity.pid,
                    config_path: attachment.identity.config_path.clone(),
                };
                let early_child = EarlyChild {
                    child,
                    identity: attachment.identity.clone(),
                    custody: None,
                    control: None,
                    evidence,
                };
                let sender = self.completion_sender.clone();
                let task = tokio::spawn(async move {
                    let result = commit_custody(attachment).await;
                    let event = LifecycleCompletion::CustodyCommitted { token, result };
                    let _ = sender.send(event).await;
                });
                self.state =
                    LifecycleState::Operating(LifecycleOperation::Launching(LaunchingOperation {
                        token,
                        snapshot,
                        origin,
                        child: early_child,
                        intent,
                        attachment: AttachmentStage::Committing(task),
                    }));
                self.publish_current();
            }
            Err(failure) => self.finish_launch_failure(intent, origin, failure),
        }
    }

    fn begin_stop_state(
        &mut self,
        child: ManagedChild,
        intent: StopIntent,
        running: &RunningLifecycleSnapshot,
    ) -> LifecycleState {
        let token = self.take_token();
        let snapshot = StoppingLifecycleSnapshot {
            header: next_header(&running.header),
            phase: StoppingPhase::Stopping {
                runtime: child.identity.clone(),
            },
        };
        let control = child.control.clone();
        let runtime = child.identity.clone();
        self.observations.spawn(async move {
            match tokio::time::timeout(STOP_DEADLINE, control.stop(&runtime)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, pid = runtime.pid, "managed runtime stop was refused");
                }
                Err(_) => {
                    tracing::warn!(pid = runtime.pid, "managed runtime stop request timed out");
                }
            }
        });
        LifecycleState::Operating(LifecycleOperation::Stopping(StoppingOperation {
            token,
            snapshot,
            intent,
            child: Some(child),
            task: None,
            deadline: tokio::time::Instant::now() + STOP_DEADLINE,
            forced: false,
        }))
    }

    async fn handle_completion(&mut self, completion: LifecycleCompletion) {
        match completion {
            LifecycleCompletion::CustodyCommitted { token, result } => {
                self.handle_custody_commit(token, result).await;
            }
            LifecycleCompletion::RuntimeConnected { token, result } => {
                self.handle_runtime_connected(token, result).await;
            }
            LifecycleCompletion::EarlyStopRequested { token, accepted } => {
                self.handle_early_stop_requested(token, accepted);
            }
            LifecycleCompletion::Stopped { token, result } => {
                self.handle_stop_completion(token, result).await;
            }
            LifecycleCompletion::Document { token, document } => {
                if token == self.next_token.wrapping_sub(1)
                    && let LifecycleState::NoRuntime(current) = &self.state
                {
                    let failure = no_runtime_failure(&current.phase);
                    let document = document.unwrap_or(ConfigDocument::Unreadable {
                        phase: tribal_wire::management::ConfigPersistencePhase::DurabilityUncertain,
                    });
                    self.state = LifecycleState::NoRuntime(no_runtime_snapshot(
                        next_header(&current.header),
                        &document,
                        failure,
                    ));
                    self.publish_current();
                }
            }
        }
    }

    async fn handle_custody_commit(
        &mut self,
        token: u64,
        result: Result<CommittedCustody, LaunchFailure>,
    ) {
        let state = std::mem::replace(&mut self.state, placeholder_state());
        self.state = match state {
            LifecycleState::Operating(LifecycleOperation::Launching(mut operation))
                if operation.token == token
                    && matches!(operation.attachment, AttachmentStage::Committing(_)) =>
            {
                let task = take_attachment_task(&mut operation.attachment);
                self.track_task(task, "custody commit");
                match result {
                    Ok(committed) => {
                        operation.child.evidence =
                            tribal_wire::management::EarlyChildTerminationEvidence::Recoverable {
                                runtime: operation.child.identity.clone(),
                            };
                        operation.child.custody = Some(committed.custody);
                        operation.attachment = self.spawn_runtime_handshake(
                            token,
                            operation.child.identity.clone(),
                            operation.snapshot.header.manager_instance_id.clone(),
                            committed.control_proof,
                        );
                        LifecycleState::Operating(LifecycleOperation::Launching(operation))
                    }
                    Err(failure) => {
                        operation.child.evidence = failure.evidence;
                        terminate_early_child(&mut operation.child).await;
                        let failed = failed_no_runtime_from_origin(
                            &operation.origin,
                            failure.failure.clone(),
                        );
                        resolve_launch_failure(operation.intent, &failed);
                        LifecycleState::NoRuntime(with_failure(operation.origin, failure.failure))
                    }
                }
            }
            LifecycleState::Operating(LifecycleOperation::CancellingLaunch(mut operation))
                if operation.token == token
                    && matches!(operation.attachment, AttachmentStage::Committing(_)) =>
            {
                let task = take_attachment_task(&mut operation.attachment);
                self.track_task(task, "custody commit");
                match result {
                    Ok(committed) => {
                        operation.child.evidence =
                            tribal_wire::management::EarlyChildTerminationEvidence::Recoverable {
                                runtime: operation.child.identity.clone(),
                            };
                        operation.child.custody = Some(committed.custody);
                        if matches!(
                            operation.termination,
                            EarlyTerminationStage::Graceful { .. }
                        ) {
                            operation.attachment = self.spawn_runtime_handshake(
                                token,
                                operation.child.identity.clone(),
                                operation.header.manager_instance_id.clone(),
                                committed.control_proof,
                            );
                        } else {
                            operation.attachment = AttachmentStage::Settled;
                        }
                    }
                    Err(failure) => {
                        operation.child.evidence = failure.evidence;
                        operation.attachment = AttachmentStage::Settled;
                        self.force_early_reap(&mut operation);
                    }
                }
                if early_child_exited(&mut operation.child) {
                    self.abort_early_tasks(&mut operation);
                    resolve_cancel_without_child(operation.intent, &operation.origin);
                    LifecycleState::NoRuntime(operation.origin)
                } else {
                    LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation))
                }
            }
            other => {
                drop(result);
                self.state = other;
                return;
            }
        };
        self.publish_current();
    }

    async fn handle_runtime_connected(
        &mut self,
        token: u64,
        result: Result<RuntimeControlConnection, RuntimeControlError>,
    ) {
        let state = std::mem::replace(&mut self.state, placeholder_state());
        self.state = match state {
            LifecycleState::Operating(LifecycleOperation::Launching(mut operation))
                if operation.token == token
                    && matches!(operation.attachment, AttachmentStage::Handshaking(_)) =>
            {
                let task = take_attachment_task(&mut operation.attachment);
                self.track_task(task, "runtime handshake");
                match result {
                    Ok(control) => {
                        if let Ok(Some(status)) = operation.child.child.try_wait() {
                            let failure = StoppedProcessFailure::RuntimeAnnouncementFailed {
                                presentation: failure_presentation(
                                    "managed runtime exited during launch",
                                    &status.to_string(),
                                ),
                            };
                            let failed =
                                failed_no_runtime_from_origin(&operation.origin, failure.clone());
                            resolve_launch_failure(operation.intent, &failed);
                            return self.finish_completion_state(LifecycleState::NoRuntime(
                                with_failure(operation.origin, failure),
                            ));
                        }
                        let Some(custody) = operation.child.custody.take() else {
                            let failure = StoppedProcessFailure::RuntimeHandshakeFailed {
                                presentation: failure_presentation(
                                    "managed runtime handshake lost custody",
                                    "the committed custody resource is unavailable",
                                ),
                            };
                            terminate_early_child(&mut operation.child).await;
                            let failed =
                                failed_no_runtime_from_origin(&operation.origin, failure.clone());
                            resolve_launch_failure(operation.intent, &failed);
                            return self.finish_completion_state(LifecycleState::NoRuntime(
                                with_failure(operation.origin, failure),
                            ));
                        };
                        let child = ManagedChild {
                            process: ManagedProcess::Owned(operation.child.child),
                            identity: operation.child.identity,
                            custody,
                            control,
                        };
                        let snapshot = RunningLifecycleSnapshot {
                            header: next_header(&operation.snapshot.header),
                            phase: running_phase(&child.identity, &child.control, false),
                        };
                        resolve_launch_success(operation.intent, &snapshot);
                        LifecycleState::Running { snapshot, child }
                    }
                    Err(error) => {
                        let failure = StoppedProcessFailure::RuntimeHandshakeFailed {
                            presentation: failure_presentation(
                                "managed runtime handshake failed",
                                &error.to_string(),
                            ),
                        };
                        terminate_early_child(&mut operation.child).await;
                        let failed =
                            failed_no_runtime_from_origin(&operation.origin, failure.clone());
                        resolve_launch_failure(operation.intent, &failed);
                        LifecycleState::NoRuntime(with_failure(operation.origin, failure))
                    }
                }
            }
            LifecycleState::Operating(LifecycleOperation::CancellingLaunch(mut operation))
                if operation.token == token
                    && matches!(operation.attachment, AttachmentStage::Handshaking(_)) =>
            {
                let task = take_attachment_task(&mut operation.attachment);
                self.track_task(task, "runtime handshake");
                operation.attachment = AttachmentStage::Settled;
                match result {
                    Ok(control) => {
                        operation.child.control = Some(control);
                        self.start_early_stop(&mut operation);
                    }
                    Err(_) => self.force_early_reap(&mut operation),
                }
                if early_child_exited(&mut operation.child) {
                    self.abort_early_tasks(&mut operation);
                    resolve_cancel_without_child(operation.intent, &operation.origin);
                    LifecycleState::NoRuntime(operation.origin)
                } else {
                    LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation))
                }
            }
            other => {
                drop(result);
                self.state = other;
                return;
            }
        };
        self.publish_current();
    }

    fn handle_early_stop_requested(&mut self, token: u64, accepted: bool) {
        let state = std::mem::replace(&mut self.state, placeholder_state());
        let LifecycleState::Operating(LifecycleOperation::CancellingLaunch(mut operation)) = state
        else {
            self.state = state;
            return;
        };
        if operation.token != token {
            self.state = LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation));
            return;
        }
        let EarlyTerminationStage::Graceful { task, .. } = &mut operation.termination else {
            self.state = LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation));
            return;
        };
        if !matches!(task, GracefulStopTask::Requesting(_)) {
            self.state = LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation));
            return;
        }
        let completed = std::mem::replace(task, GracefulStopTask::Accepted);
        let task = match completed {
            GracefulStopTask::Requesting(task) => Some(task),
            GracefulStopTask::AwaitingCapability | GracefulStopTask::Accepted => None,
        };
        if !accepted {
            self.force_early_reap(&mut operation);
        }
        self.track_task(task, "early runtime stop");
        self.state = LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation));
    }

    fn spawn_runtime_handshake(
        &self,
        token: u64,
        identity: RuntimeIdentity,
        manager_instance_id: String,
        proof: RuntimeCustodyProof,
    ) -> AttachmentStage {
        let path = self.authority.paths().runtime_control_socket_path.clone();
        let sender = self.completion_sender.clone();
        let task = tokio::spawn(async move {
            let result =
                RuntimeControlClient::connect(&path, &manager_instance_id, &identity, proof).await;
            let _ = sender
                .send(LifecycleCompletion::RuntimeConnected { token, result })
                .await;
        });
        AttachmentStage::Handshaking(task)
    }

    fn start_early_stop(&self, operation: &mut CancellingLaunchOperation) {
        let EarlyTerminationStage::Graceful { task, .. } = &mut operation.termination else {
            return;
        };
        if !matches!(task, GracefulStopTask::AwaitingCapability) {
            return;
        }
        let Some(control) = operation.child.control.clone() else {
            return;
        };
        let runtime = operation.child.identity.clone();
        let token = operation.token;
        let sender = self.completion_sender.clone();
        let stop_task = tokio::spawn(async move {
            let accepted = control.stop(&runtime).await.is_ok();
            let _ = sender
                .send(LifecycleCompletion::EarlyStopRequested { token, accepted })
                .await;
        });
        *task = GracefulStopTask::Requesting(stop_task);
    }

    fn track_task(&mut self, task: Option<JoinHandle<()>>, name: &'static str) {
        if let Some(task) = task {
            self.observations.spawn(async move {
                if let Err(error) = task.await {
                    tracing::error!(%error, task = name, "lifecycle task failed");
                }
            });
        }
    }

    fn abort_early_tasks(&mut self, operation: &mut CancellingLaunchOperation) {
        let attachment = take_attachment_task(&mut operation.attachment);
        if let Some(task) = &attachment {
            task.abort();
        }
        self.track_task(attachment, "cancelled attachment");
        let stop = take_graceful_stop_task(&mut operation.termination);
        if let Some(task) = &stop {
            task.abort();
        }
        self.track_task(stop, "cancelled early stop");
    }

    fn force_early_reap(&mut self, operation: &mut CancellingLaunchOperation) {
        if matches!(
            operation.termination,
            EarlyTerminationStage::ForcedReap { .. }
        ) {
            return;
        }
        let stop = take_graceful_stop_task(&mut operation.termination);
        if let Some(task) = &stop {
            task.abort();
        }
        self.track_task(stop, "superseded early stop");
        let _ = operation.child.child.start_kill();
        operation.termination = EarlyTerminationStage::ForcedReap {
            deadline: tokio::time::Instant::now() + STOP_DEADLINE,
        };
    }

    fn finish_completion_state(&mut self, state: LifecycleState) {
        self.state = state;
        self.publish_current();
    }

    async fn handle_stop_completion(&mut self, token: u64, result: StopCompletion) {
        let state = std::mem::replace(&mut self.state, placeholder_state());
        self.state = match state {
            LifecycleState::Operating(LifecycleOperation::Stopping(operation))
                if operation.token == token =>
            {
                if let Some(task) = operation.task {
                    let _ = task.await;
                }
                match result {
                    StopCompletion::Stopped { document } => {
                        let document = document.unwrap_or(ConfigDocument::Unreadable {
                            phase: tribal_wire::management::ConfigPersistencePhase::DurabilityUncertain,
                        });
                        let no_runtime = no_runtime_snapshot(
                            next_header(&operation.snapshot.header),
                            &document,
                            None,
                        );
                        match operation.intent {
                            StopIntent::Stop { waiters } => {
                                let clean = clean_no_runtime_from_no_runtime(&no_runtime);
                                for waiter in waiters {
                                    let _ = waiter.send(RuntimeStopResult::Stopped {
                                        snapshot: clean.clone(),
                                    });
                                }
                                LifecycleState::NoRuntime(no_runtime)
                            }
                            StopIntent::Shutdown { waiters } => {
                                for waiter in waiters {
                                    let _ = waiter.send(ManagerShutdownResult::ShuttingDown {
                                        snapshot: no_runtime.clone(),
                                    });
                                }
                                self.shutdown_seen = true;
                                self.shutdown.cancel();
                                LifecycleState::NoRuntime(no_runtime)
                            }
                            StopIntent::Restart {
                                start_waiters,
                                restart_waiters,
                            } => match no_runtime.phase {
                                NoRuntimePhase::Unconfigured { .. } => {
                                    let blocked = clean_unconfigured_from_no_runtime(&no_runtime);
                                    for waiter in restart_waiters {
                                        let _ = waiter.send(RuntimeRestartResult::Blocked {
                                            snapshot: blocked.clone(),
                                        });
                                    }
                                    for waiter in start_waiters {
                                        let _ = waiter.send(RuntimeStartResult::Blocked {
                                            snapshot: unconfigured(&no_runtime),
                                        });
                                    }
                                    LifecycleState::NoRuntime(no_runtime)
                                }
                                NoRuntimePhase::Stopped { .. } => {
                                    self.state = LifecycleState::NoRuntime(no_runtime.clone());
                                    self.begin_launch(
                                        LaunchIntent::Restart {
                                            start_waiters,
                                            restart_waiters,
                                        },
                                        no_runtime,
                                    );
                                    return;
                                }
                            },
                        }
                    }
                }
            }
            LifecycleState::TerminatingOperation {
                snapshot,
                operation: LifecycleOperation::Stopping(operation),
            } if operation.token == token => {
                if let Some(task) = operation.task {
                    let _ = task.await;
                }
                drop(result);
                LifecycleState::Terminating(snapshot)
            }
            other => other,
        };
        self.publish_current();
    }

    fn finish_launch_failure(
        &mut self,
        intent: LaunchIntent,
        origin: NoRuntimeLifecycleSnapshot,
        failure: StoppedProcessFailure,
    ) {
        let failed = failed_no_runtime_from_origin(&origin, failure.clone());
        resolve_launch_failure(intent, &failed);
        self.state = LifecycleState::NoRuntime(with_failure(origin, failure));
        self.publish_current();
    }

    async fn observe_exit(&mut self) {
        if matches!(
            self.state,
            LifecycleState::Operating(LifecycleOperation::Stopping(_))
        ) {
            self.observe_stopping();
            return;
        }
        if matches!(
            self.state,
            LifecycleState::Operating(LifecycleOperation::CancellingLaunch(_))
        ) {
            self.observe_early_cancellation().await;
            return;
        }
        let state = std::mem::replace(&mut self.state, placeholder_state());
        match state {
            LifecycleState::Running { snapshot, child } => {
                let header = snapshot.header.clone();
                self.observe_managed_state(&header, child, |child| LifecycleState::Running {
                    snapshot,
                    child,
                });
            }
            LifecycleState::Unresponsive { snapshot, child } => {
                let header = snapshot.header.clone();
                self.observe_managed_state(&header, child, |child| LifecycleState::Unresponsive {
                    snapshot,
                    child,
                });
            }
            other => self.state = other,
        }
    }

    fn observe_managed_state(
        &mut self,
        header: &LifecycleSnapshotHeader,
        mut child: ManagedChild,
        retain: impl FnOnce(ManagedChild) -> LifecycleState,
    ) {
        match managed_child_exit_detail(&mut child) {
            Ok(Some(detail)) => {
                let failure = StoppedProcessFailure::RuntimeExited {
                    failure: RuntimeExitFailure {
                        presentation: failure_presentation("managed runtime exited", &detail),
                    },
                };
                let document = ConfigDocument::Unreadable {
                    phase: tribal_wire::management::ConfigPersistencePhase::DurabilityUncertain,
                };
                self.state = LifecycleState::NoRuntime(no_runtime_snapshot(
                    next_header(header),
                    &document,
                    Some(failure),
                ));
                self.publish_current();
                self.request_document_refresh();
            }
            Ok(None) if child.custody.is_closed() => {
                let snapshot = custody_loss_snapshot(header, &child.identity);
                self.state = LifecycleState::TerminatingManaged { snapshot, child };
                self.publish_current();
                self.shutdown_seen = true;
                self.shutdown.cancel();
            }
            Ok(None) => self.state = retain(child),
            Err(error) => {
                tracing::warn!(%error, "managed runtime status unavailable");
                self.state = retain(child);
            }
        }
    }

    fn observe_stopping(&mut self) {
        let LifecycleState::Operating(LifecycleOperation::Stopping(operation)) = &mut self.state
        else {
            return;
        };
        let Some(child) = &mut operation.child else {
            return;
        };
        if managed_child_exited(child) {
            drop(operation.child.take());
            let token = operation.token;
            let sender = self.completion_sender.clone();
            let config = self.config.clone();
            operation.task = Some(tokio::spawn(async move {
                let result = StopCompletion::Stopped {
                    document: config.document().await.ok(),
                };
                let _ = sender
                    .send(LifecycleCompletion::Stopped { token, result })
                    .await;
            }));
            return;
        }
        if child.custody.is_closed() {
            let snapshot = custody_loss_snapshot(&operation.snapshot.header, &child.identity);
            resolve_all_waiters_for_termination(&mut self.state, &snapshot);
            let state = std::mem::replace(&mut self.state, placeholder_state());
            let LifecycleState::Operating(LifecycleOperation::Stopping(mut operation)) = state
            else {
                self.state = state;
                return;
            };
            let Some(child) = operation.child.take() else {
                self.state = LifecycleState::Terminating(snapshot);
                return;
            };
            self.state = LifecycleState::TerminatingManaged { snapshot, child };
            self.publish_current();
            self.shutdown_seen = true;
            self.shutdown.cancel();
            return;
        }
        if tokio::time::Instant::now() < operation.deadline {
            return;
        }
        if !operation.forced
            && let ManagedProcess::Owned(process) = &mut child.process
        {
            let _ = process.start_kill();
            operation.forced = true;
            operation.deadline = tokio::time::Instant::now() + STOP_DEADLINE;
            return;
        }
        let state = std::mem::replace(&mut self.state, placeholder_state());
        let LifecycleState::Operating(LifecycleOperation::Stopping(mut operation)) = state else {
            self.state = state;
            return;
        };
        let Some(child) = operation.child.take() else {
            self.state = LifecycleState::Operating(LifecycleOperation::Stopping(operation));
            return;
        };
        let failure = RuntimeStopTimedOutFailure {
            presentation: failure_presentation(
                "managed runtime did not stop before the deadline",
                &format!("runtime pid {} remains active", child.identity.pid),
            ),
        };
        let snapshot = RuntimeUnresponsiveLifecycleSnapshot {
            header: next_header(&operation.snapshot.header),
            phase: RuntimeUnresponsivePhase::RuntimeUnresponsive {
                runtime: child.identity.clone(),
                operation: stop_intent_operation(&operation.intent),
                failure: failure.clone(),
            },
        };
        resolve_unresponsive(operation.intent, &snapshot);
        self.state = LifecycleState::Unresponsive { snapshot, child };
        self.publish_current();
    }

    async fn observe_early_cancellation(&mut self) {
        let state = std::mem::replace(&mut self.state, placeholder_state());
        let LifecycleState::Operating(LifecycleOperation::CancellingLaunch(mut operation)) = state
        else {
            self.state = state;
            return;
        };
        if early_child_exited(&mut operation.child) {
            self.abort_early_tasks(&mut operation);
            resolve_cancel_without_child(operation.intent, &operation.origin);
            self.state = LifecycleState::NoRuntime(operation.origin);
            self.publish_current();
            return;
        }
        if operation
            .child
            .custody
            .as_ref()
            .is_some_and(ManagerCustody::is_closed)
        {
            let snapshot = custody_loss_snapshot(&operation.header, &operation.child.identity);
            self.state = LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation));
            resolve_all_waiters_for_termination(&mut self.state, &snapshot);
            let state = std::mem::replace(&mut self.state, placeholder_state());
            let LifecycleState::Operating(operation) = state else {
                tracing::error!("restored cancellation operation changed before custody loss");
                self.state = state;
                return;
            };
            self.state = LifecycleState::TerminatingOperation {
                snapshot,
                operation,
            };
            self.publish_current();
            self.shutdown_seen = true;
            self.shutdown.cancel();
            return;
        }
        let attachment_failed = attachment_task_finished(&operation.attachment);
        let stop_failed = graceful_stop_task_finished(&operation.termination);
        if attachment_failed {
            let committing = matches!(operation.attachment, AttachmentStage::Committing(_));
            let joined = if let Some(task) = take_attachment_task(&mut operation.attachment) {
                match task.await {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::error!(%error, "early lifecycle attachment task failed");
                        false
                    }
                }
            } else {
                tracing::error!("finished lifecycle attachment lost its task");
                false
            };
            if joined && let Ok(completion) = self.completions.try_recv() {
                self.state =
                    LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation));
                self.handle_completion(completion).await;
                return;
            }
            if committing {
                operation.child.mark_commit_outcome_unknown();
            }
        }
        if attachment_failed || stop_failed {
            if stop_failed {
                let joined = if let Some(task) = take_graceful_stop_task(&mut operation.termination)
                {
                    match task.await {
                        Ok(()) => true,
                        Err(error) => {
                            tracing::error!(%error, "early runtime stop task failed");
                            false
                        }
                    }
                } else {
                    tracing::error!("finished early runtime stop lost its task");
                    false
                };
                if joined && let Ok(completion) = self.completions.try_recv() {
                    self.state =
                        LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation));
                    self.handle_completion(completion).await;
                    return;
                }
            }
            self.force_early_reap(&mut operation);
        }
        let now = tokio::time::Instant::now();
        if matches!(
            operation.termination,
            EarlyTerminationStage::Graceful { deadline, .. } if now >= deadline
        ) {
            self.force_early_reap(&mut operation);
        }
        let timed_out = matches!(
            operation.termination,
            EarlyTerminationStage::ForcedReap { deadline } if now >= deadline
        );
        if timed_out {
            let snapshot = ManagerTerminatingLifecycleSnapshot {
                header: next_header(&operation.header),
                phase: ManagerTerminatingPhase::ManagerTerminating {
                    termination: ManagerTermination::ChildReapTimedOut {
                        operation: match &operation.intent {
                            CancellationIntent::Stop { .. } => {
                                tribal_wire::management::EarlyChildTerminationOperation::Stop
                            }
                            CancellationIntent::Shutdown { .. } => {
                                tribal_wire::management::EarlyChildTerminationOperation::ManagerShutdown
                            }
                        },
                        evidence: operation.child.evidence.clone(),
                        presentation: failure_presentation(
                            "managed runtime could not be reaped",
                            "the manager is terminating with the child resource retained",
                        ),
                    },
                },
            };
            self.state = LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation));
            resolve_all_waiters_for_termination(&mut self.state, &snapshot);
            let state = std::mem::replace(&mut self.state, placeholder_state());
            let LifecycleState::Operating(operation) = state else {
                tracing::error!("restored cancellation operation changed before timeout");
                self.state = state;
                return;
            };
            self.state = LifecycleState::TerminatingOperation {
                snapshot,
                operation,
            };
            self.publish_current();
            self.shutdown_seen = true;
            self.shutdown.cancel();
            return;
        }
        self.state = LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation));
    }

    fn request_document_refresh(&mut self) {
        if !matches!(self.state, LifecycleState::NoRuntime(_)) || !self.observations.is_empty() {
            return;
        }
        let token = self.take_token();
        let sender = self.completion_sender.clone();
        let config = self.config.clone();
        self.observations.spawn(async move {
            let document = config.document().await.ok();
            let _ = sender
                .send(LifecycleCompletion::Document { token, document })
                .await;
        });
    }

    fn apply_config_change(&mut self) {
        match &mut self.state {
            LifecycleState::NoRuntime(_) => self.request_document_refresh(),
            LifecycleState::Running { snapshot, .. } => match snapshot.phase.clone() {
                RunningPhase::Healthy { runtime, .. } => {
                    snapshot.header = next_header(&snapshot.header);
                    snapshot.phase = RunningPhase::Healthy {
                        runtime,
                        restart_pending: true,
                    };
                    self.publish_current();
                }
                RunningPhase::Degraded {
                    runtime, reason, ..
                } => {
                    snapshot.header = next_header(&snapshot.header);
                    snapshot.phase = RunningPhase::Degraded {
                        runtime,
                        reason,
                        restart_pending: true,
                    };
                    self.publish_current();
                }
                RunningPhase::VersionMismatch { .. } => {}
            },
            LifecycleState::Operating(_)
            | LifecycleState::Unresponsive { .. }
            | LifecycleState::TerminatingOperation { .. }
            | LifecycleState::TerminatingManaged { .. }
            | LifecycleState::Terminating(_) => {}
        }
    }

    fn apply_readiness(&mut self, report: ReadinessReport) {
        match &mut self.state {
            LifecycleState::NoRuntime(snapshot) => {
                let failure = no_runtime_failure(&snapshot.phase);
                let prior_focus = match &snapshot.phase {
                    NoRuntimePhase::Unconfigured { focus, .. } => focus.clone(),
                    NoRuntimePhase::Stopped { .. } => None,
                };
                if matches!(report.start, StartVerdict::Blocked { .. }) {
                    if let Ok(readiness) = StartBlockedReadinessReport::try_from(report) {
                        snapshot.header = next_header(&snapshot.header);
                        snapshot.phase = NoRuntimePhase::Unconfigured {
                            focus: readiness_focus(&readiness.checks).or(prior_focus),
                            readiness,
                            failure,
                        };
                        self.publish_current();
                    }
                } else if let Ok(readiness) = StartClearReadinessReport::try_from(report) {
                    snapshot.header = next_header(&snapshot.header);
                    snapshot.phase = NoRuntimePhase::Stopped {
                        state: StoppedState::Ready { readiness, failure },
                    };
                    self.publish_current();
                }
            }
            LifecycleState::Running { snapshot, .. } => match snapshot.phase.clone() {
                RunningPhase::Healthy {
                    runtime,
                    restart_pending,
                }
                | RunningPhase::Degraded {
                    runtime,
                    restart_pending,
                    ..
                } => {
                    if matches!(report.health, HealthVerdict::Degraded { .. }) {
                        if let Ok(report) = HealthDegradedReadinessReport::try_from(report) {
                            snapshot.header = next_header(&snapshot.header);
                            snapshot.phase = RunningPhase::Degraded {
                                runtime,
                                reason: DegradedReason::Readiness { report },
                                restart_pending,
                            };
                            self.publish_current();
                        }
                    } else if matches!(report.health, HealthVerdict::Clear) {
                        snapshot.header = next_header(&snapshot.header);
                        snapshot.phase = RunningPhase::Healthy {
                            runtime,
                            restart_pending,
                        };
                        self.publish_current();
                    }
                }
                RunningPhase::VersionMismatch { .. } => {}
            },
            LifecycleState::Operating(_)
            | LifecycleState::Unresponsive { .. }
            | LifecycleState::TerminatingOperation { .. }
            | LifecycleState::TerminatingManaged { .. }
            | LifecycleState::Terminating(_) => {}
        }
    }

    async fn fold_ready_terminal_completions(&mut self) {
        let mut yielded_after_drain = false;
        loop {
            let mut folded = false;
            while let Ok(completion) = self.completions.try_recv() {
                self.fold_terminal_completion(completion);
                folded = true;
            }
            let ready = match &mut self.state {
                LifecycleState::Operating(LifecycleOperation::Launching(operation)) => {
                    attachment_task_finished(&operation.attachment).then(|| {
                        let committing =
                            matches!(operation.attachment, AttachmentStage::Committing(_));
                        (take_attachment_task(&mut operation.attachment), committing)
                    })
                }
                LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation)) => {
                    attachment_task_finished(&operation.attachment).then(|| {
                        let committing =
                            matches!(operation.attachment, AttachmentStage::Committing(_));
                        (take_attachment_task(&mut operation.attachment), committing)
                    })
                }
                LifecycleState::NoRuntime(_)
                | LifecycleState::Running { .. }
                | LifecycleState::Operating(LifecycleOperation::Stopping(_))
                | LifecycleState::Unresponsive { .. }
                | LifecycleState::TerminatingOperation { .. }
                | LifecycleState::TerminatingManaged { .. }
                | LifecycleState::Terminating(_) => None,
            };
            let Some((Some(task), committing)) = ready else {
                if folded && !yielded_after_drain {
                    yielded_after_drain = true;
                    tokio::task::yield_now().await;
                    continue;
                }
                break;
            };
            yielded_after_drain = false;
            if let Err(error) = task.await {
                tracing::error!(%error, "terminal lifecycle attachment task failed");
                if committing {
                    self.mark_active_commit_unknown();
                }
            }
        }
    }

    fn fold_terminal_completion(&mut self, completion: LifecycleCompletion) {
        let mut completed_task = None;
        match completion {
            LifecycleCompletion::CustodyCommitted { token, result } => {
                let operation = match &mut self.state {
                    LifecycleState::Operating(LifecycleOperation::Launching(operation))
                        if operation.token == token
                            && matches!(operation.attachment, AttachmentStage::Committing(_)) =>
                    {
                        Some((&mut operation.child, &mut operation.attachment))
                    }
                    LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation))
                        if operation.token == token
                            && matches!(operation.attachment, AttachmentStage::Committing(_)) =>
                    {
                        Some((&mut operation.child, &mut operation.attachment))
                    }
                    _ => None,
                };
                if let Some((child, attachment)) = operation {
                    completed_task = take_attachment_task(attachment);
                    *attachment = AttachmentStage::Settled;
                    match result {
                        Ok(committed) => {
                            child.evidence = tribal_wire::management::EarlyChildTerminationEvidence::Recoverable {
                                runtime: child.identity.clone(),
                            };
                            child.custody = Some(committed.custody);
                        }
                        Err(failure) => child.evidence = failure.evidence,
                    }
                }
            }
            LifecycleCompletion::RuntimeConnected { token, result } => {
                let operation = match &mut self.state {
                    LifecycleState::Operating(LifecycleOperation::Launching(operation))
                        if operation.token == token
                            && matches!(operation.attachment, AttachmentStage::Handshaking(_)) =>
                    {
                        Some((&mut operation.child, &mut operation.attachment))
                    }
                    LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation))
                        if operation.token == token
                            && matches!(operation.attachment, AttachmentStage::Handshaking(_)) =>
                    {
                        Some((&mut operation.child, &mut operation.attachment))
                    }
                    _ => None,
                };
                if let Some((child, attachment)) = operation {
                    completed_task = take_attachment_task(attachment);
                    *attachment = AttachmentStage::Settled;
                    if let Ok(control) = result {
                        child.control = Some(control);
                    }
                }
            }
            LifecycleCompletion::EarlyStopRequested { token, .. } => {
                if let LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation)) =
                    &mut self.state
                    && operation.token == token
                    && matches!(
                        operation.termination,
                        EarlyTerminationStage::Graceful {
                            task: GracefulStopTask::Requesting(_),
                            ..
                        }
                    )
                {
                    completed_task = take_graceful_stop_task(&mut operation.termination);
                }
            }
            LifecycleCompletion::Stopped { .. } | LifecycleCompletion::Document { .. } => {}
        }
        self.track_task(completed_task, "terminal lifecycle completion");
    }

    fn mark_active_commit_unknown(&mut self) {
        let child = match &mut self.state {
            LifecycleState::Operating(LifecycleOperation::Launching(operation)) => {
                Some(&mut operation.child)
            }
            LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation)) => {
                Some(&mut operation.child)
            }
            _ => None,
        };
        if let Some(child) = child {
            child.mark_commit_outcome_unknown();
        }
    }

    async fn terminate_for_worker(
        &mut self,
        correlation: Option<tribal_wire::management::PanicCorrelationId>,
    ) {
        self.fold_ready_terminal_completions().await;
        let exact_exit = match &mut self.state {
            LifecycleState::Running { child, .. } | LifecycleState::Unresponsive { child, .. } => {
                managed_child_exited(child)
            }
            LifecycleState::Operating(LifecycleOperation::Launching(operation)) => {
                early_child_exited(&mut operation.child)
            }
            LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation)) => {
                early_child_exited(&mut operation.child)
            }
            LifecycleState::Operating(LifecycleOperation::Stopping(operation)) => {
                operation.child.as_mut().is_some_and(managed_child_exited)
            }
            LifecycleState::NoRuntime(_)
            | LifecycleState::TerminatingOperation { .. }
            | LifecycleState::TerminatingManaged { .. }
            | LifecycleState::Terminating(_) => false,
        };
        let runtime = if exact_exit {
            ManagerTerminationRuntime::Absent
        } else {
            match &self.state {
                LifecycleState::Running { child, .. }
                | LifecycleState::Unresponsive { child, .. } => {
                    ManagerTerminationRuntime::Recoverable {
                        runtime: child.identity.clone(),
                    }
                }
                LifecycleState::Operating(LifecycleOperation::Launching(operation)) => {
                    termination_runtime(&operation.child.evidence)
                }
                LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation)) => {
                    termination_runtime(&operation.child.evidence)
                }
                LifecycleState::Operating(LifecycleOperation::Stopping(operation)) => operation
                    .child
                    .as_ref()
                    .map_or(ManagerTerminationRuntime::Absent, |child| {
                        ManagerTerminationRuntime::Recoverable {
                            runtime: child.identity.clone(),
                        }
                    }),
                LifecycleState::NoRuntime(_)
                | LifecycleState::TerminatingOperation { .. }
                | LifecycleState::TerminatingManaged { .. }
                | LifecycleState::Terminating(_) => ManagerTerminationRuntime::Absent,
            }
        };
        let snapshot = ManagerTerminatingLifecycleSnapshot {
            header: next_header(&self.state.snapshot().header),
            phase: ManagerTerminatingPhase::ManagerTerminating {
                termination: ManagerTermination::ConfigWorkerPanicked {
                    correlation,
                    runtime,
                },
            },
        };
        resolve_all_waiters_for_termination(&mut self.state, &snapshot);
        let state = std::mem::replace(&mut self.state, placeholder_state());
        self.state = match state {
            LifecycleState::Operating(LifecycleOperation::Stopping(mut operation)) => {
                if exact_exit {
                    drop(operation.child.take());
                    LifecycleState::Terminating(snapshot)
                } else if let Some(child) = operation.child.take() {
                    LifecycleState::TerminatingManaged { snapshot, child }
                } else {
                    LifecycleState::Terminating(snapshot)
                }
            }
            LifecycleState::Operating(operation) => LifecycleState::TerminatingOperation {
                snapshot,
                operation,
            },
            LifecycleState::TerminatingOperation {
                snapshot,
                operation,
            } => LifecycleState::TerminatingOperation {
                snapshot,
                operation,
            },
            LifecycleState::Running { child, .. } | LifecycleState::Unresponsive { child, .. } => {
                if exact_exit {
                    drop(child);
                    LifecycleState::Terminating(snapshot)
                } else {
                    LifecycleState::TerminatingManaged { snapshot, child }
                }
            }
            LifecycleState::TerminatingManaged { snapshot, child } => {
                LifecycleState::TerminatingManaged { snapshot, child }
            }
            LifecycleState::NoRuntime(_) | LifecycleState::Terminating(_) => {
                LifecycleState::Terminating(snapshot)
            }
        };
        self.publish_current();
        self.shutdown_seen = true;
        self.shutdown.cancel();
    }

    fn begin_external_shutdown(&mut self) -> bool {
        match &self.state {
            LifecycleState::NoRuntime(_)
            | LifecycleState::TerminatingManaged { .. }
            | LifecycleState::Terminating(_) => true,
            _ => {
                let (sender, receiver) = oneshot::channel();
                self.admit_shutdown(sender);
                drop(receiver);
                false
            }
        }
    }

    fn take_token(&mut self) -> u64 {
        let token = self.next_token;
        self.next_token = self.next_token.wrapping_add(1).max(1);
        token
    }

    fn publish_current(&self) {
        self.publisher.send_replace(self.state.snapshot());
    }
}

fn prepare_child(
    config_path: &PathBuf,
    authority: &AuthorityLease,
    manager_instance_id: &str,
) -> Result<PreparedChild, StoppedProcessFailure> {
    let inherited = authority
        .inheritable_clone()
        .map_err(|error| spawn_failure("preparing delegated authority", &error.to_string()))?;
    let fd = inherited.as_raw_fd();
    let runtime_instance_id = uuid::Uuid::new_v4().to_string();
    let proof = generate_proof()
        .map_err(|error| spawn_failure("generating custody proof", &error.to_string()))?;
    let control_proof = RuntimeCustodyProof::new(proof.expose_secret().to_owned());
    let mut command = tokio::process::Command::new(
        std::env::current_exe()
            .map_err(|error| spawn_failure("resolving runtime binary", &error.to_string()))?,
    );
    command
        .arg("serve")
        .arg("--config")
        .arg(config_path)
        .env(MANAGED_AUTHORITY_FD, fd.to_string())
        .env(MANAGED_RUNTIME_INSTANCE_ID, &runtime_instance_id)
        .env(MANAGED_MANAGER_INSTANCE_ID, manager_instance_id)
        .env(MANAGED_CUSTODY_PROOF, proof.expose_secret())
        .kill_on_drop(false);
    let child = command
        .spawn()
        .map_err(|error| spawn_failure("spawning managed runtime", &error.to_string()))?;
    let pid = child
        .id()
        .ok_or_else(|| spawn_failure("spawning managed runtime", "child has no process id"))?;
    drop(inherited);
    Ok(PreparedChild {
        child,
        attachment: PendingAttachment {
            identity: RuntimeIdentity {
                instance_id: runtime_instance_id,
                pid,
                binary_version: env!("CARGO_PKG_VERSION").to_owned(),
                config_path: ConfigFilePath {
                    path: config_path.to_string_lossy().into_owned(),
                },
            },
            paths: authority.paths().clone(),
            manager_instance_id: manager_instance_id.to_owned(),
            custody_proof: proof,
            control_proof,
        },
    })
}

async fn commit_custody(pending: PendingAttachment) -> Result<CommittedCustody, LaunchFailure> {
    let PendingAttachment {
        identity,
        paths,
        manager_instance_id,
        custody_proof,
        control_proof,
    } = pending;
    let custody = match tokio::task::spawn_blocking(move || {
        ManagerCustody::attach_initial(&paths, &manager_instance_id, custody_proof)
    })
    .await
    {
        Ok(Ok(custody)) => custody,
        Ok(Err(error)) => {
            return Err(LaunchFailure {
                failure: StoppedProcessFailure::RuntimeCustodyCommitFailed {
                    presentation: failure_presentation(
                        "managed runtime custody could not be committed",
                        &error.to_string(),
                    ),
                },
                evidence:
                    tribal_wire::management::EarlyChildTerminationEvidence::CommitOutcomeUnknown {
                        runtime: identity,
                    },
            });
        }
        Err(error) => {
            return Err(LaunchFailure {
                failure: StoppedProcessFailure::RuntimeCustodyCommitFailed {
                    presentation: failure_presentation(
                        "managed runtime custody task failed",
                        &error.to_string(),
                    ),
                },
                evidence:
                    tribal_wire::management::EarlyChildTerminationEvidence::CommitOutcomeUnknown {
                        runtime: identity,
                    },
            });
        }
    };
    Ok(CommittedCustody {
        custody,
        control_proof,
    })
}

async fn terminate_early_child(child: &mut EarlyChild) {
    if let Some(control) = &mut child.control {
        let _ = control.stop(&child.identity).await;
    }
    if let Some(custody) = &mut child.custody {
        let _ = custody.stop(&child.identity);
    }
    let _ = child.child.start_kill();
    let _ = tokio::time::timeout(STOP_DEADLINE, child.child.wait()).await;
}

fn early_child_exited(child: &mut EarlyChild) -> bool {
    matches!(child.child.try_wait(), Ok(Some(_)))
}

fn take_attachment_task(stage: &mut AttachmentStage) -> Option<JoinHandle<()>> {
    match std::mem::replace(stage, AttachmentStage::Settled) {
        AttachmentStage::Committing(task) | AttachmentStage::Handshaking(task) => Some(task),
        AttachmentStage::Settled => None,
    }
}

fn attachment_task_finished(stage: &AttachmentStage) -> bool {
    match stage {
        AttachmentStage::Committing(task) | AttachmentStage::Handshaking(task) => {
            task.is_finished()
        }
        AttachmentStage::Settled => false,
    }
}

fn take_graceful_stop_task(stage: &mut EarlyTerminationStage) -> Option<JoinHandle<()>> {
    let EarlyTerminationStage::Graceful { task, .. } = stage else {
        return None;
    };
    match std::mem::replace(task, GracefulStopTask::Accepted) {
        GracefulStopTask::Requesting(task) => Some(task),
        GracefulStopTask::AwaitingCapability | GracefulStopTask::Accepted => None,
    }
}

fn graceful_stop_task_finished(stage: &EarlyTerminationStage) -> bool {
    matches!(
        stage,
        EarlyTerminationStage::Graceful {
            task: GracefulStopTask::Requesting(task),
            ..
        } if task.is_finished()
    )
}

fn managed_child_exited(child: &mut ManagedChild) -> bool {
    matches!(managed_child_exit_detail(child), Ok(Some(_)))
}

fn managed_child_exit_detail(child: &mut ManagedChild) -> std::io::Result<Option<String>> {
    match &mut child.process {
        ManagedProcess::Owned(process) => process
            .try_wait()
            .map(|status| status.map(|value| value.to_string())),
        ManagedProcess::Recovered => Ok(child
            .control
            .is_closed()
            .then(|| "authenticated runtime-control session closed".to_owned())),
    }
}

fn resolve_launch_success(intent: LaunchIntent, snapshot: &RunningLifecycleSnapshot) {
    match intent {
        LaunchIntent::Start { waiters } => {
            for waiter in waiters {
                let _ = waiter.send(RuntimeStartResult::Started {
                    snapshot: snapshot.clone(),
                });
            }
        }
        LaunchIntent::Restart {
            start_waiters,
            restart_waiters,
        } => {
            for waiter in start_waiters {
                let _ = waiter.send(RuntimeStartResult::Started {
                    snapshot: snapshot.clone(),
                });
            }
            for waiter in restart_waiters {
                let _ = waiter.send(RuntimeRestartResult::Restarted {
                    snapshot: snapshot.clone(),
                });
            }
        }
    }
}

fn resolve_launch_failure(intent: LaunchIntent, snapshot: &FailedNoRuntimeLifecycleSnapshot) {
    match intent {
        LaunchIntent::Start { waiters } => {
            for waiter in waiters {
                let _ = waiter.send(RuntimeStartResult::Failed {
                    snapshot: snapshot.clone(),
                });
            }
        }
        LaunchIntent::Restart {
            start_waiters,
            restart_waiters,
        } => {
            for waiter in start_waiters {
                let _ = waiter.send(RuntimeStartResult::Failed {
                    snapshot: snapshot.clone(),
                });
            }
            for waiter in restart_waiters {
                let _ = waiter.send(RuntimeRestartResult::Failed {
                    snapshot: snapshot.clone(),
                });
            }
        }
    }
}

fn supersede_launch(intent: &mut LaunchIntent, by: StartSuperseder) {
    match intent {
        LaunchIntent::Start { waiters } => {
            for waiter in waiters.drain(..) {
                let _ = waiter.send(RuntimeStartResult::Superseded { by });
            }
        }
        LaunchIntent::Restart {
            start_waiters,
            restart_waiters,
        } => {
            for waiter in start_waiters.drain(..) {
                let _ = waiter.send(RuntimeStartResult::Superseded { by });
            }
            let restart_by = match by {
                StartSuperseder::Stop => tribal_wire::management::RestartSuperseder::Stop,
                StartSuperseder::ManagerShutdown => {
                    tribal_wire::management::RestartSuperseder::ManagerShutdown
                }
            };
            for waiter in restart_waiters.drain(..) {
                let _ = waiter.send(RuntimeRestartResult::Superseded { by: restart_by });
            }
        }
    }
}

fn resolve_cancel_without_child(intent: CancellationIntent, origin: &NoRuntimeLifecycleSnapshot) {
    match intent {
        CancellationIntent::Stop { waiters } => {
            let snapshot = clean_no_runtime_from_no_runtime(origin);
            for waiter in waiters {
                let _ = waiter.send(RuntimeStopResult::Stopped {
                    snapshot: snapshot.clone(),
                });
            }
        }
        CancellationIntent::Shutdown { waiters } => {
            for waiter in waiters {
                let _ = waiter.send(ManagerShutdownResult::ShuttingDown {
                    snapshot: origin.clone(),
                });
            }
        }
    }
}

fn resolve_unresponsive(intent: StopIntent, snapshot: &RuntimeUnresponsiveLifecycleSnapshot) {
    let (runtime, failure) = match &snapshot.phase {
        RuntimeUnresponsivePhase::RuntimeUnresponsive {
            runtime, failure, ..
        } => (runtime.clone(), failure.clone()),
    };
    match intent {
        StopIntent::Stop { waiters } => {
            let narrowed = StopRuntimeUnresponsiveLifecycleSnapshot {
                header: snapshot.header.clone(),
                phase: StopRuntimeUnresponsivePhase::RuntimeUnresponsive {
                    runtime,
                    operation: StopRuntimeOperation::Stop,
                    failure,
                },
            };
            for waiter in waiters {
                let _ = waiter.send(RuntimeStopResult::RuntimeUnresponsive {
                    snapshot: narrowed.clone(),
                });
            }
        }
        StopIntent::Restart {
            start_waiters,
            restart_waiters,
        } => {
            for waiter in start_waiters {
                let _ = waiter.send(RuntimeStartResult::RuntimeUnresponsive {
                    snapshot: snapshot.clone(),
                });
            }
            let narrowed = RestartRuntimeUnresponsiveLifecycleSnapshot {
                header: snapshot.header.clone(),
                phase: RestartRuntimeUnresponsivePhase::RuntimeUnresponsive {
                    runtime,
                    operation: RestartRuntimeOperation::Restart,
                    failure,
                },
            };
            for waiter in restart_waiters {
                let _ = waiter.send(RuntimeRestartResult::RuntimeUnresponsive {
                    snapshot: narrowed.clone(),
                });
            }
        }
        StopIntent::Shutdown { waiters } => {
            let narrowed = ShutdownRuntimeUnresponsiveLifecycleSnapshot {
                header: snapshot.header.clone(),
                phase: ShutdownRuntimeUnresponsivePhase::RuntimeUnresponsive {
                    runtime,
                    operation: ManagerShutdownOperation::ManagerShutdown,
                    failure,
                },
            };
            for waiter in waiters {
                let _ = waiter.send(ManagerShutdownResult::RuntimeUnresponsive {
                    snapshot: narrowed.clone(),
                });
            }
        }
    }
}

fn resolve_all_waiters_for_termination(
    state: &mut LifecycleState,
    snapshot: &ManagerTerminatingLifecycleSnapshot,
) {
    let LifecycleState::Operating(operation) = state else {
        return;
    };
    match operation {
        LifecycleOperation::Launching(operation) => match &mut operation.intent {
            LaunchIntent::Start { waiters } => {
                for waiter in waiters.drain(..) {
                    let _ = waiter.send(RuntimeStartResult::ManagerTerminating {
                        snapshot: snapshot.clone(),
                    });
                }
            }
            LaunchIntent::Restart {
                start_waiters,
                restart_waiters,
            } => {
                for waiter in start_waiters.drain(..) {
                    let _ = waiter.send(RuntimeStartResult::ManagerTerminating {
                        snapshot: snapshot.clone(),
                    });
                }
                for waiter in restart_waiters.drain(..) {
                    let _ = waiter.send(RuntimeRestartResult::ManagerTerminating {
                        snapshot: snapshot.clone(),
                    });
                }
            }
        },
        LifecycleOperation::Stopping(operation) => match &mut operation.intent {
            StopIntent::Stop { waiters } => {
                for waiter in waiters.drain(..) {
                    let _ = waiter.send(RuntimeStopResult::ManagerTerminating {
                        snapshot: snapshot.clone(),
                    });
                }
            }
            StopIntent::Restart {
                start_waiters,
                restart_waiters,
            } => {
                for waiter in start_waiters.drain(..) {
                    let _ = waiter.send(RuntimeStartResult::ManagerTerminating {
                        snapshot: snapshot.clone(),
                    });
                }
                for waiter in restart_waiters.drain(..) {
                    let _ = waiter.send(RuntimeRestartResult::ManagerTerminating {
                        snapshot: snapshot.clone(),
                    });
                }
            }
            StopIntent::Shutdown { waiters } => {
                for waiter in waiters.drain(..) {
                    let _ = waiter.send(ManagerShutdownResult::ManagerTerminating {
                        snapshot: snapshot.clone(),
                    });
                }
            }
        },
        LifecycleOperation::CancellingLaunch(operation) => match &mut operation.intent {
            CancellationIntent::Stop { waiters } => {
                for waiter in waiters.drain(..) {
                    let _ = waiter.send(RuntimeStopResult::ManagerTerminating {
                        snapshot: snapshot.clone(),
                    });
                }
            }
            CancellationIntent::Shutdown { waiters } => {
                for waiter in waiters.drain(..) {
                    let _ = waiter.send(ManagerShutdownResult::ManagerTerminating {
                        snapshot: snapshot.clone(),
                    });
                }
            }
        },
    }
}

fn stop_intent_operation(intent: &StopIntent) -> RuntimeOperation {
    match intent {
        StopIntent::Stop { .. } => RuntimeOperation::Stop,
        StopIntent::Restart { .. } => RuntimeOperation::Restart,
        StopIntent::Shutdown { .. } => RuntimeOperation::ManagerShutdown,
    }
}

fn termination_runtime(
    evidence: &tribal_wire::management::EarlyChildTerminationEvidence,
) -> ManagerTerminationRuntime {
    match evidence {
        tribal_wire::management::EarlyChildTerminationEvidence::PreCommit { .. } => {
            ManagerTerminationRuntime::Absent
        }
        tribal_wire::management::EarlyChildTerminationEvidence::CommitOutcomeUnknown {
            runtime,
        } => ManagerTerminationRuntime::CommitOutcomeUnknown {
            runtime: runtime.clone(),
        },
        tribal_wire::management::EarlyChildTerminationEvidence::Recoverable { runtime } => {
            ManagerTerminationRuntime::Recoverable {
                runtime: runtime.clone(),
            }
        }
    }
}

fn no_runtime_failure(phase: &NoRuntimePhase) -> Option<StoppedProcessFailure> {
    match phase {
        NoRuntimePhase::Unconfigured { failure, .. }
        | NoRuntimePhase::Stopped {
            state: StoppedState::Ready { failure, .. },
        } => failure.clone(),
        NoRuntimePhase::Stopped {
            state:
                StoppedState::ReadinessUnavailable {
                    process_failure, ..
                },
        } => process_failure.clone(),
    }
}

fn no_runtime_snapshot(
    header: LifecycleSnapshotHeader,
    document: &ConfigDocument,
    failure: Option<StoppedProcessFailure>,
) -> NoRuntimeLifecycleSnapshot {
    match document {
        ConfigDocument::DurableValid { .. } => NoRuntimeLifecycleSnapshot {
            header,
            phase: NoRuntimePhase::Stopped {
                state: StoppedState::Ready {
                    readiness: start_clear(),
                    failure,
                },
            },
        },
        ConfigDocument::DurableInvalid { .. }
        | ConfigDocument::UncertainValid { .. }
        | ConfigDocument::UncertainInvalid { .. }
        | ConfigDocument::Unreadable { .. } => {
            let readiness = start_blocked();
            NoRuntimeLifecycleSnapshot {
                header,
                phase: NoRuntimePhase::Unconfigured {
                    focus: readiness_focus(&readiness.checks),
                    readiness,
                    failure,
                },
            }
        }
    }
}

fn with_failure(
    mut snapshot: NoRuntimeLifecycleSnapshot,
    failure: StoppedProcessFailure,
) -> NoRuntimeLifecycleSnapshot {
    snapshot.header = next_header(&snapshot.header);
    match &mut snapshot.phase {
        NoRuntimePhase::Unconfigured {
            failure: current, ..
        }
        | NoRuntimePhase::Stopped {
            state: StoppedState::Ready {
                failure: current, ..
            },
        } => *current = Some(failure),
        NoRuntimePhase::Stopped {
            state:
                StoppedState::ReadinessUnavailable {
                    process_failure, ..
                },
        } => *process_failure = Some(failure),
    }
    snapshot
}

fn unconfigured(
    snapshot: &NoRuntimeLifecycleSnapshot,
) -> tribal_wire::management::UnconfiguredLifecycleSnapshot {
    let NoRuntimePhase::Unconfigured {
        readiness,
        focus,
        failure,
    } = &snapshot.phase
    else {
        return tribal_wire::management::UnconfiguredLifecycleSnapshot {
            header: snapshot.header.clone(),
            phase: tribal_wire::management::UnconfiguredPhase::Unconfigured {
                readiness: start_blocked(),
                focus: None,
                failure: None,
            },
        };
    };
    tribal_wire::management::UnconfiguredLifecycleSnapshot {
        header: snapshot.header.clone(),
        phase: tribal_wire::management::UnconfiguredPhase::Unconfigured {
            readiness: readiness.clone(),
            focus: focus.clone(),
            failure: failure.clone(),
        },
    }
}

fn readiness_unavailable(
    snapshot: &NoRuntimeLifecycleSnapshot,
) -> tribal_wire::management::ReadinessUnavailableLifecycleSnapshot {
    let NoRuntimePhase::Stopped {
        state:
            StoppedState::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure,
            },
    } = &snapshot.phase
    else {
        return tribal_wire::management::ReadinessUnavailableLifecycleSnapshot {
            header: snapshot.header.clone(),
            phase: tribal_wire::management::ReadinessUnavailablePhase::Stopped {
                state: tribal_wire::management::ReadinessUnavailableStoppedState::ReadinessUnavailable {
                    last_report: None,
                    presentation: failure_presentation(
                        "runtime readiness is unavailable",
                        "retry the readiness check",
                    ),
                    process_failure: None,
                },
            },
        };
    };
    tribal_wire::management::ReadinessUnavailableLifecycleSnapshot {
        header: snapshot.header.clone(),
        phase: tribal_wire::management::ReadinessUnavailablePhase::Stopped {
            state:
                tribal_wire::management::ReadinessUnavailableStoppedState::ReadinessUnavailable {
                    last_report: last_report.clone(),
                    presentation: presentation.clone(),
                    process_failure: process_failure.clone(),
                },
        },
    }
}

fn clean_no_runtime_from_no_runtime(
    snapshot: &NoRuntimeLifecycleSnapshot,
) -> CleanNoRuntimeLifecycleSnapshot {
    let phase = match &snapshot.phase {
        NoRuntimePhase::Unconfigured {
            readiness, focus, ..
        } => CleanNoRuntimePhase::Unconfigured {
            readiness: readiness.clone(),
            focus: focus.clone(),
            failure: None,
        },
        NoRuntimePhase::Stopped {
            state: StoppedState::Ready { readiness, .. },
        } => CleanNoRuntimePhase::Stopped {
            state: CleanStoppedState::Ready {
                readiness: readiness.clone(),
                failure: None,
            },
        },
        NoRuntimePhase::Stopped {
            state:
                StoppedState::ReadinessUnavailable {
                    last_report,
                    presentation,
                    ..
                },
        } => CleanNoRuntimePhase::Stopped {
            state: CleanStoppedState::ReadinessUnavailable {
                last_report: last_report.clone(),
                presentation: presentation.clone(),
                process_failure: None,
            },
        },
    };
    CleanNoRuntimeLifecycleSnapshot {
        header: snapshot.header.clone(),
        phase,
    }
}

fn clean_unconfigured_from_no_runtime(
    snapshot: &NoRuntimeLifecycleSnapshot,
) -> CleanUnconfiguredLifecycleSnapshot {
    let (readiness, focus) = match &snapshot.phase {
        NoRuntimePhase::Unconfigured {
            readiness, focus, ..
        } => (readiness.clone(), focus.clone()),
        NoRuntimePhase::Stopped { .. } => (start_blocked(), None),
    };
    CleanUnconfiguredLifecycleSnapshot {
        header: snapshot.header.clone(),
        phase: CleanUnconfiguredPhase::Unconfigured {
            readiness,
            focus,
            failure: None,
        },
    }
}

fn clean_readiness_unavailable(
    snapshot: &NoRuntimeLifecycleSnapshot,
) -> tribal_wire::management::CleanReadinessUnavailableLifecycleSnapshot {
    let (last_report, presentation) = match &snapshot.phase {
        NoRuntimePhase::Stopped {
            state:
                StoppedState::ReadinessUnavailable {
                    last_report,
                    presentation,
                    ..
                },
        } => (last_report.clone(), presentation.clone()),
        NoRuntimePhase::Unconfigured { .. }
        | NoRuntimePhase::Stopped {
            state: StoppedState::Ready { .. },
        } => (
            None,
            failure_presentation(
                "runtime readiness is unavailable",
                "retry the readiness check",
            ),
        ),
    };
    tribal_wire::management::CleanReadinessUnavailableLifecycleSnapshot {
        header: snapshot.header.clone(),
        phase: tribal_wire::management::CleanReadinessUnavailablePhase::Stopped {
            state: tribal_wire::management::CleanReadinessUnavailableStoppedState::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure: None,
            },
        },
    }
}

fn failed_no_runtime_from_origin(
    snapshot: &NoRuntimeLifecycleSnapshot,
    failure: StoppedProcessFailure,
) -> FailedNoRuntimeLifecycleSnapshot {
    let phase = match &snapshot.phase {
        NoRuntimePhase::Unconfigured {
            readiness, focus, ..
        } => FailedNoRuntimePhase::Unconfigured {
            readiness: readiness.clone(),
            focus: focus.clone(),
            failure,
        },
        NoRuntimePhase::Stopped {
            state: StoppedState::Ready { readiness, .. },
        } => FailedNoRuntimePhase::Stopped {
            state: tribal_wire::management::FailedStoppedState::Ready {
                readiness: readiness.clone(),
                failure,
            },
        },
        NoRuntimePhase::Stopped {
            state:
                StoppedState::ReadinessUnavailable {
                    last_report,
                    presentation,
                    ..
                },
        } => FailedNoRuntimePhase::Stopped {
            state: tribal_wire::management::FailedStoppedState::ReadinessUnavailable {
                last_report: last_report.clone(),
                presentation: presentation.clone(),
                process_failure: failure,
            },
        },
    };
    FailedNoRuntimeLifecycleSnapshot {
        header: snapshot.header.clone(),
        phase,
    }
}

fn running_from_unresponsive(
    snapshot: &RuntimeUnresponsiveLifecycleSnapshot,
) -> RunningLifecycleSnapshot {
    let RuntimeUnresponsivePhase::RuntimeUnresponsive { runtime, .. } = &snapshot.phase;
    RunningLifecycleSnapshot {
        header: snapshot.header.clone(),
        phase: RunningPhase::Degraded {
            runtime: runtime.clone(),
            reason: DegradedReason::RuntimeControlLost {
                report: readiness::derive(Vec::new(), false),
                presentation: failure_presentation(
                    "runtime control is unavailable",
                    "retry the requested lifecycle operation",
                ),
            },
            restart_pending: false,
        },
    }
}

fn running_phase(
    runtime: &RuntimeIdentity,
    control: &RuntimeControlConnection,
    restart_pending: bool,
) -> RunningPhase {
    if control.is_compatible() {
        RunningPhase::Healthy {
            runtime: runtime.clone(),
            restart_pending,
        }
    } else {
        RunningPhase::VersionMismatch {
            runtime: runtime.clone(),
            manager_version: env!("CARGO_PKG_VERSION").to_owned(),
            runtime_version: runtime.binary_version.clone(),
        }
    }
}

fn shutdown_stopping(snapshot: &StoppingLifecycleSnapshot) -> ShutdownInProgressLifecycleSnapshot {
    let StoppingPhase::Stopping { runtime } = &snapshot.phase;
    ShutdownInProgressLifecycleSnapshot {
        header: snapshot.header.clone(),
        phase: ShutdownInProgressPhase::Stopping {
            runtime: runtime.clone(),
        },
    }
}

fn shutdown_cancelling(
    operation: &CancellingLaunchOperation,
) -> ShutdownInProgressLifecycleSnapshot {
    ShutdownInProgressLifecycleSnapshot {
        header: operation.header.clone(),
        phase: ShutdownInProgressPhase::CancellingEarlyChild {
            operation: ManagerShutdownOperation::ManagerShutdown,
            evidence: operation.child.evidence.clone(),
        },
    }
}

fn cancellation_snapshot(operation: &CancellingLaunchOperation) -> LifecycleSnapshot {
    let intent = match operation.intent {
        CancellationIntent::Stop { .. } => {
            tribal_wire::management::EarlyChildTerminationOperation::Stop
        }
        CancellationIntent::Shutdown { .. } => {
            tribal_wire::management::EarlyChildTerminationOperation::ManagerShutdown
        }
    };
    LifecycleSnapshot {
        header: operation.header.clone(),
        phase: LifecyclePhase::CancellingEarlyChild {
            operation: intent,
            evidence: operation.child.evidence.clone(),
        },
    }
}

fn placeholder_state() -> LifecycleState {
    LifecycleState::NoRuntime(NoRuntimeLifecycleSnapshot {
        header: LifecycleSnapshotHeader {
            manager_instance_id: String::new(),
            revision: 0,
            manager_version: String::new(),
        },
        phase: NoRuntimePhase::Unconfigured {
            readiness: start_blocked(),
            focus: None,
            failure: None,
        },
    })
}

fn next_header(header: &LifecycleSnapshotHeader) -> LifecycleSnapshotHeader {
    LifecycleSnapshotHeader {
        manager_instance_id: header.manager_instance_id.clone(),
        revision: header.revision.saturating_add(1),
        manager_version: header.manager_version.clone(),
    }
}

fn readiness_focus(
    checks: &[tribal_wire::management::CheckObservation],
) -> Option<tribal_domain::ConfigFieldPath> {
    for subject in checks.iter().flat_map(|check| &check.subjects) {
        let CheckSubject::Configuration { location } = subject else {
            continue;
        };
        match location {
            ConfigDiagnosticLocation::Field { path } => return Some(path.clone()),
            ConfigDiagnosticLocation::CredentialEntry { .. } => {
                if let Ok(path) = tribal_domain::ConfigFieldPath::parse("credentials") {
                    return Some(path);
                }
            }
        }
    }
    None
}

fn start_clear() -> StartClearReadinessReport {
    let report = readiness::derive(
        vec![readiness::observation(
            CheckResult::Pass {
                name: CheckName::ConfigValidate,
                detail: "configuration is valid".to_owned(),
            },
            Vec::new(),
        )],
        false,
    );
    StartClearReadinessReport::try_from(report).unwrap_or(StartClearReadinessReport {
        start: StartClearVerdict::Clear,
        health: HealthVerdict::NotApplicable,
        checks: Vec::new(),
    })
}

fn start_blocked() -> StartBlockedReadinessReport {
    let report = readiness::derive(
        vec![readiness::observation(
            CheckResult::Fail {
                name: CheckName::ConfigParse,
                detail: "configuration is invalid".to_owned(),
                remediation: "repair the configuration before starting the runtime".to_owned(),
            },
            tribal_domain::ConfigFieldPath::parse("database")
                .ok()
                .map(|path| CheckSubject::Configuration {
                    location: ConfigDiagnosticLocation::Field { path },
                })
                .into_iter()
                .collect(),
        )],
        false,
    );
    StartBlockedReadinessReport::try_from(report).unwrap_or(StartBlockedReadinessReport {
        start: StartBlockedVerdict::Blocked {
            first: CheckName::ConfigParse,
            rest: Vec::new(),
        },
        health: HealthVerdict::NotApplicable,
        checks: Vec::new(),
    })
}

fn spawn_failure(message: &str, detail: &str) -> StoppedProcessFailure {
    StoppedProcessFailure::SpawnFailed {
        presentation: failure_presentation(message, detail),
    }
}

fn failure_presentation(
    message: &str,
    detail: &str,
) -> tribal_wire::management::FailurePresentation {
    tribal_wire::management::FailurePresentation {
        message: format!("{message}: {detail}"),
        remediation: Some("inspect runtime logs and retry".to_owned()),
    }
}

fn custody_loss_snapshot(
    header: &LifecycleSnapshotHeader,
    runtime: &RuntimeIdentity,
) -> ManagerTerminatingLifecycleSnapshot {
    ManagerTerminatingLifecycleSnapshot {
        header: next_header(header),
        phase: ManagerTerminatingPhase::ManagerTerminating {
            termination: ManagerTermination::CustodyLost {
                presentation: failure_presentation(
                    "managed runtime custody was lost",
                    "the manager will exit so a successor can recover the runtime",
                ),
                runtime: CustodyLossTerminationRuntime::Recoverable {
                    runtime: runtime.clone(),
                },
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::management::{authority, configuration, worker};

    /// Local lifecycle observations have no external service dependency.
    const TEST_OBSERVATION_DEADLINE: Duration = Duration::from_secs(2);

    fn header() -> LifecycleSnapshotHeader {
        LifecycleSnapshotHeader {
            manager_instance_id: "manager".to_owned(),
            revision: 1,
            manager_version: "test".to_owned(),
        }
    }

    fn no_runtime_origin() -> NoRuntimeLifecycleSnapshot {
        let LifecycleState::NoRuntime(snapshot) = placeholder_state() else {
            unreachable!("the placeholder is a no-runtime state");
        };
        snapshot
    }

    fn early_child() -> EarlyChild {
        let child = tokio::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("sleep 30")
            .kill_on_drop(true)
            .spawn()
            .expect("test child starts");
        let pid = child.id().expect("test child has a process id");
        let config_path = ConfigFilePath {
            path: "/tmp/tribal.yaml".to_owned(),
        };
        EarlyChild {
            child,
            identity: RuntimeIdentity {
                instance_id: "early-child".to_owned(),
                pid,
                binary_version: "test".to_owned(),
                config_path: config_path.clone(),
            },
            custody: None,
            control: None,
            evidence: tribal_wire::management::EarlyChildTerminationEvidence::PreCommit {
                pid,
                config_path,
            },
        }
    }

    fn a_launching_operation(token: u64, attachment: AttachmentStage) -> LaunchingOperation {
        let (start, _start_result) = oneshot::channel();
        LaunchingOperation {
            token,
            snapshot: tribal_wire::management::StartingLifecycleSnapshot {
                header: header(),
                phase: tribal_wire::management::StartingPhase::Starting,
            },
            origin: no_runtime_origin(),
            child: early_child(),
            intent: LaunchIntent::Start {
                waiters: vec![start],
            },
            attachment,
        }
    }

    async fn reap_launching_operation(owner: &mut LifecycleOwner) {
        let state = std::mem::replace(&mut owner.state, placeholder_state());
        let LifecycleState::Operating(LifecycleOperation::Launching(mut operation)) = state else {
            panic!("test owner retains its launching operation");
        };
        if let Some(task) = take_attachment_task(&mut operation.attachment) {
            task.abort();
            assert!(
                task.await.is_err(),
                "the pending attachment task is aborted"
            );
        }
        operation
            .child
            .child
            .start_kill()
            .expect("test child accepts forced termination");
        tokio::time::timeout(TEST_OBSERVATION_DEADLINE, operation.child.child.wait())
            .await
            .expect("test child is reaped")
            .expect("test child status is available");
    }

    fn test_owner() -> (
        tempfile::TempDir,
        LifecycleOwner,
        worker::ConfigWorkerRuntime,
    ) {
        let temp = tempfile::tempdir().expect("temporary authority root");
        let config_path = temp.path().join("tribal.yaml");
        let config = tribal_config::TribalConfig::minimum_valid(
            "postgres://user:pass@localhost:5432/tribal",
        );
        std::fs::write(
            &config_path,
            serde_yaml::to_string(&config).expect("configuration serialises"),
        )
        .expect("configuration writes");
        let authority = authority::AuthorityLease::acquire(&config_path)
            .expect("authority acquisition succeeds");
        let authority::AuthorityAcquire::Acquired(authority) = authority else {
            panic!("temporary config path has one authority");
        };
        let (config, mut worker_runtime) =
            worker::spawn(configuration::ConfigAuthority::new(config_path.clone()))
                .expect("configuration worker starts");
        let config_terminal = worker_runtime
            .take_terminal()
            .expect("worker terminal has one owner");
        let (_command_sender, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let (completion_sender, completions) = mpsc::channel(COMPLETION_CAPACITY);
        let state = placeholder_state();
        let (publisher, _snapshots) = watch::channel(state.snapshot());
        let owner = LifecycleOwner {
            receiver,
            completions,
            completion_sender,
            observations: tokio::task::JoinSet::new(),
            publisher,
            state,
            next_token: 1,
            config_path,
            config,
            authority: Arc::new(authority),
            shutdown: CancellationToken::new(),
            shutdown_seen: false,
            config_terminal,
            worker_exit: None,
        };
        (temp, owner, worker_runtime)
    }

    #[test]
    fn test_invalid_document_projects_only_unconfigured() {
        let snapshot = no_runtime_snapshot(
            header(),
            &ConfigDocument::DurableInvalid {
                revision: tribal_wire::management::ConfigRevision::from_digest(
                    &tribal_wire::management::ConfigDigest::from_bytes(b"invalid"),
                ),
            },
            None,
        );
        assert!(matches!(
            snapshot.phase,
            NoRuntimePhase::Unconfigured { .. }
        ));
    }

    #[test]
    fn test_stop_intent_classification_is_total() {
        let (stop_sender, _stop_receiver) = oneshot::channel();
        let (restart_sender, _restart_receiver) = oneshot::channel();
        let (shutdown_sender, _shutdown_receiver) = oneshot::channel();
        let intents = [
            StopIntent::Stop {
                waiters: vec![stop_sender],
            },
            StopIntent::Restart {
                start_waiters: Vec::new(),
                restart_waiters: vec![restart_sender],
            },
            StopIntent::Shutdown {
                waiters: vec![shutdown_sender],
            },
        ];
        assert_eq!(
            intents
                .iter()
                .map(stop_intent_operation)
                .collect::<Vec<_>>(),
            [
                RuntimeOperation::Stop,
                RuntimeOperation::Restart,
                RuntimeOperation::ManagerShutdown,
            ]
        );
    }

    #[test]
    fn test_restart_launch_resolves_start_and_restart_waiters_at_one_revision() {
        let (start_sender, start_receiver) = oneshot::channel();
        let (restart_sender, restart_receiver) = oneshot::channel();
        let snapshot = RunningLifecycleSnapshot {
            header: header(),
            phase: RunningPhase::Healthy {
                runtime: RuntimeIdentity {
                    instance_id: "runtime".to_owned(),
                    pid: 7,
                    binary_version: "test".to_owned(),
                    config_path: ConfigFilePath {
                        path: "/tmp/tribal.yaml".to_owned(),
                    },
                },
                restart_pending: false,
            },
        };
        resolve_launch_success(
            LaunchIntent::Restart {
                start_waiters: vec![start_sender],
                restart_waiters: vec![restart_sender],
            },
            &snapshot,
        );
        let start = start_receiver
            .blocking_recv()
            .expect("start waiter resolves");
        let restart = restart_receiver
            .blocking_recv()
            .expect("restart waiter resolves");
        assert!(matches!(
            (start, restart),
            (
                RuntimeStartResult::Started { snapshot: start },
                RuntimeRestartResult::Restarted { snapshot: restart }
            ) if start.header.revision == restart.header.revision
        ));
    }

    #[tokio::test]
    async fn test_early_child_termination_reaps_the_owned_process() {
        let child = tokio::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("sleep 30")
            .kill_on_drop(false)
            .spawn()
            .expect("test child starts");
        let pid = child.id().expect("test child has a process id");
        let mut child = EarlyChild {
            child,
            identity: RuntimeIdentity {
                instance_id: "early-child".to_owned(),
                pid,
                binary_version: "test".to_owned(),
                config_path: ConfigFilePath {
                    path: "/tmp/tribal.yaml".to_owned(),
                },
            },
            custody: None,
            control: None,
            evidence: tribal_wire::management::EarlyChildTerminationEvidence::PreCommit {
                pid,
                config_path: ConfigFilePath {
                    path: "/tmp/tribal.yaml".to_owned(),
                },
            },
        };

        terminate_early_child(&mut child).await;

        assert!(early_child_exited(&mut child), "the owned child is reaped");
    }

    #[tokio::test]
    async fn test_duplicate_custody_completion_does_not_publish() {
        let (_temp, mut owner, worker_runtime) = test_owner();
        owner.state =
            LifecycleState::Operating(LifecycleOperation::Launching(a_launching_operation(
                39,
                AttachmentStage::Handshaking(tokio::spawn(std::future::pending())),
            )));
        let snapshots = owner.publisher.subscribe();
        let runtime = match &owner.state {
            LifecycleState::Operating(LifecycleOperation::Launching(operation)) => {
                operation.child.identity.clone()
            }
            _ => panic!("test owner has a launching operation"),
        };

        owner
            .handle_custody_commit(
                39,
                Err(LaunchFailure {
                    failure: StoppedProcessFailure::RuntimeCustodyCommitFailed {
                        presentation: failure_presentation(
                            "custody commit failed",
                            "duplicate completion fixture",
                        ),
                    },
                    evidence:
                        tribal_wire::management::EarlyChildTerminationEvidence::CommitOutcomeUnknown {
                            runtime,
                        },
                }),
            )
            .await;

        assert!(matches!(
            owner.state,
            LifecycleState::Operating(LifecycleOperation::Launching(LaunchingOperation {
                attachment: AttachmentStage::Handshaking(_),
                ..
            }))
        ));
        assert!(
            !snapshots
                .has_changed()
                .expect("lifecycle publisher remains live"),
            "a duplicate custody completion emits no lifecycle change"
        );
        reap_launching_operation(&mut owner).await;
        drop(owner);
        worker_runtime.join().expect("worker thread joins");
    }

    #[tokio::test]
    async fn test_duplicate_runtime_completion_does_not_publish() {
        let (_temp, mut owner, worker_runtime) = test_owner();
        owner.state =
            LifecycleState::Operating(LifecycleOperation::Launching(a_launching_operation(
                40,
                AttachmentStage::Committing(tokio::spawn(std::future::pending())),
            )));
        let snapshots = owner.publisher.subscribe();

        owner
            .handle_runtime_connected(40, Err(RuntimeControlError::Closed))
            .await;

        assert!(matches!(
            owner.state,
            LifecycleState::Operating(LifecycleOperation::Launching(LaunchingOperation {
                attachment: AttachmentStage::Committing(_),
                ..
            }))
        ));
        assert!(
            !snapshots
                .has_changed()
                .expect("lifecycle publisher remains live"),
            "a duplicate runtime completion emits no lifecycle change"
        );
        reap_launching_operation(&mut owner).await;
        drop(owner);
        worker_runtime.join().expect("worker thread joins");
    }

    #[tokio::test]
    async fn test_ready_custody_commit_is_folded_before_terminal_publication() {
        let (_temp, mut owner, worker_runtime) = test_owner();
        let child = early_child();
        let runtime = child.identity.clone();
        let (custody, _runtime_custody) =
            ManagerCustody::pair_for_test().expect("custody pair creates");
        let (start, _start_result) = oneshot::channel();
        owner.state =
            LifecycleState::Operating(LifecycleOperation::Launching(LaunchingOperation {
                token: 41,
                snapshot: tribal_wire::management::StartingLifecycleSnapshot {
                    header: header(),
                    phase: tribal_wire::management::StartingPhase::Starting,
                },
                origin: no_runtime_origin(),
                child,
                intent: LaunchIntent::Start {
                    waiters: vec![start],
                },
                attachment: AttachmentStage::Committing(tokio::spawn(async {})),
            }));
        owner
            .completion_sender
            .try_send(LifecycleCompletion::CustodyCommitted {
                token: 41,
                result: Ok(CommittedCustody {
                    custody,
                    control_proof: RuntimeCustodyProof::new("proof".to_owned()),
                }),
            })
            .expect("ready commit is queued");

        owner.terminate_for_worker(None).await;

        assert!(matches!(
            owner.state.snapshot().phase,
            LifecyclePhase::ManagerTerminating {
                termination: ManagerTermination::ConfigWorkerPanicked {
                    runtime: ManagerTerminationRuntime::Recoverable { runtime: observed },
                    ..
                }
            } if observed == runtime
        ));
        let LifecycleState::TerminatingOperation {
            operation: LifecycleOperation::Launching(operation),
            ..
        } = &mut owner.state
        else {
            panic!("terminal state retains the launch resources");
        };
        assert!(operation.child.custody.is_some());
        drop(operation.child.custody.take());
        let _ = operation.child.child.start_kill();
        tokio::time::timeout(TEST_OBSERVATION_DEADLINE, operation.child.child.wait())
            .await
            .expect("test child is reaped")
            .expect("test child status is available");
        drop(owner);
        worker_runtime.join().expect("worker thread joins");
    }

    #[tokio::test]
    async fn test_forced_reap_commit_upgrades_custody_without_extending_deadline() {
        let (_temp, mut owner, worker_runtime) = test_owner();
        let child = early_child();
        let runtime = child.identity.clone();
        let deadline = tokio::time::Instant::now() + STOP_DEADLINE;
        let (stop, _stop_result) = oneshot::channel();
        owner.state = LifecycleState::Operating(LifecycleOperation::CancellingLaunch(
            CancellingLaunchOperation {
                token: 42,
                header: header(),
                child,
                origin: no_runtime_origin(),
                intent: CancellationIntent::Stop {
                    waiters: vec![stop],
                },
                attachment: AttachmentStage::Committing(tokio::spawn(async {})),
                termination: EarlyTerminationStage::ForcedReap { deadline },
            },
        ));
        let (custody, _runtime_custody) =
            ManagerCustody::pair_for_test().expect("custody pair creates");

        owner
            .handle_custody_commit(
                42,
                Ok(CommittedCustody {
                    custody,
                    control_proof: RuntimeCustodyProof::new("proof".to_owned()),
                }),
            )
            .await;

        let LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation)) =
            &mut owner.state
        else {
            panic!("forced cancellation remains in flight");
        };
        assert!(matches!(
            operation.termination,
            EarlyTerminationStage::ForcedReap { deadline: observed } if observed == deadline
        ));
        assert!(matches!(
            operation.child.evidence,
            tribal_wire::management::EarlyChildTerminationEvidence::Recoverable {
                runtime: ref observed,
            } if observed == &runtime
        ));
        assert!(operation.child.custody.is_some());
        drop(operation.child.custody.take());
        let _ = operation.child.child.start_kill();
        tokio::time::timeout(TEST_OBSERVATION_DEADLINE, operation.child.child.wait())
            .await
            .expect("test child is reaped")
            .expect("test child status is available");
        drop(owner);
        worker_runtime.join().expect("worker thread joins");
    }

    #[tokio::test]
    async fn test_panicked_attachment_forces_then_reaps_early_child() {
        let (_temp, mut owner, worker_runtime) = test_owner();
        let task = tokio::spawn(async { panic!("attachment task panic") });
        let (stop, _stop_result) = oneshot::channel();
        let child = early_child();
        let runtime = child.identity.clone();
        owner.state = LifecycleState::Operating(LifecycleOperation::CancellingLaunch(
            CancellingLaunchOperation {
                token: 43,
                header: header(),
                child,
                origin: no_runtime_origin(),
                intent: CancellationIntent::Stop {
                    waiters: vec![stop],
                },
                attachment: AttachmentStage::Committing(task),
                termination: EarlyTerminationStage::Graceful {
                    task: GracefulStopTask::AwaitingCapability,
                    deadline: tokio::time::Instant::now() + STOP_DEADLINE,
                },
            },
        ));
        tokio::time::timeout(TEST_OBSERVATION_DEADLINE, async {
            while let LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation)) =
                &owner.state
            {
                if attachment_task_finished(&operation.attachment) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("attachment panic becomes observable");

        owner.observe_early_cancellation().await;

        assert!(matches!(
            owner.state.snapshot().phase,
            LifecyclePhase::CancellingEarlyChild {
                evidence:
                    tribal_wire::management::EarlyChildTerminationEvidence::CommitOutcomeUnknown {
                        runtime: observed,
                    },
                ..
            } if observed == runtime
        ));
        assert!(matches!(
            owner.state,
            LifecycleState::Operating(LifecycleOperation::CancellingLaunch(
                CancellingLaunchOperation {
                    termination: EarlyTerminationStage::ForcedReap { .. },
                    ..
                }
            ))
        ));
        tokio::time::timeout(TEST_OBSERVATION_DEADLINE, async {
            loop {
                owner.observe_early_cancellation().await;
                if matches!(owner.state, LifecycleState::NoRuntime(_)) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("forced child is reaped without waiting for the deadline");
        drop(owner);
        worker_runtime.join().expect("worker thread joins");
    }

    #[tokio::test]
    async fn test_commit_unknown_survives_forced_reap_timeout() {
        let (_temp, mut owner, worker_runtime) = test_owner();
        let (stop, _stop_result) = oneshot::channel();
        let mut child = early_child();
        let runtime = child.identity.clone();
        child.mark_commit_outcome_unknown();
        owner.state = LifecycleState::Operating(LifecycleOperation::CancellingLaunch(
            CancellingLaunchOperation {
                token: 44,
                header: header(),
                child,
                origin: no_runtime_origin(),
                intent: CancellationIntent::Stop {
                    waiters: vec![stop],
                },
                attachment: AttachmentStage::Settled,
                termination: EarlyTerminationStage::ForcedReap {
                    deadline: tokio::time::Instant::now(),
                },
            },
        ));

        owner.observe_early_cancellation().await;

        assert!(matches!(
            owner.state.snapshot().phase,
            LifecyclePhase::ManagerTerminating {
                termination: ManagerTermination::ChildReapTimedOut {
                    evidence:
                        tribal_wire::management::EarlyChildTerminationEvidence::CommitOutcomeUnknown {
                            runtime: observed,
                        },
                    ..
                }
            } if observed == runtime
        ));
        let LifecycleState::TerminatingOperation {
            operation: LifecycleOperation::CancellingLaunch(operation),
            ..
        } = &mut owner.state
        else {
            panic!("timed-out cancellation retains its resources");
        };
        operation
            .child
            .child
            .start_kill()
            .expect("test child accepts forced termination");
        tokio::time::timeout(TEST_OBSERVATION_DEADLINE, operation.child.child.wait())
            .await
            .expect("test child is reaped")
            .expect("test child status is available");
        drop(owner);
        worker_runtime.join().expect("worker thread joins");
    }

    #[tokio::test]
    async fn test_config_worker_panic_publishes_the_terminal_snapshot_before_owner_exit() {
        let temp = tempfile::tempdir().expect("temporary authority root");
        let config_path = temp.path().join("tribal.yaml");
        let config = tribal_config::TribalConfig::minimum_valid(
            "postgres://user:pass@localhost:5432/tribal",
        );
        std::fs::write(
            &config_path,
            serde_yaml::to_string(&config).expect("configuration serialises"),
        )
        .expect("configuration writes");
        let authority = authority::AuthorityLease::acquire(&config_path)
            .expect("authority acquisition succeeds");
        let authority::AuthorityAcquire::Acquired(authority) = authority else {
            panic!("temporary config path has one authority");
        };
        let (config, mut worker_runtime) =
            worker::spawn(configuration::ConfigAuthority::new(config_path.clone()))
                .expect("configuration worker starts");
        let terminal = worker_runtime
            .take_terminal()
            .expect("worker terminal has one owner");
        let shutdown = CancellationToken::new();
        let (lifecycle, lifecycle_task) = LifecycleController::spawn(
            "manager".to_owned(),
            config_path,
            config.clone(),
            Arc::new(authority),
            shutdown,
            terminal,
            None,
        )
        .await
        .expect("lifecycle owner starts");

        assert!(
            config.panic_for_test().await,
            "the panic command is admitted"
        );
        let exit = tokio::time::timeout(TEST_OBSERVATION_DEADLINE, lifecycle_task)
            .await
            .expect("lifecycle owner observes the terminal channel")
            .expect("lifecycle task joins");
        let snapshot = lifecycle.snapshots.borrow().clone();

        assert!(matches!(
            exit,
            LifecycleExit::ConfigWorkerTerminated(ConfigWorkerExit::Panicked {
                correlation: Some(_)
            })
        ));
        assert!(matches!(
            snapshot.phase,
            LifecyclePhase::ManagerTerminating {
                termination: ManagerTermination::ConfigWorkerPanicked {
                    runtime: ManagerTerminationRuntime::Absent,
                    ..
                }
            }
        ));
        drop(config);
        drop(lifecycle);
        worker_runtime.join().expect("worker thread joins");
    }
}
