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
    LifecycleSnapshotHeader, ManagerShutdownOperation, ManagerShutdownResult,
    ManagerTerminatingLifecycleSnapshot, ManagerTerminatingPhase, ManagerTermination,
    ManagerTerminationRuntime, NoRuntimeLifecycleSnapshot, NoRuntimePhase, ReadinessReport,
    RestartOperationInProgress, RestartRuntimeOperation,
    RestartRuntimeUnresponsiveLifecycleSnapshot, RestartRuntimeUnresponsivePhase,
    RunningLifecycleSnapshot, RunningPhase, RuntimeExitFailure, RuntimeIdentity, RuntimeOperation,
    RuntimeRestartResult, RuntimeStartResult, RuntimeStopResult, RuntimeStopTimedOutFailure,
    RuntimeUnresponsiveLifecycleSnapshot, RuntimeUnresponsivePhase,
    ShutdownInProgressLifecycleSnapshot, ShutdownInProgressPhase,
    ShutdownRuntimeUnresponsiveLifecycleSnapshot, ShutdownRuntimeUnresponsivePhase,
    StartBlockedReadinessReport, StartBlockedVerdict, StartClearReadinessReport, StartClearVerdict,
    StartOperationInProgress, StartSuperseder, StartVerdict, StopRuntimeOperation,
    StopRuntimeUnresponsiveLifecycleSnapshot, StopRuntimeUnresponsivePhase, StoppedProcessFailure,
    StoppedState, StoppingLifecycleSnapshot, StoppingPhase,
};
use tribal_wire::runtime_control::RuntimeCustodyProof;

use crate::commands::serve::MANAGED_AUTHORITY_FD;

use super::{
    authority::{AuthorityLease, AuthorityPaths},
    custody::{
        MANAGED_CUSTODY_PROOF, MANAGED_MANAGER_INSTANCE_ID, MANAGED_RUNTIME_INSTANCE_ID,
        ManagerCustody, generate_proof,
    },
    readiness,
    worker::ConfigWorkerClient,
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
    Refresh,
    ConfigChanged,
    Readiness(ReadinessReport),
    ConfigWorkerFatal(Option<tribal_wire::management::PanicCorrelationId>),
}

enum ManagedProcess {
    Owned(Child),
    Recovered,
}

struct ManagedChild {
    process: ManagedProcess,
    identity: RuntimeIdentity,
    custody: ManagerCustody,
}

struct PendingChild {
    child: Child,
    identity: RuntimeIdentity,
    paths: AuthorityPaths,
    manager_instance_id: String,
    proof: RuntimeCustodyProof,
}

/// Runtime recovered through an authenticated lifetime-custody handoff.
pub(crate) struct RecoveredRuntime {
    pub(crate) identity: RuntimeIdentity,
    pub(crate) custody: ManagerCustody,
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
    evidence: tribal_wire::management::EarlyChildTerminationEvidence,
    intent: LaunchIntent,
    task: JoinHandle<()>,
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
    evidence: tribal_wire::management::EarlyChildTerminationEvidence,
    origin: NoRuntimeLifecycleSnapshot,
    intent: CancellationIntent,
    task: JoinHandle<()>,
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
    task: JoinHandle<()>,
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
    Launched {
        token: u64,
        result: Result<ManagedChild, StoppedProcessFailure>,
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
    Stopped {
        document: Option<ConfigDocument>,
    },
    Unresponsive {
        child: Box<ManagedChild>,
        failure: RuntimeStopTimedOutFailure,
    },
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
}

/// Failure creating the lifecycle owner.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LifecycleStartError {
    #[error("configuration worker is unavailable during lifecycle initialisation")]
    ConfigUnavailable,
}

impl LifecycleState {
    fn snapshot(&self) -> LifecycleSnapshot {
        match self {
            Self::NoRuntime(snapshot) => snapshot.clone().into(),
            Self::Running { snapshot, .. } => snapshot.clone().into(),
            Self::Operating(operation) => operation.snapshot(),
            Self::Unresponsive { snapshot, .. } => snapshot.clone().into(),
            Self::TerminatingOperation { snapshot, .. } | Self::Terminating(snapshot) => {
                snapshot.clone().into()
            }
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
        recovered: Option<RecoveredRuntime>,
    ) -> Result<(Self, JoinHandle<()>), LifecycleStartError> {
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
            Some(recovered) => LifecycleState::Running {
                snapshot: RunningLifecycleSnapshot {
                    header,
                    phase: RunningPhase::Healthy {
                        runtime: recovered.identity.clone(),
                        restart_pending: false,
                    },
                },
                child: ManagedChild {
                    process: ManagedProcess::Recovered,
                    identity: recovered.identity,
                    custody: recovered.custody,
                },
            },
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

    pub(crate) async fn refresh(&self) {
        let _ = self.sender.send(LifecycleCommand::Refresh).await;
    }

    pub(crate) async fn config_changed(&self) {
        let _ = self.sender.send(LifecycleCommand::ConfigChanged).await;
    }

    pub(crate) async fn update_readiness(&self, report: ReadinessReport) {
        let _ = self.sender.send(LifecycleCommand::Readiness(report)).await;
    }

    pub(crate) async fn config_worker_fatal(
        &self,
        correlation: Option<tribal_wire::management::PanicCorrelationId>,
    ) {
        let _ = self
            .sender
            .send(LifecycleCommand::ConfigWorkerFatal(correlation))
            .await;
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
    async fn run(mut self) {
        let mut process_poll = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                biased;
                completion = self.completions.recv() => {
                    if let Some(completion) = completion {
                        self.handle_completion(completion).await;
                    }
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
                _ = process_poll.tick(), if matches!(self.state, LifecycleState::Running { .. } | LifecycleState::Unresponsive { .. }) => {
                    self.observe_exit();
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
                    LifecycleState::NoRuntime(_) | LifecycleState::Terminating(_)
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
            LifecycleCommand::Refresh => self.request_document_refresh(),
            LifecycleCommand::ConfigChanged => self.apply_config_change(),
            LifecycleCommand::Readiness(report) => self.apply_readiness(report),
            LifecycleCommand::ConfigWorkerFatal(correlation) => {
                self.terminate_for_worker(correlation);
            }
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
            | LifecycleState::TerminatingOperation { snapshot, .. } => {
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
                        evidence: operation.evidence,
                        origin: operation.origin,
                        intent: CancellationIntent::Stop {
                            waiters: vec![response],
                        },
                        task: operation.task,
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
                        evidence: operation.evidence,
                        origin: operation.origin,
                        intent: CancellationIntent::Shutdown {
                            waiters: vec![response],
                        },
                        task: operation.task,
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
            Ok(pending) => {
                let evidence = tribal_wire::management::EarlyChildTerminationEvidence::PreCommit {
                    pid: pending.identity.pid,
                    config_path: pending.identity.config_path.clone(),
                };
                let sender = self.completion_sender.clone();
                let task = tokio::spawn(async move {
                    let result = finish_launch(pending).await;
                    let event = LifecycleCompletion::Launched { token, result };
                    if let Err(error) = sender.send(event).await
                        && let LifecycleCompletion::Launched {
                            result: Ok(child), ..
                        } = error.0
                    {
                        let _ = stop_managed_child(child).await;
                    }
                });
                self.state =
                    LifecycleState::Operating(LifecycleOperation::Launching(LaunchingOperation {
                        token,
                        snapshot,
                        origin,
                        evidence,
                        intent,
                        task,
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
        let sender = self.completion_sender.clone();
        let config = self.config.clone();
        let task = tokio::spawn(async move {
            let result = match stop_managed_child(child).await {
                Ok(()) => StopCompletion::Stopped {
                    document: config.document().await.ok(),
                },
                Err((child, failure)) => StopCompletion::Unresponsive {
                    child: Box::new(child),
                    failure,
                },
            };
            let _ = sender
                .send(LifecycleCompletion::Stopped { token, result })
                .await;
        });
        LifecycleState::Operating(LifecycleOperation::Stopping(StoppingOperation {
            token,
            snapshot,
            intent,
            task,
        }))
    }

    async fn handle_completion(&mut self, completion: LifecycleCompletion) {
        match completion {
            LifecycleCompletion::Launched { token, result } => {
                self.handle_launch_completion(token, result).await;
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

    async fn handle_launch_completion(
        &mut self,
        token: u64,
        result: Result<ManagedChild, StoppedProcessFailure>,
    ) {
        let state = std::mem::replace(&mut self.state, placeholder_state());
        self.state = match state {
            LifecycleState::Operating(LifecycleOperation::Launching(operation))
                if operation.token == token =>
            {
                let _ = operation.task.await;
                match result {
                    Ok(child) => {
                        let snapshot = RunningLifecycleSnapshot {
                            header: next_header(&operation.snapshot.header),
                            phase: RunningPhase::Healthy {
                                runtime: child.identity.clone(),
                                restart_pending: false,
                            },
                        };
                        resolve_launch_success(operation.intent, &snapshot);
                        LifecycleState::Running { snapshot, child }
                    }
                    Err(failure) => {
                        let failed =
                            failed_no_runtime_from_origin(&operation.origin, failure.clone());
                        resolve_launch_failure(operation.intent, &failed);
                        LifecycleState::NoRuntime(with_failure(operation.origin, failure))
                    }
                }
            }
            LifecycleState::Operating(LifecycleOperation::CancellingLaunch(operation))
                if operation.token == token =>
            {
                let _ = operation.task.await;
                if let Ok(child) = result {
                    let running = RunningLifecycleSnapshot {
                        header: next_header(&operation.header),
                        phase: RunningPhase::Healthy {
                            runtime: child.identity.clone(),
                            restart_pending: false,
                        },
                    };
                    let intent = match operation.intent {
                        CancellationIntent::Stop { waiters } => StopIntent::Stop { waiters },
                        CancellationIntent::Shutdown { waiters } => {
                            StopIntent::Shutdown { waiters }
                        }
                    };
                    self.begin_stop_state(child, intent, &running)
                } else {
                    resolve_cancel_without_child(operation.intent, &operation.origin);
                    LifecycleState::NoRuntime(operation.origin)
                }
            }
            LifecycleState::TerminatingOperation {
                snapshot,
                operation: LifecycleOperation::Launching(operation),
            } if operation.token == token => {
                let _ = operation.task.await;
                drop(result);
                LifecycleState::Terminating(snapshot)
            }
            LifecycleState::TerminatingOperation {
                snapshot,
                operation: LifecycleOperation::CancellingLaunch(operation),
            } if operation.token == token => {
                let _ = operation.task.await;
                drop(result);
                LifecycleState::Terminating(snapshot)
            }
            other => {
                if let Ok(child) = result {
                    let _ = stop_managed_child(child).await;
                }
                other
            }
        };
        self.publish_current();
    }

    async fn handle_stop_completion(&mut self, token: u64, result: StopCompletion) {
        let state = std::mem::replace(&mut self.state, placeholder_state());
        self.state = match state {
            LifecycleState::Operating(LifecycleOperation::Stopping(operation))
                if operation.token == token =>
            {
                let _ = operation.task.await;
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
                    StopCompletion::Unresponsive { child, failure } => {
                        let snapshot = RuntimeUnresponsiveLifecycleSnapshot {
                            header: next_header(&operation.snapshot.header),
                            phase: RuntimeUnresponsivePhase::RuntimeUnresponsive {
                                runtime: child.identity.clone(),
                                operation: stop_intent_operation(&operation.intent),
                                failure: failure.clone(),
                            },
                        };
                        resolve_unresponsive(operation.intent, &snapshot);
                        LifecycleState::Unresponsive {
                            snapshot,
                            child: *child,
                        }
                    }
                }
            }
            LifecycleState::TerminatingOperation {
                snapshot,
                operation: LifecycleOperation::Stopping(operation),
            } if operation.token == token => {
                let _ = operation.task.await;
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

    fn observe_exit(&mut self) {
        let LifecycleState::Running { snapshot, child } = &mut self.state else {
            return;
        };
        if child.custody.is_closed() {
            let terminating = ManagerTerminatingLifecycleSnapshot {
                header: next_header(&snapshot.header),
                phase: ManagerTerminatingPhase::ManagerTerminating {
                    termination: ManagerTermination::CustodyLost {
                        presentation: failure_presentation(
                            "managed runtime custody was lost",
                            "the manager will exit so a successor can recover the runtime",
                        ),
                        runtime: CustodyLossTerminationRuntime::Recoverable {
                            runtime: child.identity.clone(),
                        },
                    },
                },
            };
            self.state = LifecycleState::Terminating(terminating);
            self.publish_current();
            self.shutdown_seen = true;
            self.shutdown.cancel();
            return;
        }
        let exit = match &mut child.process {
            ManagedProcess::Owned(process) => match process.try_wait() {
                Ok(Some(status)) => Some(status.to_string()),
                Ok(None) => None,
                Err(error) => {
                    tracing::warn!(%error, "managed runtime status unavailable");
                    None
                }
            },
            ManagedProcess::Recovered => {
                (!process_exists(child.identity.pid)).then(|| "process exited".to_owned())
            }
        };
        if let Some(status) = exit {
            let failure = StoppedProcessFailure::RuntimeExited {
                failure: RuntimeExitFailure {
                    presentation: failure_presentation(
                        "managed runtime exited unexpectedly",
                        &status,
                    ),
                },
            };
            let header = next_header(&snapshot.header);
            let document = ConfigDocument::Unreadable {
                phase: tribal_wire::management::ConfigPersistencePhase::DurabilityUncertain,
            };
            self.state =
                LifecycleState::NoRuntime(no_runtime_snapshot(header, &document, Some(failure)));
            self.publish_current();
            self.request_document_refresh();
        }
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
            | LifecycleState::Terminating(_) => {}
        }
    }

    fn terminate_for_worker(
        &mut self,
        correlation: Option<tribal_wire::management::PanicCorrelationId>,
    ) {
        let runtime = match &self.state {
            LifecycleState::Running { child, .. } | LifecycleState::Unresponsive { child, .. } => {
                ManagerTerminationRuntime::Recoverable {
                    runtime: child.identity.clone(),
                }
            }
            LifecycleState::NoRuntime(_)
            | LifecycleState::Operating(_)
            | LifecycleState::TerminatingOperation { .. }
            | LifecycleState::Terminating(_) => ManagerTerminationRuntime::Absent,
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
            LifecycleState::NoRuntime(_)
            | LifecycleState::Running { .. }
            | LifecycleState::Unresponsive { .. }
            | LifecycleState::Terminating(_) => LifecycleState::Terminating(snapshot),
        };
        self.publish_current();
        self.shutdown_seen = true;
        self.shutdown.cancel();
    }

    fn begin_external_shutdown(&mut self) -> bool {
        match &self.state {
            LifecycleState::NoRuntime(_) | LifecycleState::Terminating(_) => true,
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
) -> Result<PendingChild, StoppedProcessFailure> {
    let inherited = authority
        .inheritable_clone()
        .map_err(|error| spawn_failure("preparing delegated authority", &error.to_string()))?;
    let fd = inherited.as_raw_fd();
    let runtime_instance_id = uuid::Uuid::new_v4().to_string();
    let proof = generate_proof()
        .map_err(|error| spawn_failure("generating custody proof", &error.to_string()))?;
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
    Ok(PendingChild {
        child,
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
        proof,
    })
}

async fn finish_launch(mut pending: PendingChild) -> Result<ManagedChild, StoppedProcessFailure> {
    let paths = pending.paths.clone();
    let manager_instance_id = pending.manager_instance_id.clone();
    let proof = pending.proof;
    let custody = match tokio::task::spawn_blocking(move || {
        ManagerCustody::attach_initial(&paths, &manager_instance_id, proof)
    })
    .await
    {
        Ok(Ok(custody)) => custody,
        Ok(Err(error)) => {
            let _ = pending.child.kill().await;
            return Err(StoppedProcessFailure::RuntimeCustodyCommitFailed {
                presentation: failure_presentation(
                    "managed runtime custody could not be committed",
                    &error.to_string(),
                ),
            });
        }
        Err(error) => {
            let _ = pending.child.kill().await;
            return Err(StoppedProcessFailure::RuntimeCustodyCommitFailed {
                presentation: failure_presentation(
                    "managed runtime custody task failed",
                    &error.to_string(),
                ),
            });
        }
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    match pending.child.try_wait() {
        Ok(Some(status)) => Err(StoppedProcessFailure::RuntimeAnnouncementFailed {
            presentation: failure_presentation(
                "managed runtime exited during launch",
                &status.to_string(),
            ),
        }),
        Ok(None) => Ok(ManagedChild {
            process: ManagedProcess::Owned(pending.child),
            identity: pending.identity,
            custody,
        }),
        Err(error) => Err(StoppedProcessFailure::RuntimeAnnouncementFailed {
            presentation: failure_presentation(
                "managed runtime launch status was unavailable",
                &error.to_string(),
            ),
        }),
    }
}

async fn stop_managed_child(
    mut managed: ManagedChild,
) -> Result<(), (ManagedChild, RuntimeStopTimedOutFailure)> {
    if let Err(error) = managed.custody.stop(&managed.identity) {
        tracing::warn!(%error, pid = managed.identity.pid, "managed runtime stop request failed");
    }
    let stopped = match &mut managed.process {
        ManagedProcess::Owned(child) => {
            match tokio::time::timeout(STOP_DEADLINE, child.wait()).await {
                Ok(Ok(_)) => true,
                Ok(Err(error)) => {
                    tracing::warn!(%error, pid = managed.identity.pid, "managed runtime wait failed");
                    false
                }
                Err(_) => matches!(
                    tokio::time::timeout(STOP_DEADLINE, child.kill()).await,
                    Ok(Ok(()))
                ),
            }
        }
        ManagedProcess::Recovered => {
            let deadline = tokio::time::Instant::now() + STOP_DEADLINE;
            while process_exists(managed.identity.pid) && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            !process_exists(managed.identity.pid)
        }
    };
    if stopped {
        Ok(())
    } else {
        let pid = managed.identity.pid;
        Err((
            managed,
            RuntimeStopTimedOutFailure {
                presentation: failure_presentation(
                    "managed runtime did not stop before the deadline",
                    &format!("runtime pid {pid} remains active"),
                ),
            },
        ))
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
            evidence: operation.evidence.clone(),
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
            evidence: operation.evidence.clone(),
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

fn process_exists(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    // SAFETY: signal zero does not signal the process; it asks the kernel
    // whether the exact PID remains observable to this user.
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || io_permission_denied()
}

fn io_permission_denied() -> bool {
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> LifecycleSnapshotHeader {
        LifecycleSnapshotHeader {
            manager_instance_id: "manager".to_owned(),
            revision: 1,
            manager_version: "test".to_owned(),
        }
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
}
