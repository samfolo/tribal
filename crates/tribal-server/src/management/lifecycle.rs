//! One-owner lifecycle reducer for managed runtime processes.

use std::{os::fd::AsRawFd as _, path::PathBuf, sync::Arc, time::Duration};

use tokio::{
    process::Child,
    sync::{mpsc, oneshot, watch},
};
use tokio_util::sync::CancellationToken;
use tribal_wire::management::{
    CheckName, CheckResult, CheckSubject, CleanNoRuntimeLifecycleSnapshot, CleanNoRuntimePhase,
    CleanStoppedState, CleanUnconfiguredLifecycleSnapshot, CleanUnconfiguredPhase,
    ConfigDiagnosticLocation, ConfigDocument, ConfigFilePath, CustodyLossTerminationRuntime,
    DegradedReason, FailedNoRuntimeLifecycleSnapshot, FailedNoRuntimePhase,
    HealthDegradedReadinessReport, HealthVerdict, LifecyclePhase, LifecycleSnapshot,
    LifecycleSnapshotHeader, ManagerShutdownOperation, ManagerShutdownResult, ManagerTermination,
    NoRuntimeLifecycleSnapshot, NoRuntimePhase, ReadinessReport, RestartRuntimeOperation,
    RestartRuntimeUnresponsiveLifecycleSnapshot, RestartRuntimeUnresponsivePhase,
    RunningLifecycleSnapshot, RunningPhase, RuntimeExitFailure, RuntimeIdentity, RuntimeOperation,
    RuntimeRestartResult, RuntimeStartResult, RuntimeStopResult, RuntimeStopTimedOutFailure,
    ShutdownRuntimeUnresponsiveLifecycleSnapshot, ShutdownRuntimeUnresponsivePhase,
    StartBlockedReadinessReport, StartBlockedVerdict, StartClearReadinessReport, StartClearVerdict,
    StartVerdict, StopRuntimeOperation, StopRuntimeUnresponsiveLifecycleSnapshot,
    StopRuntimeUnresponsivePhase, StoppedProcessFailure, StoppedState,
};

use crate::commands::serve::MANAGED_AUTHORITY_FD;

use super::{
    authority::AuthorityLease,
    custody::{
        MANAGED_CUSTODY_PROOF, MANAGED_MANAGER_INSTANCE_ID, MANAGED_RUNTIME_INSTANCE_ID,
        ManagerCustody, generate_proof,
    },
    readiness,
    worker::ConfigWorkerClient,
};

const COMMAND_CAPACITY: usize = 16;
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

/// Runtime recovered through an authenticated lifetime-custody handoff.
pub(crate) struct RecoveredRuntime {
    pub(crate) identity: RuntimeIdentity,
    pub(crate) custody: ManagerCustody,
}

struct LifecycleOwner {
    receiver: mpsc::Receiver<LifecycleCommand>,
    publisher: watch::Sender<LifecycleSnapshot>,
    snapshot: LifecycleSnapshot,
    child: Option<ManagedChild>,
    config_path: PathBuf,
    config: ConfigWorkerClient,
    authority: Arc<AuthorityLease>,
    shutdown: CancellationToken,
}

/// Failure creating the lifecycle owner.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LifecycleStartError {
    #[error("configuration worker is unavailable during lifecycle initialisation")]
    ConfigUnavailable,
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
    ) -> Result<Self, LifecycleStartError> {
        let document = config
            .document()
            .await
            .map_err(|_| LifecycleStartError::ConfigUnavailable)?;
        let header = LifecycleSnapshotHeader {
            manager_instance_id,
            revision: 1,
            manager_version: env!("CARGO_PKG_VERSION").to_owned(),
        };
        let snapshot = match recovered.as_ref() {
            Some(recovered) => LifecycleSnapshot {
                header,
                phase: LifecyclePhase::Healthy {
                    runtime: recovered.identity.clone(),
                    restart_pending: false,
                },
            },
            None => no_runtime_snapshot(header, document, None).0,
        };
        let (publisher, snapshots) = watch::channel(snapshot.clone());
        let (sender, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let owner = LifecycleOwner {
            receiver,
            publisher,
            snapshot,
            child: recovered.map(|recovered| ManagedChild {
                process: ManagedProcess::Recovered,
                identity: recovered.identity,
                custody: recovered.custody,
            }),
            config_path,
            config,
            authority,
            shutdown,
        };
        drop(tokio::spawn(owner.run()));
        Ok(Self { sender, snapshots })
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
                command = self.receiver.recv() => match command {
                    Some(command) => self.handle(command).await,
                    None => break,
                },
                _ = process_poll.tick(), if self.child.is_some() => self.observe_exit().await,
                () = self.shutdown.cancelled() => {
                    if !matches!(self.snapshot.phase, LifecyclePhase::ManagerTerminating { .. }) {
                        let _ = self.stop_child().await;
                    }
                    break;
                }
            }
        }
    }

    async fn handle(&mut self, command: LifecycleCommand) {
        match command {
            LifecycleCommand::Snapshot(response) => {
                let _ = response.send(self.snapshot.clone());
            }
            LifecycleCommand::Start(response) => {
                let _ = response.send(self.start_runtime().await);
            }
            LifecycleCommand::Stop(response) => {
                let _ = response.send(self.stop_runtime().await);
            }
            LifecycleCommand::Restart(response) => {
                let _ = response.send(self.restart_runtime().await);
            }
            LifecycleCommand::Shutdown(response) => {
                let result = self.shutdown_manager().await;
                let terminating = matches!(result, ManagerShutdownResult::ShuttingDown { .. });
                let _ = response.send(result);
                if terminating {
                    self.shutdown.cancel();
                }
            }
            LifecycleCommand::Refresh => self.refresh_no_runtime().await,
            LifecycleCommand::ConfigChanged => self.apply_config_change().await,
            LifecycleCommand::Readiness(report) => self.apply_readiness(report),
            LifecycleCommand::ConfigWorkerFatal(correlation) => {
                let runtime = self.child.as_ref().map_or(
                    tribal_wire::management::ManagerTerminationRuntime::Absent,
                    |managed| tribal_wire::management::ManagerTerminationRuntime::Recoverable {
                        runtime: managed.identity.clone(),
                    },
                );
                self.publish(LifecyclePhase::ManagerTerminating {
                    termination: ManagerTermination::ConfigWorkerPanicked {
                        correlation,
                        runtime,
                    },
                });
                self.shutdown.cancel();
            }
        }
    }

    async fn start_runtime(&mut self) -> RuntimeStartResult {
        match self.snapshot.phase.clone() {
            LifecyclePhase::Unconfigured {
                readiness,
                focus,
                failure,
            } => {
                return RuntimeStartResult::Blocked {
                    snapshot: tribal_wire::management::UnconfiguredLifecycleSnapshot {
                        header: self.snapshot.header.clone(),
                        phase: tribal_wire::management::UnconfiguredPhase::Unconfigured {
                            readiness,
                            focus,
                            failure,
                        },
                    },
                };
            }
            LifecyclePhase::Healthy {
                runtime,
                restart_pending,
            } => {
                return RuntimeStartResult::AlreadyRunning {
                    snapshot: RunningLifecycleSnapshot {
                        header: self.snapshot.header.clone(),
                        phase: RunningPhase::Healthy {
                            runtime,
                            restart_pending,
                        },
                    },
                };
            }
            LifecyclePhase::Degraded {
                runtime,
                reason,
                restart_pending,
            } => {
                return RuntimeStartResult::AlreadyRunning {
                    snapshot: RunningLifecycleSnapshot {
                        header: self.snapshot.header.clone(),
                        phase: RunningPhase::Degraded {
                            runtime,
                            reason,
                            restart_pending,
                        },
                    },
                };
            }
            LifecyclePhase::VersionMismatch {
                runtime,
                manager_version,
                runtime_version,
            } => {
                return RuntimeStartResult::AlreadyRunning {
                    snapshot: RunningLifecycleSnapshot {
                        header: self.snapshot.header.clone(),
                        phase: RunningPhase::VersionMismatch {
                            runtime,
                            manager_version,
                            runtime_version,
                        },
                    },
                };
            }
            LifecyclePhase::RuntimeUnresponsive {
                runtime,
                operation,
                failure,
            } => {
                return RuntimeStartResult::RuntimeUnresponsive {
                    snapshot: tribal_wire::management::RuntimeUnresponsiveLifecycleSnapshot {
                        header: self.snapshot.header.clone(),
                        phase:
                            tribal_wire::management::RuntimeUnresponsivePhase::RuntimeUnresponsive {
                                runtime,
                                operation,
                                failure,
                            },
                    },
                };
            }
            LifecyclePhase::ManagerTerminating { termination } => {
                return RuntimeStartResult::ManagerTerminating {
                    snapshot: tribal_wire::management::ManagerTerminatingLifecycleSnapshot {
                        header: self.snapshot.header.clone(),
                        phase:
                            tribal_wire::management::ManagerTerminatingPhase::ManagerTerminating {
                                termination,
                            },
                    },
                };
            }
            _ => {}
        }
        self.publish(LifecyclePhase::Starting);
        match self.spawn_child().await {
            Ok(managed) => {
                let runtime = managed.identity.clone();
                self.child = Some(managed);
                self.publish(LifecyclePhase::Healthy {
                    runtime: runtime.clone(),
                    restart_pending: false,
                });
                RuntimeStartResult::Started {
                    snapshot: RunningLifecycleSnapshot {
                        header: self.snapshot.header.clone(),
                        phase: RunningPhase::Healthy {
                            runtime,
                            restart_pending: false,
                        },
                    },
                }
            }
            Err(error) => {
                let failure = StoppedProcessFailure::SpawnFailed {
                    presentation: failure_presentation("managed runtime could not start", &error),
                };
                let document = self.config.document().await.ok();
                self.publish_no_runtime(document, Some(failure.clone()));
                RuntimeStartResult::Failed {
                    snapshot: failed_no_runtime(&self.snapshot, failure),
                }
            }
        }
    }

    async fn stop_runtime(&mut self) -> RuntimeStopResult {
        let Some(runtime) = self.child.as_ref().map(|child| child.identity.clone()) else {
            return RuntimeStopResult::AlreadyStopped {
                snapshot: no_runtime(&self.snapshot),
            };
        };
        self.publish(LifecyclePhase::Stopping {
            runtime: runtime.clone(),
        });
        if let Err(failure) = self.stop_child().await {
            self.publish(LifecyclePhase::RuntimeUnresponsive {
                runtime: runtime.clone(),
                operation: RuntimeOperation::Stop,
                failure: failure.clone(),
            });
            return RuntimeStopResult::RuntimeUnresponsive {
                snapshot: StopRuntimeUnresponsiveLifecycleSnapshot {
                    header: self.snapshot.header.clone(),
                    phase: StopRuntimeUnresponsivePhase::RuntimeUnresponsive {
                        runtime,
                        operation: StopRuntimeOperation::Stop,
                        failure,
                    },
                },
            };
        }
        let document = self.config.document().await.ok();
        self.publish_no_runtime(document, None);
        RuntimeStopResult::Stopped {
            snapshot: clean_no_runtime(&self.snapshot),
        }
    }

    async fn restart_runtime(&mut self) -> RuntimeRestartResult {
        if self.child.is_none() {
            return match self.snapshot.phase {
                LifecyclePhase::Unconfigured { .. } => RuntimeRestartResult::Blocked {
                    snapshot: clean_unconfigured(&self.snapshot),
                },
                _ => RuntimeRestartResult::NotRunning {
                    snapshot: no_runtime(&self.snapshot),
                },
            };
        }
        let Some(runtime) = self.child.as_ref().map(|managed| managed.identity.clone()) else {
            return RuntimeRestartResult::NotRunning {
                snapshot: no_runtime(&self.snapshot),
            };
        };
        self.publish(LifecyclePhase::Stopping {
            runtime: runtime.clone(),
        });
        if let Err(failure) = self.stop_child().await {
            self.publish(LifecyclePhase::RuntimeUnresponsive {
                runtime: runtime.clone(),
                operation: RuntimeOperation::Restart,
                failure: failure.clone(),
            });
            return RuntimeRestartResult::RuntimeUnresponsive {
                snapshot: RestartRuntimeUnresponsiveLifecycleSnapshot {
                    header: self.snapshot.header.clone(),
                    phase: RestartRuntimeUnresponsivePhase::RuntimeUnresponsive {
                        runtime,
                        operation: RestartRuntimeOperation::Restart,
                        failure,
                    },
                },
            };
        }
        let document = self.config.document().await.ok();
        self.publish_no_runtime(document, None);
        match self.start_runtime().await {
            RuntimeStartResult::Started { snapshot } => {
                RuntimeRestartResult::Restarted { snapshot }
            }
            RuntimeStartResult::Failed { snapshot } => RuntimeRestartResult::Failed { snapshot },
            RuntimeStartResult::Blocked { snapshot } => RuntimeRestartResult::Blocked {
                snapshot: CleanUnconfiguredLifecycleSnapshot {
                    header: snapshot.header,
                    phase: match snapshot.phase {
                        tribal_wire::management::UnconfiguredPhase::Unconfigured {
                            readiness,
                            focus,
                            ..
                        } => CleanUnconfiguredPhase::Unconfigured {
                            readiness,
                            focus,
                            failure: None,
                        },
                    },
                },
            },
            _ => RuntimeRestartResult::NotRunning {
                snapshot: no_runtime(&self.snapshot),
            },
        }
    }

    async fn shutdown_manager(&mut self) -> ManagerShutdownResult {
        if let Some(runtime) = self.child.as_ref().map(|managed| managed.identity.clone()) {
            self.publish(LifecyclePhase::Stopping {
                runtime: runtime.clone(),
            });
            if let Err(failure) = self.stop_child().await {
                self.publish(LifecyclePhase::RuntimeUnresponsive {
                    runtime: runtime.clone(),
                    operation: RuntimeOperation::ManagerShutdown,
                    failure: failure.clone(),
                });
                return ManagerShutdownResult::RuntimeUnresponsive {
                    snapshot: ShutdownRuntimeUnresponsiveLifecycleSnapshot {
                        header: self.snapshot.header.clone(),
                        phase: ShutdownRuntimeUnresponsivePhase::RuntimeUnresponsive {
                            runtime,
                            operation: ManagerShutdownOperation::ManagerShutdown,
                            failure,
                        },
                    },
                };
            }
        }
        let document = self.config.document().await.ok();
        self.publish_no_runtime(document, None);
        ManagerShutdownResult::ShuttingDown {
            snapshot: no_runtime(&self.snapshot),
        }
    }

    async fn spawn_child(&self) -> Result<ManagedChild, String> {
        let inherited = self
            .authority
            .inheritable_clone()
            .map_err(|error| error.to_string())?;
        let fd = inherited.as_raw_fd();
        let runtime_instance_id = uuid::Uuid::new_v4().to_string();
        let proof = generate_proof().map_err(|error| error.to_string())?;
        let mut command = tokio::process::Command::new(
            std::env::current_exe().map_err(|error| error.to_string())?,
        );
        command
            .arg("serve")
            .arg("--config")
            .arg(&self.config_path)
            .env(MANAGED_AUTHORITY_FD, fd.to_string())
            .env(MANAGED_RUNTIME_INSTANCE_ID, &runtime_instance_id)
            .env(
                MANAGED_MANAGER_INSTANCE_ID,
                &self.snapshot.header.manager_instance_id,
            )
            .env(MANAGED_CUSTODY_PROOF, proof.expose_secret())
            .kill_on_drop(false);
        let child = command.spawn().map_err(|error| error.to_string())?;
        let pid = child
            .id()
            .ok_or_else(|| "managed runtime has no process id".to_owned())?;
        drop(inherited);
        let paths = self.authority.paths().clone();
        let manager_instance_id = self.snapshot.header.manager_instance_id.clone();
        let custody = tokio::task::spawn_blocking(move || {
            ManagerCustody::attach_initial(&paths, &manager_instance_id, proof)
        })
        .await
        .map_err(|error| format!("joining runtime custody attachment: {error}"))?
        .map_err(|error| error.to_string())?;
        let mut managed = ManagedChild {
            process: ManagedProcess::Owned(child),
            identity: RuntimeIdentity {
                instance_id: runtime_instance_id,
                pid,
                binary_version: env!("CARGO_PKG_VERSION").to_owned(),
                config_path: ConfigFilePath {
                    path: self.config_path.to_string_lossy().into_owned(),
                },
            },
            custody,
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        let ManagedProcess::Owned(child) = &mut managed.process else {
            return Err("newly spawned runtime lost its child handle".to_owned());
        };
        match child.try_wait() {
            Ok(Some(status)) => Err(format!("managed runtime exited during launch: {status}")),
            Ok(None) => Ok(managed),
            Err(error) => Err(format!("reading managed runtime launch status: {error}")),
        }
    }

    async fn stop_child(&mut self) -> Result<(), RuntimeStopTimedOutFailure> {
        let Some(mut managed) = self.child.take() else {
            return Ok(());
        };
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
                while process_exists(managed.identity.pid) && tokio::time::Instant::now() < deadline
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                !process_exists(managed.identity.pid)
            }
        };
        if stopped {
            Ok(())
        } else {
            let pid = managed.identity.pid;
            self.child = Some(managed);
            Err(RuntimeStopTimedOutFailure {
                presentation: failure_presentation(
                    "managed runtime did not stop before the deadline",
                    &format!("runtime pid {pid} remains active"),
                ),
            })
        }
    }

    async fn observe_exit(&mut self) {
        let Some(managed) = self.child.as_mut() else {
            return;
        };
        if managed.custody.is_closed() {
            let runtime = managed.identity.clone();
            self.publish(LifecyclePhase::ManagerTerminating {
                termination: ManagerTermination::CustodyLost {
                    presentation: failure_presentation(
                        "managed runtime custody was lost",
                        "the manager will exit so a successor can recover the runtime",
                    ),
                    runtime: CustodyLossTerminationRuntime::Recoverable { runtime },
                },
            });
            self.shutdown.cancel();
            return;
        }
        let exit = match &mut managed.process {
            ManagedProcess::Owned(child) => match child.try_wait() {
                Ok(Some(status)) => Some(status.to_string()),
                Ok(None) => None,
                Err(error) => {
                    tracing::warn!(%error, "managed runtime status unavailable");
                    return;
                }
            },
            ManagedProcess::Recovered => {
                (!process_exists(managed.identity.pid)).then(|| "process exited".to_owned())
            }
        };
        if let Some(status) = exit {
            let failure = StoppedProcessFailure::RuntimeExited {
                failure: RuntimeExitFailure {
                    presentation: failure_presentation(
                        "managed runtime exited unexpectedly",
                        &status.to_string(),
                    ),
                },
            };
            self.child = None;
            let document = self.config.document().await.ok();
            self.publish_no_runtime(document, Some(failure));
        }
    }

    async fn refresh_no_runtime(&mut self) {
        if self.child.is_none() {
            let document = self.config.document().await.ok();
            self.publish_no_runtime(document, None);
        }
    }

    async fn apply_config_change(&mut self) {
        match self.snapshot.phase.clone() {
            LifecyclePhase::Healthy { runtime, .. } => self.publish(LifecyclePhase::Healthy {
                runtime,
                restart_pending: true,
            }),
            LifecyclePhase::Degraded {
                runtime, reason, ..
            } => self.publish(LifecyclePhase::Degraded {
                runtime,
                reason,
                restart_pending: true,
            }),
            LifecyclePhase::VersionMismatch { .. }
            | LifecyclePhase::Starting
            | LifecyclePhase::Stopping { .. }
            | LifecyclePhase::CancellingEarlyChild { .. }
            | LifecyclePhase::RuntimeUnresponsive { .. }
            | LifecyclePhase::ManagerTerminating { .. } => {}
            LifecyclePhase::Unconfigured { .. } | LifecyclePhase::Stopped { .. } => {
                self.refresh_no_runtime().await;
            }
        }
    }

    fn apply_readiness(&mut self, report: ReadinessReport) {
        let failure = no_runtime_failure(&self.snapshot.phase);
        let prior_focus = match &self.snapshot.phase {
            LifecyclePhase::Unconfigured { focus, .. } => focus.clone(),
            _ => None,
        };
        match self.snapshot.phase.clone() {
            LifecyclePhase::Unconfigured { .. } | LifecyclePhase::Stopped { .. } => {
                if matches!(report.start, StartVerdict::Blocked { .. }) {
                    if let Ok(readiness) = StartBlockedReadinessReport::try_from(report) {
                        let focus = readiness_focus(&readiness.checks).or(prior_focus);
                        self.publish(LifecyclePhase::Unconfigured {
                            readiness,
                            focus,
                            failure,
                        });
                    }
                } else if let Ok(readiness) = StartClearReadinessReport::try_from(report) {
                    self.publish(LifecyclePhase::Stopped {
                        state: StoppedState::Ready { readiness, failure },
                    });
                }
            }
            LifecyclePhase::Healthy {
                runtime,
                restart_pending,
            }
            | LifecyclePhase::Degraded {
                runtime,
                restart_pending,
                ..
            } => {
                if matches!(report.health, HealthVerdict::Degraded { .. }) {
                    if let Ok(report) = HealthDegradedReadinessReport::try_from(report) {
                        self.publish(LifecyclePhase::Degraded {
                            runtime,
                            reason: DegradedReason::Readiness { report },
                            restart_pending,
                        });
                    }
                } else if matches!(report.health, HealthVerdict::Clear) {
                    self.publish(LifecyclePhase::Healthy {
                        runtime,
                        restart_pending,
                    });
                }
            }
            LifecyclePhase::Starting
            | LifecyclePhase::VersionMismatch { .. }
            | LifecyclePhase::Stopping { .. }
            | LifecyclePhase::CancellingEarlyChild { .. }
            | LifecyclePhase::RuntimeUnresponsive { .. }
            | LifecyclePhase::ManagerTerminating { .. } => {}
        }
    }

    fn publish_no_runtime(
        &mut self,
        document: Option<ConfigDocument>,
        failure: Option<StoppedProcessFailure>,
    ) {
        let header = self.next_header();
        let (snapshot, _) = no_runtime_snapshot(
            header,
            document.unwrap_or(ConfigDocument::Unreadable {
                phase: tribal_wire::management::ConfigPersistencePhase::DurabilityUncertain,
            }),
            failure,
        );
        self.snapshot = snapshot;
        self.publisher.send_replace(self.snapshot.clone());
    }

    fn publish(&mut self, phase: LifecyclePhase) {
        self.snapshot = LifecycleSnapshot {
            header: self.next_header(),
            phase,
        };
        self.publisher.send_replace(self.snapshot.clone());
    }

    fn next_header(&self) -> LifecycleSnapshotHeader {
        LifecycleSnapshotHeader {
            manager_instance_id: self.snapshot.header.manager_instance_id.clone(),
            revision: self.snapshot.header.revision.saturating_add(1),
            manager_version: self.snapshot.header.manager_version.clone(),
        }
    }
}

fn no_runtime_failure(phase: &LifecyclePhase) -> Option<StoppedProcessFailure> {
    match phase {
        LifecyclePhase::Unconfigured { failure, .. } => failure.clone(),
        LifecyclePhase::Stopped {
            state: StoppedState::Ready { failure, .. },
        } => failure.clone(),
        LifecyclePhase::Stopped {
            state:
                StoppedState::ReadinessUnavailable {
                    process_failure, ..
                },
        } => process_failure.clone(),
        _ => None,
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

fn no_runtime_snapshot(
    header: LifecycleSnapshotHeader,
    document: ConfigDocument,
    failure: Option<StoppedProcessFailure>,
) -> (LifecycleSnapshot, bool) {
    match document {
        ConfigDocument::DurableValid { .. } => (
            LifecycleSnapshot {
                header,
                phase: LifecyclePhase::Stopped {
                    state: StoppedState::Ready {
                        readiness: start_clear(),
                        failure,
                    },
                },
            },
            true,
        ),
        _ => (
            {
                let readiness = start_blocked();
                let focus = readiness_focus(&readiness.checks);
                LifecycleSnapshot {
                    header,
                    phase: LifecyclePhase::Unconfigured {
                        readiness,
                        focus,
                        failure,
                    },
                }
            },
            false,
        ),
    }
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
    match StartClearReadinessReport::try_from(report) {
        Ok(readiness) => readiness,
        Err(_) => StartClearReadinessReport {
            start: StartClearVerdict::Clear,
            health: HealthVerdict::NotApplicable,
            checks: Vec::new(),
        },
    }
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
    match StartBlockedReadinessReport::try_from(report) {
        Ok(readiness) => readiness,
        Err(_) => StartBlockedReadinessReport {
            start: StartBlockedVerdict::Blocked {
                first: CheckName::ConfigParse,
                rest: Vec::new(),
            },
            health: HealthVerdict::NotApplicable,
            checks: Vec::new(),
        },
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

fn no_runtime(snapshot: &LifecycleSnapshot) -> NoRuntimeLifecycleSnapshot {
    let phase = match snapshot.phase.clone() {
        LifecyclePhase::Unconfigured {
            readiness,
            focus,
            failure,
        } => NoRuntimePhase::Unconfigured {
            readiness,
            focus,
            failure,
        },
        LifecyclePhase::Stopped { state } => NoRuntimePhase::Stopped { state },
        _ => NoRuntimePhase::Unconfigured {
            readiness: start_blocked(),
            focus: None,
            failure: None,
        },
    };
    NoRuntimeLifecycleSnapshot {
        header: snapshot.header.clone(),
        phase,
    }
}

fn clean_no_runtime(snapshot: &LifecycleSnapshot) -> CleanNoRuntimeLifecycleSnapshot {
    let phase = match snapshot.phase.clone() {
        LifecyclePhase::Unconfigured {
            readiness, focus, ..
        } => CleanNoRuntimePhase::Unconfigured {
            readiness,
            focus,
            failure: None,
        },
        LifecyclePhase::Stopped {
            state: StoppedState::Ready { readiness, .. },
        } => CleanNoRuntimePhase::Stopped {
            state: CleanStoppedState::Ready {
                readiness,
                failure: None,
            },
        },
        LifecyclePhase::Stopped {
            state:
                StoppedState::ReadinessUnavailable {
                    last_report,
                    presentation,
                    ..
                },
        } => CleanNoRuntimePhase::Stopped {
            state: CleanStoppedState::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure: None,
            },
        },
        _ => CleanNoRuntimePhase::Unconfigured {
            readiness: start_blocked(),
            focus: None,
            failure: None,
        },
    };
    CleanNoRuntimeLifecycleSnapshot {
        header: snapshot.header.clone(),
        phase,
    }
}

fn clean_unconfigured(snapshot: &LifecycleSnapshot) -> CleanUnconfiguredLifecycleSnapshot {
    let (readiness, focus) = match snapshot.phase.clone() {
        LifecyclePhase::Unconfigured {
            readiness, focus, ..
        } => (readiness, focus),
        _ => (start_blocked(), None),
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

fn failed_no_runtime(
    snapshot: &LifecycleSnapshot,
    failure: StoppedProcessFailure,
) -> FailedNoRuntimeLifecycleSnapshot {
    let phase = match snapshot.phase.clone() {
        LifecyclePhase::Unconfigured {
            readiness, focus, ..
        } => FailedNoRuntimePhase::Unconfigured {
            readiness,
            focus,
            failure,
        },
        LifecyclePhase::Stopped {
            state: StoppedState::Ready { readiness, .. },
        } => FailedNoRuntimePhase::Stopped {
            state: tribal_wire::management::FailedStoppedState::Ready { readiness, failure },
        },
        LifecyclePhase::Stopped {
            state:
                StoppedState::ReadinessUnavailable {
                    last_report,
                    presentation,
                    ..
                },
        } => FailedNoRuntimePhase::Stopped {
            state: tribal_wire::management::FailedStoppedState::ReadinessUnavailable {
                last_report,
                presentation,
                process_failure: failure,
            },
        },
        _ => FailedNoRuntimePhase::Unconfigured {
            readiness: start_blocked(),
            focus: None,
            failure,
        },
    };
    FailedNoRuntimeLifecycleSnapshot {
        header: snapshot.header.clone(),
        phase,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_document_projects_only_unconfigured() {
        let snapshot = no_runtime_snapshot(
            LifecycleSnapshotHeader {
                manager_instance_id: "manager".to_owned(),
                revision: 1,
                manager_version: "test".to_owned(),
            },
            ConfigDocument::DurableInvalid {
                revision: tribal_wire::management::ConfigRevision::from_digest(
                    &tribal_wire::management::ConfigDigest::from_bytes(b"invalid"),
                ),
            },
            None,
        )
        .0;
        assert!(matches!(
            snapshot.phase,
            LifecyclePhase::Unconfigured { .. }
        ));
    }
}
