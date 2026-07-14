//! `tribal manager`: run or stop one per-config authority.

use std::{
    io::{self, Write},
    os::fd::AsRawFd as _,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio_util::sync::CancellationToken;
use tribal_wire::management::{
    AuthorityUnavailableReason, ConfigDocument, ConfigFilePath, ConfigRevision,
    ConflictingRuntimeIdentity, MANAGEMENT_CONTRACT_VERSION, ManagerAnnouncement,
    ManagerLaunchDisposition, ManagerLaunchFailure, ManagerLaunchRecord, ManagerShutdownCall,
    ManagerStartupFailure,
};

use crate::{
    cli::ManageArgs,
    error::AppError,
    management::{
        application::credential::{CredentialCoordinator, CredentialCoordinatorExit},
        authority::{
            AuthorityAcquire, AuthorityConflict, AuthorityDescriptor, AuthorityError,
            AuthorityLease, AuthorityOwnerKind,
        },
        client::{ManagementClient, ManagementClientError},
        configuration::ConfigAuthority,
        connector::{ManagerConnector, ManagerConnectorError},
        custody::{CustodyError, PendingRecovery},
        lifecycle::{LifecycleController, LifecycleExit, LifecycleStartError, RecoveredRuntime},
        probe::ProbeService,
        product::ProductService,
        readiness,
        socket::{self, ManagerSocketError, ManagerSocketIdentity, ManagerSocketServices},
        worker::{self, ConfigWorkerExit, ConfigWorkerStartError},
    },
};

const MAX_LAUNCH_RECORD_BYTES: usize = 64 * 1024;
const INITIAL_PUBLICATION_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);
const INITIAL_PUBLICATION_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
const REPLACEMENT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(12);
const REPLACEMENT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

/// Failure running the local management authority.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ManageError {
    #[error("configuration authority failed: {source}")]
    Authority {
        #[source]
        source: AuthorityError,
    },
    #[error("management socket failed: {source}")]
    Socket {
        #[source]
        source: ManagerSocketError,
    },
    #[error("management launch output failed: {source}")]
    Output {
        #[source]
        source: io::Error,
    },
    #[error("manager launch failed")]
    LaunchFailed,
    #[error("registering management shutdown signal: {source}")]
    Signal {
        #[source]
        source: io::Error,
    },
    #[error("configuration worker startup failed: {source}")]
    ConfigWorkerStart {
        #[source]
        source: ConfigWorkerStartError,
    },
    #[error("configuration worker terminated")]
    ConfigWorkerTerminated,
    #[error("configuration worker terminal channel is unavailable")]
    ConfigWorkerTerminalUnavailable,
    #[error("configuration worker thread panicked while joining")]
    ConfigWorkerJoin,
    #[error("lifecycle owner startup failed: {source}")]
    LifecycleStart {
        #[source]
        source: LifecycleStartError,
    },
    #[error("joining lifecycle owner: {source}")]
    LifecycleJoin {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("managed runtime recovery failed: {source}")]
    Custody {
        #[source]
        source: CustodyError,
    },
    #[error("joining managed runtime recovery: {source}")]
    RecoveryTask {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("management bootstrap client failed: {source}")]
    Client {
        #[source]
        source: ManagementClientError,
    },
    #[error("management connector failed: {source}")]
    Connector {
        #[source]
        source: ManagerConnectorError,
    },
    #[error("credential coordinator terminated unexpectedly")]
    CredentialCoordinatorTerminated,
    #[error("joining credential coordinator: {source}")]
    CredentialCoordinatorJoin {
        #[source]
        source: tokio::task::JoinError,
    },
}

/// Acquires or attaches to the authority and blocks while a winning manager serves.
pub(crate) async fn run(config_path: &str, args: &ManageArgs) -> Result<(), AppError> {
    let stdout = io::stdout();
    let stdout_fd = stdout.as_raw_fd();
    let mut writer = stdout.lock();
    let outcome = run_async(
        Path::new(config_path),
        args.announce_json,
        &mut writer,
        args.announce_json.then_some(stdout_fd),
        CancellationToken::new(),
    )
    .await;
    drop(writer);
    outcome.map(|_| ()).map_err(AppError::from)
}

/// Shuts down the compatible manager for one configuration path.
pub(crate) async fn shutdown(config_path: &str) -> Result<(), AppError> {
    let mut connection = ManagerConnector::new(config_path)
        .map_err(|source| ManageError::Connector { source })?
        .connect()
        .await
        .map_err(|source| ManageError::Connector { source })?;
    let result = connection
        .client_mut()
        .call::<ManagerShutdownCall>(&())
        .await
        .map_err(|source| ManageError::Client { source })?;
    let stdout = io::stdout();
    let mut writer = stdout.lock();
    serde_json::to_writer(&mut writer, &result).map_err(|source| ManageError::Output {
        source: io::Error::new(io::ErrorKind::InvalidData, source),
    })?;
    writer
        .write_all(b"\n")
        .map_err(|source| ManageError::Output { source })?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManageOutcome {
    Serving,
    Attached,
}

struct PreparedAuthority {
    lease: AuthorityLease,
    listener: tokio::net::UnixListener,
    recovered: Option<RecoveredRuntime>,
}

#[allow(
    clippy::too_many_lines,
    reason = "manager startup and ordered owner shutdown form one auditable sequence"
)]
async fn run_async(
    config_path: &Path,
    announce_json: bool,
    writer: &mut impl Write,
    machine_stdout_fd: Option<std::os::fd::RawFd>,
    shutdown: CancellationToken,
) -> Result<ManageOutcome, ManageError> {
    let instance_id = uuid::Uuid::new_v4().to_string();
    let Some(prepared) =
        prepare_authority(config_path, &instance_id, writer, announce_json).await?
    else {
        return Ok(ManageOutcome::Attached);
    };
    let PreparedAuthority {
        mut lease,
        listener,
        recovered,
    } = prepared;

    let announcement = ManagerAnnouncement {
        instance_id: instance_id.clone(),
        socket_path: lease.paths().socket_path.to_string_lossy().into_owned(),
        protocol_version: MANAGEMENT_CONTRACT_VERSION,
        binary_version: env!("CARGO_PKG_VERSION").to_owned(),
        config_path: ConfigFilePath {
            path: lease
                .paths()
                .canonical_config_path
                .to_string_lossy()
                .into_owned(),
        },
        pid: std::process::id(),
    };
    let descriptor = AuthorityDescriptor {
        kind: AuthorityOwnerKind::Manager,
        instance_id: instance_id.clone(),
        pid: announcement.pid,
        binary_version: announcement.binary_version.clone(),
        canonical_config_path: lease.paths().canonical_config_path.clone(),
        socket_path: Some(lease.paths().socket_path.clone()),
        protocol_version: Some(MANAGEMENT_CONTRACT_VERSION),
    };
    if let Err(source) = lease.publish(&descriptor) {
        let record = startup_failure_record(
            &lease.paths().canonical_config_path,
            ManagerStartupFailure::AuthorityDescriptorPersistenceFailed,
        );
        emit_if_requested(writer, announce_json, &record)?;
        return Err(ManageError::Authority { source });
    }
    let identity = ManagerSocketIdentity {
        instance_id: instance_id.clone(),
        binary_version: env!("CARGO_PKG_VERSION").to_owned(),
    };
    let lease = Arc::new(lease);
    let (config, mut worker_runtime) = worker::spawn(ConfigAuthority::new(
        lease.paths().canonical_config_path.clone(),
    ))
    .map_err(|source| ManageError::ConfigWorkerStart { source })?;
    let config_terminal = worker_runtime
        .take_terminal()
        .ok_or(ManageError::ConfigWorkerTerminalUnavailable)?;
    let (lifecycle, mut lifecycle_task) = LifecycleController::spawn(
        instance_id,
        lease.paths().canonical_config_path.clone(),
        config.clone(),
        Arc::clone(&lease),
        shutdown.clone(),
        config_terminal,
        recovered,
    )
    .await
    .map_err(|source| ManageError::LifecycleStart { source })?;
    let probe = ProbeService::new(config.clone());
    observe_readiness_once(&config, &probe, &lifecycle).await;
    let product = ProductService::new(config.clone());
    let (credentials, credential_runtime) =
        CredentialCoordinator::spawn(lease.paths().namespace.clone());
    let mut credential_terminal = credential_runtime.terminal();
    let config_watch_task = tokio::spawn(watch_config_file(
        config.clone(),
        lifecycle.clone(),
        shutdown.clone(),
    ));
    let readiness_task = tokio::spawn(refresh_readiness(
        config.clone(),
        probe.clone(),
        lifecycle.clone(),
        shutdown.clone(),
    ));
    let lifecycle_for_socket = lifecycle.clone();
    let socket_task = tokio::spawn(socket::serve(
        listener,
        identity,
        ManagerSocketServices::new(
            config,
            product,
            probe,
            lifecycle_for_socket,
            credentials.clone(),
            shutdown.clone(),
        ),
    ));
    emit_if_requested(
        writer,
        announce_json,
        &ManagerLaunchRecord::Ready {
            announcement,
            disposition: ManagerLaunchDisposition::ManagerContinues,
        },
    )?;
    if let Some(fd) = machine_stdout_fd {
        close_machine_stdout(fd);
    }
    let mut lifecycle_exit = None;
    let mut credential_exit = None;
    tokio::select! {
        () = shutdown.cancelled() => {}
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|source| ManageError::Signal { source })?;
        }
        joined = &mut lifecycle_task => {
            lifecycle_exit = Some(joined.map_err(|source| ManageError::LifecycleJoin { source })?);
        }
        changed = credential_terminal.changed() => {
            credential_exit = Some(if changed.is_ok() {
                *credential_terminal.borrow()
            } else {
                CredentialCoordinatorExit::InputClosed
            });
        }
    }
    shutdown.cancel();
    if let Err(error) = socket_task.await {
        tracing::error!(%error, "management socket task failed");
    }
    if let Err(error) = config_watch_task.await {
        tracing::error!(%error, "configuration observation task failed");
    }
    if let Err(error) = readiness_task.await {
        tracing::error!(%error, "readiness task failed");
    }
    drop(credentials);
    credential_runtime
        .join()
        .await
        .map_err(|source| ManageError::CredentialCoordinatorJoin { source })?;
    drop(lifecycle);
    let lifecycle_exit = match lifecycle_exit {
        Some(exit) => exit,
        None => lifecycle_task
            .await
            .map_err(|source| ManageError::LifecycleJoin { source })?,
    };
    worker_runtime
        .join()
        .map_err(|_| ManageError::ConfigWorkerJoin)?;
    if credential_exit.is_some() {
        return Err(ManageError::CredentialCoordinatorTerminated);
    }
    if let LifecycleExit::ConfigWorkerTerminated(exit) = lifecycle_exit {
        match exit {
            ConfigWorkerExit::InputClosed => tracing::error!("configuration worker input closed"),
            ConfigWorkerExit::Panicked { correlation } => {
                tracing::error!(?correlation, "configuration worker panicked");
            }
        }
        return Err(ManageError::ConfigWorkerTerminated);
    }
    Ok(ManageOutcome::Serving)
}

async fn prepare_authority(
    config_path: &Path,
    instance_id: &str,
    writer: &mut impl Write,
    announce_json: bool,
) -> Result<Option<PreparedAuthority>, ManageError> {
    let acquire =
        AuthorityLease::acquire(config_path).map_err(|source| ManageError::Authority { source })?;
    let prepared = match acquire {
        AuthorityAcquire::Acquired(lease) => {
            let listener = bind_or_report(&lease, writer, announce_json).await?;
            PreparedAuthority {
                lease,
                listener,
                recovered: None,
            }
        }
        AuthorityAcquire::Occupied(AuthorityConflict::Manager(descriptor)) => {
            match ManagementClient::connect(&descriptor).await {
                Ok(_) => {
                    let record = conflict_record(AuthorityConflict::Manager(descriptor));
                    emit_if_requested(writer, announce_json, &record)?;
                    return Ok(None);
                }
                Err(ManagementClientError::VersionMismatch) => {
                    if let Err(source) = ManagementClient::request_shutdown(&descriptor).await {
                        let record = authority_unavailable_record(
                            &descriptor.canonical_config_path,
                            AuthorityUnavailableReason::LeaseOwnerUnreachable,
                        );
                        emit_if_requested(writer, announce_json, &record)?;
                        return Err(ManageError::Client { source });
                    }
                    return prepare_after_restricted_shutdown(
                        config_path,
                        instance_id,
                        descriptor.canonical_config_path,
                        writer,
                        announce_json,
                    )
                    .await
                    .map(Some);
                }
                Err(_) => {
                    let (lease, listener, recovered) = recover_or_report(
                        config_path,
                        instance_id,
                        descriptor.canonical_config_path,
                        writer,
                        announce_json,
                    )
                    .await?;
                    PreparedAuthority {
                        lease,
                        listener,
                        recovered,
                    }
                }
            }
        }
        AuthorityAcquire::Occupied(AuthorityConflict::Recovering {
            canonical_config_path,
        }) => {
            let paths = AuthorityLease::paths_for(config_path)
                .map_err(|source| ManageError::Authority { source })?;
            if !paths.delegated_descriptor_path.exists() {
                return await_initial_publication(config_path, writer, announce_json).await;
            }
            let (lease, listener, recovered) = recover_or_report(
                config_path,
                instance_id,
                canonical_config_path,
                writer,
                announce_json,
            )
            .await?;
            PreparedAuthority {
                lease,
                listener,
                recovered,
            }
        }
        AuthorityAcquire::Occupied(conflict) => {
            let record = conflict_record(conflict);
            emit_if_requested(writer, announce_json, &record)?;
            return Err(ManageError::LaunchFailed);
        }
    };
    Ok(Some(prepared))
}

async fn await_initial_publication(
    config_path: &Path,
    writer: &mut impl Write,
    announce_json: bool,
) -> Result<Option<PreparedAuthority>, ManageError> {
    let deadline = tokio::time::Instant::now() + INITIAL_PUBLICATION_DEADLINE;
    loop {
        tokio::time::sleep(INITIAL_PUBLICATION_POLL_INTERVAL).await;
        let acquire = AuthorityLease::acquire(config_path)
            .map_err(|source| ManageError::Authority { source })?;
        match acquire {
            AuthorityAcquire::Occupied(AuthorityConflict::Manager(descriptor)) => {
                if ManagementClient::connect(&descriptor).await.is_ok() {
                    let record = conflict_record(AuthorityConflict::Manager(descriptor));
                    emit_if_requested(writer, announce_json, &record)?;
                    return Ok(None);
                }
                if tokio::time::Instant::now() >= deadline {
                    let record = authority_unavailable_record(
                        &descriptor.canonical_config_path,
                        AuthorityUnavailableReason::LeaseOwnerUnreachable,
                    );
                    emit_if_requested(writer, announce_json, &record)?;
                    return Err(ManageError::LaunchFailed);
                }
            }
            AuthorityAcquire::Acquired(lease) => {
                let listener = bind_or_report(&lease, writer, announce_json).await?;
                return Ok(Some(PreparedAuthority {
                    lease,
                    listener,
                    recovered: None,
                }));
            }
            AuthorityAcquire::Occupied(AuthorityConflict::Recovering {
                canonical_config_path,
            }) if tokio::time::Instant::now() >= deadline => {
                let record = conflict_record(AuthorityConflict::Recovering {
                    canonical_config_path,
                });
                emit_if_requested(writer, announce_json, &record)?;
                return Err(ManageError::LaunchFailed);
            }
            AuthorityAcquire::Occupied(AuthorityConflict::Recovering { .. }) => {}
            AuthorityAcquire::Occupied(conflict) => {
                let record = conflict_record(conflict);
                emit_if_requested(writer, announce_json, &record)?;
                return Err(ManageError::LaunchFailed);
            }
        }
    }
}

async fn watch_config_file(
    config: worker::ConfigWorkerClient,
    lifecycle: LifecycleController,
    shutdown: CancellationToken,
) {
    let mut last_revision = config.document().await.ok().and_then(document_revision);
    let mut managed_changes = config.subscribe();
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            changed = managed_changes.recv() => {
                match changed {
                    Ok(change) => last_revision = Some(change.revision),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
            _ = interval.tick() => {
                let Some(revision) = config.document().await.ok().and_then(document_revision) else {
                    continue;
                };
                if last_revision.as_ref() == Some(&revision) {
                    continue;
                }
                last_revision = Some(revision.clone());
                config.publish_raw_change(revision);
                lifecycle.refresh().await;
            }
        }
    }
}

async fn refresh_readiness(
    config: worker::ConfigWorkerClient,
    probe: ProbeService,
    lifecycle: LifecycleController,
    shutdown: CancellationToken,
) {
    let mut config_events = config.subscribe();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = interval.tick() => {}
            event = config_events.recv() => match event {
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
        observe_readiness_once(&config, &probe, &lifecycle).await;
    }
}

async fn observe_readiness_once(
    config: &worker::ConfigWorkerClient,
    probe: &ProbeService,
    lifecycle: &LifecycleController,
) {
    let runtime_present = lifecycle
        .snapshot()
        .await
        .is_some_and(|snapshot| lifecycle_phase_has_runtime(&snapshot.phase));
    match readiness::automatic(config, probe, runtime_present).await {
        Ok(report) => lifecycle.update_readiness(report).await,
        Err(error) => {
            tracing::warn!(%error, "automatic readiness observation unavailable");
            lifecycle.readiness_unavailable().await;
        }
    }
}

fn lifecycle_phase_has_runtime(phase: &tribal_wire::management::LifecyclePhase) -> bool {
    matches!(
        phase,
        tribal_wire::management::LifecyclePhase::Healthy { .. }
            | tribal_wire::management::LifecyclePhase::Degraded { .. }
            | tribal_wire::management::LifecyclePhase::VersionMismatch { .. }
            | tribal_wire::management::LifecyclePhase::Stopping { .. }
            | tribal_wire::management::LifecyclePhase::RuntimeUnresponsive { .. }
    )
}

fn document_revision(document: ConfigDocument) -> Option<ConfigRevision> {
    match document {
        ConfigDocument::DurableValid { revision, .. }
        | ConfigDocument::DurableInvalid { revision } => Some(revision),
        ConfigDocument::UncertainValid { .. }
        | ConfigDocument::UncertainInvalid { .. }
        | ConfigDocument::Unreadable { .. } => None,
    }
}

async fn bind_or_report(
    lease: &AuthorityLease,
    writer: &mut impl Write,
    announce_json: bool,
) -> Result<tokio::net::UnixListener, ManageError> {
    match socket::bind(&lease.paths().socket_path).await {
        Ok(listener) => Ok(listener),
        Err(source) => {
            let record = startup_failure_record(
                &lease.paths().canonical_config_path,
                ManagerStartupFailure::ManagementSocketUnavailable,
            );
            emit_if_requested(writer, announce_json, &record)?;
            Err(ManageError::Socket { source })
        }
    }
}

async fn prepare_after_restricted_shutdown(
    config_path: &Path,
    manager_instance_id: &str,
    canonical_config_path: PathBuf,
    writer: &mut impl Write,
    announce_json: bool,
) -> Result<PreparedAuthority, ManageError> {
    let deadline = tokio::time::Instant::now() + REPLACEMENT_DEADLINE;
    loop {
        let acquire = AuthorityLease::acquire(config_path)
            .map_err(|source| ManageError::Authority { source })?;
        match acquire {
            AuthorityAcquire::Acquired(lease) => {
                let listener = bind_or_report(&lease, writer, announce_json).await?;
                return Ok(PreparedAuthority {
                    lease,
                    listener,
                    recovered: None,
                });
            }
            AuthorityAcquire::Occupied(AuthorityConflict::Recovering {
                canonical_config_path,
            }) => {
                let (lease, listener, recovered) = recover_or_report(
                    config_path,
                    manager_instance_id,
                    canonical_config_path,
                    writer,
                    announce_json,
                )
                .await?;
                return Ok(PreparedAuthority {
                    lease,
                    listener,
                    recovered,
                });
            }
            AuthorityAcquire::Occupied(AuthorityConflict::Manager(_))
                if tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(REPLACEMENT_POLL_INTERVAL).await;
            }
            AuthorityAcquire::Occupied(AuthorityConflict::Manager(_)) => {
                let paths = AuthorityLease::paths_for(config_path)
                    .map_err(|source| ManageError::Authority { source })?;
                if paths.delegated_descriptor_path.exists() {
                    let (lease, listener, recovered) = recover_or_report(
                        config_path,
                        manager_instance_id,
                        canonical_config_path,
                        writer,
                        announce_json,
                    )
                    .await?;
                    return Ok(PreparedAuthority {
                        lease,
                        listener,
                        recovered,
                    });
                }
                let record = authority_unavailable_record(
                    &canonical_config_path,
                    AuthorityUnavailableReason::LeaseOwnerUnreachable,
                );
                emit_if_requested(writer, announce_json, &record)?;
                return Err(ManageError::LaunchFailed);
            }
            AuthorityAcquire::Occupied(conflict) => {
                let record = conflict_record(conflict);
                emit_if_requested(writer, announce_json, &record)?;
                return Err(ManageError::LaunchFailed);
            }
        }
    }
}

async fn recover_or_report(
    config_path: &Path,
    manager_instance_id: &str,
    canonical_config_path: PathBuf,
    writer: &mut impl Write,
    announce_json: bool,
) -> Result<
    (
        AuthorityLease,
        tokio::net::UnixListener,
        Option<RecoveredRuntime>,
    ),
    ManageError,
> {
    let config_path = config_path.to_owned();
    let manager_instance_id = manager_instance_id.to_owned();
    let pending = match tokio::task::spawn_blocking(move || {
        PendingRecovery::begin(&config_path, manager_instance_id)
    })
    .await
    .map_err(|source| ManageError::RecoveryTask { source })?
    {
        Ok(pending) => pending,
        Err(source) => {
            let record = conflict_record(AuthorityConflict::Recovering {
                canonical_config_path,
            });
            emit_if_requested(writer, announce_json, &record)?;
            return Err(ManageError::Custody { source });
        }
    };
    let listener = bind_or_report(pending.lease(), writer, announce_json).await?;
    let recovered = tokio::task::spawn_blocking(move || pending.commit())
        .await
        .map_err(|source| ManageError::RecoveryTask { source })?
        .map_err(|source| ManageError::Custody { source })?;
    Ok((
        recovered.lease,
        listener,
        Some(RecoveredRuntime {
            identity: recovered.runtime,
            custody: recovered.custody,
            control_proof: recovered.control_proof,
        }),
    ))
}

fn conflict_record(conflict: AuthorityConflict) -> ManagerLaunchRecord {
    match conflict {
        AuthorityConflict::Manager(descriptor) => ManagerLaunchRecord::Ready {
            announcement: announcement_from_descriptor(descriptor),
            disposition: ManagerLaunchDisposition::ContenderExits,
        },
        AuthorityConflict::StandaloneRuntime(descriptor) => ManagerLaunchRecord::Failed {
            failure: ManagerLaunchFailure::DirectRuntimeConflict {
                runtime: ConflictingRuntimeIdentity {
                    pid: descriptor.pid,
                    binary_version: descriptor.binary_version,
                    config_path: ConfigFilePath {
                        path: descriptor
                            .canonical_config_path
                            .to_string_lossy()
                            .into_owned(),
                    },
                },
            },
        },
        AuthorityConflict::OneShot(descriptor) => ManagerLaunchRecord::Failed {
            failure: ManagerLaunchFailure::AuthorityUnavailable {
                config_path: config_path(&descriptor.canonical_config_path),
                reason: AuthorityUnavailableReason::LeaseOwnerUnreachable,
            },
        },
        AuthorityConflict::Recovering {
            canonical_config_path,
        } => ManagerLaunchRecord::Failed {
            failure: ManagerLaunchFailure::AuthorityRecovering {
                config_path: config_path(&canonical_config_path),
            },
        },
    }
}

fn announcement_from_descriptor(descriptor: AuthorityDescriptor) -> ManagerAnnouncement {
    ManagerAnnouncement {
        instance_id: descriptor.instance_id,
        socket_path: descriptor
            .socket_path
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned(),
        protocol_version: descriptor.protocol_version.unwrap_or_default(),
        binary_version: descriptor.binary_version,
        config_path: config_path(&descriptor.canonical_config_path),
        pid: descriptor.pid,
    }
}

fn startup_failure_record(path: &Path, failure: ManagerStartupFailure) -> ManagerLaunchRecord {
    ManagerLaunchRecord::Failed {
        failure: ManagerLaunchFailure::ManagerStartupFailed {
            config_path: config_path(path),
            failure,
        },
    }
}

fn authority_unavailable_record(
    path: &Path,
    reason: AuthorityUnavailableReason,
) -> ManagerLaunchRecord {
    ManagerLaunchRecord::Failed {
        failure: ManagerLaunchFailure::AuthorityUnavailable {
            config_path: config_path(path),
            reason,
        },
    }
}

fn config_path(path: &Path) -> ConfigFilePath {
    ConfigFilePath {
        path: path.to_string_lossy().into_owned(),
    }
}

fn emit_if_requested(
    writer: &mut impl Write,
    announce_json: bool,
    record: &ManagerLaunchRecord,
) -> Result<(), ManageError> {
    if !announce_json {
        return Ok(());
    }
    let mut bytes = serde_json::to_vec(record).map_err(|source| ManageError::Output {
        source: io::Error::new(io::ErrorKind::InvalidData, source),
    })?;
    bytes.push(b'\n');
    if bytes.len() > MAX_LAUNCH_RECORD_BYTES {
        return Err(ManageError::Output {
            source: io::Error::new(
                io::ErrorKind::InvalidData,
                "launch record exceeds size limit",
            ),
        });
    }
    writer
        .write_all(&bytes)
        .and_then(|()| writer.flush())
        .map_err(|source| ManageError::Output { source })
}

/// Closes the bounded stdout machine channel after its sole record is flushed.
fn close_machine_stdout(fd: std::os::fd::RawFd) {
    // SAFETY: `fd` is captured from process stdout and closed exactly once
    // after the sole record is flushed; the live lock is never used again.
    let _ = unsafe { libc::close(fd) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_launch_record_is_single_bounded_line() {
        let record = ManagerLaunchRecord::Failed {
            failure: ManagerLaunchFailure::AuthorityRecovering {
                config_path: ConfigFilePath {
                    path: "/tmp/tribal.yaml".to_owned(),
                },
            },
        };
        let mut output = Vec::new();
        emit_if_requested(&mut output, true, &record).expect("record writes");
        let newline = output
            .iter()
            .position(|byte| *byte == b'\n')
            .expect("record is newline terminated");
        assert!(!output[newline + 1..].contains(&b'\n'));
        assert!(output.len() <= MAX_LAUNCH_RECORD_BYTES);
        assert_eq!(
            serde_json::from_slice::<ManagerLaunchRecord>(&output).expect("record parses"),
            record,
        );
    }
}
