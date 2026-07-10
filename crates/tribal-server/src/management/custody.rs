//! Lease-backed lifetime custody and manager recovery for managed runtimes.

use std::{
    fs::File,
    io::{self, Read as _, Write as _},
    mem,
    os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd},
    os::unix::{
        fs::PermissionsExt as _,
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    ptr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tribal_config::write_atomically;
use tribal_wire::{
    management::{ConfigFilePath, RuntimeIdentity},
    runtime_control::RuntimeCustodyProof,
};

use super::authority::{AuthorityError, AuthorityLease, AuthorityPaths};

const OWNER_FILE_MODE: u32 = 0o600;
const OWNER_DIRECTORY_MODE: u32 = 0o700;
const MAX_FRAME_BYTES: usize = 16 * 1024;
const CONNECT_DEADLINE: Duration = Duration::from_secs(10);
const CLAIM_DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) const MANAGED_RUNTIME_INSTANCE_ID: &str = "TRIBAL_MANAGED_RUNTIME_INSTANCE_ID";
pub(crate) const MANAGED_MANAGER_INSTANCE_ID: &str = "TRIBAL_MANAGED_MANAGER_INSTANCE_ID";
pub(crate) const MANAGED_CUSTODY_PROOF: &str = "TRIBAL_MANAGED_CUSTODY_PROOF";

/// Process-local proof registry shared by custody rotation and runtime control.
pub(crate) type RuntimeProofRegistry = Arc<RwLock<zeroize::Zeroizing<String>>>;

/// Durable runtime-owned recovery record.
#[derive(Serialize, Deserialize)]
pub(crate) struct DelegatedRuntimeDescriptor {
    runtime: RuntimeIdentity,
    manager_instance_id: String,
    generation: u64,
    proof: RuntimeCustodyProof,
    custody_socket_path: PathBuf,
    canonical_config_path: PathBuf,
}

impl std::fmt::Debug for DelegatedRuntimeDescriptor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DelegatedRuntimeDescriptor")
            .field("runtime", &self.runtime)
            .field("manager_instance_id", &self.manager_instance_id)
            .field("generation", &self.generation)
            .field("proof", &"<redacted>")
            .field("custody_socket_path", &self.custody_socket_path)
            .field("canonical_config_path", &self.canonical_config_path)
            .finish()
    }
}

/// Runtime-side custody bootstrap retained for artifact cleanup.
#[derive(Debug)]
pub(crate) struct RuntimeCustodyGuard {
    socket_path: PathBuf,
    descriptor_path: PathBuf,
    cancellation: CancellationToken,
    thread: Option<std::thread::JoinHandle<()>>,
    control_proof: RuntimeProofRegistry,
}

/// Manager-side lifetime custody connection.
#[derive(Debug)]
pub(crate) struct ManagerCustody {
    stream: UnixStream,
}

/// Recovery claim holding the transferred lease until durable attachment.
#[derive(Debug)]
pub(crate) struct PendingRecovery {
    stream: UnixStream,
    lease: AuthorityLease,
    runtime: RuntimeIdentity,
    manager_instance_id: String,
    next_proof: RuntimeCustodyProof,
}

/// Successful recovery result.
#[derive(Debug)]
pub(crate) struct RecoveredAuthority {
    pub(crate) custody: ManagerCustody,
    pub(crate) lease: AuthorityLease,
    pub(crate) runtime: RuntimeIdentity,
    pub(crate) control_proof: RuntimeCustodyProof,
}

#[derive(Clone, Copy)]
struct RecoveryContext<'a> {
    listener: &'a UnixListener,
    lease_file: &'a File,
    cancellation: &'a CancellationToken,
    descriptor_path: &'a Path,
    socket_path: &'a Path,
    control_proof: &'a RuntimeProofRegistry,
}

/// Custody or recovery failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CustodyError {
    #[error("custody environment is incomplete")]
    EnvironmentIncomplete,
    #[error("custody proof entropy is unavailable: {source}")]
    Entropy {
        #[source]
        source: getrandom::Error,
    },
    #[error("custody I/O failed at '{}': {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("custody frame encoding failed: {source}")]
    Encoding {
        #[source]
        source: serde_json::Error,
    },
    #[error("custody request was refused: {reason:?}")]
    Refused { reason: CustodyRefusal },
    #[error("custody connection closed before attachment completed")]
    Closed,
    #[error("custody claim timed out")]
    TimedOut,
    #[error("inherited authority lease was invalid: {source}")]
    Authority {
        #[source]
        source: AuthorityError,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CustodyRefusal {
    ProofInvalid,
    RuntimeIdentityMismatch,
    RecoveryInProgress,
    RecoveryBlocked,
    ManagerMismatch,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
enum CustodyRequest {
    InitialCommit {
        manager_instance_id: String,
        proof: RuntimeCustodyProof,
    },
    Recover {
        manager_instance_id: String,
        proof: RuntimeCustodyProof,
    },
    CommitRecovery {
        manager_instance_id: String,
        proof: RuntimeCustodyProof,
    },
    Stop {
        runtime_instance_id: String,
    },
}

impl std::fmt::Debug for CustodyRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InitialCommit {
                manager_instance_id,
                ..
            } => formatter
                .debug_struct("InitialCommit")
                .field("manager_instance_id", manager_instance_id)
                .field("proof", &"<redacted>")
                .finish(),
            Self::Recover {
                manager_instance_id,
                ..
            } => formatter
                .debug_struct("Recover")
                .field("manager_instance_id", manager_instance_id)
                .field("proof", &"<redacted>")
                .finish(),
            Self::CommitRecovery {
                manager_instance_id,
                ..
            } => formatter
                .debug_struct("CommitRecovery")
                .field("manager_instance_id", manager_instance_id)
                .field("proof", &"<redacted>")
                .finish(),
            Self::Stop {
                runtime_instance_id,
            } => formatter
                .debug_struct("Stop")
                .field("runtime_instance_id", runtime_instance_id)
                .finish(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", content = "data", rename_all = "snake_case")]
enum CustodyResponse {
    Committed,
    ClaimAccepted,
    StopAccepted,
    Refused { reason: CustodyRefusal },
}

impl RuntimeCustodyGuard {
    /// Establishes initial durable attachment before runtime services start.
    pub(crate) fn bootstrap_from_environment(
        lease: &AuthorityLease,
        cancellation: CancellationToken,
    ) -> Result<Option<Self>, CustodyError> {
        let runtime_instance_id = std::env::var(MANAGED_RUNTIME_INSTANCE_ID).ok();
        let manager_instance_id = std::env::var(MANAGED_MANAGER_INSTANCE_ID).ok();
        let proof = std::env::var(MANAGED_CUSTODY_PROOF).ok();
        if runtime_instance_id.is_none() && manager_instance_id.is_none() && proof.is_none() {
            return Ok(None);
        }
        let (Some(runtime_instance_id), Some(manager_instance_id), Some(proof)) =
            (runtime_instance_id, manager_instance_id, proof)
        else {
            return Err(CustodyError::EnvironmentIncomplete);
        };
        Self::bootstrap(
            lease,
            cancellation,
            runtime_instance_id,
            manager_instance_id,
            proof,
        )
    }

    fn bootstrap(
        lease: &AuthorityLease,
        cancellation: CancellationToken,
        runtime_instance_id: String,
        manager_instance_id: String,
        proof: String,
    ) -> Result<Option<Self>, CustodyError> {
        let paths = lease.paths();
        prepare_socket_path(&paths.custody_socket_path)?;
        let listener = UnixListener::bind(&paths.custody_socket_path)
            .map_err(|source| io_error(&paths.custody_socket_path, source))?;
        std::fs::set_permissions(
            &paths.custody_socket_path,
            std::fs::Permissions::from_mode(OWNER_FILE_MODE),
        )
        .map_err(|source| io_error(&paths.custody_socket_path, source))?;
        let runtime = RuntimeIdentity {
            instance_id: runtime_instance_id,
            pid: std::process::id(),
            binary_version: env!("CARGO_PKG_VERSION").to_owned(),
            config_path: ConfigFilePath {
                path: paths.canonical_config_path.to_string_lossy().into_owned(),
            },
        };
        let deadline = Instant::now() + CONNECT_DEADLINE;
        listener
            .set_nonblocking(true)
            .map_err(|source| io_error(&paths.custody_socket_path, source))?;
        let (mut stream, _) = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(source)
                    if source.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    std::thread::sleep(POLL_INTERVAL);
                }
                Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                    return Err(CustodyError::TimedOut);
                }
                Err(source) => return Err(io_error(&paths.custody_socket_path, source)),
            }
        };
        stream
            .set_nonblocking(false)
            .map_err(|source| io_error(&paths.custody_socket_path, source))?;
        admit_same_uid(&stream, &paths.custody_socket_path)?;
        stream
            .set_read_timeout(Some(CLAIM_DEADLINE))
            .map_err(|source| io_error(&paths.custody_socket_path, source))?;
        let request = read_frame::<CustodyRequest>(&mut stream, &paths.custody_socket_path)?;
        match request {
            CustodyRequest::InitialCommit {
                manager_instance_id: candidate,
                proof: candidate_proof,
            } if candidate == manager_instance_id && candidate_proof.expose_secret() == proof => {}
            _ => {
                write_frame(
                    &mut stream,
                    &CustodyResponse::Refused {
                        reason: CustodyRefusal::ProofInvalid,
                    },
                    &paths.custody_socket_path,
                )?;
                return Err(CustodyError::Refused {
                    reason: CustodyRefusal::ProofInvalid,
                });
            }
        }
        let descriptor = DelegatedRuntimeDescriptor {
            runtime: runtime.clone(),
            manager_instance_id,
            generation: 1,
            proof: RuntimeCustodyProof::new(proof),
            custody_socket_path: paths.custody_socket_path.clone(),
            canonical_config_path: paths.canonical_config_path.clone(),
        };
        persist_descriptor(&paths.delegated_descriptor_path, &descriptor)?;
        let control_proof = Arc::new(RwLock::new(zeroize::Zeroizing::new(
            descriptor.proof.expose_secret().to_owned(),
        )));
        write_frame(
            &mut stream,
            &CustodyResponse::Committed,
            &paths.custody_socket_path,
        )?;
        stream
            .set_read_timeout(None)
            .map_err(|source| io_error(&paths.custody_socket_path, source))?;
        let descriptor_path = paths.delegated_descriptor_path.clone();
        let socket_path = paths.custody_socket_path.clone();
        let lease_file = lease
            .inheritable_clone()
            .map_err(|source| CustodyError::Authority { source })?;
        let thread_descriptor_path = descriptor_path.clone();
        let thread_socket_path = socket_path.clone();
        let thread_cancellation = cancellation.clone();
        let thread_control_proof = Arc::clone(&control_proof);
        let thread = std::thread::Builder::new()
            .name("tribal-runtime-custody".to_owned())
            .spawn(move || {
                let context = RecoveryContext {
                    listener: &listener,
                    lease_file: &lease_file,
                    cancellation: &thread_cancellation,
                    descriptor_path: &thread_descriptor_path,
                    socket_path: &thread_socket_path,
                    control_proof: &thread_control_proof,
                };
                serve_recovery(&context, stream, descriptor);
            })
            .map_err(|source| io_error(&socket_path, source))?;
        Ok(Some(Self {
            socket_path,
            descriptor_path,
            cancellation,
            thread: Some(thread),
            control_proof,
        }))
    }

    /// Shares the current proof with the private runtime-control listener.
    pub(crate) fn control_proof(&self) -> RuntimeProofRegistry {
        Arc::clone(&self.control_proof)
    }
}

impl Drop for RuntimeCustodyGuard {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            tracing::error!("runtime custody thread panicked");
        }
        let _ = std::fs::remove_file(&self.socket_path);
        let _ = std::fs::remove_file(&self.descriptor_path);
    }
}

impl ManagerCustody {
    #[cfg(test)]
    pub(crate) fn pair_for_test() -> io::Result<(Self, UnixStream)> {
        let (manager, runtime) = UnixStream::pair()?;
        Ok((Self { stream: manager }, runtime))
    }

    /// Completes the initial runtime attachment and retains its lifetime stream.
    pub(crate) fn attach_initial(
        paths: &AuthorityPaths,
        manager_instance_id: &str,
        proof: RuntimeCustodyProof,
    ) -> Result<Self, CustodyError> {
        let deadline = Instant::now() + CONNECT_DEADLINE;
        let mut stream = loop {
            match UnixStream::connect(&paths.custody_socket_path) {
                Ok(stream) => break stream,
                Err(source)
                    if matches!(
                        source.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                    ) && Instant::now() < deadline =>
                {
                    std::thread::sleep(POLL_INTERVAL);
                }
                Err(source) => return Err(io_error(&paths.custody_socket_path, source)),
            }
        };
        admit_same_uid(&stream, &paths.custody_socket_path)?;
        stream
            .set_read_timeout(Some(CLAIM_DEADLINE))
            .map_err(|source| io_error(&paths.custody_socket_path, source))?;
        write_frame(
            &mut stream,
            &CustodyRequest::InitialCommit {
                manager_instance_id: manager_instance_id.to_owned(),
                proof,
            },
            &paths.custody_socket_path,
        )?;
        match read_frame::<CustodyResponse>(&mut stream, &paths.custody_socket_path)? {
            CustodyResponse::Committed => {
                stream
                    .set_read_timeout(None)
                    .map_err(|source| io_error(&paths.custody_socket_path, source))?;
                Ok(Self { stream })
            }
            CustodyResponse::Refused { reason } => Err(CustodyError::Refused { reason }),
            CustodyResponse::ClaimAccepted | CustodyResponse::StopAccepted => {
                Err(CustodyError::Closed)
            }
        }
    }

    /// Requests authenticated stop from a recovered runtime.
    pub(crate) fn stop(&mut self, runtime: &RuntimeIdentity) -> Result<(), CustodyError> {
        write_frame(
            &mut self.stream,
            &CustodyRequest::Stop {
                runtime_instance_id: runtime.instance_id.clone(),
            },
            Path::new("<custody-stream>"),
        )?;
        match read_frame::<CustodyResponse>(&mut self.stream, Path::new("<custody-stream>"))? {
            CustodyResponse::StopAccepted => Ok(()),
            CustodyResponse::Refused { reason } => Err(CustodyError::Refused { reason }),
            CustodyResponse::Committed | CustodyResponse::ClaimAccepted => {
                Err(CustodyError::Closed)
            }
        }
    }

    /// Reports whether the lifetime session has closed without consuming data.
    pub(crate) fn is_closed(&self) -> bool {
        let fd = self.stream.as_raw_fd();
        let mut byte = 0_u8;
        // SAFETY: `fd` is a live Unix-stream descriptor, and `byte` is a valid
        // one-byte output buffer. `MSG_PEEK | MSG_DONTWAIT` consumes no data.
        let read = unsafe {
            libc::recv(
                fd,
                ptr::from_mut(&mut byte).cast(),
                1,
                libc::MSG_PEEK | libc::MSG_DONTWAIT,
            )
        };
        read == 0
    }
}

impl PendingRecovery {
    /// Starts an exclusive recovery claim and adopts the transferred lease.
    pub(crate) fn begin(
        config_path: &Path,
        manager_instance_id: String,
    ) -> Result<Self, CustodyError> {
        let paths = AuthorityLease::paths_for(config_path)
            .map_err(|source| CustodyError::Authority { source })?;
        Self::begin_with_paths(manager_instance_id, paths)
    }

    fn begin_with_paths(
        manager_instance_id: String,
        paths: AuthorityPaths,
    ) -> Result<Self, CustodyError> {
        let descriptor = read_descriptor(&paths.delegated_descriptor_path)?;
        if descriptor.canonical_config_path != paths.canonical_config_path {
            return Err(CustodyError::Refused {
                reason: CustodyRefusal::RuntimeIdentityMismatch,
            });
        }
        let mut stream = UnixStream::connect(&descriptor.custody_socket_path)
            .map_err(|source| io_error(&descriptor.custody_socket_path, source))?;
        admit_same_uid(&stream, &descriptor.custody_socket_path)?;
        stream
            .set_read_timeout(Some(CLAIM_DEADLINE))
            .map_err(|source| io_error(&descriptor.custody_socket_path, source))?;
        write_frame(
            &mut stream,
            &CustodyRequest::Recover {
                manager_instance_id: manager_instance_id.clone(),
                proof: descriptor.proof,
            },
            &descriptor.custody_socket_path,
        )?;
        match read_frame::<CustodyResponse>(&mut stream, &descriptor.custody_socket_path)? {
            CustodyResponse::ClaimAccepted => {}
            CustodyResponse::Refused { reason } => return Err(CustodyError::Refused { reason }),
            CustodyResponse::Committed | CustodyResponse::StopAccepted => {
                return Err(CustodyError::Closed);
            }
        }
        let descriptor_fd = receive_fd(&stream, &descriptor.custody_socket_path)?;
        let lease = AuthorityLease::from_inherited_with_paths(descriptor_fd, paths)
            .map_err(|source| CustodyError::Authority { source })?;
        Ok(Self {
            stream,
            lease,
            runtime: descriptor.runtime,
            manager_instance_id,
            next_proof: generate_proof()?,
        })
    }

    pub(crate) fn lease(&self) -> &AuthorityLease {
        &self.lease
    }

    /// Commits the new proof after the successor's public socket is bound.
    pub(crate) fn commit(mut self) -> Result<RecoveredAuthority, CustodyError> {
        let socket_path = self.lease.paths().custody_socket_path.clone();
        let control_proof = RuntimeCustodyProof::new(self.next_proof.expose_secret().to_owned());
        write_frame(
            &mut self.stream,
            &CustodyRequest::CommitRecovery {
                manager_instance_id: self.manager_instance_id,
                proof: self.next_proof,
            },
            &socket_path,
        )?;
        match read_frame::<CustodyResponse>(&mut self.stream, &socket_path)? {
            CustodyResponse::Committed => {
                self.stream
                    .set_read_timeout(None)
                    .map_err(|source| io_error(&socket_path, source))?;
                Ok(RecoveredAuthority {
                    custody: ManagerCustody {
                        stream: self.stream,
                    },
                    lease: self.lease,
                    runtime: self.runtime,
                    control_proof,
                })
            }
            CustodyResponse::Refused { reason } => Err(CustodyError::Refused { reason }),
            CustodyResponse::ClaimAccepted | CustodyResponse::StopAccepted => {
                Err(CustodyError::Closed)
            }
        }
    }
}

/// Generates one zero-knowledge recovery proof from operating-system entropy.
pub(crate) fn generate_proof() -> Result<RuntimeCustodyProof, CustodyError> {
    let mut bytes = zeroize::Zeroizing::new([0_u8; 32]);
    getrandom::fill(bytes.as_mut()).map_err(|source| CustodyError::Entropy { source })?;
    Ok(RuntimeCustodyProof::new(
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes.as_ref()),
    ))
}

fn serve_recovery(
    context: &RecoveryContext<'_>,
    mut lifetime: UnixStream,
    mut descriptor: DelegatedRuntimeDescriptor,
) {
    let RecoveryContext {
        listener,
        lease_file,
        cancellation,
        descriptor_path,
        socket_path,
        control_proof,
    } = *context;
    let mut blocked = false;
    loop {
        if cancellation.is_cancelled() {
            return;
        }
        match stream_state(&lifetime) {
            Ok(StreamState::Data) => match read_frame::<CustodyRequest>(&mut lifetime, socket_path)
            {
                Ok(CustodyRequest::Stop {
                    runtime_instance_id,
                }) => {
                    let response = if runtime_instance_id == descriptor.runtime.instance_id {
                        cancellation.cancel();
                        CustodyResponse::StopAccepted
                    } else {
                        CustodyResponse::Refused {
                            reason: CustodyRefusal::RuntimeIdentityMismatch,
                        }
                    };
                    let _ = write_frame(&mut lifetime, &response, socket_path);
                    if cancellation.is_cancelled() {
                        return;
                    }
                }
                Ok(_) => {
                    let _ = write_frame(
                        &mut lifetime,
                        &CustodyResponse::Refused {
                            reason: CustodyRefusal::ManagerMismatch,
                        },
                        socket_path,
                    );
                }
                Err(_) => {}
            },
            Ok(StreamState::Open) => {
                std::thread::sleep(POLL_INTERVAL);
                continue;
            }
            Ok(StreamState::Closed) | Err(_) => {}
        }
        let claim = accept_claim(listener, cancellation, socket_path);
        let Some(mut claimant) = claim else {
            return;
        };
        let Ok(request) = read_frame::<CustodyRequest>(&mut claimant, socket_path) else {
            continue;
        };
        if blocked {
            let _ = write_frame(
                &mut claimant,
                &CustodyResponse::Refused {
                    reason: CustodyRefusal::RecoveryBlocked,
                },
                socket_path,
            );
            continue;
        }
        let CustodyRequest::Recover {
            manager_instance_id,
            proof,
        } = request
        else {
            let _ = write_frame(
                &mut claimant,
                &CustodyResponse::Refused {
                    reason: CustodyRefusal::RecoveryInProgress,
                },
                socket_path,
            );
            continue;
        };
        if proof.expose_secret() != descriptor.proof.expose_secret() {
            let _ = write_frame(
                &mut claimant,
                &CustodyResponse::Refused {
                    reason: CustodyRefusal::ProofInvalid,
                },
                socket_path,
            );
            continue;
        }
        if write_frame(&mut claimant, &CustodyResponse::ClaimAccepted, socket_path).is_err()
            || send_fd(&claimant, lease_file.as_raw_fd(), socket_path).is_err()
        {
            continue;
        }
        claimant.set_read_timeout(Some(CLAIM_DEADLINE)).ok();
        let commit = read_frame::<CustodyRequest>(&mut claimant, socket_path);
        let Ok(request) = commit else {
            match generate_proof().and_then(|proof| {
                descriptor.proof = proof;
                descriptor.generation = descriptor.generation.saturating_add(1);
                persist_descriptor(descriptor_path, &descriptor)
            }) {
                Ok(()) => replace_runtime_proof(control_proof, &descriptor.proof),
                Err(_) => blocked = true,
            }
            continue;
        };
        let CustodyRequest::CommitRecovery {
            manager_instance_id: committed_manager,
            proof: next_proof,
        } = request
        else {
            continue;
        };
        if committed_manager != manager_instance_id {
            let _ = write_frame(
                &mut claimant,
                &CustodyResponse::Refused {
                    reason: CustodyRefusal::ManagerMismatch,
                },
                socket_path,
            );
            continue;
        }
        descriptor.manager_instance_id = committed_manager;
        descriptor.proof = next_proof;
        descriptor.generation = descriptor.generation.saturating_add(1);
        if persist_descriptor(descriptor_path, &descriptor).is_err() {
            blocked = true;
            let _ = write_frame(
                &mut claimant,
                &CustodyResponse::Refused {
                    reason: CustodyRefusal::RecoveryBlocked,
                },
                socket_path,
            );
            continue;
        }
        replace_runtime_proof(control_proof, &descriptor.proof);
        if write_frame(&mut claimant, &CustodyResponse::Committed, socket_path).is_err() {
            continue;
        }
        claimant.set_read_timeout(None).ok();
        lifetime = claimant;
    }
}

fn replace_runtime_proof(registry: &RuntimeProofRegistry, proof: &RuntimeCustodyProof) {
    let mut current = registry
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *current = zeroize::Zeroizing::new(proof.expose_secret().to_owned());
}

fn accept_claim(
    listener: &UnixListener,
    cancellation: &CancellationToken,
    socket_path: &Path,
) -> Option<UnixStream> {
    loop {
        if cancellation.is_cancelled() {
            return None;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                if stream.set_nonblocking(false).is_err() {
                    continue;
                }
                if admit_same_uid(&stream, socket_path).is_ok() {
                    return Some(stream);
                }
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(POLL_INTERVAL);
            }
            Err(_) => return None,
        }
    }
}

fn prepare_socket_path(path: &Path) -> Result<(), CustodyError> {
    let parent = path
        .parent()
        .ok_or_else(|| io_error(path, io::Error::from(io::ErrorKind::InvalidInput)))?;
    std::fs::create_dir_all(parent).map_err(|source| io_error(parent, source))?;
    std::fs::set_permissions(
        parent,
        std::fs::Permissions::from_mode(OWNER_DIRECTORY_MODE),
    )
    .map_err(|source| io_error(parent, source))?;
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(io_error(path, source)),
    }
}

fn persist_descriptor(
    path: &Path,
    descriptor: &DelegatedRuntimeDescriptor,
) -> Result<(), CustodyError> {
    let bytes = serde_json::to_vec_pretty(descriptor)
        .map_err(|source| CustodyError::Encoding { source })?;
    write_atomically(path, &bytes, Some(OWNER_FILE_MODE)).map_err(|source| io_error(path, source))
}

fn read_descriptor(path: &Path) -> Result<DelegatedRuntimeDescriptor, CustodyError> {
    let bytes = std::fs::read(path).map_err(|source| io_error(path, source))?;
    serde_json::from_slice(&bytes).map_err(|source| CustodyError::Encoding { source })
}

fn write_frame<T: Serialize>(
    stream: &mut UnixStream,
    value: &T,
    path: &Path,
) -> Result<(), CustodyError> {
    let bytes = serde_json::to_vec(value).map_err(|source| CustodyError::Encoding { source })?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(io_error(
            path,
            io::Error::new(
                io::ErrorKind::InvalidData,
                "custody frame exceeds size limit",
            ),
        ));
    }
    let length = u32::try_from(bytes.len()).map_err(|_| {
        io_error(
            path,
            io::Error::new(io::ErrorKind::InvalidData, "custody frame length overflow"),
        )
    })?;
    stream
        .write_all(&length.to_be_bytes())
        .and_then(|()| stream.write_all(&bytes))
        .map_err(|source| io_error(path, source))
}

fn read_frame<T: serde::de::DeserializeOwned>(
    stream: &mut UnixStream,
    path: &Path,
) -> Result<T, CustodyError> {
    read_raw_frame(stream, path).and_then(|bytes| {
        serde_json::from_slice(&bytes).map_err(|source| CustodyError::Encoding { source })
    })
}

fn read_raw_frame(stream: &mut UnixStream, path: &Path) -> Result<Vec<u8>, CustodyError> {
    let mut length = [0_u8; 4];
    stream
        .read_exact(&mut length)
        .map_err(|source| io_error(path, source))?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(io_error(
            path,
            io::Error::new(
                io::ErrorKind::InvalidData,
                "custody frame exceeds size limit",
            ),
        ));
    }
    let mut bytes = vec![0_u8; length];
    stream
        .read_exact(&mut bytes)
        .map_err(|source| io_error(path, source))?;
    Ok(bytes)
}

enum StreamState {
    Open,
    Data,
    Closed,
}

fn stream_state(stream: &UnixStream) -> Result<StreamState, io::Error> {
    let mut byte = 0_u8;
    // SAFETY: the descriptor is a live Unix stream and `byte` is a valid
    // one-byte output. Peeking never consumes protocol data.
    let read = unsafe {
        libc::recv(
            stream.as_raw_fd(),
            ptr::from_mut(&mut byte).cast(),
            1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    match read {
        0 => Ok(StreamState::Closed),
        1 => Ok(StreamState::Data),
        -1 if io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock => {
            Ok(StreamState::Open)
        }
        _ => Err(io::Error::last_os_error()),
    }
}

fn send_fd(stream: &UnixStream, descriptor: RawFd, path: &Path) -> Result<(), CustodyError> {
    let mut byte = [0_u8; 1];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: byte.len(),
    };
    let descriptor_size = mem::size_of::<RawFd>()
        .try_into()
        .map_err(|_| invalid_control_size(path))?;
    // SAFETY: `CMSG_SPACE` computes storage for one `RawFd` control payload.
    let control_len = usize::try_from(unsafe { libc::CMSG_SPACE(descriptor_size) })
        .map_err(|_| invalid_control_size(path))?;
    let mut control = vec![0_u8; control_len];
    let message_control_len = control
        .len()
        .try_into()
        .map_err(|_| invalid_control_size(path))?;
    // SAFETY: `CMSG_LEN` computes the header length for one `RawFd` payload.
    let descriptor_control_len = unsafe { libc::CMSG_LEN(descriptor_size) };
    // SAFETY: every pointer in `message` refers to live stack/vector storage
    // for the duration of `sendmsg`; the control header and payload fit the
    // `CMSG_SPACE` allocation above.
    let sent = unsafe {
        let mut message: libc::msghdr = mem::zeroed();
        message.msg_iov = ptr::from_mut(&mut iov);
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = message_control_len;
        let header = libc::CMSG_FIRSTHDR(&raw const message);
        if header.is_null() {
            return Err(io_error(
                path,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "custody control header unavailable",
                ),
            ));
        }
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = descriptor_control_len;
        ptr::copy_nonoverlapping(
            ptr::from_ref(&descriptor).cast::<u8>(),
            libc::CMSG_DATA(header),
            mem::size_of::<RawFd>(),
        );
        libc::sendmsg(stream.as_raw_fd(), &raw const message, 0)
    };
    if sent == 1 {
        Ok(())
    } else {
        Err(io_error(path, io::Error::last_os_error()))
    }
}

fn receive_fd(stream: &UnixStream, path: &Path) -> Result<OwnedFd, CustodyError> {
    let mut byte = [0_u8; 1];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: byte.len(),
    };
    let descriptor_size = mem::size_of::<RawFd>()
        .try_into()
        .map_err(|_| invalid_control_size(path))?;
    // SAFETY: `CMSG_SPACE` computes storage for one `RawFd` control payload.
    let control_len = usize::try_from(unsafe { libc::CMSG_SPACE(descriptor_size) })
        .map_err(|_| invalid_control_size(path))?;
    let mut control = vec![0_u8; control_len];
    let message_control_len = control
        .len()
        .try_into()
        .map_err(|_| invalid_control_size(path))?;
    // SAFETY: `CMSG_LEN` computes the header length for one `RawFd` payload.
    let descriptor_control_len = unsafe { libc::CMSG_LEN(descriptor_size) };
    // SAFETY: `recvmsg` writes only into the live iovec/control allocations;
    // the returned control header is validated before the descriptor is read.
    let descriptor = unsafe {
        let mut message: libc::msghdr = mem::zeroed();
        message.msg_iov = ptr::from_mut(&mut iov);
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = message_control_len;
        let received = libc::recvmsg(stream.as_raw_fd(), &raw mut message, 0);
        if received != 1 || message.msg_flags & libc::MSG_CTRUNC != 0 {
            return Err(io_error(path, io::Error::last_os_error()));
        }
        let header = libc::CMSG_FIRSTHDR(&raw const message);
        if header.is_null()
            || (*header).cmsg_level != libc::SOL_SOCKET
            || (*header).cmsg_type != libc::SCM_RIGHTS
            || (*header).cmsg_len < descriptor_control_len
        {
            return Err(io_error(
                path,
                io::Error::new(io::ErrorKind::InvalidData, "custody descriptor was absent"),
            ));
        }
        ptr::read_unaligned(libc::CMSG_DATA(header).cast::<RawFd>())
    };
    if descriptor < 0 {
        return Err(io_error(
            path,
            io::Error::new(io::ErrorKind::InvalidData, "custody descriptor was invalid"),
        ));
    }
    // SAFETY: SCM_RIGHTS installed a new descriptor owned by this process,
    // and this is its single ownership conversion.
    let owned = unsafe { OwnedFd::from_raw_fd(descriptor) };
    set_close_on_exec(&owned, path)?;
    Ok(owned)
}

fn invalid_control_size(path: &Path) -> CustodyError {
    io_error(
        path,
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "custody control payload does not fit the platform ABI",
        ),
    )
}

fn set_close_on_exec(descriptor: &OwnedFd, path: &Path) -> Result<(), CustodyError> {
    // SAFETY: `descriptor` is live and owned; these calls only inspect and set
    // its descriptor-local close-on-exec flag.
    let flags = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_GETFD) };
    if flags < 0
        || unsafe {
            libc::fcntl(
                descriptor.as_raw_fd(),
                libc::F_SETFD,
                flags | libc::FD_CLOEXEC,
            )
        } < 0
    {
        return Err(io_error(path, io::Error::last_os_error()));
    }
    Ok(())
}

fn admit_same_uid(stream: &UnixStream, path: &Path) -> Result<(), CustodyError> {
    let uid = peer_uid(stream).map_err(|source| io_error(path, source))?;
    // SAFETY: `geteuid` has no preconditions and reads process credentials.
    let owner = unsafe { libc::geteuid() };
    if uid == owner {
        Ok(())
    } else {
        Err(CustodyError::Refused {
            reason: CustodyRefusal::ManagerMismatch,
        })
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd"
))]
fn peer_uid(stream: &UnixStream) -> Result<libc::uid_t, io::Error> {
    let mut uid = 0;
    let mut gid = 0;
    // SAFETY: the descriptor is a connected Unix stream and both output
    // pointers name live credential values.
    if unsafe { libc::getpeereid(stream.as_raw_fd(), &raw mut uid, &raw mut gid) } == 0 {
        Ok(uid)
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn peer_uid(stream: &UnixStream) -> Result<libc::uid_t, io::Error> {
    // SAFETY: `credentials` and `length` are valid outputs for SO_PEERCRED on
    // a connected Unix stream.
    unsafe {
        let mut credentials: libc::ucred = mem::zeroed();
        let mut length = mem::size_of::<libc::ucred>() as libc::socklen_t;
        if libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            ptr::from_mut(&mut credentials).cast(),
            &mut length,
        ) == 0
        {
            Ok(credentials.uid)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

fn io_error(path: &Path, source: io::Error) -> CustodyError {
    CustodyError::Io {
        path: path.to_owned(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt as _;

    #[test]
    fn test_scm_rights_transfers_the_same_open_file_description() {
        let temp = tempfile::tempdir_in("/tmp").expect("temporary custody root");
        let path = temp.path().join("lease");
        let file = File::create(&path).expect("lease file creates");
        let (sender, receiver) = UnixStream::pair().expect("socket pair creates");
        send_fd(&sender, file.as_raw_fd(), &path).expect("descriptor sends");
        let transferred = receive_fd(&receiver, &path).expect("descriptor receives");
        let expected = file.metadata().expect("source metadata");
        let actual = File::from(transferred)
            .metadata()
            .expect("received metadata");
        assert_eq!(
            (actual.dev(), actual.ino()),
            (expected.dev(), expected.ino())
        );
    }

    #[test]
    fn test_proofs_are_random_and_debug_redacted() {
        let first = generate_proof().expect("first proof generates");
        let second = generate_proof().expect("second proof generates");
        assert_ne!(first.expose_secret(), second.expose_secret());
        assert!(!format!("{first:?}").contains(first.expose_secret()));
    }

    #[test]
    fn test_two_successive_recoveries_keep_one_lease_and_rotate_proofs() {
        let temp = tempfile::tempdir_in("/tmp").expect("temporary custody root");
        let config = temp.path().join("tribal.yaml");
        let acquired = AuthorityLease::acquire_with_roots(
            &config,
            &temp.path().join("state"),
            &temp.path().join("run"),
        )
        .expect("authority prepares");
        let super::super::authority::AuthorityAcquire::Acquired(lease) = acquired else {
            panic!("unique fixture path must acquire authority");
        };
        let paths = lease.paths().clone();
        let cancellation = CancellationToken::new();
        let proof = generate_proof().expect("initial proof generates");
        let proof_text = proof.expose_secret().to_owned();
        let runtime_cancellation = cancellation.clone();
        let runtime = std::thread::spawn(move || {
            RuntimeCustodyGuard::bootstrap(
                &lease,
                runtime_cancellation,
                "runtime-one".to_owned(),
                "manager-one".to_owned(),
                proof_text,
            )
            .expect("runtime custody bootstraps")
            .expect("managed environment creates custody")
        });
        let first = ManagerCustody::attach_initial(&paths, "manager-one", proof)
            .expect("initial manager attaches");
        let guard = runtime.join().expect("runtime bootstrap joins");
        let control_proof = guard.control_proof();
        drop(first);

        let first_recovery =
            PendingRecovery::begin_with_paths("manager-two".to_owned(), paths.clone())
                .expect("first successor claims")
                .commit()
                .expect("first successor commits");
        assert_eq!(first_recovery.runtime.instance_id, "runtime-one");
        assert_eq!(
            control_proof.read().expect("control proof reads").as_str(),
            first_recovery.control_proof.expose_secret(),
        );
        let RecoveredAuthority {
            custody,
            lease,
            runtime: _,
            control_proof: _,
        } = first_recovery;
        drop(custody);
        drop(lease);

        let second_recovery = PendingRecovery::begin_with_paths("manager-three".to_owned(), paths)
            .expect("second successor claims")
            .commit()
            .expect("second successor commits");
        assert_eq!(second_recovery.runtime.instance_id, "runtime-one");
        assert_eq!(
            control_proof.read().expect("control proof reads").as_str(),
            second_recovery.control_proof.expose_secret(),
        );
        let RecoveredAuthority {
            mut custody,
            lease,
            runtime,
            control_proof: _,
        } = second_recovery;
        custody.stop(&runtime).expect("recovered runtime stops");
        assert!(cancellation.is_cancelled());
        drop(custody);
        drop(lease);
        drop(guard);
    }
}
