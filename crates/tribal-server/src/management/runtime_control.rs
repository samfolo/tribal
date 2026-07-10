//! Private authenticated control seam between a manager and its runtime.

use std::{
    io,
    os::fd::AsRawFd as _,
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use sqlx::PgPool;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::{UnixListener, UnixStream},
    sync::{Mutex, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tribal_config::{ReloadClass, TribalConfig, load_config, reload_class, validate};
use tribal_telemetry::{LogFilterHandle, LogRing};
use tribal_wire::{
    management::{ConfigDigest, ConfigRevision, RuntimeIdentity, TokenList},
    runtime_control::{
        ManagedRuntimeStatus, RUNTIME_CONTROL_CONTRACT_VERSION, RuntimeBootstrapRefusal,
        RuntimeBootstrapRequest, RuntimeBootstrapResponse, RuntimeConfigApplyOutcome,
        RuntimeConfigApplyRefusal, RuntimeConfigChange, RuntimeControlClientHello,
        RuntimeControlRefusal, RuntimeControlRequest, RuntimeControlResponse,
        RuntimeControlServerHello, RuntimeCustodyProof,
    },
};

use super::{custody::RuntimeProofRegistry, readiness};

const SOCKET_MODE: u32 = 0o600;
const SOCKET_DIRECTORY_MODE: u32 = 0o700;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const CONNECT_DEADLINE: Duration = Duration::from_secs(10);
const REQUEST_DEADLINE: Duration = Duration::from_secs(10);

type FramedStream = BufReader<UnixStream>;

/// Runtime-side dependencies used by the private protocol.
#[derive(Clone)]
pub(crate) struct RuntimeControlService {
    pub(crate) runtime: RuntimeIdentity,
    pub(crate) config_path: PathBuf,
    pub(crate) config: watch::Sender<Arc<TribalConfig>>,
    pub(crate) log_filter: LogFilterHandle,
    pub(crate) log_ring: LogRing,
    pub(crate) pool: PgPool,
    pub(crate) shutdown: CancellationToken,
}

/// Authenticated capability retained by a lease-backed lifecycle state.
#[derive(Clone)]
pub(crate) enum RuntimeControlConnection {
    Compatible(RuntimeControlClient),
    VersionMismatch(BootstrapStopClient),
}

/// Cloneable compatible client; the stream remains serialised behind one lock.
#[derive(Clone)]
pub(crate) struct RuntimeControlClient {
    stream: Arc<Mutex<FramedStream>>,
}

/// Restricted stop capability retained across a private protocol mismatch.
#[derive(Clone)]
pub(crate) struct BootstrapStopClient {
    stream: Arc<Mutex<FramedStream>>,
    proof: Arc<zeroize::Zeroizing<String>>,
}

/// Failure binding, serving, or connecting the private control seam.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RuntimeControlError {
    #[error("runtime-control I/O failed at '{}': {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("runtime-control frame encoding failed: {source}")]
    Encoding {
        #[source]
        source: serde_json::Error,
    },
    #[error("runtime-control peer identity was refused")]
    PeerIdentity,
    #[error("runtime-control custody proof was refused")]
    ProofRefused,
    #[error("runtime-control returned a different runtime identity")]
    RuntimeIdentityMismatch,
    #[error("runtime-control connection closed before a response")]
    Closed,
    #[error("runtime-control connection timed out")]
    TimedOut,
}

impl std::fmt::Debug for RuntimeControlConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Compatible(_) => "RuntimeControlConnection::Compatible",
            Self::VersionMismatch(_) => "RuntimeControlConnection::VersionMismatch",
        })
    }
}

impl RuntimeControlConnection {
    #[cfg(test)]
    pub(crate) fn pair_for_test() -> io::Result<(Self, UnixStream)> {
        let (manager, runtime) = UnixStream::pair()?;
        let client = RuntimeControlClient {
            stream: Arc::new(Mutex::new(BufReader::new(manager))),
        };
        Ok((Self::Compatible(client), runtime))
    }

    #[cfg(test)]
    pub(crate) fn version_mismatch_pair_for_test() -> io::Result<(Self, UnixStream)> {
        let (manager, runtime) = UnixStream::pair()?;
        let client = BootstrapStopClient {
            stream: Arc::new(Mutex::new(BufReader::new(manager))),
            proof: Arc::new(zeroize::Zeroizing::new("test-proof".to_owned())),
        };
        Ok((Self::VersionMismatch(client), runtime))
    }

    pub(crate) fn is_compatible(&self) -> bool {
        matches!(self, Self::Compatible(_))
    }

    pub(crate) fn compatible(&self) -> Option<RuntimeControlClient> {
        match self {
            Self::Compatible(client) => Some(client.clone()),
            Self::VersionMismatch(_) => None,
        }
    }

    /// Observes authenticated runtime-control EOF without consuming a frame.
    pub(crate) fn is_closed(&self) -> bool {
        match self {
            Self::Compatible(client) => stream_is_closed(&client.stream),
            Self::VersionMismatch(client) => stream_is_closed(&client.stream),
        }
    }

    pub(crate) async fn stop(&self, runtime: &RuntimeIdentity) -> Result<(), RuntimeControlError> {
        match self {
            Self::Compatible(client) => client.stop(runtime).await,
            Self::VersionMismatch(client) => client.stop(runtime).await,
        }
    }
}

impl RuntimeControlClient {
    /// Connects through the stable bootstrap and retains the admitted capability.
    pub(crate) async fn connect(
        path: &Path,
        manager_instance_id: &str,
        expected_runtime: &RuntimeIdentity,
        proof: RuntimeCustodyProof,
    ) -> Result<RuntimeControlConnection, RuntimeControlError> {
        let proof_copy = zeroize::Zeroizing::new(proof.expose_secret().to_owned());
        let stream = tokio::time::timeout(CONNECT_DEADLINE, connect_with_retry(path))
            .await
            .map_err(|_| RuntimeControlError::TimedOut)??;
        let mut stream = BufReader::new(stream);
        write_frame(
            stream.get_mut(),
            &RuntimeBootstrapRequest::Handshake {
                hello: RuntimeControlClientHello {
                    protocol_version: RUNTIME_CONTROL_CONTRACT_VERSION,
                    manager_instance_id: manager_instance_id.to_owned(),
                },
                proof,
            },
        )
        .await?;
        let response: RuntimeBootstrapResponse = read_frame(&mut stream)
            .await?
            .ok_or(RuntimeControlError::Closed)?;
        match response {
            RuntimeBootstrapResponse::Compatible { hello } => {
                verify_runtime(&hello, expected_runtime)?;
                Ok(RuntimeControlConnection::Compatible(Self {
                    stream: Arc::new(Mutex::new(stream)),
                }))
            }
            RuntimeBootstrapResponse::VersionMismatch { hello } => {
                verify_runtime(&hello, expected_runtime)?;
                Ok(RuntimeControlConnection::VersionMismatch(
                    BootstrapStopClient {
                        stream: Arc::new(Mutex::new(stream)),
                        proof: Arc::new(proof_copy),
                    },
                ))
            }
            RuntimeBootstrapResponse::Refused { reason } => Err(match reason {
                RuntimeBootstrapRefusal::PeerIdentityMismatch => RuntimeControlError::PeerIdentity,
                RuntimeBootstrapRefusal::CustodyProofInvalid
                | RuntimeBootstrapRefusal::RuntimeIdentityMismatch
                | RuntimeBootstrapRefusal::RuntimeTerminating => RuntimeControlError::ProofRefused,
            }),
            RuntimeBootstrapResponse::StopAccepted => Err(RuntimeControlError::ProofRefused),
        }
    }

    pub(crate) async fn apply_config(
        &self,
        revision: ConfigRevision,
        changes: Vec<RuntimeConfigChange>,
    ) -> Result<RuntimeConfigApplyOutcome, RuntimeControlError> {
        let response = self
            .request(RuntimeControlRequest::ApplyConfig { revision, changes })
            .await?;
        match response {
            RuntimeControlResponse::ApplyConfig { outcome } => Ok(outcome),
            RuntimeControlResponse::Refused { .. }
            | RuntimeControlResponse::Status { .. }
            | RuntimeControlResponse::Readiness { .. }
            | RuntimeControlResponse::StopAccepted
            | RuntimeControlResponse::LogsTail { .. }
            | RuntimeControlResponse::TokenList { .. } => Err(RuntimeControlError::Closed),
        }
    }

    pub(crate) async fn status(&self) -> Result<ManagedRuntimeStatus, RuntimeControlError> {
        match self.request(RuntimeControlRequest::Status).await? {
            RuntimeControlResponse::Status { status } => Ok(status),
            RuntimeControlResponse::Readiness { .. }
            | RuntimeControlResponse::ApplyConfig { .. }
            | RuntimeControlResponse::StopAccepted
            | RuntimeControlResponse::LogsTail { .. }
            | RuntimeControlResponse::TokenList { .. }
            | RuntimeControlResponse::Refused { .. } => Err(RuntimeControlError::Closed),
        }
    }

    pub(crate) async fn logs_tail(&self, lines: u32) -> Result<Vec<String>, RuntimeControlError> {
        match self
            .request(RuntimeControlRequest::LogsTail { lines })
            .await?
        {
            RuntimeControlResponse::LogsTail { lines } => Ok(lines),
            RuntimeControlResponse::Status { .. }
            | RuntimeControlResponse::Readiness { .. }
            | RuntimeControlResponse::ApplyConfig { .. }
            | RuntimeControlResponse::StopAccepted
            | RuntimeControlResponse::TokenList { .. }
            | RuntimeControlResponse::Refused { .. } => Err(RuntimeControlError::Closed),
        }
    }

    pub(crate) async fn token_list(&self) -> Result<TokenList, RuntimeControlError> {
        match self.request(RuntimeControlRequest::TokenList).await? {
            RuntimeControlResponse::TokenList { list } => Ok(list),
            RuntimeControlResponse::Status { .. }
            | RuntimeControlResponse::Readiness { .. }
            | RuntimeControlResponse::ApplyConfig { .. }
            | RuntimeControlResponse::StopAccepted
            | RuntimeControlResponse::LogsTail { .. }
            | RuntimeControlResponse::Refused { .. } => Err(RuntimeControlError::Closed),
        }
    }

    async fn stop(&self, runtime: &RuntimeIdentity) -> Result<(), RuntimeControlError> {
        let response = self
            .request(RuntimeControlRequest::Stop {
                runtime: runtime.clone(),
            })
            .await?;
        match response {
            RuntimeControlResponse::StopAccepted => Ok(()),
            RuntimeControlResponse::Refused { .. }
            | RuntimeControlResponse::Status { .. }
            | RuntimeControlResponse::Readiness { .. }
            | RuntimeControlResponse::ApplyConfig { .. }
            | RuntimeControlResponse::LogsTail { .. }
            | RuntimeControlResponse::TokenList { .. } => Err(RuntimeControlError::Closed),
        }
    }

    async fn request(
        &self,
        request: RuntimeControlRequest,
    ) -> Result<RuntimeControlResponse, RuntimeControlError> {
        tokio::time::timeout(REQUEST_DEADLINE, async {
            let mut stream = self.stream.lock().await;
            write_frame(stream.get_mut(), &request).await?;
            read_frame(&mut stream)
                .await?
                .ok_or(RuntimeControlError::Closed)
        })
        .await
        .map_err(|_| RuntimeControlError::TimedOut)?
    }
}

impl BootstrapStopClient {
    async fn stop(&self, runtime: &RuntimeIdentity) -> Result<(), RuntimeControlError> {
        tokio::time::timeout(REQUEST_DEADLINE, async {
            let mut stream = self.stream.lock().await;
            write_frame(
                stream.get_mut(),
                &RuntimeBootstrapRequest::RestrictedStop {
                    runtime: runtime.clone(),
                    proof: RuntimeCustodyProof::new(self.proof.to_string()),
                },
            )
            .await?;
            match read_frame::<RuntimeBootstrapResponse>(&mut stream).await? {
                Some(RuntimeBootstrapResponse::StopAccepted) => Ok(()),
                Some(
                    RuntimeBootstrapResponse::Compatible { .. }
                    | RuntimeBootstrapResponse::VersionMismatch { .. }
                    | RuntimeBootstrapResponse::Refused { .. },
                )
                | None => Err(RuntimeControlError::Closed),
            }
        })
        .await
        .map_err(|_| RuntimeControlError::TimedOut)?
    }
}

/// Binds the owner-only runtime socket and returns its tracked serving task.
pub(crate) async fn spawn_server(
    path: PathBuf,
    expected_proof: RuntimeProofRegistry,
    service: RuntimeControlService,
) -> Result<JoinHandle<()>, RuntimeControlError> {
    let listener = bind(&path).await?;
    Ok(tokio::spawn(serve(listener, path, expected_proof, service)))
}

async fn serve(
    listener: UnixListener,
    path: PathBuf,
    proof: RuntimeProofRegistry,
    service: RuntimeControlService,
) {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = service.shutdown.cancelled() => break,
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined {
                    tracing::error!(%error, "runtime-control connection task failed");
                }
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let proof = Arc::clone(&proof);
                    let service = service.clone();
                    let path = path.clone();
                    connections.spawn(async move {
                        if let Err(error) = handle_connection(stream, &path, &proof, &service).await {
                            tracing::warn!(%error, "runtime-control connection closed");
                        }
                    });
                }
                Err(error) => tracing::warn!(%error, "runtime-control accept failed"),
            }
        }
    }
    let drain = async { while connections.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(1), drain)
        .await
        .is_err()
    {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
    let _ = std::fs::remove_file(path);
}

async fn handle_connection(
    stream: UnixStream,
    path: &Path,
    expected_proof: &RuntimeProofRegistry,
    service: &RuntimeControlService,
) -> Result<(), RuntimeControlError> {
    let credentials = stream
        .peer_cred()
        .map_err(|source| io_error(path, source))?;
    // SAFETY: `geteuid` reads immutable process identity and has no preconditions.
    let owner_uid = unsafe { libc::geteuid() };
    if credentials.uid() != owner_uid {
        return Err(RuntimeControlError::PeerIdentity);
    }
    let mut stream = BufReader::new(stream);
    let request: RuntimeBootstrapRequest = read_frame(&mut stream)
        .await?
        .ok_or(RuntimeControlError::Closed)?;
    let RuntimeBootstrapRequest::Handshake { hello, proof } = request else {
        write_frame(
            stream.get_mut(),
            &RuntimeBootstrapResponse::Refused {
                reason: RuntimeBootstrapRefusal::CustodyProofInvalid,
            },
        )
        .await?;
        return Ok(());
    };
    if !proof_matches(expected_proof, proof.expose_secret()) {
        write_frame(
            stream.get_mut(),
            &RuntimeBootstrapResponse::Refused {
                reason: RuntimeBootstrapRefusal::CustodyProofInvalid,
            },
        )
        .await?;
        return Ok(());
    }
    let server_hello = RuntimeControlServerHello {
        protocol_version: RUNTIME_CONTROL_CONTRACT_VERSION,
        binary_version: env!("CARGO_PKG_VERSION").to_owned(),
        runtime: service.runtime.clone(),
    };
    if hello.protocol_version == RUNTIME_CONTROL_CONTRACT_VERSION {
        write_frame(
            stream.get_mut(),
            &RuntimeBootstrapResponse::Compatible {
                hello: server_hello,
            },
        )
        .await?;
        serve_compatible(&mut stream, service).await
    } else {
        write_frame(
            stream.get_mut(),
            &RuntimeBootstrapResponse::VersionMismatch {
                hello: server_hello,
            },
        )
        .await?;
        serve_restricted(&mut stream, expected_proof, service).await
    }
}

async fn serve_compatible(
    stream: &mut FramedStream,
    service: &RuntimeControlService,
) -> Result<(), RuntimeControlError> {
    while let Some(request) = read_frame::<RuntimeControlRequest>(stream).await? {
        let response = dispatch(request, service).await;
        write_frame(stream.get_mut(), &response).await?;
        if matches!(response, RuntimeControlResponse::StopAccepted) {
            return Ok(());
        }
    }
    Ok(())
}

async fn serve_restricted(
    stream: &mut FramedStream,
    expected_proof: &RuntimeProofRegistry,
    service: &RuntimeControlService,
) -> Result<(), RuntimeControlError> {
    while let Some(request) = read_frame::<RuntimeBootstrapRequest>(stream).await? {
        let response = match request {
            RuntimeBootstrapRequest::RestrictedStop { runtime, proof }
                if runtime == service.runtime
                    && proof_matches(expected_proof, proof.expose_secret()) =>
            {
                service.shutdown.cancel();
                RuntimeBootstrapResponse::StopAccepted
            }
            RuntimeBootstrapRequest::RestrictedStop { .. }
            | RuntimeBootstrapRequest::Handshake { .. } => RuntimeBootstrapResponse::Refused {
                reason: RuntimeBootstrapRefusal::RuntimeIdentityMismatch,
            },
        };
        write_frame(stream.get_mut(), &response).await?;
        if matches!(response, RuntimeBootstrapResponse::StopAccepted) {
            return Ok(());
        }
    }
    Ok(())
}

async fn dispatch(
    request: RuntimeControlRequest,
    service: &RuntimeControlService,
) -> RuntimeControlResponse {
    match request {
        RuntimeControlRequest::Status => RuntimeControlResponse::Status {
            status: ManagedRuntimeStatus {
                runtime: service.runtime.clone(),
            },
        },
        RuntimeControlRequest::Readiness => {
            let config = service.config.borrow().as_ref().clone();
            let report = crate::commands::run_report_async(crate::commands::CheckReportOptions {
                config_path: &service.config_path,
                source: crate::commands::CheckConfigSource::Parsed(Box::new(config)),
                providers: false,
                project: None,
                token: None,
            })
            .await
            .map_or_else(
                |_| readiness::derive(Vec::new(), true, Vec::new()),
                |report| readiness::from_results(report.checks, true),
            );
            RuntimeControlResponse::Readiness { report }
        }
        RuntimeControlRequest::ApplyConfig { revision, changes } => {
            RuntimeControlResponse::ApplyConfig {
                outcome: apply_config(service, &revision, &changes).await,
            }
        }
        RuntimeControlRequest::Stop { runtime } if runtime == service.runtime => {
            service.shutdown.cancel();
            RuntimeControlResponse::StopAccepted
        }
        RuntimeControlRequest::Stop { .. } => RuntimeControlResponse::Refused {
            reason: RuntimeControlRefusal::RuntimeIdentityMismatch,
        },
        RuntimeControlRequest::LogsTail { lines } => RuntimeControlResponse::LogsTail {
            lines: service
                .log_ring
                .tail(lines as usize)
                .into_iter()
                .map(|line| line.message)
                .collect(),
        },
        RuntimeControlRequest::TokenList => {
            match crate::control::list_local_token_metadata(&service.pool).await {
                Ok(list) => RuntimeControlResponse::TokenList { list },
                Err(_) => RuntimeControlResponse::Refused {
                    reason: RuntimeControlRefusal::OperationUnavailable,
                },
            }
        }
    }
}

async fn apply_config(
    service: &RuntimeControlService,
    revision: &ConfigRevision,
    changes: &[RuntimeConfigChange],
) -> RuntimeConfigApplyOutcome {
    if changes
        .iter()
        .any(|change| reload_class(change.key.as_str()) != ReloadClass::Hot)
    {
        return RuntimeConfigApplyOutcome::RestartRequired;
    }
    let path = service.config_path.clone();
    let observed = tokio::task::spawn_blocking(move || {
        let bytes = std::fs::read(&path)?;
        let config =
            load_config(path.to_string_lossy().as_ref(), None, None).map_err(io::Error::other)?;
        validate(&config).map_err(io::Error::other)?;
        Ok::<_, io::Error>((bytes, config))
    })
    .await;
    let Ok(Ok(observed)) = observed else {
        return RuntimeConfigApplyOutcome::Refused {
            reason: RuntimeConfigApplyRefusal::ConfigUnavailable,
        };
    };
    let observed_revision = ConfigRevision::from_digest(&ConfigDigest::from_bytes(&observed.0));
    if &observed_revision != revision {
        return RuntimeConfigApplyOutcome::Refused {
            reason: RuntimeConfigApplyRefusal::RevisionStale,
        };
    }
    for change in changes {
        if change.key.as_str() == "logging.level" {
            if change.value.expose_sensitive()
                != &serde_json::Value::String(observed.1.logging.level.clone())
            {
                return RuntimeConfigApplyOutcome::Refused {
                    reason: RuntimeConfigApplyRefusal::RevisionStale,
                };
            }
            if service
                .log_filter
                .reload(&observed.1.logging.level)
                .is_err()
            {
                return RuntimeConfigApplyOutcome::RestartRequired;
            }
        }
    }
    service.config.send_replace(Arc::new(observed.1));
    RuntimeConfigApplyOutcome::Applied
}

async fn bind(path: &Path) -> Result<UnixListener, RuntimeControlError> {
    let parent = path.parent().ok_or_else(|| {
        io_error(
            path,
            io::Error::new(io::ErrorKind::InvalidInput, "socket has no parent"),
        )
    })?;
    std::fs::create_dir_all(parent).map_err(|source| io_error(parent, source))?;
    std::fs::set_permissions(
        parent,
        std::fs::Permissions::from_mode(SOCKET_DIRECTORY_MODE),
    )
    .map_err(|source| io_error(parent, source))?;
    if path.exists() {
        match UnixStream::connect(path).await {
            Ok(_) => {
                return Err(io_error(
                    path,
                    io::Error::new(io::ErrorKind::AddrInUse, "runtime-control socket is live"),
                ));
            }
            Err(_) => std::fs::remove_file(path).map_err(|source| io_error(path, source))?,
        }
    }
    let listener = UnixListener::bind(path).map_err(|source| io_error(path, source))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE))
        .map_err(|source| io_error(path, source))?;
    Ok(listener)
}

async fn connect_with_retry(path: &Path) -> Result<UnixStream, RuntimeControlError> {
    loop {
        match UnixStream::connect(path).await {
            Ok(stream) => return Ok(stream),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) =>
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(source) => return Err(io_error(path, source)),
        }
    }
}

async fn write_frame<T: serde::Serialize>(
    stream: &mut UnixStream,
    value: &T,
) -> Result<(), RuntimeControlError> {
    let mut bytes =
        serde_json::to_vec(value).map_err(|source| RuntimeControlError::Encoding { source })?;
    bytes.push(b'\n');
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(io_error(
            Path::new("runtime-control-frame"),
            io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime-control frame is oversized",
            ),
        ));
    }
    stream
        .write_all(&bytes)
        .await
        .map_err(|source| io_error(Path::new("runtime-control-stream"), source))
}

async fn read_frame<T: serde::de::DeserializeOwned>(
    stream: &mut FramedStream,
) -> Result<Option<T>, RuntimeControlError> {
    let mut bytes = Vec::new();
    let count = stream
        .take(MAX_FRAME_BYTES as u64)
        .read_until(b'\n', &mut bytes)
        .await
        .map_err(|source| io_error(Path::new("runtime-control-stream"), source))?;
    if count == 0 {
        return Ok(None);
    }
    if bytes.last() != Some(&b'\n') {
        return Err(io_error(
            Path::new("runtime-control-frame"),
            io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime-control frame is oversized",
            ),
        ));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|source| RuntimeControlError::Encoding { source })
}

fn verify_runtime(
    hello: &RuntimeControlServerHello,
    expected_runtime: &RuntimeIdentity,
) -> Result<(), RuntimeControlError> {
    if &hello.runtime == expected_runtime {
        Ok(())
    } else {
        Err(RuntimeControlError::RuntimeIdentityMismatch)
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn proof_matches(registry: &RuntimeProofRegistry, candidate: &str) -> bool {
    let expected = registry
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    constant_time_eq(candidate.as_bytes(), expected.as_bytes())
}

fn stream_is_closed(stream: &Arc<Mutex<FramedStream>>) -> bool {
    let Ok(stream) = stream.try_lock() else {
        return false;
    };
    let fd = stream.get_ref().as_raw_fd();
    let mut byte = 0_u8;
    // SAFETY: `fd` is a live Unix-stream descriptor and `byte` is a valid
    // one-byte output buffer. Peeking consumes no protocol data.
    let read = unsafe {
        libc::recv(
            fd,
            std::ptr::from_mut(&mut byte).cast(),
            1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    read == 0
}

fn io_error(path: &Path, source: io::Error) -> RuntimeControlError {
    RuntimeControlError::Io {
        path: path.to_owned(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::{Registry, layer::SubscriberExt as _};

    use super::*;
    use tribal_wire::management::ConfigFilePath;

    #[tokio::test]
    async fn test_compatible_client_authenticates_and_stops_exact_runtime() {
        let temp = tempfile::tempdir().expect("temporary runtime-control root");
        let socket_path = temp.path().join("runtime.sock");
        let config_path = temp.path().join("tribal.yaml");
        let config = TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/tribal");
        std::fs::write(
            &config_path,
            serde_yaml::to_string(&config).expect("configuration serialises"),
        )
        .expect("configuration writes");
        let (filter_layer, log_filter) =
            tribal_telemetry::reloadable_env_filter("info").expect("filter starts");
        let _subscriber = Registry::default().with(filter_layer);
        let shutdown = CancellationToken::new();
        let config_sender = watch::Sender::new(Arc::new(config.clone()));
        let runtime = RuntimeIdentity {
            instance_id: "runtime-one".to_owned(),
            pid: std::process::id(),
            binary_version: env!("CARGO_PKG_VERSION").to_owned(),
            config_path: ConfigFilePath {
                path: config_path.to_string_lossy().into_owned(),
            },
        };
        let proof = "runtime-proof";
        let proof_registry = Arc::new(std::sync::RwLock::new(zeroize::Zeroizing::new(
            proof.to_owned(),
        )));
        let task = spawn_server(
            socket_path.clone(),
            proof_registry,
            RuntimeControlService {
                runtime: runtime.clone(),
                config_path: config_path.clone(),
                config: config_sender.clone(),
                log_filter,
                log_ring: LogRing::new(8),
                pool: sqlx::postgres::PgPoolOptions::new()
                    .connect_lazy("postgres://localhost/tribal")
                    .expect("lazy pool builds"),
                shutdown: shutdown.clone(),
            },
        )
        .await
        .expect("runtime-control server binds");
        let connection = RuntimeControlClient::connect(
            &socket_path,
            "manager-one",
            &runtime,
            RuntimeCustodyProof::new(proof.to_owned()),
        )
        .await
        .expect("runtime-control handshake succeeds");

        assert!(connection.is_compatible());
        let full_client = connection
            .compatible()
            .expect("compatible connection has a full client");
        assert_eq!(
            full_client.status().await.expect("status returns").runtime,
            runtime,
        );
        assert!(
            full_client
                .logs_tail(8)
                .await
                .expect("log tail returns")
                .is_empty()
        );
        let mut updated = config;
        updated.logging.level = "debug".to_owned();
        let bytes = serde_yaml::to_string(&updated)
            .expect("updated configuration serialises")
            .into_bytes();
        std::fs::write(&config_path, &bytes).expect("updated configuration writes");
        let outcome = full_client
            .apply_config(
                ConfigRevision::from_digest(&ConfigDigest::from_bytes(&bytes)),
                vec![RuntimeConfigChange {
                    key: tribal_domain::ConfigFieldPath::parse("logging.level")
                        .expect("logging field path parses"),
                    value: tribal_wire::management::ConfigLiteral::new(serde_json::json!("debug")),
                }],
            )
            .await
            .expect("live apply returns");
        assert_eq!(outcome, RuntimeConfigApplyOutcome::Applied);
        assert_eq!(config_sender.borrow().logging.level, "debug");
        connection.stop(&runtime).await.expect("stop is accepted");
        assert!(shutdown.is_cancelled());
        task.await.expect("runtime-control server joins");
    }
}
