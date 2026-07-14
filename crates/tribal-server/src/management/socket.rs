//! Owner-only management socket and stable bootstrap handshake.

use std::{
    io,
    os::unix::fs::{MetadataExt as _, PermissionsExt as _},
    path::Path,
};

use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::{UnixListener, UnixStream},
};
use tokio_util::sync::CancellationToken;
use tribal_wire::management::{
    BootstrapShutdownRefusal, MANAGEMENT_CONTRACT_VERSION, ManagementBootstrapRequest,
    ManagementBootstrapResponse, ManagementEvent, ManagementLogLoss, ManagementMethod,
    ManagementResponseError, ManagementServerHello,
};

use super::{
    application::ManagementApplication, lifecycle::LifecycleController, probe::ProbeService,
    product::ProductService, worker::ConfigWorkerClient,
};

const SOCKET_MODE: u32 = 0o600;
const SOCKET_DIRECTORY_MODE: u32 = 0o700;
pub(crate) const MAX_FRAME_BYTES: usize = 64 * 1024;
const MAX_CONNECTIONS: usize = 32;

/// Identity a bound management socket presents during handshake.
#[derive(Debug, Clone)]
pub(crate) struct ManagerSocketIdentity {
    pub(crate) instance_id: String,
    pub(crate) binary_version: String,
}

/// Shared services retained by the management listener.
#[derive(Clone)]
pub(crate) struct ManagerSocketServices {
    config: ConfigWorkerClient,
    product: ProductService,
    probe: ProbeService,
    lifecycle: LifecycleController,
    shutdown: CancellationToken,
}

impl ManagerSocketServices {
    pub(crate) fn new(
        config: ConfigWorkerClient,
        product: ProductService,
        probe: ProbeService,
        lifecycle: LifecycleController,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            config,
            product,
            probe,
            lifecycle,
            shutdown,
        }
    }
}

struct ConnectionServices<'a> {
    application: ManagementApplication<'a>,
    shutdown: &'a CancellationToken,
}

/// Failure binding or serving the local management socket.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ManagerSocketError {
    #[error("management socket I/O failed at '{}': {source}", path.display())]
    Io {
        path: std::path::PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Binds an owner-only management socket, reclaiming only an unbound path.
pub(crate) async fn bind(path: &Path) -> Result<UnixListener, ManagerSocketError> {
    let parent = path.parent().ok_or_else(|| {
        socket_error(
            path,
            io::Error::new(io::ErrorKind::InvalidInput, "socket has no parent"),
        )
    })?;
    std::fs::create_dir_all(parent).map_err(|source| socket_error(parent, source))?;
    std::fs::set_permissions(
        parent,
        std::fs::Permissions::from_mode(SOCKET_DIRECTORY_MODE),
    )
    .map_err(|source| socket_error(parent, source))?;

    if path.exists() {
        match UnixStream::connect(path).await {
            Ok(_) => {
                return Err(socket_error(
                    path,
                    io::Error::new(io::ErrorKind::AddrInUse, "management socket is live"),
                ));
            }
            Err(_) => std::fs::remove_file(path).map_err(|source| socket_error(path, source))?,
        }
    }
    let listener = UnixListener::bind(path).map_err(|source| socket_error(path, source))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE))
        .map_err(|source| socket_error(path, source))?;
    Ok(listener)
}

/// Serves admitted bootstrap connections until shutdown is requested.
pub(crate) async fn serve(
    listener: UnixListener,
    identity: ManagerSocketIdentity,
    services: ManagerSocketServices,
) {
    let Some(owner_uid) = listener_owner_uid(&listener) else {
        return;
    };

    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = services.shutdown.cancelled() => break,
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined {
                    tracing::error!(%error, "management connection task failed");
                }
            }
            accepted = listener.accept(), if connections.len() < MAX_CONNECTIONS => match accepted {
                Ok((stream, _)) => {
                    let identity = identity.clone();
                    let services = services.clone();
                    connections.spawn(async move {
                        handle_connection(
                            stream,
                            owner_uid,
                            identity,
                            services,
                        )
                        .await;
                    });
                }
                Err(error) => tracing::warn!(%error, "management socket accept failed"),
            }
        }
    }
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            tracing::error!(%error, "management connection task failed during shutdown");
        }
    }
}

async fn handle_connection(
    stream: UnixStream,
    owner_uid: u32,
    identity: ManagerSocketIdentity,
    services: ManagerSocketServices,
) {
    let peer_uid = match stream.peer_cred() {
        Ok(credentials) => credentials.uid(),
        Err(error) => {
            tracing::warn!(%error, "management peer credentials unavailable");
            return;
        }
    };
    if peer_uid != owner_uid {
        tracing::warn!(peer_uid, owner_uid, "management peer identity refused");
        return;
    }

    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let Some(request) = read_frame::<ManagementBootstrapRequest>(&mut reader).await else {
        return;
    };
    let hello = ManagementServerHello {
        protocol_version: MANAGEMENT_CONTRACT_VERSION,
        binary_version: identity.binary_version,
        manager_instance_id: identity.instance_id,
    };
    let compatible = matches!(
        request,
        ManagementBootstrapRequest::Handshake { ref hello }
            if hello.protocol_version == MANAGEMENT_CONTRACT_VERSION
    );
    let response = match request {
        ManagementBootstrapRequest::Handshake { .. } if compatible => {
            ManagementBootstrapResponse::Compatible { hello }
        }
        ManagementBootstrapRequest::Handshake { .. } => {
            ManagementBootstrapResponse::VersionMismatch { hello }
        }
        ManagementBootstrapRequest::Shutdown => {
            services.shutdown.cancel();
            ManagementBootstrapResponse::ShutdownAccepted
        }
    };
    if write_frame(&mut write, &response).await.is_err() {
        return;
    }

    if compatible {
        let mut lifecycle_updates = services.lifecycle.subscribe();
        let mut lifecycle_events = services.lifecycle.subscribe_events();
        let mut config_updates = services.config.subscribe();
        let product = services.product.session();
        let connection = ConnectionServices {
            application: ManagementApplication::new(
                &services.config,
                &product,
                &services.probe,
                &services.lifecycle,
            ),
            shutdown: &services.shutdown,
        };
        serve_full(
            &mut reader,
            &mut write,
            &connection,
            &mut lifecycle_updates,
            &mut lifecycle_events,
            &mut config_updates,
        )
        .await;
    } else {
        serve_restricted(&mut reader, &mut write, &services.shutdown).await;
    }
}

async fn serve_restricted(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    write: &mut tokio::net::unix::OwnedWriteHalf,
    shutdown: &CancellationToken,
) {
    loop {
        let request = tokio::select! {
            () = shutdown.cancelled() => return,
            request = read_frame::<ManagementBootstrapRequest>(reader) => request,
        };
        let Some(request) = request else {
            return;
        };
        let response = match request {
            ManagementBootstrapRequest::Shutdown => {
                shutdown.cancel();
                ManagementBootstrapResponse::ShutdownAccepted
            }
            ManagementBootstrapRequest::Handshake { .. } => {
                ManagementBootstrapResponse::ShutdownRefused {
                    reason: BootstrapShutdownRefusal::ManagerTerminating,
                }
            }
        };
        if write_frame(write, &response).await.is_err() {
            return;
        }
    }
}

async fn serve_full(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    write: &mut tokio::net::unix::OwnedWriteHalf,
    services: &ConnectionServices<'_>,
    lifecycle_updates: &mut tokio::sync::watch::Receiver<
        tribal_wire::management::LifecycleSnapshot,
    >,
    lifecycle_events: &mut tokio::sync::broadcast::Receiver<ManagementEvent>,
    config_updates: &mut tokio::sync::broadcast::Receiver<
        tribal_wire::management::ConfigChangeEvent,
    >,
) {
    loop {
        tokio::select! {
            biased;
            () = services.shutdown.cancelled() => return,
            request = read_frame::<ManagementRequest>(reader) => {
                let Some(request) = request else {
                    return;
                };
                let id = request.id;
                let result = services
                    .application
                    .dispatch(request.method, request.params)
                    .await;
                let response = match result {
                    Ok(result) => ManagementResponse::Success { id, result },
                    Err(error) => ManagementResponse::Failure { id, error },
                };
                if write_frame(write, &response).await.is_err() {
                    return;
                }
            }
            changed = lifecycle_updates.changed() => {
                if changed.is_err() {
                    return;
                }
                let event = ManagementEvent::LifecycleChanged {
                    snapshot: Box::new(lifecycle_updates.borrow_and_update().clone()),
                };
                if write_frame(write, &event).await.is_err() {
                    return;
                }
            }
            event = lifecycle_events.recv() => {
                let Some(event) = public_lifecycle_event(event) else {
                    return;
                };
                if write_frame(write, &event).await.is_err() {
                    return;
                }
            }
            changed = config_updates.recv() => {
                if let Some(event) = public_config_event(changed) {
                    if write_frame(write, &event).await.is_err() {
                        return;
                    }
                } else {
                    return;
                }
            }
        }
    }
}

fn public_config_event(
    event: Result<
        tribal_wire::management::ConfigChangeEvent,
        tokio::sync::broadcast::error::RecvError,
    >,
) -> Option<ManagementEvent> {
    match event {
        Ok(change) => Some(ManagementEvent::ConfigChanged { change }),
        // Reconnection gives clients a fresh config read surface after position loss.
        Err(
            tokio::sync::broadcast::error::RecvError::Lagged(_)
            | tokio::sync::broadcast::error::RecvError::Closed,
        ) => None,
    }
}

fn public_lifecycle_event(
    event: Result<ManagementEvent, tokio::sync::broadcast::error::RecvError>,
) -> Option<ManagementEvent> {
    match event {
        Ok(event) => Some(event),
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
            Some(ManagementEvent::LogsLost {
                loss: ManagementLogLoss::ObservationInterrupted,
            })
        }
        Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
    }
}

fn listener_owner_uid(listener: &UnixListener) -> Option<u32> {
    let Some(path) = listener
        .local_addr()
        .ok()
        .and_then(|address| address.as_pathname().map(std::path::Path::to_owned))
    else {
        tracing::error!("management socket has no filesystem pathname");
        return None;
    };
    match std::fs::metadata(&path) {
        Ok(metadata) => Some(metadata.uid()),
        Err(error) => {
            tracing::error!(%error, path = %path.display(), "management socket metadata unavailable");
            None
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct ManagementRequest {
    id: u64,
    method: ManagementMethod,
    #[serde(default)]
    params: Option<serde_json::Value>,
}

#[derive(Debug, serde::Serialize)]
#[serde(untagged)]
enum ManagementResponse {
    Success {
        id: u64,
        result: serde_json::Value,
    },
    Failure {
        id: u64,
        error: ManagementResponseError,
    },
}

async fn read_frame<T: serde::de::DeserializeOwned>(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> Option<T> {
    let mut bytes = Vec::new();
    let read = reader
        .take((MAX_FRAME_BYTES + 1) as u64)
        .read_until(b'\n', &mut bytes)
        .await
        .ok()?;
    if read == 0 || read > MAX_FRAME_BYTES || bytes.last() != Some(&b'\n') {
        return None;
    }
    bytes.pop();
    serde_json::from_slice(&bytes).ok()
}

async fn write_frame<T: serde::Serialize>(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    value: &T,
) -> Result<(), io::Error> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
    if bytes.len() + 1 > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "management frame exceeds size limit",
        ));
    }
    bytes.push(b'\n');
    writer.write_all(&bytes).await
}

fn socket_error(path: &Path, source: io::Error) -> ManagerSocketError {
    ManagerSocketError::Io {
        path: path.to_owned(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_wire::management::ManagementClientHello;

    use super::*;

    #[test]
    fn test_mixed_public_event_lag_never_claims_a_line_count() {
        assert_eq!(
            public_lifecycle_event(Err(tokio::sync::broadcast::error::RecvError::Lagged(7))),
            Some(ManagementEvent::LogsLost {
                loss: ManagementLogLoss::ObservationInterrupted,
            })
        );
    }

    #[test]
    fn test_config_lag_terminates_the_stream_for_a_fresh_read() {
        assert!(
            public_config_event(Err(tokio::sync::broadcast::error::RecvError::Lagged(3))).is_none()
        );
    }

    #[tokio::test]
    async fn test_compatible_and_mismatched_clients_get_restricted_handshakes() {
        let temp = tempfile::tempdir().expect("temporary socket root");
        for (version, expected_compatible) in [
            (MANAGEMENT_CONTRACT_VERSION, true),
            (MANAGEMENT_CONTRACT_VERSION + 1, false),
        ] {
            let path = temp.path().join(format!("manager-{version}.sock"));
            let config_path = temp.path().join(format!("tribal-{version}.yaml"));
            let config = tribal_config::TribalConfig::minimum_valid(
                "postgres://user:pass@localhost:5432/tribal",
            );
            std::fs::write(
                &config_path,
                serde_yaml::to_string(&config).expect("config serialises"),
            )
            .expect("config writes");
            let (config, mut worker_runtime) = super::super::worker::spawn(
                super::super::configuration::ConfigAuthority::new(config_path.clone()),
            )
            .expect("config worker starts");
            let config_terminal = worker_runtime
                .take_terminal()
                .expect("worker terminal has one owner");
            let authority = super::super::authority::AuthorityLease::acquire(&config_path)
                .expect("authority acquisition succeeds");
            let super::super::authority::AuthorityAcquire::Acquired(authority) = authority else {
                panic!("test owns its unique config authority");
            };
            let listener = bind(&path).await.expect("management socket binds");
            let shutdown = CancellationToken::new();
            let (lifecycle, lifecycle_task) = super::super::lifecycle::LifecycleController::spawn(
                "manager".to_owned(),
                config_path,
                config.clone(),
                Arc::new(authority),
                shutdown.clone(),
                config_terminal,
                None,
            )
            .await
            .expect("lifecycle starts");
            let task = tokio::spawn(serve(
                listener,
                ManagerSocketIdentity {
                    instance_id: "manager".to_owned(),
                    binary_version: "test".to_owned(),
                },
                ManagerSocketServices::new(
                    config.clone(),
                    ProductService::new(config.clone()),
                    ProbeService::new(config),
                    lifecycle,
                    shutdown.clone(),
                ),
            ));
            let mut stream = UnixStream::connect(&path).await.expect("client connects");
            let request = ManagementBootstrapRequest::Handshake {
                hello: ManagementClientHello {
                    protocol_version: version,
                },
            };
            let mut bytes = serde_json::to_vec(&request).expect("request serialises");
            bytes.push(b'\n');
            stream.write_all(&bytes).await.expect("request writes");
            let mut reader = BufReader::new(stream);
            let mut response = String::new();
            reader
                .read_line(&mut response)
                .await
                .expect("response reads");
            let response: ManagementBootstrapResponse =
                serde_json::from_str(&response).expect("response deserialises");
            assert_eq!(
                matches!(response, ManagementBootstrapResponse::Compatible { .. }),
                expected_compatible,
            );
            if expected_compatible {
                let mut request = serde_json::to_vec(&serde_json::json!({
                    "id": 1,
                    "method": ManagementMethod::ConfigSchema,
                }))
                .expect("schema request serialises");
                request.push(b'\n');
                reader
                    .get_mut()
                    .write_all(&request)
                    .await
                    .expect("schema request writes");
                let mut response = String::new();
                reader
                    .read_line(&mut response)
                    .await
                    .expect("schema response reads");
                let response: serde_json::Value =
                    serde_json::from_str(&response).expect("schema response parses");
                let schema: tribal_wire::management::ConfigSchema =
                    serde_json::from_value(response["result"].clone())
                        .expect("schema response uses the public DTO");
                assert!(schema.schema.is_object());
                assert!(!schema.fields.is_empty());
                assert_eq!(schema.groups, tribal_config::config_schema().groups);
                shutdown.cancel();
            } else {
                super::super::client::ManagementClient::request_shutdown(
                    &super::super::authority::AuthorityDescriptor {
                        kind: super::super::authority::AuthorityOwnerKind::Manager,
                        instance_id: "manager".to_owned(),
                        pid: std::process::id(),
                        binary_version: "test".to_owned(),
                        canonical_config_path: temp.path().join(format!("tribal-{version}.yaml")),
                        socket_path: Some(path.clone()),
                        protocol_version: Some(version),
                    },
                )
                .await
                .expect("restricted replacement requests shutdown");
                assert!(shutdown.is_cancelled());
            }
            task.await.expect("socket task joins");
            lifecycle_task.await.expect("lifecycle task joins");
            worker_runtime.join().expect("config worker joins");
        }
    }
}
