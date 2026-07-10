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
    BootstrapShutdownRefusal, ConfigPersistenceObservation, ConfigPersistencePhase,
    MANAGEMENT_CONTRACT_VERSION, ManagementBootstrapRequest, ManagementBootstrapResponse,
    ManagementError, ManagementEvent, ManagementResponseError, ManagementServerHello,
};

use super::{
    configuration::ConfigAuthorityError,
    lifecycle::LifecycleController,
    product::{ProductService, ProductSession},
    readiness,
    worker::ConfigWorkerClient,
};

const SOCKET_MODE: u32 = 0o600;
const SOCKET_DIRECTORY_MODE: u32 = 0o700;
const MAX_FRAME_BYTES: usize = 64 * 1024;
const MAX_CONNECTIONS: usize = 32;

/// Identity a bound management socket presents during handshake.
#[derive(Debug, Clone)]
pub(crate) struct ManagerSocketIdentity {
    pub(crate) instance_id: String,
    pub(crate) binary_version: String,
}

struct ConnectionServices<'a> {
    config: &'a ConfigWorkerClient,
    product: &'a ProductSession,
    lifecycle: &'a LifecycleController,
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
    config: ConfigWorkerClient,
    product: ProductService,
    lifecycle: LifecycleController,
    shutdown: CancellationToken,
) {
    let Some(owner_uid) = listener_owner_uid(&listener) else {
        return;
    };

    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined {
                    tracing::error!(%error, "management connection task failed");
                }
            }
            accepted = listener.accept(), if connections.len() < MAX_CONNECTIONS => match accepted {
                Ok((stream, _)) => {
                    let identity = identity.clone();
                    let config = config.clone();
                    let product = product.clone();
                    let lifecycle = lifecycle.clone();
                    let shutdown = shutdown.clone();
                    connections.spawn(async move {
                        handle_connection(
                            stream,
                            owner_uid,
                            identity,
                            config,
                            product,
                            lifecycle,
                            shutdown,
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
    config: ConfigWorkerClient,
    product: ProductService,
    lifecycle: LifecycleController,
    shutdown: CancellationToken,
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
            shutdown.cancel();
            ManagementBootstrapResponse::ShutdownAccepted
        }
    };
    if write_frame(&mut write, &response).await.is_err() {
        return;
    }

    if compatible {
        let mut lifecycle_updates = lifecycle.subscribe();
        let mut config_updates = config.subscribe();
        let product = product.session();
        let services = ConnectionServices {
            config: &config,
            product: &product,
            lifecycle: &lifecycle,
            shutdown: &shutdown,
        };
        serve_full(
            &mut reader,
            &mut write,
            &services,
            &mut lifecycle_updates,
            &mut config_updates,
        )
        .await;
    } else {
        serve_restricted(&mut reader, &mut write, &shutdown).await;
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
                let response = match dispatch(
                    services.config,
                    services.product,
                    services.lifecycle,
                    request,
                ).await {
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
            changed = config_updates.recv() => match changed {
                Ok(change) => {
                    let event = ManagementEvent::ConfigChanged { change };
                    if write_frame(write, &event).await.is_err() {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
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
    method: String,
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

async fn dispatch(
    config: &ConfigWorkerClient,
    product: &ProductSession,
    lifecycle: &LifecycleController,
    request: ManagementRequest,
) -> Result<serde_json::Value, ManagementResponseError> {
    match request.method.as_str() {
        "manager.snapshot" => lifecycle_value(lifecycle.snapshot().await),
        "runtime.start" => lifecycle_value(lifecycle.start().await),
        "runtime.stop" => lifecycle_value(lifecycle.stop().await),
        "runtime.restart" => lifecycle_value(lifecycle.restart().await),
        "manager.shutdown" => lifecycle_value(lifecycle.shutdown().await),
        "check.report" | "database.probe" => readiness_value(config, lifecycle, false).await,
        "credential.probe" => readiness_value(config, lifecycle, true).await,
        "config.getAll" => to_value(config.document().await),
        "config.path" => to_value(config.path().await),
        "config.schema" => serde_json::to_value(tribal_config::config_schema())
            .map_err(|_| invalid_request("configuration schema encoding failed")),
        "config.get" => to_value(config.get(parse_params(request.params)?).await),
        "config.validate" => {
            let request: ConfigValidateRequest = parse_params(request.params)?;
            let violations = config
                .validate(request.key.as_str().to_owned(), request.value)
                .await
                .map_err(management_error)?;
            Ok(serde_json::json!({
                "valid": violations.is_empty(),
                "violations": violations
                    .into_iter()
                    .map(|violation| serde_json::json!({
                        "key": violation.key,
                        "message": violation.message,
                    }))
                    .collect::<Vec<_>>(),
            }))
        }
        "config.set" => {
            let mut outcome = config
                .set(parse_params(request.params)?)
                .await
                .map_err(management_error)?;
            project_runtime_effect(lifecycle, &mut outcome.effect).await;
            if !matches!(
                outcome.effect,
                tribal_wire::management::ConfigWriteEffect::Unchanged
            ) {
                lifecycle.config_changed().await;
            }
            product_value(Ok(outcome))
        }
        "config.patch" => {
            let mut outcome = config
                .patch(parse_params(request.params)?)
                .await
                .map_err(management_error)?;
            project_patch_effects(lifecycle, &mut outcome).await;
            if patch_changed(&outcome) {
                lifecycle.config_changed().await;
            }
            product_value(Ok(outcome))
        }
        "models.catalogue" => product_value(product.models_catalogue().await),
        "models.select" => {
            let mut outcome = product.select_model(parse_params(request.params)?).await?;
            project_patch_effects(lifecycle, &mut outcome).await;
            if patch_changed(&outcome) {
                lifecycle.config_changed().await;
            }
            product_value(Ok(outcome))
        }
        "credential.sources" => product_value(
            product
                .credential_sources(parse_params(request.params)?)
                .await,
        ),
        "graph.genesisOptions" => product_value(product.genesis_options().await),
        "graph.embedding_profile" => product_value(product.embedding_profile().await),
        "graph.configureGenesis" => {
            let mut outcome = product
                .configure_genesis(parse_params(request.params)?)
                .await?;
            project_patch_effects(lifecycle, &mut outcome).await;
            if patch_changed(&outcome) {
                lifecycle.config_changed().await;
            }
            product_value(Ok(outcome))
        }
        "graph.convergeGenesis" => product_value(
            product
                .converge_genesis(parse_params(request.params)?)
                .await,
        ),
        _ => Err(invalid_request("unknown management method")),
    }
}

#[derive(serde::Deserialize)]
struct ConfigValidateRequest {
    key: tribal_domain::ConfigFieldPath,
    value: serde_json::Value,
}

fn lifecycle_value<T: serde::Serialize>(
    result: Option<T>,
) -> Result<serde_json::Value, ManagementResponseError> {
    let value = result.ok_or_else(|| invalid_request("lifecycle owner is unavailable"))?;
    serde_json::to_value(value).map_err(|_| invalid_request("lifecycle response encoding failed"))
}

async fn readiness_value(
    config: &ConfigWorkerClient,
    lifecycle: &LifecycleController,
    providers: bool,
) -> Result<serde_json::Value, ManagementResponseError> {
    let path = config.path().await.map_err(management_error)?;
    let report = crate::commands::run_report_async(crate::commands::CheckReportOptions {
        config_path: Path::new(&path.path),
        providers,
        project: None,
        token: None,
    })
    .await
    .map_err(|_| invalid_request("readiness observation failed"))?;
    let runtime_present = lifecycle
        .snapshot()
        .await
        .is_some_and(|snapshot| lifecycle_has_runtime(&snapshot.phase));
    serde_json::to_value(readiness::from_results(report.checks, runtime_present))
        .map_err(|_| invalid_request("readiness response encoding failed"))
}

fn parse_params<T: serde::de::DeserializeOwned>(
    params: Option<serde_json::Value>,
) -> Result<T, ManagementResponseError> {
    serde_json::from_value(params.unwrap_or(serde_json::Value::Null))
        .map_err(|_| invalid_request("management request parameters are invalid"))
}

fn to_value<T: serde::Serialize>(
    result: Result<T, ConfigAuthorityError>,
) -> Result<serde_json::Value, ManagementResponseError> {
    let value = result.map_err(management_error)?;
    serde_json::to_value(value).map_err(|_| invalid_request("management response encoding failed"))
}

fn product_value<T: serde::Serialize>(
    result: Result<T, ManagementResponseError>,
) -> Result<serde_json::Value, ManagementResponseError> {
    let value = result?;
    serde_json::to_value(value).map_err(|_| invalid_request("management response encoding failed"))
}

async fn project_runtime_effect(
    lifecycle: &LifecycleController,
    effect: &mut tribal_wire::management::ConfigWriteEffect,
) {
    if matches!(
        effect,
        tribal_wire::management::ConfigWriteEffect::OnNextStart
    ) && lifecycle
        .snapshot()
        .await
        .is_some_and(|snapshot| lifecycle_has_runtime(&snapshot.phase))
    {
        *effect = tribal_wire::management::ConfigWriteEffect::AwaitingRestart;
    }
}

async fn project_patch_effects(
    lifecycle: &LifecycleController,
    outcome: &mut tribal_wire::management::ConfigPatchOutcome,
) {
    let running = lifecycle
        .snapshot()
        .await
        .is_some_and(|snapshot| lifecycle_has_runtime(&snapshot.phase));
    if !running {
        return;
    }
    for field in &mut outcome.fields {
        if matches!(
            field.effect,
            tribal_wire::management::ConfigWriteEffect::OnNextStart
        ) {
            field.effect = tribal_wire::management::ConfigWriteEffect::AwaitingRestart;
        }
    }
}

fn lifecycle_has_runtime(phase: &tribal_wire::management::LifecyclePhase) -> bool {
    matches!(
        phase,
        tribal_wire::management::LifecyclePhase::Healthy { .. }
            | tribal_wire::management::LifecyclePhase::Degraded { .. }
            | tribal_wire::management::LifecyclePhase::VersionMismatch { .. }
            | tribal_wire::management::LifecyclePhase::Stopping { .. }
            | tribal_wire::management::LifecyclePhase::RuntimeUnresponsive { .. }
    )
}

fn patch_changed(outcome: &tribal_wire::management::ConfigPatchOutcome) -> bool {
    outcome.fields.iter().any(|field| {
        !matches!(
            field.effect,
            tribal_wire::management::ConfigWriteEffect::Unchanged
        )
    })
}

fn invalid_request(message: &str) -> ManagementResponseError {
    ManagementResponseError {
        message: message.to_owned(),
        error: ManagementError::ConfigurationInvalid { fields: Vec::new() },
    }
}

pub(super) fn management_error(error: ConfigAuthorityError) -> ManagementResponseError {
    let message = error.to_string();
    let error = match error {
        ConfigAuthorityError::Conflict { expected, actual } => {
            ManagementError::ConfigConflict { expected, actual }
        }
        ConfigAuthorityError::PatchRefused { reason } => {
            ManagementError::ConfigPatchRefused { reason }
        }
        ConfigAuthorityError::Write { source } => {
            let fields = source
                .violations()
                .into_iter()
                .flatten()
                .filter_map(|violation| tribal_domain::ConfigFieldPath::parse(&violation.key).ok())
                .collect();
            if source.violations().is_some() {
                ManagementError::ConfigurationInvalid { fields }
            } else {
                ManagementError::ConfigPersistenceUnavailable {
                    phase: ConfigPersistencePhase::NotCommitted,
                    observation: ConfigPersistenceObservation::Unreadable,
                }
            }
        }
        ConfigAuthorityError::DurabilityUncertain {
            observed_digest, ..
        } => ManagementError::ConfigPersistenceUnavailable {
            phase: ConfigPersistencePhase::DurabilityUncertain,
            observation: ConfigPersistenceObservation::Observed {
                digest: observed_digest,
            },
        },
        ConfigAuthorityError::Io { .. } | ConfigAuthorityError::StableWinnerUnavailable => {
            ManagementError::ConfigPersistenceUnavailable {
                phase: ConfigPersistencePhase::DurabilityUncertain,
                observation: ConfigPersistenceObservation::Unreadable,
            }
        }
        ConfigAuthorityError::Invalid { .. }
        | ConfigAuthorityError::UnknownKey { .. }
        | ConfigAuthorityError::WorkerUnavailable => {
            ManagementError::ConfigurationInvalid { fields: Vec::new() }
        }
    };
    ManagementResponseError { message, error }
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

    use super::*;
    use tribal_wire::management::ManagementClientHello;

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
            let (config, _terminal) = super::super::worker::spawn(
                super::super::configuration::ConfigAuthority::new(config_path.clone()),
            )
            .expect("config worker starts");
            let authority = super::super::authority::AuthorityLease::acquire(&config_path)
                .expect("authority acquisition succeeds");
            let super::super::authority::AuthorityAcquire::Acquired(authority) = authority else {
                panic!("test owns its unique config authority");
            };
            let listener = bind(&path).await.expect("management socket binds");
            let shutdown = CancellationToken::new();
            let (lifecycle, _lifecycle_task) = super::super::lifecycle::LifecycleController::spawn(
                "manager".to_owned(),
                config_path,
                config.clone(),
                Arc::new(authority),
                shutdown.clone(),
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
                config.clone(),
                ProductService::new(config),
                lifecycle,
                shutdown.clone(),
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
            let mut response = String::new();
            BufReader::new(stream)
                .read_line(&mut response)
                .await
                .expect("response reads");
            let response: ManagementBootstrapResponse =
                serde_json::from_str(&response).expect("response deserialises");
            assert_eq!(
                matches!(response, ManagementBootstrapResponse::Compatible { .. }),
                expected_compatible,
            );
            shutdown.cancel();
            task.await.expect("socket task joins");
        }
    }
}
