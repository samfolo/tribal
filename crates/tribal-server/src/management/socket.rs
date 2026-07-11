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
    ManagementError, ManagementEvent, ManagementLogLoss, ManagementMethod, ManagementResponseError,
    ManagementServerHello,
};

use super::{
    config_schema,
    configuration::ConfigAuthorityError,
    lifecycle::LifecycleController,
    probe::{ProbeError, ProbeService},
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
    config: &'a ConfigWorkerClient,
    product: &'a ProductSession,
    probe: &'a ProbeService,
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
            config: &services.config,
            product: &product,
            probe: &services.probe,
            lifecycle: &services.lifecycle,
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
                let response = match dispatch(
                    services.config,
                    services.product,
                    services.probe,
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
    probe: &ProbeService,
    lifecycle: &LifecycleController,
    request: ManagementRequest,
) -> Result<serde_json::Value, ManagementResponseError> {
    let method =
        serde_json::from_value::<ManagementMethod>(serde_json::Value::String(request.method))
            .map_err(|_| invalid_request("unknown management method"))?;
    match method {
        ManagementMethod::ManagerSnapshot => lifecycle_value(lifecycle.snapshot().await),
        ManagementMethod::RuntimeStart => lifecycle_value(lifecycle.start().await),
        ManagementMethod::RuntimeStop => lifecycle_value(lifecycle.stop().await),
        ManagementMethod::RuntimeRestart => lifecycle_value(lifecycle.restart().await),
        ManagementMethod::ManagerShutdown => lifecycle_value(lifecycle.shutdown().await),
        ManagementMethod::ServerStatus => lifecycle_value(lifecycle.runtime_status().await),
        ManagementMethod::LogsTail => {
            let request: tribal_wire::management::RuntimeLogsTailRequest =
                parse_params(request.params)?;
            lifecycle_value(lifecycle.runtime_logs_tail(request.lines).await)
        }
        ManagementMethod::TokenList => lifecycle_value(lifecycle.runtime_token_list().await),
        ManagementMethod::CheckReport => readiness_value(config, probe, lifecycle).await,
        ManagementMethod::DatabaseProbe => {
            let receipt = probe.database().await.map_err(probe_error)?;
            refresh_readiness(config, probe, lifecycle).await?;
            product_value(Ok(receipt))
        }
        ManagementMethod::CredentialProbe => {
            let receipts = probe.credentials().await.map_err(probe_error)?;
            refresh_readiness(config, probe, lifecycle).await?;
            product_value(Ok(receipts))
        }
        ManagementMethod::ConfigGetAll => to_value(config.document().await),
        ManagementMethod::ConfigPath => to_value(config.path().await),
        ManagementMethod::ConfigSchema => serde_json::to_value(
            config_schema::project(tribal_config::config_schema())
                .map_err(|_| invalid_request("configuration schema projection failed"))?,
        )
        .map_err(|_| invalid_request("configuration schema encoding failed")),
        ManagementMethod::ConfigGet => to_value(config.get(parse_params(request.params)?).await),
        ManagementMethod::ConfigValidate => {
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
        ManagementMethod::ConfigSet => {
            let request: tribal_wire::management::ConfigSetRequest = parse_params(request.params)?;
            let runtime_changes = vec![tribal_wire::runtime_control::RuntimeConfigChange {
                key: request.key.clone(),
                value: tribal_wire::management::ConfigLiteral::new(
                    request.value.expose_sensitive().clone(),
                ),
            }];
            let mut outcome = config.set(request).await.map_err(management_error)?;
            project_runtime_effect(
                lifecycle,
                &mut outcome.effect,
                outcome.revision.clone(),
                runtime_changes,
            )
            .await;
            if !matches!(
                outcome.effect,
                tribal_wire::management::ConfigWriteEffect::Unchanged
                    | tribal_wire::management::ConfigWriteEffect::AppliedLive
            ) {
                lifecycle.config_changed().await;
            }
            product_value(Ok(outcome))
        }
        ManagementMethod::ConfigPatch => {
            let request: tribal_wire::management::ConfigPatchRequest =
                parse_params(request.params)?;
            let runtime_changes = request
                .changes
                .iter()
                .map(|change| tribal_wire::runtime_control::RuntimeConfigChange {
                    key: change.key.clone(),
                    value: tribal_wire::management::ConfigLiteral::new(
                        change.value.expose_sensitive().clone(),
                    ),
                })
                .collect();
            let mut outcome = config.patch(request).await.map_err(management_error)?;
            project_patch_effects(lifecycle, &mut outcome, runtime_changes).await;
            if patch_requires_lifecycle_update(&outcome) {
                lifecycle.config_changed().await;
            }
            product_value(Ok(outcome))
        }
        ManagementMethod::ModelsCatalogue => product_value(product.models_catalogue().await),
        ManagementMethod::ModelsSelect => {
            let mut outcome = product.select_model(parse_params(request.params)?).await?;
            project_patch_effects(lifecycle, &mut outcome, Vec::new()).await;
            if patch_requires_lifecycle_update(&outcome) {
                lifecycle.config_changed().await;
            }
            product_value(Ok(outcome))
        }
        ManagementMethod::CredentialSources => product_value(
            product
                .credential_sources(parse_params(request.params)?)
                .await,
        ),
        ManagementMethod::GraphGenesisOptions => product_value(product.genesis_options().await),
        ManagementMethod::GraphEmbeddingProfile => product_value(product.embedding_profile().await),
        ManagementMethod::GraphConfigureGenesis => {
            let mut outcome = product
                .configure_genesis(parse_params(request.params)?)
                .await?;
            project_patch_effects(lifecycle, &mut outcome, Vec::new()).await;
            if patch_requires_lifecycle_update(&outcome) {
                lifecycle.config_changed().await;
            }
            product_value(Ok(outcome))
        }
        ManagementMethod::GraphConvergeGenesis => product_value(
            product
                .converge_genesis(parse_params(request.params)?)
                .await,
        ),
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
    probe: &ProbeService,
    lifecycle: &LifecycleController,
) -> Result<serde_json::Value, ManagementResponseError> {
    let report = readiness_report(config, probe, lifecycle).await?;
    serde_json::to_value(report).map_err(|_| invalid_request("readiness response encoding failed"))
}

async fn refresh_readiness(
    config: &ConfigWorkerClient,
    probe: &ProbeService,
    lifecycle: &LifecycleController,
) -> Result<(), ManagementResponseError> {
    let report = readiness_report(config, probe, lifecycle).await?;
    lifecycle.update_readiness(report).await;
    Ok(())
}

async fn readiness_report(
    config: &ConfigWorkerClient,
    probe: &ProbeService,
    lifecycle: &LifecycleController,
) -> Result<tribal_wire::management::ReadinessReport, ManagementResponseError> {
    let runtime_present = lifecycle
        .snapshot()
        .await
        .is_some_and(|snapshot| lifecycle_has_runtime(&snapshot.phase));
    readiness::automatic(config, probe, runtime_present)
        .await
        .map_err(|_| invalid_request("readiness observation failed"))
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
    revision: tribal_wire::management::ConfigRevision,
    changes: Vec<tribal_wire::runtime_control::RuntimeConfigChange>,
) {
    if matches!(
        effect,
        tribal_wire::management::ConfigWriteEffect::OnNextStart
    ) {
        let running = lifecycle
            .snapshot()
            .await
            .is_some_and(|snapshot| lifecycle_has_runtime(&snapshot.phase));
        if !running {
            return;
        }
        let hot = !changes.is_empty()
            && changes.iter().all(|change| {
                tribal_config::reload_class(change.key.as_str()) == tribal_config::ReloadClass::Hot
            });
        let applied = if hot {
            lifecycle.apply_config(revision, changes).await
        } else {
            None
        };
        *effect = if matches!(
            applied,
            Some(tribal_wire::runtime_control::RuntimeConfigApplyOutcome::Applied)
        ) {
            tribal_wire::management::ConfigWriteEffect::AppliedLive
        } else {
            tribal_wire::management::ConfigWriteEffect::AwaitingRestart
        };
    }
}

async fn project_patch_effects(
    lifecycle: &LifecycleController,
    outcome: &mut tribal_wire::management::ConfigPatchOutcome,
    changes: Vec<tribal_wire::runtime_control::RuntimeConfigChange>,
) {
    let running = lifecycle
        .snapshot()
        .await
        .is_some_and(|snapshot| lifecycle_has_runtime(&snapshot.phase));
    if !running {
        return;
    }
    let hot = !changes.is_empty()
        && changes.iter().all(|change| {
            tribal_config::reload_class(change.key.as_str()) == tribal_config::ReloadClass::Hot
        });
    let applied = if hot {
        lifecycle
            .apply_config(outcome.revision.clone(), changes)
            .await
    } else {
        None
    };
    for field in &mut outcome.fields {
        if matches!(
            field.effect,
            tribal_wire::management::ConfigWriteEffect::OnNextStart
        ) {
            field.effect = if matches!(
                applied,
                Some(tribal_wire::runtime_control::RuntimeConfigApplyOutcome::Applied)
            ) {
                tribal_wire::management::ConfigWriteEffect::AppliedLive
            } else {
                tribal_wire::management::ConfigWriteEffect::AwaitingRestart
            };
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

fn patch_requires_lifecycle_update(outcome: &tribal_wire::management::ConfigPatchOutcome) -> bool {
    outcome.fields.iter().any(|field| {
        !matches!(
            field.effect,
            tribal_wire::management::ConfigWriteEffect::Unchanged
                | tribal_wire::management::ConfigWriteEffect::AppliedLive
        )
    })
}

fn invalid_request(message: &str) -> ManagementResponseError {
    ManagementResponseError {
        message: message.to_owned(),
        error: ManagementError::ConfigurationInvalid { fields: Vec::new() },
    }
}

fn probe_error(_error: ProbeError) -> ManagementResponseError {
    ManagementResponseError {
        message: "external probe is unavailable".to_owned(),
        error: ManagementError::ProbeUnavailable,
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
