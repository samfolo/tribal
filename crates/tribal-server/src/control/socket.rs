//! The control socket: binding it, authenticating each peer, and serving the
//! JSON-RPC request loop.
//!
//! A connection is admitted only when its peer credential's UID matches the
//! UID that owns the socket file — the local operator, and no one else on the
//! machine — and only after a handshake in which the client's control-contract
//! version is one the server speaks. The plane owns a drop-guard that removes
//! the socket and its descriptor on clean shutdown, so a stale descriptor never
//! outlives the server that wrote it.

use std::{
    os::unix::fs::{MetadataExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{
    io::BufReader,
    net::{UnixListener, UnixStream},
};
use tokio_util::sync::CancellationToken;
use tribal_config::TribalConfig;
use tribal_wire::control::{
    CONTROL_CONTRACT_VERSION, ClientHello, ControlRequest, ControlResponse, JsonRpcVersion,
    ResponseResult, ServerHello,
};

use super::{
    descriptor::{self, RuntimeDescriptor},
    dispatch::{ConfigContext, dispatch},
    error::ControlError,
    framing::{read_typed_frame, write_frame},
};

/// The owner-only permission bits the socket is created with — defence in depth
/// beside the peer-credential check.
const SOCKET_MODE: u32 = 0o600;

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// Everything a served connection reads: the running configuration, the file a
/// write persists to, and the identity the handshake and descriptor report.
pub(crate) struct ControlContext {
    /// The resolved configuration the server is running with.
    pub config: Arc<TribalConfig>,
    /// The YAML file `config.set` writes.
    pub config_path: PathBuf,
    /// The binary's build version, reported in the handshake and descriptor.
    pub binary_version: Arc<str>,
    /// The per-serve instance identity, reported in the descriptor.
    pub instance_id: Arc<str>,
    /// Whether a supervisor owns this process (governs the future restart
    /// contract; recorded in the descriptor).
    pub supervised: bool,
}

// ---------------------------------------------------------------------------
// The plane
// ---------------------------------------------------------------------------

/// A bound control socket, ready to serve, holding the cleanup guard for its
/// socket and descriptor.
pub(crate) struct ControlPlane {
    listener: UnixListener,
    context: Arc<ControlContext>,
    /// The UID that owns the socket file, compared against each peer's.
    owner_uid: u32,
    guard: DescriptorGuard,
}

impl ControlPlane {
    /// Binds the control socket at the derived runtime path and writes its
    /// descriptor to the derived state path.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError`] when a path does not fit, another server already
    /// serves, or the bind or descriptor write fails.
    pub(crate) async fn bind(context: Arc<ControlContext>) -> Result<Self, ControlError> {
        Self::bind_at(
            descriptor::socket_path()?,
            descriptor::descriptor_path(),
            context,
        )
        .await
    }

    /// Binds at explicit paths — the seam production derivation and tests share.
    pub(crate) async fn bind_at(
        socket_path: PathBuf,
        descriptor_path: PathBuf,
        context: Arc<ControlContext>,
    ) -> Result<Self, ControlError> {
        // A descriptor whose server still answers is a live instance: refuse
        // rather than seize its path. A dead or absent one is reclaimed.
        if let Some(existing) = RuntimeDescriptor::read(&descriptor_path)
            && existing.is_reachable().await
        {
            return Err(ControlError::AlreadyServing {
                path: existing.socket_path,
            });
        }
        prepare_socket_path(&socket_path)?;
        let listener = UnixListener::bind(&socket_path).map_err(|source| ControlError::Bind {
            path: socket_path.clone(),
            source,
        })?;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(SOCKET_MODE))
            .map_err(fs_error(&socket_path))?;
        let owner_uid = std::fs::metadata(&socket_path)
            .map_err(fs_error(&socket_path))?
            .uid();

        let descriptor = RuntimeDescriptor {
            socket_path: socket_path.clone(),
            protocol_version: CONTROL_CONTRACT_VERSION,
            pid: std::process::id(),
            instance_id: context.instance_id.to_string(),
            binary_version: context.binary_version.to_string(),
            supervised: context.supervised,
        };
        descriptor.write_atomically(&descriptor_path)?;

        Ok(Self {
            listener,
            context,
            owner_uid,
            guard: DescriptorGuard {
                socket_path,
                descriptor_path,
            },
        })
    }

    /// Serves connections until the token is cancelled, then removes the socket
    /// and descriptor via the guard.
    pub(crate) async fn serve(self, cancellation_token: CancellationToken) {
        let Self {
            listener,
            context,
            owner_uid,
            guard,
        } = self;
        loop {
            tokio::select! {
                () = cancellation_token.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _address)) => {
                        drop(tokio::spawn(handle_connection(
                            stream,
                            Arc::clone(&context),
                            owner_uid,
                        )));
                    }
                    Err(error) => tracing::warn!(%error, "control: accept failed"),
                }
            }
        }
        drop(guard);
    }
}

/// Removes the socket and its descriptor when the served plane is dropped, so a
/// clean shutdown — or a panic — leaves no stale runtime state behind.
struct DescriptorGuard {
    socket_path: PathBuf,
    descriptor_path: PathBuf,
}

impl Drop for DescriptorGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.descriptor_path);
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

/// Binds the control plane and spawns its accept loop, logging and continuing
/// without it on failure — the control plane never blocks the binary from
/// serving MCP.
pub(crate) async fn spawn_control_plane(
    context: ControlContext,
    cancellation_token: CancellationToken,
) {
    match ControlPlane::bind(Arc::new(context)).await {
        Ok(plane) => drop(tokio::spawn(plane.serve(cancellation_token))),
        Err(error) => tracing::warn!(%error, "control socket unavailable; serving without it"),
    }
}

// ---------------------------------------------------------------------------
// Connection handling
// ---------------------------------------------------------------------------

/// Authenticates one peer, completes the version handshake, and serves its
/// request loop until it closes.
async fn handle_connection(stream: UnixStream, context: Arc<ControlContext>, owner_uid: u32) {
    let peer = match stream.peer_cred() {
        Ok(credential) => credential,
        Err(error) => {
            tracing::warn!(%error, "control: could not read peer credentials; refusing");
            return;
        }
    };
    if !peer_is_owner(peer.uid(), owner_uid) {
        tracing::warn!(
            peer_uid = peer.uid(),
            owner_uid,
            "control: peer credential mismatch refused",
        );
        return;
    }

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let hello: ClientHello = match read_typed_frame(&mut reader).await {
        Ok(Some(hello)) => hello,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(%error, "control: malformed client hello; refusing");
            return;
        }
    };
    if hello.protocol_version != CONTROL_CONTRACT_VERSION {
        tracing::warn!(
            client = hello.protocol_version,
            server = CONTROL_CONTRACT_VERSION,
            "control: unsupported protocol version refused",
        );
        return;
    }
    let server_hello = ServerHello {
        protocol_version: CONTROL_CONTRACT_VERSION,
        binary_version: context.binary_version.to_string(),
    };
    if let Err(error) = write_frame(&mut write_half, &server_hello).await {
        tracing::warn!(%error, "control: could not send server hello; closing");
        return;
    }

    loop {
        let request: ControlRequest = match read_typed_frame(&mut reader).await {
            Ok(Some(request)) => request,
            Ok(None) => break,
            Err(error) => {
                tracing::warn!(%error, "control: malformed request; closing");
                break;
            }
        };
        let config_context = ConfigContext {
            config: context.config.as_ref(),
            config_file: context.config_path.as_path(),
        };
        let outcome = match dispatch(&config_context, &request.method, request.params) {
            Ok(result) => ResponseResult::Success { result },
            Err(error) => ResponseResult::Failure { error },
        };
        let response = ControlResponse {
            jsonrpc: JsonRpcVersion,
            id: request.id,
            outcome,
        };
        if let Err(error) = write_frame(&mut write_half, &response).await {
            tracing::warn!(%error, "control: could not send response; closing");
            break;
        }
    }
}

/// Whether a peer's UID is the socket owner's — the whole peer-credential check.
fn peer_is_owner(peer_uid: u32, owner_uid: u32) -> bool {
    peer_uid == owner_uid
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Ensures the socket's parent directory exists and clears a leftover socket
/// file. Liveness is already settled by the descriptor check in `bind_at`, so a
/// file remaining here is a dead server's stale entry, reclaimed.
fn prepare_socket_path(socket_path: &Path) -> Result<(), ControlError> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).map_err(fs_error(parent))?;
    }
    if socket_path.exists() {
        std::fs::remove_file(socket_path).map_err(fs_error(socket_path))?;
    }
    Ok(())
}

/// A closure mapping an I/O error to [`ControlError::Filesystem`] for `path`.
fn fs_error(path: &Path) -> impl FnOnce(std::io::Error) -> ControlError {
    let path = path.to_owned();
    move |source| ControlError::Filesystem { path, source }
}

#[cfg(test)]
mod tests {
    use tribal_wire::control::{ConfigPath, RequestId};

    use super::*;

    fn test_context() -> Arc<ControlContext> {
        Arc::new(ControlContext {
            config: Arc::new(TribalConfig::minimum_valid(
                "postgres://user:pass@localhost:5432/tribal",
            )),
            config_path: PathBuf::from("/tmp/tribal.yaml"),
            binary_version: Arc::from("test-build"),
            instance_id: Arc::from("test-instance"),
            supervised: false,
        })
    }

    #[test]
    fn test_peer_is_owner_compares_uids() {
        assert!(peer_is_owner(1000, 1000));
        assert!(!peer_is_owner(1000, 1001));
    }

    /// Reads the client end back after the server sends its hello: `Some` when
    /// the handshake was accepted, `None` when the server refused and closed.
    async fn hello_after_client_hello(
        owner_uid_offset: u32,
        client_version: u16,
    ) -> Option<ServerHello> {
        let (client, server) = UnixStream::pair().expect("socket pair");
        let real_uid = client.peer_cred().expect("peer cred").uid();
        let owner_uid = real_uid.wrapping_add(owner_uid_offset);
        let task = tokio::spawn(handle_connection(server, test_context(), owner_uid));

        let (read_half, mut write_half) = client.into_split();
        let mut reader = BufReader::new(read_half);
        let _ = write_frame(
            &mut write_half,
            &ClientHello {
                protocol_version: client_version,
            },
        )
        .await;
        let hello = read_typed_frame(&mut reader).await.unwrap_or(None);
        drop(write_half);
        drop(reader);
        let _ = task.await;
        hello
    }

    #[tokio::test]
    async fn test_a_same_uid_peer_completes_the_handshake() {
        let hello = hello_after_client_hello(0, CONTROL_CONTRACT_VERSION).await;
        let hello = hello.expect("a same-uid peer receives a ServerHello");
        assert_eq!(hello.protocol_version, CONTROL_CONTRACT_VERSION);
        assert_eq!(hello.binary_version, "test-build");
    }

    #[tokio::test]
    async fn test_a_peer_credential_mismatch_is_refused() {
        let hello = hello_after_client_hello(1, CONTROL_CONTRACT_VERSION).await;
        assert!(hello.is_none(), "a mismatched peer receives no ServerHello");
    }

    #[tokio::test]
    async fn test_an_unsupported_protocol_version_is_refused() {
        let hello = hello_after_client_hello(0, CONTROL_CONTRACT_VERSION + 1).await;
        assert!(
            hello.is_none(),
            "an unknown version receives no ServerHello"
        );
    }

    #[tokio::test]
    async fn test_a_request_round_trips_over_the_connection() {
        let (client, server) = UnixStream::pair().expect("socket pair");
        let uid = client.peer_cred().expect("peer cred").uid();
        let task = tokio::spawn(handle_connection(server, test_context(), uid));

        let (read_half, mut write_half) = client.into_split();
        let mut reader = BufReader::new(read_half);
        write_frame(
            &mut write_half,
            &ClientHello {
                protocol_version: CONTROL_CONTRACT_VERSION,
            },
        )
        .await
        .unwrap();
        let _hello: ServerHello = read_typed_frame(&mut reader).await.unwrap().unwrap();

        let request = ControlRequest {
            jsonrpc: JsonRpcVersion,
            id: RequestId(1),
            method: "config.path".to_owned(),
            params: None,
        };
        write_frame(&mut write_half, &request).await.unwrap();
        let response: ControlResponse = read_typed_frame(&mut reader).await.unwrap().unwrap();
        assert_eq!(response.id, RequestId(1));
        match response.outcome {
            ResponseResult::Success { result } => {
                let path: ConfigPath = serde_json::from_value(result).unwrap();
                assert_eq!(path.path, "/tmp/tribal.yaml");
            }
            ResponseResult::Failure { error } => panic!("expected success, got {error:?}"),
        }
        drop(write_half);
        drop(reader);
        let _ = task.await;
    }

    #[tokio::test]
    async fn test_the_descriptor_is_written_on_serve_and_removed_on_shutdown() {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket_path = dir.path().join("control.sock");
        let descriptor_path = dir.path().join("control.json");
        let cancellation_token = CancellationToken::new();

        let plane =
            ControlPlane::bind_at(socket_path.clone(), descriptor_path.clone(), test_context())
                .await
                .expect("the plane binds");
        assert!(
            descriptor_path.exists(),
            "the descriptor is written on serve"
        );
        assert!(socket_path.exists(), "the socket is bound on serve");

        // A client connects and completes the handshake, proving the bound plane serves.
        let serve = tokio::spawn(plane.serve(cancellation_token.clone()));
        let client = UnixStream::connect(&socket_path).await.expect("connect");
        let (read_half, mut write_half) = client.into_split();
        let mut reader = BufReader::new(read_half);
        write_frame(
            &mut write_half,
            &ClientHello {
                protocol_version: CONTROL_CONTRACT_VERSION,
            },
        )
        .await
        .unwrap();
        let hello: ServerHello = read_typed_frame(&mut reader).await.unwrap().unwrap();
        assert_eq!(hello.protocol_version, CONTROL_CONTRACT_VERSION);
        drop(write_half);
        drop(reader);

        cancellation_token.cancel();
        serve.await.expect("the serve task ends on cancellation");
        assert!(
            !descriptor_path.exists(),
            "the descriptor is removed on clean shutdown"
        );
        assert!(
            !socket_path.exists(),
            "the socket is removed on clean shutdown"
        );
    }
}
