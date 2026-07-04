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

use sqlx::PgPool;
use tokio::{
    io::{AsyncWriteExt as _, BufReader},
    net::{UnixListener, UnixStream, unix::OwnedWriteHalf},
    sync::{Semaphore, broadcast, mpsc},
};
use tribal_auth::{AuthenticatedPrincipal, Authenticator};
use tribal_db::{PgAuthTokenRepository, PgPrincipalRepository};
use tribal_wire::control::{
    CONTROL_CONTRACT_VERSION, ClientHello, ControlEvent, ControlRequest, ControlResponse,
    JsonRpcVersion, ResponseResult, ServerHello,
};

use super::{
    ControlContext,
    descriptor::{self, RuntimeDescriptor},
    dispatch::dispatch,
    error::ControlError,
    event::notification_for,
    framing::{encode_frame, read_typed_frame, write_frame},
};

/// The owner-only permission bits the socket is created with — defence in depth
/// beside the peer-credential check.
const SOCKET_MODE: u32 = 0o600;

/// How many outgoing frames a connection queues before a slow client applies
/// back-pressure to the request loop and the event forwarder.
const OUTGOING_CAPACITY: usize = 64;

/// The most connections the control plane serves at once. The operator socket
/// sees a handful; a larger burst is a flood, refused rather than allowed to
/// spawn handlers without bound.
const MAX_CONCURRENT_CONNECTIONS: usize = 64;

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

    /// Serves connections until the serve token is cancelled, then removes the
    /// socket and descriptor via the guard.
    pub(crate) async fn serve(self) {
        let Self {
            listener,
            context,
            owner_uid,
            guard,
        } = self;
        let cancellation_token = context.cancellation_token.clone();
        // Each connection is served by a best-effort task: it speaks to one
        // operator client, and its exit or panic concerns only that client,
        // never the plane. The permits bound how many run at once, so a
        // connection flood cannot spawn handlers without limit; shutdown abandons
        // them by construction, their in-flight work discardable.
        let connections = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
        loop {
            tokio::select! {
                () = cancellation_token.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _address)) => {
                        let Ok(permit) = Arc::clone(&connections).try_acquire_owned() else {
                            tracing::warn!("control: connection cap reached; refusing peer");
                            continue;
                        };
                        let context = Arc::clone(&context);
                        drop(tokio::spawn(async move {
                            let _permit = permit;
                            handle_connection(stream, context, owner_uid).await;
                        }));
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
pub(crate) async fn spawn_control_plane(context: ControlContext) {
    match ControlPlane::bind(Arc::new(context)).await {
        Ok(plane) => drop(tokio::spawn(plane.serve())),
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

    // Responses and server-initiated events share one writer: the request loop
    // and the event forwarder both queue frames onto a channel a single writer
    // task drains. This keeps writes serialised and the frame reader off the
    // cancellation path (it is not cancel-safe), unlike a `select!` over both.
    let (outgoing, outgoing_rx) = mpsc::channel::<Vec<u8>>(OUTGOING_CAPACITY);
    let writer = tokio::spawn(drain_to_writer(write_half, outgoing_rx));
    // Subscribe before the DB round-trip below, so no event is missed in the gap.
    let forwarder = tokio::spawn(forward_events(context.events.subscribe(), outgoing.clone()));

    // Resolve the local principal once for the connection. Config crossings need
    // none, so a failure here is not fatal — only the principal-scoped crossings
    // refuse when it is absent.
    let principal = resolve_principal(&context.pool).await;

    loop {
        let request: ControlRequest = match read_typed_frame(&mut reader).await {
            Ok(Some(request)) => request,
            Ok(None) => break,
            Err(error) => {
                tracing::warn!(%error, "control: malformed request; closing");
                break;
            }
        };
        let outcome = match dispatch(
            &context,
            principal.as_ref(),
            &request.method,
            request.params,
        )
        .await
        {
            Ok(result) => ResponseResult::Success { result },
            Err(error) => ResponseResult::Failure { error },
        };
        let response = ControlResponse {
            jsonrpc: JsonRpcVersion,
            id: request.id,
            outcome,
        };
        match encode_frame(&response) {
            Ok(frame) => {
                if outgoing.send(frame).await.is_err() {
                    break;
                }
            }
            Err(error) => {
                tracing::warn!(%error, "control: could not encode response; closing");
                break;
            }
        }
    }

    // The reader ended: dropping our sender and stopping the forwarder closes
    // the writer's channel, and it drains and exits.
    drop(outgoing);
    forwarder.abort();
    let _ = writer.await;
}

/// Drains queued frames to the connection's write half until the channel closes
/// or a write fails.
async fn drain_to_writer(mut write_half: OwnedWriteHalf, mut outgoing: mpsc::Receiver<Vec<u8>>) {
    while let Some(frame) = outgoing.recv().await {
        if write_half.write_all(&frame).await.is_err() || write_half.flush().await.is_err() {
            break;
        }
    }
}

/// Forwards bus events to the connection as notification frames. A lagged
/// subscriber drops events by design and re-reads state; a closed bus ends it.
async fn forward_events(
    mut events: broadcast::Receiver<ControlEvent>,
    outgoing: mpsc::Sender<Vec<u8>>,
) {
    loop {
        match events.recv().await {
            Ok(event) => match encode_frame(&notification_for(&event)) {
                Ok(frame) => {
                    if outgoing.send(frame).await.is_err() {
                        break;
                    }
                }
                Err(error) => tracing::warn!(%error, "control: could not encode event; dropping"),
            },
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                tracing::warn!(missed, "control: subscriber lagged; dropped events");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

/// Whether a peer's UID is the socket owner's — the whole peer-credential check.
fn peer_is_owner(peer_uid: u32, owner_uid: u32) -> bool {
    peer_uid == owner_uid
}

/// Resolves the local principal for the connection, best-effort. Config reads
/// need no principal, so a failure (no database, `tribal setup` not run) leaves
/// it `None`, and only the principal-scoped crossings refuse.
async fn resolve_principal(pool: &PgPool) -> Option<AuthenticatedPrincipal> {
    let authenticator = Authenticator::new(
        Arc::new(PgAuthTokenRepository),
        Arc::new(PgPrincipalRepository),
    );
    let mut connection = pool.acquire().await.ok()?;
    authenticator
        .resolve_stdio_principal(&mut connection)
        .await
        .ok()
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
    use std::time::Instant;

    use tokio_util::sync::CancellationToken;
    use tribal_config::{CliShadow, TribalConfig};
    use tribal_telemetry::LogRing;
    use tribal_test_utils::lazy_pool;
    use tribal_wire::control::{ConfigPath, RequestId};

    use super::*;
    use crate::startup::SelfWriteSentinel;

    fn test_context_with(cancellation_token: CancellationToken) -> Arc<ControlContext> {
        // A lazy pool never connects unless a principal-scoped method is called,
        // and `resolve_principal` treats its refusal as "no principal", so the
        // transport tests need no live database.
        let (events, _) = broadcast::channel(super::super::EVENT_BUS_CAPACITY);
        Arc::new(ControlContext {
            config: Arc::new(TribalConfig::minimum_valid(
                "postgres://user:pass@localhost:5432/tribal",
            )),
            config_path: PathBuf::from("/tmp/tribal.yaml"),
            cli_shadow: CliShadow::default(),
            self_write: SelfWriteSentinel::default(),
            pool: lazy_pool(),
            events,
            log_ring: LogRing::new(16),
            project: None,
            cancellation_token,
            started_at: Instant::now(),
            binary_version: Arc::from("test-build"),
            instance_id: Arc::from("test-instance"),
            supervised: false,
        })
    }

    fn test_context() -> Arc<ControlContext> {
        test_context_with(CancellationToken::new())
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
    async fn test_a_published_event_reaches_a_subscribed_connection() {
        use std::time::Duration;

        use tribal_wire::control::{ControlNotification, WriteEffect};

        let context = test_context();
        let events = context.events.clone();
        let (client, server) = UnixStream::pair().expect("socket pair");
        let uid = client.peer_cred().expect("peer cred").uid();
        let task = tokio::spawn(handle_connection(server, context, uid));

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

        // Publish only once the connection's forwarder has subscribed, else the
        // broadcast delivers to no one.
        while events.receiver_count() == 0 {
            tokio::task::yield_now().await;
        }
        events
            .send(ControlEvent::ConfigChanged {
                keys: vec!["logging.level".to_owned()],
                effect: WriteEffect::NeedsRestart,
            })
            .expect("a subscriber is present");

        let frame: Option<ControlNotification> =
            tokio::time::timeout(Duration::from_secs(2), read_typed_frame(&mut reader))
                .await
                .expect("an event frame arrives")
                .expect("the frame reads");
        let notification = frame.expect("a notification is present");
        assert_eq!(notification.method, "config.changed");

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
        let context = test_context_with(cancellation_token.clone());

        let plane = ControlPlane::bind_at(socket_path.clone(), descriptor_path.clone(), context)
            .await
            .expect("the plane binds");
        assert!(
            descriptor_path.exists(),
            "the descriptor is written on serve"
        );
        assert!(socket_path.exists(), "the socket is bound on serve");

        // A client connects and completes the handshake, proving the bound plane serves.
        let serve = tokio::spawn(plane.serve());
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
