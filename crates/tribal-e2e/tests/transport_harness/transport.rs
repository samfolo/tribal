//! Transport spawning, readiness polling, and clean shutdown.

use std::{future::Future, net::SocketAddr, time::Duration};

use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tribal_config::ServerConfig;

// ---------------------------------------------------------------------------
// Transport handle
// ---------------------------------------------------------------------------

/// Handle to a running transport, ensuring clean shutdown.
pub struct TransportHandle {
    pub addr: SocketAddr,
    ct: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl TransportHandle {
    /// Cancels the transport and waits for the task to finish, ensuring
    /// all pool connections are released before the next test.
    pub async fn shutdown(self) {
        self.ct.cancel();
        self.join.await.expect("transport task must not panic");
    }
}

// ---------------------------------------------------------------------------
// Spawning
// ---------------------------------------------------------------------------

/// Spawns a transport using the provided runner closure and returns a
/// handle for clean teardown.
///
/// The `runner` receives a `CancellationToken`, a `ServerConfig`, and
/// a pre-bound `TcpListener`, and returns a future that runs the
/// transport until shutdown.
pub async fn spawn_transport<F, Fut>(
    ct: CancellationToken,
    server_config: ServerConfig,
    runner: F,
) -> TransportHandle
where
    F: FnOnce(CancellationToken, ServerConfig, TcpListener) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local address");

    let task_ct = ct.clone();
    let join = tokio::spawn(async move {
        runner(task_ct, server_config, listener).await;
    });

    // Wait until the server is accepting connections.
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            return TransportHandle { addr, ct, join };
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("transport did not become ready within 500ms");
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

/// Builds a reqwest client configured for transport tests.
///
/// Disables connection pooling and proxies to avoid interference
/// between tests.
pub fn test_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .build()
        .expect("build reqwest client")
}
