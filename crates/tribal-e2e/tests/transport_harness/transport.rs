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

    // Wait until the server is accepting connections.  Uses a
    // 3-second deadline with increasing backoff to tolerate slow CI.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut interval = Duration::from_millis(10);
    let mut last_error = None;

    while tokio::time::Instant::now() < deadline {
        match TcpStream::connect(addr).await {
            Ok(_) => return TransportHandle { addr, ct, join },
            Err(err) => last_error = Some(err),
        }
        tokio::time::sleep(interval).await;
        interval = (interval * 2).min(Duration::from_millis(200));
    }
    panic!(
        "transport did not become ready within 3s; last error: {}",
        last_error.map_or_else(|| "none".to_owned(), |e| e.to_string()),
    );
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
