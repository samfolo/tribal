use std::sync::Arc;

use rmcp::{
    ServiceExt,
    service::{RoleClient, RunningService},
};
use sqlx::PgPool;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tribal_config::TribalConfig;
use tribal_mcp::{
    ConnectionRepositories, HandlerConfig, SessionContext, SessionProject, TribalServerHandler,
};
use tribal_server::{ServerHandle, start_server};
use tribal_test_utils::truncate_all_tables;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Buffer size for the in-process duplex transport between server and client.
const DUPLEX_BUFFER_SIZE: usize = 65_536;

// ---------------------------------------------------------------------------
// TestHarness
// ---------------------------------------------------------------------------

/// Handle to a running E2E test server with an MCP client.
pub struct TestHarness {
    server_handle: Option<ServerHandle>,
    cancellation_token: CancellationToken,

    /// MCP client peer for making tool calls.
    pub client: RunningService<RoleClient, ()>,

    /// Connection pool for direct database access in assertions.
    pub pool: PgPool,

    /// Prompt template directory — cleaned up on drop.
    _prompts_dir: TempDir,
}

impl TestHarness {
    /// Initiates graceful shutdown of the server.
    ///
    /// # Panics
    ///
    /// Panics if shutdown has already been called.
    pub async fn shutdown(&mut self) {
        self.cancellation_token.cancel();
        let handle = self
            .server_handle
            .take()
            .expect("shutdown called more than once");
        tokio::task::spawn_blocking(move || {
            let _ = handle.shutdown();
        })
        .await
        .expect("shutdown task panicked");
    }

    /// Truncates all application tables for test isolation.
    pub async fn teardown(&self) {
        let mut conn = self
            .pool
            .acquire()
            .await
            .expect("acquire connection for teardown");
        truncate_all_tables(&mut conn).await;
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Starts the full Tribal server and connects an MCP client over a duplex
/// transport.
///
/// # Panics
///
/// Panics if server startup or client connection fails.
/// Starts the full Tribal server and connects an MCP client over a duplex
/// transport.
///
/// `pool` is an independently-owned pool for test setup, assertions, and
/// teardown.  It must not be the shared `test_context().pool()` — use
/// `test_context().create_pool()` instead to avoid connection exhaustion
/// across sequential test runs.
///
/// # Panics
///
/// Panics if server startup or client connection fails.
pub async fn build_harness(
    config: TribalConfig,
    prompts_dir: TempDir,
    cli_project: Option<String>,
    principal_key: &str,
    pool: PgPool,
) -> TestHarness {
    let token = CancellationToken::new();
    let spawn_token = token.clone();

    // start_server creates its own runtimes via block_on — must run
    // outside the test's tokio runtime.
    let handle = tokio::task::spawn_blocking(move || {
        start_server(&config, cli_project, spawn_token).expect("server startup failed")
    })
    .await
    .expect("start_server task panicked");

    let state = Arc::clone(handle.state());

    // Wire session project from the startup cascade.
    let session_project = state.resolved_project().map(SessionProject::from);
    let session = SessionContext::new(session_project, principal_key.to_owned());
    let repositories = ConnectionRepositories::new();
    let handler_config = HandlerConfig::default();
    let handler = TribalServerHandler::new(state, repositories, session, handler_config);

    // In-process duplex transport — server on one half, client on the other.
    let (server_transport, client_transport) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);

    tokio::spawn(async move {
        let server = handler
            .serve(server_transport)
            .await
            .expect("server MCP handshake failed");
        server.waiting().await.expect("server MCP session error");
    });

    // The unit type `()` implements `ClientHandler` (a no-op handler that
    // accepts all server requests). `serve` performs the MCP initialisation
    // handshake and returns a `RunningService` whose `Deref` target is the
    // `Peer<RoleClient>` used for tool calls.
    let no_op_client_handler: () = ();
    let client = no_op_client_handler
        .serve(client_transport)
        .await
        .expect("client MCP handshake failed");

    TestHarness {
        server_handle: Some(handle),
        cancellation_token: token,
        client,
        pool,
        _prompts_dir: prompts_dir,
    }
}
