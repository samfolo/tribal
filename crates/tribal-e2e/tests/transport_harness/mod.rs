// mod.rs is required here — cargo discovers top-level files in tests/ as
// separate binaries, so transport_harness.rs would conflict with the
// transport_harness/ directory.
//
// Shared helpers for HTTP and SSE transport integration tests.
#![allow(dead_code)]

use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

use chrono::Utc;
use dashmap::DashMap;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tokio_util::sync::CancellationToken;
use tribal_common::sha256_hex;
use tribal_config::{DEFAULT_OLLAMA_BASE_URL, ProviderKind, ServerConfig, WorkerConfig};
use tribal_db::{
    AuthTokenRepository, NewAuthToken, NewPrincipal, PgAuthTokenRepository, PgPrincipalRepository,
    PrincipalRepository,
};
use tribal_domain::{PromptVersionId, Scope, full_access_scopes, is_authorised};
use tribal_mcp::tool_scope_registry;
use tribal_inference::{ProviderRegistry, RequestClass};
use tribal_mcp::{ActivePromptVersions, AppState};
use tribal_test_utils::{MockEmbeddingProvider, MockInferenceProvider, test_context};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default instance ID prefix for transport tests.
const TEST_INSTANCE_ID: &str = "transport-test-00000000-0000-0000-0000-000000000000";

/// Duration used when a lifecycle timeout should never fire during
/// a test.
pub const LIFECYCLE_FAR_FUTURE_MS: u64 = 60_000;

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

/// Creates a fresh pool against the shared test database.
///
/// Each test gets its own pool so transport shutdown in one test
/// does not starve connections in the next.
pub async fn fresh_pool() -> sqlx::PgPool {
    let ctx = test_context().await;
    sqlx::pool::PoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(5))
        .connect(ctx.database_url())
        .await
        .expect("connect fresh pool")
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

/// Builds an [`AppState`] backed by a real test database pool and mock
/// providers.
pub fn test_app_state(pool: sqlx::PgPool, ct: CancellationToken) -> Arc<AppState> {
    let provider_kind = ProviderKind::default().to_string();

    let embedding_key = tribal_inference::ProviderKey::new(
        &provider_kind,
        DEFAULT_OLLAMA_BASE_URL,
        RequestClass::Embedding,
    )
    .expect("test embedding key");

    let inference_key = tribal_inference::ProviderKey::new(
        &provider_kind,
        DEFAULT_OLLAMA_BASE_URL,
        RequestClass::Inference,
    )
    .expect("test inference key");

    Arc::new(
        AppState::builder()
            .pool_mcp(pool.clone())
            .pool_worker(pool)
            .instance_id(Arc::from(TEST_INSTANCE_ID))
            .active_prompt_versions(Arc::new(RwLock::new(ActivePromptVersions::new(
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
            ))))
            .provider_registry(Arc::new(
                ProviderRegistry::new(Vec::new())
                    .expect("empty registry construction must not fail"),
            ))
            .embedding_provider(Arc::new(MockEmbeddingProvider::builder().build()))
            .extraction_provider(Arc::new(MockInferenceProvider::builder().build()))
            .triage_provider(Arc::new(MockInferenceProvider::builder().build()))
            .relation_provider(Arc::new(MockInferenceProvider::builder().build()))
            .embedding_key(embedding_key)
            .extraction_key(inference_key.clone())
            .triage_key(inference_key.clone())
            .relation_key(inference_key)
            .worker_config(WorkerConfig::default())
            .server_config(Arc::new(ServerConfig::default()))
            .cancellation_token(ct)
            .job_state_txs(Arc::new(DashMap::new()))
            .build(),
    )
}

// ---------------------------------------------------------------------------
// Auth seeding
// ---------------------------------------------------------------------------

/// Seeds a principal and auth token with full access scopes.
pub async fn seed_auth(
    pool: &sqlx::PgPool,
    principal_key: &str,
    raw_token: &str,
    expires_in: chrono::Duration,
) {
    seed_scoped_auth(pool, principal_key, raw_token, expires_in, full_access_scopes()).await;
}

/// Seeds a principal and auth token with the given scopes.
pub async fn seed_scoped_auth(
    pool: &sqlx::PgPool,
    principal_key: &str,
    raw_token: &str,
    expires_in: chrono::Duration,
    scopes: Vec<Scope>,
) {
    let mut conn = pool.acquire().await.expect("acquire connection");

    let principal = PgPrincipalRepository
        .insert(
            &mut conn,
            &NewPrincipal::builder()
                .principal_key(principal_key.to_owned())
                .build(),
        )
        .await
        .expect("insert principal");

    PgAuthTokenRepository
        .insert(
            &mut conn,
            &NewAuthToken::builder()
                .token_hash(sha256_hex(raw_token))
                .principal_id(principal.id())
                .scopes(scopes)
                .expires_at(Utc::now() + expires_in)
                .build(),
        )
        .await
        .expect("insert auth token");
}

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

// ---------------------------------------------------------------------------
// MCP test client
// ---------------------------------------------------------------------------

/// Timeout for reading an MCP response body.
///
/// Well above any realistic handler latency so a passing test never
/// hits this, but low enough that a broken test fails fast.
const MCP_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Lightweight MCP-over-HTTP client for transport integration tests.
///
/// Manages the bearer token, session ID, and JSON-RPC message IDs
/// internally so test code focuses on assertions rather than protocol
/// plumbing.
pub struct McpTestClient {
    client: reqwest::Client,
    addr: SocketAddr,
    token: String,
    session_id: Option<String>,
    next_id: u64,
}

impl McpTestClient {
    pub fn new(addr: SocketAddr, token: &str) -> Self {
        Self {
            client: test_client(),
            addr,
            token: token.to_owned(),
            session_id: None,
            next_id: 1,
        }
    }

    /// Sends an MCP `initialize` request and stores the session ID.
    ///
    /// The response body is read (with timeout) to ensure the server
    /// has fully processed the request before subsequent calls.
    ///
    /// # Panics
    ///
    /// Panics if the server does not return 200 or omits the
    /// `mcp-session-id` header.
    pub async fn initialise(&mut self) {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "initialize",
            "id": self.next_id(),
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "0.1" },
            },
        });

        let response = self.post(&body).await;

        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "initialize must return 200",
        );

        let session_id = response
            .headers()
            .get("mcp-session-id")
            .expect("initialize response must include mcp-session-id")
            .to_str()
            .expect("session ID must be valid UTF-8")
            .to_owned();

        // Read the body to ensure the server has processed initialize
        // before we send follow-up requests.
        let _body = tokio::time::timeout(MCP_RESPONSE_TIMEOUT, response.text())
            .await
            .expect("initialize body should arrive within timeout")
            .expect("initialize body read should succeed");

        self.session_id = Some(session_id);
    }

    /// Sends an MCP `tools/list` request and returns the tool names
    /// visible to the authenticated principal.
    ///
    /// # Panics
    ///
    /// Panics if the session has not been initialised, the server does
    /// not return 200, or the response body does not contain a valid
    /// `tools/list` result.
    pub async fn list_tool_names(&mut self) -> Vec<String> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "tools/list",
            "id": self.next_id(),
            "params": {},
        });

        let response = self.post(&body).await;

        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "tools/list must return 200",
        );

        let body = tokio::time::timeout(MCP_RESPONSE_TIMEOUT, response.text())
            .await
            .expect("tools/list body should arrive within timeout")
            .expect("tools/list body read should succeed");

        extract_tool_names(&body)
    }

    async fn post(&self, body: &serde_json::Value) -> reqwest::Response {
        let mut request = self
            .client
            .post(format!("http://{}/mcp", self.addr))
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Accept", "text/event-stream, application/json");

        if let Some(session_id) = &self.session_id {
            request = request.header("Mcp-Session-Id", session_id);
        }

        request
            .body(body.to_string())
            .send()
            .await
            .expect("MCP request must succeed")
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

/// Asserts that the tools visible to a principal match exactly those
/// whose required scope is satisfied by the granted scopes.
///
/// Initialises an MCP session, calls `tools/list`, computes the
/// expected set from the tool registry, and asserts equality.
pub async fn assert_tool_visibility(
    mcp: &mut McpTestClient,
    granted_scopes: &[Scope],
) {
    let mut actual = mcp.list_tool_names().await;
    actual.sort();

    let mut expected: Vec<String> = tool_scope_registry()
        .iter()
        .filter(|(_, required)| is_authorised(granted_scopes, required))
        .map(|(name, _)| (*name).to_owned())
        .collect();
    expected.sort();

    assert_eq!(
        actual, expected,
        "tools/list should reflect the principal's scopes",
    );
}

// ---------------------------------------------------------------------------
// SSE response parsing
// ---------------------------------------------------------------------------

/// Extracts tool names from an SSE response body containing a
/// JSON-RPC `tools/list` result.
///
/// Scans for `data:` lines, parses the JSON, and collects tool names
/// from the `result.tools` array.
///
/// # Panics
///
/// Panics if the body contains no `data:` line with a valid
/// `tools/list` result.
fn extract_tool_names(sse_body: &str) -> Vec<String> {
    for line in sse_body.lines() {
        let Some(json_str) = line.strip_prefix("data:") else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(json_str.trim()) else {
            continue;
        };
        if let Some(tools) = value.pointer("/result/tools").and_then(|t| t.as_array()) {
            return tools
                .iter()
                .filter_map(|t| t["name"].as_str().map(String::from))
                .collect();
        }
    }
    panic!("no tools/list result found in SSE body:\n{sse_body}");
}
