//! Integration tests for the SSE transport stack.
//!
//! Exercises the full path: TCP listener → axum middleware → auth
//! verification → SSE lifecycle layer → `StreamableHttpService` →
//! handler.  Requires a running Postgres instance (testcontainers).

use std::{net::SocketAddr, sync::Arc, time::Duration};

use chrono::Utc;
use dashmap::DashMap;
use reqwest::StatusCode;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tokio_util::sync::CancellationToken;
use tribal_common::sha256_hex;
use tribal_config::{
    DEFAULT_OLLAMA_BASE_URL, ProviderKind, SseConfig, ServerConfig, WorkerConfig,
};
use tribal_db::{
    AuthTokenRepository, NewAuthToken, NewPrincipal, PgAuthTokenRepository, PgPrincipalRepository,
    PrincipalRepository,
};
use tribal_domain::{PromptVersionId, full_access_scopes};
use tribal_inference::{ProviderRegistry, RequestClass};
use tribal_mcp::{ActivePromptVersions, AppState, HandlerConfig};
use tribal_server::run_sse_transport;
use tribal_test_utils::{MockEmbeddingProvider, MockInferenceProvider, serial_lock, test_context};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TEST_INSTANCE_ID: &str = "sse-test-00000000-0000-0000-0000-000000000000";
const RAW_TOKEN: &str = "sse-integration-test-token";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Creates a fresh pool against the shared test database.
///
/// Each test gets its own pool so transport shutdown in one test
/// does not starve connections in the next.
async fn fresh_pool() -> sqlx::PgPool {
    let ctx = test_context().await;
    sqlx::pool::PoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(5))
        .connect(ctx.database_url())
        .await
        .expect("connect fresh pool")
}

/// Builds an [`AppState`] backed by a real test database pool and mock
/// providers.  Only the pool and server config are exercised — the
/// providers exist solely to satisfy the builder.
fn test_app_state(pool: sqlx::PgPool, ct: CancellationToken) -> Arc<AppState> {
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

/// Seeds a principal and auth token with the given raw token value and
/// expiry offset from now.
async fn seed_auth(
    pool: &sqlx::PgPool,
    principal_key: &str,
    raw_token: &str,
    expires_in: chrono::Duration,
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
                .scopes(full_access_scopes())
                .expires_at(Utc::now() + expires_in)
                .build(),
        )
        .await
        .expect("insert auth token");
}

/// Handle to a running SSE transport, ensuring clean shutdown.
struct TransportHandle {
    addr: SocketAddr,
    ct: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl TransportHandle {
    /// Cancels the transport and waits for the task to finish, ensuring
    /// all pool connections are released before the next test.
    async fn shutdown(self) {
        self.ct.cancel();
        self.join.await.expect("transport task must not panic");
    }
}

/// Spawns the SSE transport and returns a handle for clean teardown.
///
/// Accepts a `ServerConfig` so lifecycle tests can override SSE timeouts.
async fn spawn_transport(
    state: &Arc<AppState>,
    ct: CancellationToken,
    server_config: ServerConfig,
) -> TransportHandle {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local address");

    let state = Arc::clone(state);
    let task_ct = ct.clone();
    let join = tokio::spawn(async move {
        run_sse_transport(
            &state,
            &server_config,
            HandlerConfig::default(),
            task_ct,
            Some(listener),
        )
        .await
        .expect("SSE transport must not fail");
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
// Auth tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_missing_bearer_token_returns_401() {
    let _lock = serial_lock().await;
    let pool = fresh_pool().await;
    let ct = CancellationToken::new();
    let state = test_app_state(pool, ct.clone());
    let transport = spawn_transport(&state, ct, ServerConfig::default()).await;

    let client = reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .build()
        .expect("build reqwest client");
    let response = client
        .post(format!("http://{}/mcp", transport.addr))
        .header("Content-Type", "application/json")
        .body(r#"{"jsonrpc":"2.0","method":"initialize","id":1,"params":{}}"#)
        .send()
        .await
        .expect("HTTP request must succeed");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = response.json().await.expect("response must be JSON");
    assert_eq!(body["error"], "unauthorized");
    assert_eq!(body["message"], "missing bearer token");

    transport.shutdown().await;
    drop(state);
}

#[tokio::test]
async fn test_valid_bearer_token_passes_auth() {
    let _lock = serial_lock().await;
    let pool = fresh_pool().await;
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    seed_auth(&pool, "user:valid-token", RAW_TOKEN, chrono::Duration::hours(1)).await;

    let transport = spawn_transport(&state, ct, ServerConfig::default()).await;

    let client = reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .build()
        .expect("build reqwest client");
    let response = client
        .post(format!("http://{}/mcp", transport.addr))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {RAW_TOKEN}"))
        .header("Accept", "text/event-stream, application/json")
        .body(r#"{"jsonrpc":"2.0","method":"initialize","id":1,"params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}"#)
        .send()
        .await
        .expect("HTTP request must succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers().contains_key("mcp-session-id"),
        "initialize response must include a session ID",
    );

    transport.shutdown().await;
    drop(state);
}

#[tokio::test]
async fn test_expired_token_returns_401() {
    let _lock = serial_lock().await;
    let pool = fresh_pool().await;
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    let expired_raw = "expired-token-value";
    seed_auth(
        &pool,
        "user:expired-token",
        expired_raw,
        -chrono::Duration::hours(1),
    )
    .await;

    let transport = spawn_transport(&state, ct, ServerConfig::default()).await;

    let client = reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .build()
        .expect("build reqwest client");
    let response = client
        .post(format!("http://{}/mcp", transport.addr))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {expired_raw}"))
        .body(r#"{"jsonrpc":"2.0","method":"initialize","id":1,"params":{}}"#)
        .send()
        .await
        .expect("HTTP request must succeed");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = response.json().await.expect("response must be JSON");
    assert_eq!(body["error"], "unauthorized");
    assert_eq!(body["message"], "token expired");

    transport.shutdown().await;
    drop(state);
}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

/// Short max connection age for lifecycle tests.
///
/// Must be long enough for the TCP handshake and HTTP request to
/// complete, but short enough that the test finishes quickly.
const SHORT_MAX_CONNECTION_AGE_MS: u64 = 500;

/// Short idle timeout for lifecycle tests.
///
/// Must be longer than `keepalive_interval_ms` (config validation
/// constraint) but short enough for a fast test.
const SHORT_IDLE_TIMEOUT_MS: u64 = 500;

/// Keepalive interval for lifecycle tests.
///
/// Must be strictly less than `SHORT_IDLE_TIMEOUT_MS` to satisfy
/// config validation.
const SHORT_KEEPALIVE_INTERVAL_MS: u64 = 100;

fn lifecycle_server_config(sse: SseConfig) -> ServerConfig {
    ServerConfig {
        sse,
        ..ServerConfig::default()
    }
}

#[tokio::test]
async fn test_max_connection_age_closes_stream() {
    let _lock = serial_lock().await;
    let pool = fresh_pool().await;
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    seed_auth(
        &pool,
        "user:max-age-test",
        RAW_TOKEN,
        chrono::Duration::hours(1),
    )
    .await;

    let config = lifecycle_server_config(SseConfig {
        max_connection_age_ms: SHORT_MAX_CONNECTION_AGE_MS,
        idle_timeout_ms: 60_000,
        keepalive_interval_ms: SHORT_KEEPALIVE_INTERVAL_MS,
    });
    let transport = spawn_transport(&state, ct, config).await;

    let client = reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .build()
        .expect("build reqwest client");

    let response = client
        .post(format!("http://{}/mcp", transport.addr))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {RAW_TOKEN}"))
        .header("Accept", "text/event-stream, application/json")
        .body(r#"{"jsonrpc":"2.0","method":"initialize","id":1,"params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}"#)
        .send()
        .await
        .expect("HTTP request must succeed");

    assert_eq!(response.status(), StatusCode::OK);

    // Read the full body — the lifecycle layer should close the stream
    // after the max connection age elapses.
    let body = tokio::time::timeout(
        Duration::from_secs(5),
        response.bytes(),
    )
    .await
    .expect("stream should close within timeout")
    .expect("body read should succeed");

    // The stream closed (we got the full body), which means the
    // lifecycle layer terminated it.
    assert!(!body.is_empty(), "response should contain SSE data before closure");

    transport.shutdown().await;
    drop(state);
}

#[tokio::test]
async fn test_idle_timeout_closes_stream() {
    let _lock = serial_lock().await;
    let pool = fresh_pool().await;
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    seed_auth(
        &pool,
        "user:idle-timeout-test",
        RAW_TOKEN,
        chrono::Duration::hours(1),
    )
    .await;

    let config = lifecycle_server_config(SseConfig {
        max_connection_age_ms: 60_000,
        idle_timeout_ms: SHORT_IDLE_TIMEOUT_MS,
        keepalive_interval_ms: SHORT_KEEPALIVE_INTERVAL_MS,
    });
    let transport = spawn_transport(&state, ct, config).await;

    let client = reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .build()
        .expect("build reqwest client");

    let response = client
        .post(format!("http://{}/mcp", transport.addr))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {RAW_TOKEN}"))
        .header("Accept", "text/event-stream, application/json")
        .body(r#"{"jsonrpc":"2.0","method":"initialize","id":1,"params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}"#)
        .send()
        .await
        .expect("HTTP request must succeed");

    assert_eq!(response.status(), StatusCode::OK);

    // Read the full body — after the initial response events, no
    // further real events flow.  The idle timeout should close the
    // stream.  Keepalive comments do NOT reset the idle timer.
    let body = tokio::time::timeout(
        Duration::from_secs(5),
        response.bytes(),
    )
    .await
    .expect("stream should close within timeout")
    .expect("body read should succeed");

    assert!(!body.is_empty(), "response should contain SSE data before closure");

    transport.shutdown().await;
    drop(state);
}
