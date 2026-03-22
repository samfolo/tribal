//! Integration tests for the HTTP transport stack.
//!
//! Exercises the full path: TCP listener → axum middleware → auth
//! verification → `StreamableHttpService` → handler.  Requires a
//! running Postgres instance (testcontainers).

use std::{net::SocketAddr, sync::Arc};

use chrono::{Duration, Utc};
use dashmap::DashMap;
use reqwest::StatusCode;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_util::sync::CancellationToken;
use tribal_common::sha256_hex;
use tribal_config::{DEFAULT_OLLAMA_BASE_URL, ProviderKind, ServerConfig, WorkerConfig};
use tribal_db::{
    AuthTokenRepository, NewAuthToken, NewPrincipal, PgAuthTokenRepository, PgPrincipalRepository,
    PrincipalRepository,
};
use tribal_domain::{PromptVersionId, full_access_scopes};
use tribal_inference::{ProviderRegistry, RequestClass};
use tribal_mcp::{ActivePromptVersions, AppState, HandlerConfig};
use tribal_server::run_http_transport;
use tribal_test_utils::{MockEmbeddingProvider, MockInferenceProvider, serial_lock, test_context};

/// Creates a fresh pool against the shared test database.
///
/// Each test gets its own pool so transport shutdown in one test
/// does not starve connections in the next.
async fn fresh_pool() -> sqlx::PgPool {
    let ctx = test_context().await;
    sqlx::pool::PoolOptions::new()
        .max_connections(5)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(ctx.database_url())
        .await
        .expect("connect fresh pool")
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TEST_INSTANCE_ID: &str = "http-test-00000000-0000-0000-0000-000000000000";
const RAW_TOKEN: &str = "http-integration-test-token";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
    expires_in: Duration,
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

/// Handle to a running HTTP transport, ensuring clean shutdown.
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
        let _ = self.join.await;
    }
}

/// Spawns the HTTP transport and returns a handle for clean teardown.
async fn spawn_transport(state: &Arc<AppState>, ct: CancellationToken) -> TransportHandle {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local address");

    let state = Arc::clone(state);
    let task_ct = ct.clone();
    let join = tokio::spawn(async move {
        let _ = run_http_transport(
            &state,
            &ServerConfig::default(),
            HandlerConfig::default(),
            task_ct,
            Some(listener),
        )
        .await;
    });

    // Allow the server to start accepting connections.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    TransportHandle { addr, ct, join }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_missing_bearer_token_returns_401() {
    let _lock = serial_lock().await;
    let pool = fresh_pool().await;
    let ct = CancellationToken::new();
    let state = test_app_state(pool, ct.clone());
    let transport = spawn_transport(&state, ct).await;

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

    seed_auth(&pool, "user:valid-token", RAW_TOKEN, Duration::hours(1)).await;

    let transport = spawn_transport(&state, ct).await;

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

    // A valid token passes auth and reaches the MCP handler.  The
    // StreamableHttpService returns 200 with an SSE stream containing
    // the initialize response and assigns a session ID.
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
        -Duration::hours(1),
    )
    .await;

    let transport = spawn_transport(&state, ct).await;

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
