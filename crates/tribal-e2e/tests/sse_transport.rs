//! Integration tests for the SSE transport stack.
//!
//! Exercises the full path: TCP listener → axum middleware → auth
//! verification → SSE lifecycle layer → `StreamableHttpService` →
//! handler.  Requires a running Postgres instance (testcontainers).

mod transport_harness;

use std::time::Duration;

use reqwest::StatusCode;
use tokio_util::sync::CancellationToken;
use transport_harness::{
    LIFECYCLE_FAR_FUTURE_MS, McpTestClient, assert_tool_visibility, fresh_pool, seed_auth,
    seed_scoped_auth, spawn_transport, test_app_state, test_client,
};
use tribal_config::{ServerConfig, SseConfig};
use tribal_domain::Scope;
use tribal_mcp::HandlerConfig;
use tribal_server::run_sse_transport;
use tribal_test_utils::serial_lock;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Each test needs a distinct raw token because `auth_tokens.token_hash`
// has a unique constraint — reusing the same value across tests causes
// `UniqueViolation` on the second insert.
const RAW_TOKEN_VALID: &str = "sse-valid-token";
const RAW_TOKEN_MAX_AGE: &str = "sse-max-age-token";
const RAW_TOKEN_IDLE: &str = "sse-idle-timeout-token";

/// Short max connection age for lifecycle tests.
///
/// Must be long enough for the TCP handshake and HTTP request to
/// complete, but short enough that the test finishes quickly.
const SHORT_MAX_CONNECTION_AGE_MS: u64 = 500;

/// Short idle timeout for lifecycle tests.
///
/// Must be longer than `SHORT_KEEPALIVE_INTERVAL_MS` (config
/// validation constraint) but short enough for a fast test.
const SHORT_IDLE_TIMEOUT_MS: u64 = 500;

/// Keepalive interval for lifecycle tests.
///
/// Must be strictly less than `SHORT_IDLE_TIMEOUT_MS` to satisfy
/// config validation.
const SHORT_KEEPALIVE_INTERVAL_MS: u64 = 100;

/// Upper bound for waiting on a lifecycle-closed stream.
///
/// Well above any configured lifecycle timeout so a passing test
/// never hits this, but low enough that a broken test fails fast.
const STREAM_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn spawn_sse(
    state: &std::sync::Arc<tribal_mcp::AppState>,
    ct: CancellationToken,
    config: ServerConfig,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = transport_harness::TransportHandle> + Send + '_>,
> {
    let state = state.clone();
    Box::pin(spawn_transport(
        ct,
        config,
        move |task_ct, cfg, listener| async move {
            run_sse_transport(
                &state,
                &cfg,
                HandlerConfig::default(),
                task_ct,
                Some(listener),
            )
            .await
            .expect("SSE transport must not fail");
        },
    ))
}

fn lifecycle_config(sse: SseConfig) -> ServerConfig {
    ServerConfig {
        sse,
        ..ServerConfig::default()
    }
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
    let transport = spawn_sse(&state, ct, ServerConfig::default()).await;

    let response = test_client()
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
}

#[tokio::test]
async fn test_valid_bearer_token_passes_auth() {
    let _lock = serial_lock().await;
    let pool = fresh_pool().await;
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    seed_auth(
        &pool,
        "user:valid-token",
        RAW_TOKEN_VALID,
        chrono::Duration::hours(1),
    )
    .await;

    let transport = spawn_sse(&state, ct, ServerConfig::default()).await;

    let response = test_client()
        .post(format!("http://{}/mcp", transport.addr))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {RAW_TOKEN_VALID}"))
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

    let transport = spawn_sse(&state, ct, ServerConfig::default()).await;

    let response = test_client()
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
}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_max_connection_age_closes_stream() {
    let _lock = serial_lock().await;
    let pool = fresh_pool().await;
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    seed_auth(
        &pool,
        "user:max-age-test",
        RAW_TOKEN_MAX_AGE,
        chrono::Duration::hours(1),
    )
    .await;

    let config = lifecycle_config(SseConfig {
        max_connection_age_ms: SHORT_MAX_CONNECTION_AGE_MS,
        idle_timeout_ms: LIFECYCLE_FAR_FUTURE_MS,
        keepalive_interval_ms: SHORT_KEEPALIVE_INTERVAL_MS,
    });
    let transport = spawn_sse(&state, ct, config).await;

    let response = test_client()
        .post(format!("http://{}/mcp", transport.addr))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {RAW_TOKEN_MAX_AGE}"))
        .header("Accept", "text/event-stream, application/json")
        .body(r#"{"jsonrpc":"2.0","method":"initialize","id":1,"params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}"#)
        .send()
        .await
        .expect("HTTP request must succeed");

    assert_eq!(response.status(), StatusCode::OK);

    // Read the full body — the lifecycle layer should close the stream
    // after the max connection age elapses.
    let body = tokio::time::timeout(STREAM_CLOSE_TIMEOUT, response.bytes())
        .await
        .expect("stream should close within timeout")
        .expect("body read should succeed");

    // The stream closed (we got the full body), which means the
    // lifecycle layer terminated it.
    assert!(
        !body.is_empty(),
        "response should contain SSE data before closure",
    );

    transport.shutdown().await;
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
        RAW_TOKEN_IDLE,
        chrono::Duration::hours(1),
    )
    .await;

    let config = lifecycle_config(SseConfig {
        max_connection_age_ms: LIFECYCLE_FAR_FUTURE_MS,
        idle_timeout_ms: SHORT_IDLE_TIMEOUT_MS,
        keepalive_interval_ms: SHORT_KEEPALIVE_INTERVAL_MS,
    });
    let transport = spawn_sse(&state, ct, config).await;

    let response = test_client()
        .post(format!("http://{}/mcp", transport.addr))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {RAW_TOKEN_IDLE}"))
        .header("Accept", "text/event-stream, application/json")
        .body(r#"{"jsonrpc":"2.0","method":"initialize","id":1,"params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}"#)
        .send()
        .await
        .expect("HTTP request must succeed");

    assert_eq!(response.status(), StatusCode::OK);

    // Read the full body — after the initial response events, no
    // further real events flow.  The idle timeout should close the
    // stream.  Keepalive comments do NOT reset the idle timer.
    let body = tokio::time::timeout(STREAM_CLOSE_TIMEOUT, response.bytes())
        .await
        .expect("stream should close within timeout")
        .expect("body read should succeed");

    assert!(
        !body.is_empty(),
        "response should contain SSE data before closure",
    );

    transport.shutdown().await;
}

// ---------------------------------------------------------------------------
// Principal propagation
// ---------------------------------------------------------------------------

/// A spy with an underprovisioned token can only monitor job status.
/// They cannot read the knowledge base, ingest content, or adjust
/// session context.  Verifies that a minimal scope set propagates
/// through the SSE transport to `tools/list` filtering.
#[tokio::test]
async fn test_underprovisioned_principal_sees_minimal_tools() {
    let _lock = serial_lock().await;
    let pool = fresh_pool().await;
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    let spy_token = "spy-undercover-monitor-token";
    let granted_scopes = vec![Scope::parse("tribal.jobs:read").expect("valid scope")];

    seed_scoped_auth(
        &pool,
        "spy:undercover-monitor",
        spy_token,
        chrono::Duration::hours(1),
        granted_scopes.clone(),
    )
    .await;

    let transport = spawn_sse(&state, ct, ServerConfig::default()).await;

    let mut mcp = McpTestClient::new(transport.addr, spy_token);
    mcp.initialise().await;
    assert_tool_visibility(&mut mcp, &granted_scopes).await;

    transport.shutdown().await;
}
