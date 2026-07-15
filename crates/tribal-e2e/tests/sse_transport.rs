//! Integration tests for the SSE transport stack.
//!
//! Exercises the full path: TCP listener → axum middleware → auth
//! verification → SSE lifecycle layer → `StreamableHttpService` →
//! handler.  Requires a running Postgres instance (testcontainers).

mod transport_harness;

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use reqwest::StatusCode;
use tokio_util::sync::CancellationToken;
use transport_harness::{
    INITIALIZE_BODY, LIFECYCLE_FAR_FUTURE_MS, MINIMAL_INITIALIZE_BODY, McpTestClient,
    TEST_CANONICAL_RESOURCE, TransportHandle, assert_tool_visibility, fresh_pool, seed_auth,
    seed_scoped_auth, spawn_transport, test_app_state, test_client,
};
use tribal::run_sse_transport;
use tribal_auth::oauth::OAuthRuntimeConfig;
use tribal_config::{OAuthConfig, ServerConfig, SseConfig};
use tribal_domain::Scope;
use tribal_mcp::{AppState, HandlerConfig};
use url::Url;

fn test_oauth_runtime() -> Arc<OAuthRuntimeConfig> {
    let issuer = Url::parse("http://127.0.0.1:8080").unwrap();
    let resource = Url::parse(TEST_CANONICAL_RESOURCE).unwrap();
    Arc::new(OAuthRuntimeConfig::build(&OAuthConfig::default(), &issuer, &resource, true).unwrap())
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Each test needs a distinct raw token because `auth_tokens.token_hash`
// has a unique constraint — reusing the same value across tests causes
// `UniqueViolation` on the second insert.
const RAW_TOKEN_VALID: &str = "sse-valid-token";
const RAW_TOKEN_EXPIRED: &str = "sse-expired-token";
const RAW_TOKEN_MAX_AGE: &str = "sse-max-age-token";
const RAW_TOKEN_IDLE: &str = "sse-idle-timeout-token";
const RAW_TOKEN_SPY: &str = "sse-spy-undercover-token";
const RAW_TOKEN_GET_STREAM: &str = "sse-get-stream-token";
const RAW_TOKEN_GET_NO_SESSION: &str = "sse-get-no-session-token";
const RAW_TOKEN_GET_UNKNOWN: &str = "sse-get-unknown-session-token";
const RAW_TOKEN_GET_RESUME: &str = "sse-get-resume-token";

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
    state: &Arc<AppState>,
    ct: CancellationToken,
    config: ServerConfig,
) -> Pin<Box<dyn Future<Output = TransportHandle> + Send + '_>> {
    let state = state.clone();
    Box::pin(spawn_transport(
        ct,
        config,
        move |task_ct, cfg, listener| async move {
            run_sse_transport(
                &state,
                &cfg,
                test_oauth_runtime(),
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
    let db = fresh_pool().await;
    let pool = db.pool().clone();
    let ct = CancellationToken::new();
    let state = test_app_state(pool, ct.clone());
    let transport = spawn_sse(&state, ct, ServerConfig::default()).await;

    let response = test_client()
        .post(format!("http://{}/mcp", transport.addr))
        .header("Content-Type", "application/json")
        .body(MINIMAL_INITIALIZE_BODY)
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
    let db = fresh_pool().await;
    let pool = db.pool().clone();
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
        .body(INITIALIZE_BODY)
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
    let db = fresh_pool().await;
    let pool = db.pool().clone();
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    seed_auth(
        &pool,
        "user:expired-token",
        RAW_TOKEN_EXPIRED,
        -chrono::Duration::hours(1),
    )
    .await;

    let transport = spawn_sse(&state, ct, ServerConfig::default()).await;

    let response = test_client()
        .post(format!("http://{}/mcp", transport.addr))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {RAW_TOKEN_EXPIRED}"))
        .body(MINIMAL_INITIALIZE_BODY)
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
    let db = fresh_pool().await;
    let pool = db.pool().clone();
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
        .body(INITIALIZE_BODY)
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
    let db = fresh_pool().await;
    let pool = db.pool().clone();
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
        .body(INITIALIZE_BODY)
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
    let db = fresh_pool().await;
    let pool = db.pool().clone();
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    let granted_scopes = vec![Scope::parse("tribal.jobs:read").expect("valid scope")];

    seed_scoped_auth(
        &pool,
        "spy:undercover-monitor",
        RAW_TOKEN_SPY,
        chrono::Duration::hours(1),
        granted_scopes.clone(),
    )
    .await;

    let transport = spawn_sse(&state, ct, ServerConfig::default()).await;

    let mut mcp = McpTestClient::new(transport.addr, RAW_TOKEN_SPY);
    mcp.initialise().await;
    assert_tool_visibility(&mut mcp, &granted_scopes).await;

    transport.shutdown().await;
}

// ---------------------------------------------------------------------------
// GET stream tests
// ---------------------------------------------------------------------------

/// GET to an existing session returns an SSE stream (the reconnect
/// path).  This is the most SSE-specific codepath — it opens a
/// standalone event stream for an already-initialised session.
#[tokio::test]
async fn test_get_to_existing_session_returns_sse_stream() {
    let db = fresh_pool().await;
    let pool = db.pool().clone();
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    seed_auth(
        &pool,
        "user:get-stream",
        RAW_TOKEN_GET_STREAM,
        chrono::Duration::hours(1),
    )
    .await;

    let transport = spawn_sse(&state, ct, ServerConfig::default()).await;

    let mut mcp = McpTestClient::new(transport.addr, RAW_TOKEN_GET_STREAM);
    mcp.initialise().await;

    let response = mcp.get().await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "GET to existing session must return 200",
    );
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("text/event-stream")),
        "GET response must be an SSE stream",
    );

    transport.shutdown().await;
}

/// GET without a session ID is rejected with 400.
#[tokio::test]
async fn test_get_without_session_id_returns_400() {
    let db = fresh_pool().await;
    let pool = db.pool().clone();
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    seed_auth(
        &pool,
        "user:get-no-session",
        RAW_TOKEN_GET_NO_SESSION,
        chrono::Duration::hours(1),
    )
    .await;

    let transport = spawn_sse(&state, ct, ServerConfig::default()).await;

    // Create a client but do NOT initialise — no session ID.
    let mcp = McpTestClient::new(transport.addr, RAW_TOKEN_GET_NO_SESSION);
    let response = mcp.get().await;

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "GET without session ID must return 400",
    );

    transport.shutdown().await;
}

/// GET with a session ID that does not exist returns 404.
#[tokio::test]
async fn test_get_with_unknown_session_id_returns_404() {
    let db = fresh_pool().await;
    let pool = db.pool().clone();
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    seed_auth(
        &pool,
        "user:get-unknown-session",
        RAW_TOKEN_GET_UNKNOWN,
        chrono::Duration::hours(1),
    )
    .await;

    let transport = spawn_sse(&state, ct, ServerConfig::default()).await;

    // Fabricate a client with a bogus session ID.
    let mut mcp = McpTestClient::new(transport.addr, RAW_TOKEN_GET_UNKNOWN);
    mcp.initialise().await;

    // Replace the real session ID with a nonexistent one.
    let fake_client = test_client();
    let response = fake_client
        .get(format!("http://{}/mcp", transport.addr))
        .header("Authorization", format!("Bearer {RAW_TOKEN_GET_UNKNOWN}"))
        .header("Accept", "text/event-stream")
        .header("Mcp-Session-Id", "nonexistent-session-id")
        .send()
        .await
        .expect("GET request must succeed");

    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "GET with unknown session ID must return 404",
    );

    transport.shutdown().await;
}

/// GET with `Last-Event-Id` to an existing session returns an SSE
/// stream (the resume path).  Verifies the server accepts the header
/// and responds with a valid SSE stream rather than an error.
#[tokio::test]
async fn test_get_with_last_event_id_returns_sse_stream() {
    let db = fresh_pool().await;
    let pool = db.pool().clone();
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    seed_auth(
        &pool,
        "user:get-resume",
        RAW_TOKEN_GET_RESUME,
        chrono::Duration::hours(1),
    )
    .await;

    let transport = spawn_sse(&state, ct, ServerConfig::default()).await;

    let mut mcp = McpTestClient::new(transport.addr, RAW_TOKEN_GET_RESUME);
    mcp.initialise().await;

    // Resume from event ID "0" (the priming event ID assigned during
    // initialize).  The server should accept this and return an SSE
    // stream with any cached events after that ID.
    let response = mcp.get_with_last_event_id("0").await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "GET with Last-Event-Id must return 200",
    );
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("text/event-stream")),
        "resume response must be an SSE stream",
    );

    transport.shutdown().await;
}
