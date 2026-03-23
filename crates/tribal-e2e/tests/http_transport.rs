//! Integration tests for the HTTP transport stack.
//!
//! Exercises the full path: TCP listener → axum middleware → auth
//! verification → `StreamableHttpService` → handler.  Requires a
//! running Postgres instance (testcontainers).

mod transport_harness;

use chrono::Duration;
use reqwest::StatusCode;
use tokio_util::sync::CancellationToken;
use transport_harness::{
    McpTestClient, assert_tool_visibility, fresh_pool, seed_auth, seed_scoped_auth,
    spawn_transport, test_app_state, test_client,
};
use tribal_config::ServerConfig;
use tribal_domain::Scope;
use tribal_mcp::HandlerConfig;
use tribal_server::run_http_transport;
use tribal_test_utils::serial_lock;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const RAW_TOKEN: &str = "http-integration-test-token";

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_missing_bearer_token_returns_401() {
    let _lock = serial_lock().await;
    let pool = fresh_pool().await;
    let ct = CancellationToken::new();
    let state = test_app_state(pool, ct.clone());

    let transport = spawn_transport(
        ct,
        ServerConfig::default(),
        move |task_ct, config, listener| async move {
            run_http_transport(
                &state,
                &config,
                HandlerConfig::default(),
                task_ct,
                Some(listener),
            )
            .await
            .expect("HTTP transport must not fail");
        },
    )
    .await;

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

    seed_auth(&pool, "user:valid-token", RAW_TOKEN, Duration::hours(1)).await;

    let transport = spawn_transport(
        ct,
        ServerConfig::default(),
        move |task_ct, config, listener| async move {
            run_http_transport(
                &state,
                &config,
                HandlerConfig::default(),
                task_ct,
                Some(listener),
            )
            .await
            .expect("HTTP transport must not fail");
        },
    )
    .await;

    let response = test_client()
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

    let transport = spawn_transport(
        ct,
        ServerConfig::default(),
        move |task_ct, config, listener| async move {
            run_http_transport(
                &state,
                &config,
                HandlerConfig::default(),
                task_ct,
                Some(listener),
            )
            .await
            .expect("HTTP transport must not fail");
        },
    )
    .await;

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
// Principal propagation
// ---------------------------------------------------------------------------

/// A Canopy intern has read-only access to the knowledge base and can
/// check job status, but cannot ingest, provide feedback, or set
/// session context.  Verifies that scopes flow from the bearer token
/// through to `tools/list` filtering.
#[tokio::test]
async fn test_read_only_principal_sees_only_read_tools() {
    let _lock = serial_lock().await;
    let pool = fresh_pool().await;
    let ct = CancellationToken::new();
    let state = test_app_state(pool.clone(), ct.clone());

    let intern_token = "canopy-intern-token";
    let granted_scopes = vec![
        Scope::parse("tribal.knowledge:read").expect("valid scope"),
        Scope::parse("tribal.jobs:read").expect("valid scope"),
    ];

    seed_scoped_auth(
        &pool,
        "intern:canopy-reader",
        intern_token,
        Duration::hours(1),
        granted_scopes.clone(),
    )
    .await;

    let transport = spawn_transport(
        ct,
        ServerConfig::default(),
        move |task_ct, config, listener| async move {
            run_http_transport(
                &state,
                &config,
                HandlerConfig::default(),
                task_ct,
                Some(listener),
            )
            .await
            .expect("HTTP transport must not fail");
        },
    )
    .await;

    let mut mcp = McpTestClient::new(transport.addr, intern_token);
    mcp.initialise().await;
    assert_tool_visibility(&mut mcp, &granted_scopes).await;

    transport.shutdown().await;
}
