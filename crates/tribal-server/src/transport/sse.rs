//! SSE transport for the Tribal MCP server.
//!
//! Structurally identical to the HTTP transport but applies SSE-specific
//! lifecycle policies: max connection age (bounds the revocation window
//! for tokens), idle timeout (evicts dead connections), and keepalive
//! (prevents proxy timeouts).

use std::{sync::Arc, time::Duration};

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tribal_auth::{
    oauth::{OAuthRouterState, OAuthRuntimeConfig, oauth_router},
    require_bearer_auth,
};
use tribal_config::{ServerConfig, TransportKind};
use tribal_mcp::{AppState, HandlerConfig};

use super::{common, sse_lifecycle::SseLifecycleLayer};
use crate::error::AppError;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the SSE transport with lifecycle enforcement.
///
/// Binds a TCP listener, configures the axum router with bearer token
/// authentication middleware and the SSE lifecycle layer, and serves
/// requests until the cancellation token is triggered.
///
/// The lifecycle layer enforces:
/// - **Max connection age** — force-closes connections after the
///   configured duration, bounding the revocation window.
/// - **Idle timeout** — closes connections with no real events for the
///   configured duration.
/// - **Keepalive** — SSE comment sent at regular intervals to prevent
///   proxy timeouts (handled natively by rmcp).
///
/// # Parameters
///
/// * `listener` — when `Some`, the pre-bound listener is used directly
///   (useful for tests that bind to `127.0.0.1:0` for an ephemeral
///   port).  When `None`, the function binds to the address from
///   `server_config` (or [`DEFAULT_BIND_ADDRESS`](tribal_config::DEFAULT_BIND_ADDRESS)
///   as fallback).
///
/// # Panics
///
/// Panics if `listener` is `None` and `server_config.bind_address`
/// (or the default) is not a valid socket address.  This is guarded
/// by config validation at startup.
///
/// # Errors
///
/// Returns [`AppError::TransportBind`] if the TCP listener cannot bind,
/// or [`AppError::TransportServe`] if the server encounters a fatal
/// I/O error.
pub async fn run_sse_transport(
    state: &Arc<AppState>,
    server_config: &ServerConfig,
    oauth_runtime: Arc<OAuthRuntimeConfig>,
    handler_config: HandlerConfig,
    cancellation_token: CancellationToken,
    listener: Option<TcpListener>,
) -> Result<(), AppError> {
    let transport = TransportKind::Sse;

    let (listener, local_addr) = common::bind_listener(server_config, transport, listener).await?;
    let challenge = Arc::new(common::bearer_challenge_for(&oauth_runtime));
    let auth_state = common::auth_middleware_state(state, Arc::clone(&challenge));
    let mcp_service = common::mcp_service(
        state,
        handler_config,
        server_config,
        cancellation_token.clone(),
        "sse",
    );

    let lifecycle_layer = SseLifecycleLayer::new(
        Duration::from_millis(server_config.sse.max_connection_age_ms),
        Duration::from_millis(server_config.sse.idle_timeout_ms),
    );

    let oauth = oauth_router(OAuthRouterState::new(oauth_runtime));

    // Layer ordering on the MCP branch (outermost runs first):
    //   1. Bearer auth middleware — rejects unauthenticated requests
    //   2. SSE lifecycle layer — wraps authenticated SSE response bodies
    //   3. MCP service — handles the request
    // The OAuth router is merged at the top level so its endpoints sit
    // outside the bearer middleware.
    let app = axum::Router::new()
        .nest_service("/mcp", mcp_service)
        .layer(lifecycle_layer)
        .layer(axum::middleware::from_fn_with_state(
            auth_state,
            require_bearer_auth,
        ))
        .merge(oauth);

    tracing::info!(%local_addr, "SSE transport listening");

    common::serve(listener, app, cancellation_token, transport).await
}
