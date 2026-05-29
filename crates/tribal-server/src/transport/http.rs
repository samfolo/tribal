//! Streamable HTTP transport for the Tribal MCP server.
//!
//! Binds a TCP listener, configures the axum router with bearer token
//! authentication middleware, and serves MCP requests via
//! [`StreamableHttpService`] until the cancellation token is triggered.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tribal_auth::{
    oauth::{OAuthRouterState, OAuthRuntimeConfig, oauth_router},
    require_bearer_auth,
};
use tribal_config::{ServerConfig, TransportKind};
use tribal_mcp::{AppState, HandlerConfig};

use super::common;
use crate::error::AppError;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the HTTP transport with the Streamable HTTP protocol.
///
/// Binds a TCP listener, configures the axum router with bearer token
/// authentication middleware, and serves requests until the cancellation
/// token is triggered.
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
pub async fn run_http_transport(
    state: &Arc<AppState>,
    server_config: &ServerConfig,
    oauth_runtime: Arc<OAuthRuntimeConfig>,
    handler_config: HandlerConfig,
    cancellation_token: CancellationToken,
    listener: Option<TcpListener>,
) -> Result<(), AppError> {
    let transport = TransportKind::Http;

    let (listener, local_addr) = common::bind_listener(server_config, transport, listener).await?;
    let challenge = Arc::new(common::bearer_challenge_for(&oauth_runtime));
    let auth_state =
        common::auth_middleware_state(state, transport, &oauth_runtime, Arc::clone(&challenge));
    let mcp_service = common::mcp_service(
        state,
        handler_config,
        server_config,
        cancellation_token.clone(),
        "http",
    );

    let oauth = oauth_router(OAuthRouterState::new(
        oauth_runtime,
        state.mcp_pool().clone(),
    ));

    let app = axum::Router::new()
        .nest_service("/mcp", mcp_service)
        .layer(axum::middleware::from_fn_with_state(
            auth_state,
            require_bearer_auth,
        ))
        .merge(oauth);

    tracing::info!(%local_addr, "HTTP transport listening");

    common::serve(listener, app, cancellation_token, transport).await
}
