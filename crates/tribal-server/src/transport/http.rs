//! Streamable HTTP transport for the Tribal MCP server.
//!
//! Binds a TCP listener, configures the axum router with bearer token
//! authentication middleware, and serves MCP requests via
//! [`StreamableHttpService`] until the cancellation token is triggered.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager,
    tower::{StreamableHttpServerConfig, StreamableHttpService},
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tribal_config::{DEFAULT_BIND_ADDRESS, ServerConfig};
use tribal_db::{PgAuthTokenRepository, PgPrincipalRepository};
use tribal_mcp::{
    AppState, AuthMiddlewareState, Authenticator, ConnectionRepositories, HandlerConfig,
    SessionContext, TransportAuthStrategy, TribalServerHandler, require_bearer_auth,
};

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
///   `server_config` (or [`DEFAULT_BIND_ADDRESS`] as fallback).
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
    handler_config: HandlerConfig,
    cancellation_token: CancellationToken,
    listener: Option<TcpListener>,
) -> Result<(), AppError> {
    // -- Listener ------------------------------------------------------------

    let listener = if let Some(l) = listener {
        l
    } else {
        let bind_addr: SocketAddr = server_config
            .bind_address
            .as_deref()
            .unwrap_or(DEFAULT_BIND_ADDRESS)
            .parse()
            .expect("bind address validated during config validation");

        TcpListener::bind(bind_addr)
            .await
            .map_err(|source| AppError::TransportBind {
                address: bind_addr,
                source,
            })?
    };

    let local_addr = listener
        .local_addr()
        .map_err(|source| AppError::TransportServe { source })?;

    // -- Auth middleware ------------------------------------------------------

    let authenticator = Arc::new(Authenticator::new(
        Arc::new(PgAuthTokenRepository),
        Arc::new(PgPrincipalRepository),
    ));

    let auth_state = AuthMiddlewareState::new(state.mcp_pool().clone(), authenticator);

    // -- StreamableHttpService -----------------------------------------------

    let streamable_config = StreamableHttpServerConfig {
        cancellation_token: cancellation_token.clone(),
        stateful_mode: true,
        sse_keep_alive: Some(Duration::from_millis(
            server_config.sse.keepalive_interval_ms,
        )),
        ..Default::default()
    };

    let session_manager = Arc::new(LocalSessionManager::default());
    let factory_state = Arc::clone(state);
    let factory_config = handler_config;

    let mcp_service = StreamableHttpService::new(
        move || {
            let session = SessionContext::new(None);
            let repositories = ConnectionRepositories::new();
            Ok(TribalServerHandler::new(
                Arc::clone(&factory_state),
                TransportAuthStrategy::PerRequest,
                repositories,
                session,
                factory_config.clone(),
            ))
        },
        session_manager,
        streamable_config,
    );

    // -- Axum router ---------------------------------------------------------

    let app = axum::Router::new().nest_service("/mcp", mcp_service).layer(
        axum::middleware::from_fn_with_state(auth_state, require_bearer_auth),
    );

    // -- Serve ---------------------------------------------------------------

    tracing::info!(%local_addr, "HTTP transport listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(cancellation_token.cancelled_owned())
        .await
        .map_err(|source| AppError::TransportServe { source })
}
