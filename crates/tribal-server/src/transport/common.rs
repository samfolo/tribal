//! Shared setup helpers for network transports (HTTP and SSE).
//!
//! Extracts the common listener binding, auth middleware construction,
//! MCP service factory, and graceful-shutdown serve logic that both
//! TCP-based transports share.  Transport-specific router composition
//! remains in each transport module.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager,
    tower::{StreamableHttpServerConfig, StreamableHttpService},
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tribal_config::{DEFAULT_BIND_ADDRESS, ServerConfig, TransportKind};
use tribal_db::{PgAuthTokenRepository, PgPrincipalRepository};
use tribal_mcp::{
    AppState, AuthMiddlewareState, Authenticator, ConnectionRepositories, HandlerConfig,
    SessionContext, SessionProject, TransportAuthStrategy, TribalServerHandler,
};

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Listener
// ---------------------------------------------------------------------------

/// Binds or reuses a TCP listener for a network transport.
///
/// When `listener` is `Some`, the pre-bound listener is used directly
/// (useful for tests that bind to `127.0.0.1:0`).  When `None`, binds
/// to `server_config.bind_address` with [`DEFAULT_BIND_ADDRESS`] as
/// fallback.
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
/// or [`AppError::TransportServe`] if the local address cannot be read.
pub(super) async fn bind_listener(
    server_config: &ServerConfig,
    transport: TransportKind,
    listener: Option<TcpListener>,
) -> Result<(TcpListener, SocketAddr), AppError> {
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
                transport,
                address: bind_addr,
                source,
            })?
    };

    let local_addr = listener
        .local_addr()
        .map_err(|source| AppError::TransportServe { transport, source })?;

    Ok((listener, local_addr))
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

/// Creates the bearer-token auth middleware state for per-request
/// transports.
pub(super) fn auth_middleware_state(state: &Arc<AppState>) -> AuthMiddlewareState {
    let authenticator = Arc::new(Authenticator::new(
        Arc::new(PgAuthTokenRepository),
        Arc::new(PgPrincipalRepository),
    ));

    AuthMiddlewareState::new(state.mcp_pool().clone(), authenticator)
}

// ---------------------------------------------------------------------------
// MCP service
// ---------------------------------------------------------------------------

/// Type alias for the `StreamableHttpService` produced by [`mcp_service`].
pub(super) type McpService = StreamableHttpService<TribalServerHandler, LocalSessionManager>;

/// Creates a [`StreamableHttpService`] with the per-session handler
/// factory used by both HTTP and SSE transports.
pub(super) fn mcp_service(
    state: &Arc<AppState>,
    handler_config: HandlerConfig,
    server_config: &ServerConfig,
    cancellation_token: CancellationToken,
) -> McpService {
    let streamable_config = StreamableHttpServerConfig {
        cancellation_token,
        stateful_mode: true,
        sse_keep_alive: Some(Duration::from_millis(
            server_config.sse.keepalive_interval_ms,
        )),
        ..Default::default()
    };

    let session_manager = Arc::new(LocalSessionManager::default());
    let factory_state = Arc::clone(state);

    StreamableHttpService::new(
        move || {
            let session_project = factory_state.resolved_project().map(SessionProject::from);
            let session = SessionContext::new(session_project);
            let repositories = ConnectionRepositories::new();
            Ok(TribalServerHandler::new(
                Arc::clone(&factory_state),
                TransportAuthStrategy::PerRequest,
                repositories,
                session,
                handler_config.clone(),
            ))
        },
        session_manager,
        streamable_config,
    )
}

// ---------------------------------------------------------------------------
// Serve
// ---------------------------------------------------------------------------

/// Serves an axum router with graceful shutdown.
///
/// # Errors
///
/// Returns [`AppError::TransportServe`] if the server encounters a
/// fatal I/O error.
pub(super) async fn serve(
    listener: TcpListener,
    router: axum::Router,
    cancellation_token: CancellationToken,
    transport: TransportKind,
) -> Result<(), AppError> {
    axum::serve(listener, router)
        .with_graceful_shutdown(cancellation_token.cancelled_owned())
        .await
        .map_err(|source| AppError::TransportServe { transport, source })
}
