//! Stdio transport for the Tribal MCP server.
//!
//! Resolves the local principal once at startup, then serves MCP
//! requests over stdin/stdout until the cancellation token is triggered
//! or the client disconnects.

use std::sync::Arc;

use rmcp::ServiceExt;
use tokio_util::sync::CancellationToken;
use tribal_db::{PgAuthTokenRepository, PgPrincipalRepository};
use tribal_mcp::{
    AppState, AuthContext, Authenticator, ConnectionRepositories, HandlerConfig, SessionContext,
    SessionProject, TransportAuthStrategy, TribalServerHandler,
};

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the stdio transport over stdin/stdout.
///
/// Resolves the `principal:local` identity once at startup, then
/// creates a single [`TribalServerHandler`] and serves it via rmcp's
/// stdio transport until the cancellation token fires or the client
/// disconnects.
///
/// # Errors
///
/// Returns [`AppError::TransportStdio`] if principal resolution fails
/// or the transport encounters a fatal I/O error.
pub(crate) async fn run_stdio_transport(
    state: &Arc<AppState>,
    handler_config: HandlerConfig,
    cancellation_token: CancellationToken,
) -> Result<(), AppError> {
    // -- Auth resolution -----------------------------------------------------

    let authenticator = Authenticator::new(
        Arc::new(PgAuthTokenRepository),
        Arc::new(PgPrincipalRepository),
    );

    let mut conn = state
        .mcp_pool()
        .acquire()
        .await
        .map_err(|e| AppError::TransportStdio {
            source: Box::new(e),
        })?;

    let principal = authenticator
        .resolve_stdio_principal(&mut conn)
        .await
        .map_err(|e| AppError::TransportStdio {
            source: Box::new(e),
        })?;

    drop(conn);

    let auth = AuthContext::new(principal);

    // -- Handler -------------------------------------------------------------

    let session_project = state.resolved_project().map(SessionProject::from);
    let session = SessionContext::new(session_project);
    let repositories = ConnectionRepositories::new();

    let handler = TribalServerHandler::new(
        Arc::clone(state),
        TransportAuthStrategy::AtCreation(auth),
        repositories,
        session,
        handler_config,
        "stdio".to_owned(),
    );

    // -- Serve ---------------------------------------------------------------

    tracing::info!("stdio transport ready");

    let (stdin, stdout) = rmcp::transport::io::stdio();

    let service = handler
        .serve_with_ct((stdin, stdout), cancellation_token)
        .await
        .map_err(|e| AppError::TransportStdio {
            source: Box::new(e),
        })?;

    service
        .waiting()
        .await
        .map_err(|e| AppError::TransportStdio {
            source: Box::new(e),
        })?;

    Ok(())
}
