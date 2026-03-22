//! Bearer token authentication middleware for the HTTP transport.
//!
//! Extracts the `Authorization: Bearer <token>` header, verifies the
//! token via [`Authenticator`], and inserts the resolved
//! [`AuthenticatedPrincipal`] into the request extensions.  Returns
//! HTTP 401 or 503 on failure.

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use sqlx::PgPool;
use tracing::warn;

use crate::auth::{
    AUTH_FAILURE_REASON_EXPIRED, AUTH_FAILURE_REASON_INVALID, AUTH_FAILURE_REASON_MISSING,
    AUTH_FAILURE_REASON_REVOKED, AuthError, Authenticator, DISPLAY_INVALID_TOKEN,
    DISPLAY_MISSING_TOKEN,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// JSON error field value for all auth rejection responses (RFC 9110).
const UNAUTHORIZED_ERROR: &str = "unauthorized";

/// JSON error field value for service unavailable responses.
const SERVICE_UNAVAILABLE_ERROR: &str = "service unavailable";

/// `WWW-Authenticate` header challenge value.
const WWW_AUTHENTICATE_VALUE: &str = "Bearer";

/// Bearer prefix for the `Authorization` header (with trailing space).
const BEARER_PREFIX: &str = "Bearer ";

/// Response message when the database is unreachable.
const DATABASE_UNAVAILABLE_MESSAGE: &str = "database unavailable";

// ---------------------------------------------------------------------------
// AuthMiddlewareState
// ---------------------------------------------------------------------------

/// Shared state for the authentication middleware.
///
/// Cloned into every request handler.  Holds references to the MCP
/// connection pool and the authenticator.
#[derive(Clone)]
pub struct AuthMiddlewareState {
    pool: PgPool,
    authenticator: Arc<Authenticator>,
}

impl AuthMiddlewareState {
    /// Creates a new middleware state.
    #[must_use]
    pub fn new(pool: PgPool, authenticator: Arc<Authenticator>) -> Self {
        Self {
            pool,
            authenticator,
        }
    }
}

// ---------------------------------------------------------------------------
// Middleware function
// ---------------------------------------------------------------------------

/// Axum middleware that enforces bearer token authentication.
///
/// Extracts the `Authorization: Bearer <token>` header, acquires a
/// database connection, verifies the token via [`Authenticator`], and
/// inserts the resolved [`AuthenticatedPrincipal`] into the request
/// extensions.
///
/// # Response codes
///
/// * **401 Unauthorised** with `WWW-Authenticate: Bearer` for missing,
///   invalid, expired, or revoked tokens.
/// * **503 Service Unavailable** when the database pool is exhausted or
///   the database is unreachable.
pub async fn require_bearer_auth(
    State(state): State<AuthMiddlewareState>,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let Some(token) = extract_bearer_token(&request) else {
        warn!(
            auth_failure_reason = AUTH_FAILURE_REASON_MISSING,
            "auth rejected: missing bearer token",
        );
        return unauthorised_response(DISPLAY_MISSING_TOKEN);
    };

    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(error) => {
            warn!(%error, "auth rejected: pool acquisition failed");
            return service_unavailable_response(DATABASE_UNAVAILABLE_MESSAGE);
        }
    };

    match state.authenticator.verify_token(&mut conn, token).await {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(ref error) => auth_error_response(error),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extracts the raw bearer token from the `Authorization` header.
fn extract_bearer_token(request: &axum::extract::Request) -> Option<&str> {
    request
        .headers()
        .get(http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix(BEARER_PREFIX)
}

/// Maps an [`AuthError`] to the appropriate HTTP response.
fn auth_error_response(error: &AuthError) -> Response {
    match error {
        AuthError::DatabaseUnavailable { .. } => {
            warn!(%error, "auth rejected: database unavailable");
            service_unavailable_response(DATABASE_UNAVAILABLE_MESSAGE)
        }

        AuthError::InvalidToken { .. } => {
            warn!(
                auth_failure_reason = AUTH_FAILURE_REASON_INVALID,
                "auth rejected: {error}",
            );
            unauthorised_response(DISPLAY_INVALID_TOKEN)
        }

        AuthError::TokenRevoked { .. } => {
            warn!(
                auth_failure_reason = AUTH_FAILURE_REASON_REVOKED,
                "auth rejected: {error}",
            );
            unauthorised_response(&error.to_string())
        }

        AuthError::TokenExpired { .. } => {
            warn!(
                auth_failure_reason = AUTH_FAILURE_REASON_EXPIRED,
                "auth rejected: {error}",
            );
            unauthorised_response(&error.to_string())
        }

        // Defensive: verify_token cannot return these, but handle them
        // to avoid leaking internal state if the code path changes.
        AuthError::PrincipalNotFound { .. }
        | AuthError::LocalPrincipalMissing { .. }
        | AuthError::InsufficientScope { .. } => {
            warn!(
                auth_failure_reason = AUTH_FAILURE_REASON_INVALID,
                "auth rejected: {error}",
            );
            unauthorised_response(DISPLAY_INVALID_TOKEN)
        }
    }
}

/// Builds an HTTP 401 response with the canonical JSON body.
fn unauthorised_response(message: &str) -> Response {
    let body = serde_json::json!({
        "error": UNAUTHORIZED_ERROR,
        "message": message,
    });

    (
        StatusCode::UNAUTHORIZED,
        [(http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_VALUE)],
        axum::Json(body),
    )
        .into_response()
}

/// Builds an HTTP 503 response with the canonical JSON body.
fn service_unavailable_response(message: &str) -> Response {
    let body = serde_json::json!({
        "error": SERVICE_UNAVAILABLE_ERROR,
        "message": message,
    });

    (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response()
}
