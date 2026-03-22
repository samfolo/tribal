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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{body::Body, middleware, routing::get};
    use http::{Request, StatusCode, header};
    use tower::ServiceExt;
    use tribal_domain::PrincipalId;
    use tribal_test_utils::{MockAuthTokenRepository, MockPrincipalRepository, lazy_pool};

    use super::*;
    use crate::auth::{
        AuthError, Authenticator, DISPLAY_INVALID_TOKEN, DISPLAY_MISSING_TOKEN,
        DISPLAY_TOKEN_EXPIRED, DISPLAY_TOKEN_REVOKED,
    };

    // -- Helpers ------------------------------------------------------------

    fn test_state(
        auth_token_mock: MockAuthTokenRepository,
        principal_mock: MockPrincipalRepository,
    ) -> AuthMiddlewareState {
        let authenticator = Arc::new(Authenticator::new(
            Arc::new(auth_token_mock),
            Arc::new(principal_mock),
        ));
        AuthMiddlewareState::new(lazy_pool(), authenticator)
    }

    fn test_app(state: AuthMiddlewareState) -> axum::Router {
        axum::Router::new()
            .route("/test", get(|| async { "ok" }))
            .layer(middleware::from_fn_with_state(state, require_bearer_auth))
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn default_state() -> AuthMiddlewareState {
        test_state(
            MockAuthTokenRepository::builder().build(),
            MockPrincipalRepository::builder().build(),
        )
    }

    // -- Full middleware tests (via oneshot) ---------------------------------
    // These exercise the axum layer end-to-end for paths that resolve
    // before the pool acquire step.

    #[tokio::test]
    async fn test_missing_authorisation_header_returns_401() {
        let app = test_app(default_state());
        let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            WWW_AUTHENTICATE_VALUE,
        );

        let json = response_json(response).await;
        assert_eq!(json["error"], UNAUTHORIZED_ERROR);
        assert_eq!(json["message"], DISPLAY_MISSING_TOKEN);
    }

    #[tokio::test]
    async fn test_non_bearer_scheme_returns_401() {
        let app = test_app(default_state());
        let request = Request::builder()
            .uri("/test")
            .header(header::AUTHORIZATION, "Basic abc123")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let json = response_json(response).await;
        assert_eq!(json["error"], UNAUTHORIZED_ERROR);
        assert_eq!(json["message"], DISPLAY_MISSING_TOKEN);
    }

    #[tokio::test]
    async fn test_pool_acquisition_failure_returns_503() {
        // lazy_pool connects to a nonexistent database; acquire() fails.
        let app = test_app(default_state());
        let request = Request::builder()
            .uri("/test")
            .header(header::AUTHORIZATION, "Bearer some-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let json = response_json(response).await;
        assert_eq!(json["error"], SERVICE_UNAVAILABLE_ERROR);
        assert_eq!(json["message"], DATABASE_UNAVAILABLE_MESSAGE);
    }

    // -- auth_error_response tests ------------------------------------------
    // These test the AuthError → Response mapping directly, bypassing the
    // pool acquire step. Full token verification through the middleware is
    // covered by the HTTP integration test.

    #[test]
    fn test_auth_error_invalid_token_returns_401() {
        let error = AuthError::InvalidToken {
            token_hash: "abc".into(),
        };
        let response = auth_error_response(&error);

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_error_expired_token_returns_401_with_display() {
        let error = AuthError::TokenExpired {
            token_hash: "abc".into(),
        };
        let response = auth_error_response(&error);

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let json = response_json(response).await;
        assert_eq!(json["error"], UNAUTHORIZED_ERROR);
        assert_eq!(json["message"], DISPLAY_TOKEN_EXPIRED);
    }

    #[tokio::test]
    async fn test_auth_error_revoked_token_returns_401_with_display() {
        let error = AuthError::TokenRevoked {
            token_hash: "abc".into(),
        };
        let response = auth_error_response(&error);

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let json = response_json(response).await;
        assert_eq!(json["error"], UNAUTHORIZED_ERROR);
        assert_eq!(json["message"], DISPLAY_TOKEN_REVOKED);
    }

    #[tokio::test]
    async fn test_auth_error_principal_not_found_returns_401_with_invalid_message() {
        let error = AuthError::PrincipalNotFound {
            principal_id: PrincipalId::new(),
        };
        let response = auth_error_response(&error);

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let json = response_json(response).await;
        assert_eq!(json["error"], UNAUTHORIZED_ERROR);
        assert_eq!(json["message"], DISPLAY_INVALID_TOKEN);
    }

    #[tokio::test]
    async fn test_auth_error_database_unavailable_returns_503() {
        let error = AuthError::DatabaseUnavailable {
            context: "test query".into(),
            source: Box::new(std::io::Error::other("boom")),
        };
        let response = auth_error_response(&error);

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let json = response_json(response).await;
        assert_eq!(json["error"], SERVICE_UNAVAILABLE_ERROR);
        assert_eq!(json["message"], DATABASE_UNAVAILABLE_MESSAGE);
    }
}
