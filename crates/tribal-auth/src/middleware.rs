//! Bearer token authentication middleware for the HTTP transport.
//!
//! Extracts the `Authorization: Bearer <token>` header, verifies the
//! token via [`Authenticator`], and inserts the resolved
//! [`AuthenticatedPrincipal`] into the request extensions. Returns
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

use crate::{
    authenticator::Authenticator,
    error::{
        AUTH_FAILURE_REASON_INVALID, AUTH_FAILURE_REASON_MISSING, AUTH_FAILURE_REASON_UNAVAILABLE,
        AuthError, DISPLAY_INVALID_TOKEN, DISPLAY_MISSING_TOKEN, DISPLAY_TOKEN_EXPIRED,
        DISPLAY_TOKEN_REVOKED,
    },
    oauth::challenge::{
        BearerChallenge, ERROR_INVALID_TOKEN, build_bearer_challenge_header,
    },
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// JSON error field value for all auth rejection responses (RFC 9110).
const UNAUTHORIZED_ERROR: &str = "unauthorized";

/// JSON error field value for service unavailable responses.
const SERVICE_UNAVAILABLE_ERROR: &str = "service unavailable";

/// Bearer prefix for the `Authorization` header (with trailing space).
const BEARER_PREFIX: &str = "Bearer ";

/// Response message when the database is unreachable.
const DATABASE_UNAVAILABLE_MESSAGE: &str = "database unavailable";

// ---------------------------------------------------------------------------
// AuthMiddlewareState
// ---------------------------------------------------------------------------

/// Shared state for the authentication middleware.
///
/// Cloned into every request handler. Holds references to the MCP
/// connection pool, the authenticator, and the `WWW-Authenticate`
/// challenge template advertised on 401 responses.
#[derive(Clone)]
pub struct AuthMiddlewareState {
    pool: PgPool,
    authenticator: Arc<Authenticator>,
    challenge: Arc<BearerChallenge>,
}

impl AuthMiddlewareState {
    /// Creates a new middleware state.
    #[must_use]
    pub fn new(
        pool: PgPool,
        authenticator: Arc<Authenticator>,
        challenge: Arc<BearerChallenge>,
    ) -> Self {
        Self {
            pool,
            authenticator,
            challenge,
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
/// * **503 Service Unavailable** when the database pool is exhausted
///   or the database is unreachable.
pub async fn require_bearer_auth(
    State(state): State<AuthMiddlewareState>,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let challenge = state.challenge.as_ref();

    let Some(token) = extract_bearer_token(&request) else {
        warn!(
            auth_failure_reason = AUTH_FAILURE_REASON_MISSING,
            "auth rejected: missing bearer token",
        );
        return unauthorised_response(DISPLAY_MISSING_TOKEN, challenge, /* with_error */ false);
    };

    let mut conn = match state.pool.acquire().await {
        Ok(c) => c,
        Err(error) => {
            warn!(
                auth_failure_reason = AUTH_FAILURE_REASON_UNAVAILABLE,
                %error,
                "auth rejected: pool acquisition failed",
            );
            return service_unavailable_response(DATABASE_UNAVAILABLE_MESSAGE);
        }
    };

    match state.authenticator.verify_token(&mut conn, token).await {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(ref error) => auth_error_response(error, challenge),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extracts the raw bearer token from the `Authorization` header.
///
/// The auth-scheme comparison is case-insensitive per RFC 7235 §2.1.
fn extract_bearer_token(request: &axum::extract::Request) -> Option<&str> {
    let value = request
        .headers()
        .get(http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;

    let prefix_len = BEARER_PREFIX.len();
    let prefix = value.get(..prefix_len)?;
    let token = value.get(prefix_len..)?;

    if !token.is_empty() && prefix.eq_ignore_ascii_case(BEARER_PREFIX) {
        Some(token)
    } else {
        None
    }
}

/// Maps an [`AuthError`] to the appropriate HTTP response.
fn auth_error_response(error: &AuthError, challenge: &BearerChallenge) -> Response {
    match error {
        AuthError::DatabaseUnavailable { .. } => {
            warn!(
                auth_failure_reason = AUTH_FAILURE_REASON_UNAVAILABLE,
                %error,
                "auth rejected: database unavailable",
            );
            service_unavailable_response(DATABASE_UNAVAILABLE_MESSAGE)
        }

        // Logged by Authenticator::verify_token; middleware maps to response.
        AuthError::InvalidToken { .. } => {
            unauthorised_response(DISPLAY_INVALID_TOKEN, challenge, /* with_error */ true)
        }
        AuthError::TokenRevoked { .. } => {
            unauthorised_response(DISPLAY_TOKEN_REVOKED, challenge, /* with_error */ true)
        }
        AuthError::TokenExpired { .. } => {
            unauthorised_response(DISPLAY_TOKEN_EXPIRED, challenge, /* with_error */ true)
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
            unauthorised_response(DISPLAY_INVALID_TOKEN, challenge, /* with_error */ true)
        }
    }
}

/// Builds an HTTP 401 response with the canonical JSON body and the
/// spec-mandated `WWW-Authenticate` Bearer challenge carrying the
/// resource-metadata URL (and an `error="invalid_token"` parameter
/// when the failure originated from a presented but unusable token).
fn unauthorised_response(message: &str, challenge: &BearerChallenge, with_error: bool) -> Response {
    let challenge_value = build_bearer_challenge_header(&BearerChallenge {
        resource_metadata_url: challenge.resource_metadata_url.clone(),
        scope: challenge.scope.clone(),
        error: if with_error {
            Some(ERROR_INVALID_TOKEN)
        } else {
            None
        },
    });

    let body = serde_json::json!({
        "error": UNAUTHORIZED_ERROR,
        "message": message,
    });

    (
        StatusCode::UNAUTHORIZED,
        [(http::header::WWW_AUTHENTICATE, challenge_value)],
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
    use url::Url;

    use super::*;
    use crate::{
        authenticator::Authenticator,
        error::{DISPLAY_INVALID_TOKEN, DISPLAY_MISSING_TOKEN, DISPLAY_TOKEN_EXPIRED, DISPLAY_TOKEN_REVOKED},
    };

    // -- Helpers ------------------------------------------------------------

    fn test_challenge() -> Arc<BearerChallenge> {
        Arc::new(BearerChallenge {
            resource_metadata_url: Url::parse(
                "http://127.0.0.1:8080/.well-known/oauth-protected-resource/mcp",
            )
            .expect("test url"),
            scope: Some("tribal:read tribal:write".to_owned()),
            error: None,
        })
    }

    fn test_state(
        auth_token_mock: MockAuthTokenRepository,
        principal_mock: MockPrincipalRepository,
    ) -> AuthMiddlewareState {
        let authenticator = Arc::new(Authenticator::new(
            Arc::new(auth_token_mock),
            Arc::new(principal_mock),
        ));
        AuthMiddlewareState::new(lazy_pool(), authenticator, test_challenge())
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

    // -- Full middleware tests (via oneshot) --------------------------------
    // These exercise the axum layer end-to-end for paths that resolve
    // before the pool acquire step.

    #[tokio::test]
    async fn test_missing_authorisation_header_returns_401_with_bearer_challenge() {
        let app = test_app(default_state());
        let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let header_value = response
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert!(header_value.starts_with("Bearer "), "got: {header_value}");
        assert!(header_value.contains(r#"resource_metadata="http://127.0.0.1:8080/"#));
        assert!(header_value.contains(r#"scope="tribal:read tribal:write""#));
        // Missing token MUST NOT include error=invalid_token per RFC 6750 §3
        // (error is only emitted when a token was presented but unusable).
        assert!(!header_value.contains("error="));

        let json = response_json(response).await;
        assert_eq!(json["error"], UNAUTHORIZED_ERROR);
        assert_eq!(json["message"], DISPLAY_MISSING_TOKEN);
    }

    #[test]
    fn test_extract_bearer_token_case_insensitive() {
        let request = Request::builder()
            .header(header::AUTHORIZATION, "bearer my-token")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_bearer_token(&request), Some("my-token"));

        let request = Request::builder()
            .header(header::AUTHORIZATION, "BEARER my-token")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_bearer_token(&request), Some("my-token"));

        let request = Request::builder()
            .header(header::AUTHORIZATION, "Bearer my-token")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_bearer_token(&request), Some("my-token"));
    }

    #[test]
    fn test_extract_bearer_token_rejects_non_bearer() {
        let request = Request::builder()
            .header(header::AUTHORIZATION, "Basic abc123")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_bearer_token(&request), None);
    }

    #[test]
    fn test_extract_bearer_token_rejects_missing_header() {
        let request = Request::builder().body(Body::empty()).unwrap();
        assert_eq!(extract_bearer_token(&request), None);
    }

    #[test]
    fn test_extract_bearer_token_rejects_bare_scheme() {
        let request = Request::builder()
            .header(header::AUTHORIZATION, "Bearer")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_bearer_token(&request), None);
    }

    #[tokio::test]
    async fn test_non_bearer_scheme_returns_401_with_bearer_challenge() {
        let app = test_app(default_state());
        let request = Request::builder()
            .uri("/test")
            .header(header::AUTHORIZATION, "Basic abc123")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let header_value = response
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert!(header_value.starts_with("Bearer "));
        assert!(!header_value.contains("error="));

        let json = response_json(response).await;
        assert_eq!(json["error"], UNAUTHORIZED_ERROR);
        assert_eq!(json["message"], DISPLAY_MISSING_TOKEN);
    }

    // -- auth_error_response tests ------------------------------------------

    #[test]
    fn test_auth_error_invalid_token_returns_401() {
        let error = AuthError::InvalidToken {
            token_hash: "abc".into(),
        };
        let response = auth_error_response(&error, &test_challenge());

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_error_expired_token_returns_401_with_display_and_invalid_token_error() {
        let error = AuthError::TokenExpired {
            token_hash: "abc".into(),
        };
        let response = auth_error_response(&error, &test_challenge());

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let header_value = response
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert!(header_value.contains(r#"error="invalid_token""#));

        let json = response_json(response).await;
        assert_eq!(json["error"], UNAUTHORIZED_ERROR);
        assert_eq!(json["message"], DISPLAY_TOKEN_EXPIRED);
    }

    #[tokio::test]
    async fn test_auth_error_revoked_token_returns_401_with_display() {
        let error = AuthError::TokenRevoked {
            token_hash: "abc".into(),
        };
        let response = auth_error_response(&error, &test_challenge());

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
        let response = auth_error_response(&error, &test_challenge());

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
        let response = auth_error_response(&error, &test_challenge());

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let json = response_json(response).await;
        assert_eq!(json["error"], SERVICE_UNAVAILABLE_ERROR);
        assert_eq!(json["message"], DATABASE_UNAVAILABLE_MESSAGE);
    }
}
