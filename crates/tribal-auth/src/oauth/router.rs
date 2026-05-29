//! Axum router exposing the OAuth 2.1 authorisation-server surface.
//!
//! Mounted alongside the MCP transport router by `tribal-server`. The
//! OAuth endpoints sit outside the bearer-token middleware because
//! they exist to issue tokens; protecting them with a bearer would be
//! circular.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    routing::{get, post},
};
use http::header;
use serde::Serialize;

use crate::oauth::{
    authorize::{AuthorizeState, handle_authorize},
    config::OAuthRuntimeConfig,
    metadata::{
        PATH_AUTHORIZATION_SERVER_METADATA, PATH_AUTHORIZE, PATH_PROTECTED_RESOURCE_METADATA,
        PATH_REGISTER, PATH_TOKEN, authorization_server_metadata, protected_resource_metadata,
    },
    register::{RegisterState, handle_register},
    token::{TokenState, handle_token},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// `Cache-Control` value applied to metadata document responses.
const METADATA_CACHE_CONTROL: &str = "public, max-age=300";

// ---------------------------------------------------------------------------
// OAuthRouterState
// ---------------------------------------------------------------------------

/// Shared state for the OAuth router.
#[derive(Clone)]
pub struct OAuthRouterState {
    runtime: Arc<OAuthRuntimeConfig>,
    pool: sqlx::PgPool,
}

impl OAuthRouterState {
    /// Creates a new router state.
    #[must_use]
    pub fn new(runtime: Arc<OAuthRuntimeConfig>, pool: sqlx::PgPool) -> Self {
        Self { runtime, pool }
    }
}

// ---------------------------------------------------------------------------
// Router builder
// ---------------------------------------------------------------------------

/// Builds an axum router exposing all OAuth endpoints.
pub fn oauth_router(state: OAuthRouterState) -> Router {
    let metadata_state = MetadataState {
        runtime: Arc::clone(&state.runtime),
    };
    let authorize_state = AuthorizeState::new(state.pool.clone(), Arc::clone(&state.runtime));
    let token_state = TokenState::new(state.pool.clone(), Arc::clone(&state.runtime));
    let register_state = RegisterState::new(state.pool);

    let mut router = Router::new()
        .route(
            PATH_PROTECTED_RESOURCE_METADATA,
            get(serve_protected_resource_metadata).with_state(metadata_state.clone()),
        )
        .route(
            PATH_AUTHORIZATION_SERVER_METADATA,
            get(serve_authorization_server_metadata).with_state(metadata_state.clone()),
        )
        .route(
            PATH_AUTHORIZE,
            get(handle_authorize).with_state(authorize_state),
        )
        .route(PATH_TOKEN, post(handle_token).with_state(token_state));

    // RFC 9728 §3 clients query the path-suffixed protected-resource
    // metadata first. Derive the suffix from the configured resource path
    // so the served route matches the `WWW-Authenticate` challenge the
    // bearer middleware emits, rather than assuming a fixed `/mcp`. A
    // resource with no path is served by the root route alone.
    let resource_path = state.runtime.resource_url.path().trim_start_matches('/');
    if !resource_path.is_empty() {
        let suffixed = format!("{PATH_PROTECTED_RESOURCE_METADATA}/{resource_path}");
        router = router.route(
            &suffixed,
            get(serve_protected_resource_metadata).with_state(metadata_state.clone()),
        );
    }

    if state.runtime.dcr_enabled {
        router = router.route(
            PATH_REGISTER,
            post(handle_register).with_state(register_state),
        );
    }

    router
}

// ---------------------------------------------------------------------------
// Metadata handlers
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MetadataState {
    runtime: Arc<OAuthRuntimeConfig>,
}

async fn serve_protected_resource_metadata(
    State(state): State<MetadataState>,
) -> impl axum::response::IntoResponse {
    metadata_response(protected_resource_metadata(state.runtime.as_ref()))
}

async fn serve_authorization_server_metadata(
    State(state): State<MetadataState>,
) -> impl axum::response::IntoResponse {
    metadata_response(authorization_server_metadata(state.runtime.as_ref()))
}

fn metadata_response<T: Serialize>(
    body: T,
) -> (
    axum::http::StatusCode,
    [(http::HeaderName, &'static str); 1],
    Json<T>,
) {
    (
        axum::http::StatusCode::OK,
        [(header::CACHE_CONTROL, METADATA_CACHE_CONTROL)],
        Json(body),
    )
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use http::{Request, StatusCode};
    use tower::ServiceExt;
    use tribal_config::OAuthConfig;
    use tribal_test_utils::lazy_pool;
    use url::Url;

    use super::*;

    fn runtime() -> Arc<OAuthRuntimeConfig> {
        let issuer = Url::parse("http://127.0.0.1:8080").unwrap();
        let resource = Url::parse("http://127.0.0.1:8080/mcp").unwrap();
        Arc::new(OAuthRuntimeConfig::build(&OAuthConfig::default(), &issuer, &resource).unwrap())
    }

    async fn body_as_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn test_root_prm_endpoint_returns_canonical_resource() {
        let app = oauth_router(OAuthRouterState::new(runtime(), lazy_pool()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/.well-known/oauth-protected-resource")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_as_json(response).await;
        assert_eq!(json["resource"], "http://127.0.0.1:8080/mcp");
    }

    #[tokio::test]
    async fn test_path_suffixed_prm_endpoint_returns_same_body() {
        let app = oauth_router(OAuthRouterState::new(runtime(), lazy_pool()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/.well-known/oauth-protected-resource/mcp")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_as_metadata_endpoint_advertises_s256() {
        let app = oauth_router(OAuthRouterState::new(runtime(), lazy_pool()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/.well-known/oauth-authorization-server")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_as_json(response).await;
        assert_eq!(json["code_challenge_methods_supported"][0], "S256");
    }
}
