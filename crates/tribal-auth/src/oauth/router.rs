//! Axum router exposing the OAuth 2.1 authorisation-server surface.
//!
//! Mounted alongside the MCP transport router by `tribal-server`. The
//! OAuth endpoints sit outside the bearer-token middleware because
//! they exist to issue tokens; protecting them with a bearer would be
//! circular.

use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::get};
use http::header;
use serde::Serialize;

use crate::oauth::{
    config::OAuthRuntimeConfig,
    metadata::{
        PATH_AUTHORIZATION_SERVER_METADATA, PATH_PROTECTED_RESOURCE_METADATA,
        authorization_server_metadata, protected_resource_metadata,
    },
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Path under the resource-metadata root that targets the `/mcp` path.
///
/// MCP clients per RFC 9728 §3 query the path-suffixed form first;
/// serving both is the most interoperable choice.
const PATH_PROTECTED_RESOURCE_METADATA_MCP: &str = "/.well-known/oauth-protected-resource/mcp";

/// `Cache-Control` value applied to metadata document responses.
const METADATA_CACHE_CONTROL: &str = "public, max-age=300";

// ---------------------------------------------------------------------------
// OAuthRouterState
// ---------------------------------------------------------------------------

/// Shared state for the OAuth router.
#[derive(Clone)]
pub struct OAuthRouterState {
    runtime: Arc<OAuthRuntimeConfig>,
}

impl OAuthRouterState {
    /// Creates a new router state from the runtime OAuth config.
    #[must_use]
    pub fn new(runtime: Arc<OAuthRuntimeConfig>) -> Self {
        Self { runtime }
    }
}

// ---------------------------------------------------------------------------
// Router builder
// ---------------------------------------------------------------------------

/// Builds an axum router exposing the well-known metadata documents.
///
/// `/authorize`, `/token`, and `/register` are added in subsequent
/// commits.
#[must_use]
pub fn oauth_router(state: OAuthRouterState) -> Router {
    Router::new()
        .route(
            PATH_PROTECTED_RESOURCE_METADATA,
            get(serve_protected_resource_metadata),
        )
        .route(
            PATH_PROTECTED_RESOURCE_METADATA_MCP,
            get(serve_protected_resource_metadata),
        )
        .route(
            PATH_AUTHORIZATION_SERVER_METADATA,
            get(serve_authorization_server_metadata),
        )
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn serve_protected_resource_metadata(
    State(state): State<OAuthRouterState>,
) -> impl axum::response::IntoResponse {
    metadata_response(protected_resource_metadata(state.runtime.as_ref()))
}

async fn serve_authorization_server_metadata(
    State(state): State<OAuthRouterState>,
) -> impl axum::response::IntoResponse {
    metadata_response(authorization_server_metadata(state.runtime.as_ref()))
}

fn metadata_response<T: Serialize>(
    body: T,
) -> (axum::http::StatusCode, [(http::HeaderName, &'static str); 1], Json<T>) {
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
        let app = oauth_router(OAuthRouterState::new(runtime()));
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
        assert_eq!(json["authorization_servers"][0], "http://127.0.0.1:8080/");
    }

    #[tokio::test]
    async fn test_path_suffixed_prm_endpoint_returns_same_body() {
        let app = oauth_router(OAuthRouterState::new(runtime()));
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
        let json = body_as_json(response).await;
        assert_eq!(json["resource"], "http://127.0.0.1:8080/mcp");
    }

    #[tokio::test]
    async fn test_as_metadata_endpoint_advertises_s256_and_cimd() {
        let app = oauth_router(OAuthRouterState::new(runtime()));
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
        assert_eq!(json["client_id_metadata_document_supported"], true);
    }
}
