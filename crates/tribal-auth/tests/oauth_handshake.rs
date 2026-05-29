//! End-to-end integration test exercising the OAuth 2.1 handshake.
//!
//! Boots the OAuth router against a real testcontainers-backed
//! Postgres, walks DCR registration → /authorize → /token in sequence,
//! and verifies the issued access token passes through the bearer
//! middleware just like a CLI-minted token.

use std::sync::Arc;

use axum::{Router, body::Body, middleware, routing::get};
use http::{Method, Request, StatusCode, header};
use sqlx::Acquire;
use tower::ServiceExt;
use tribal_auth::{
    AuthMiddlewareState, Authenticator,
    oauth::{
        BearerChallenge, OAuthRouterState, OAuthRuntimeConfig, build_bearer_challenge_header,
        oauth_router,
    },
    require_bearer_auth,
};
use tribal_common::sha256_hex;
use tribal_config::OAuthConfig;
use tribal_db::{
    NewOauthAuthorizationCode, NewPrincipal, OauthAuthorizationCodeRepository,
    PgAuthTokenRepository, PgOauthAuthorizationCodeRepository, PgPrincipalRepository,
    PrincipalRepository,
};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, PrincipalId};
use tribal_test_utils::test_context;
use url::Url;

const ISSUER: &str = "http://127.0.0.1:8080";
const RESOURCE: &str = "http://127.0.0.1:8080/mcp";
const RESOURCE_ENCODED: &str = "http%3A%2F%2F127.0.0.1%3A8080%2Fmcp";
const REDIRECT_URI: &str = "http://127.0.0.1:53076/cb";
const REDIRECT_URI_ENCODED: &str = "http%3A%2F%2F127.0.0.1%3A53076%2Fcb";

/// Content type of the DCR registration request body.
const CONTENT_TYPE_JSON: &str = "application/json";

/// Content type of the token-endpoint request body.
const CONTENT_TYPE_FORM: &str = "application/x-www-form-urlencoded";

/// RFC 7636 Appendix B verifier and its S256 challenge, reused across
/// the handshake tests.
const VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
const CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";

fn runtime_config() -> Arc<OAuthRuntimeConfig> {
    let issuer = Url::parse(ISSUER).unwrap();
    let resource = Url::parse(RESOURCE).unwrap();
    Arc::new(OAuthRuntimeConfig::build(&OAuthConfig::default(), &issuer, &resource).unwrap())
}

fn bearer_challenge(runtime: &OAuthRuntimeConfig) -> BearerChallenge {
    let mut url = runtime.issuer_url.clone();
    url.set_path("/.well-known/oauth-protected-resource/mcp");
    BearerChallenge {
        resource_metadata_url: url,
        scope: Some("tribal:read tribal:write".to_owned()),
        error: None,
    }
}

async fn ensure_local_principal(pool: &sqlx::PgPool) -> PrincipalId {
    let mut conn = pool.acquire().await.unwrap();
    if let Some(principal) = PgPrincipalRepository
        .find_by_key(&mut conn, LOCAL_PRINCIPAL_KEY)
        .await
        .unwrap()
    {
        return principal.id();
    }
    // Concurrent tests share the testcontainers Postgres; tolerate the
    // unique-violation race so each test gets a usable principal regardless
    // of which one wins the insert.
    match PgPrincipalRepository
        .insert(
            &mut conn,
            &NewPrincipal::builder()
                .principal_key(LOCAL_PRINCIPAL_KEY.to_owned())
                .build(),
        )
        .await
    {
        Ok(principal) => principal.id(),
        Err(tribal_db::DbError::UniqueViolation { .. }) => PgPrincipalRepository
            .find_by_key(&mut conn, LOCAL_PRINCIPAL_KEY)
            .await
            .unwrap()
            .expect("local principal exists after a concurrent insert")
            .id(),
        Err(other) => panic!("insert local principal failed: {other}"),
    }
}

async fn read_body(response: axum::response::Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// Wraps the OAuth router with the request helpers the handshake cases
/// drive it through, mirroring the e2e `McpTestClient`. Centralising
/// request construction keeps the route, header, and content-type
/// literals in one place per endpoint.
struct OAuthClient {
    router: Router,
}

impl OAuthClient {
    fn new(runtime: Arc<OAuthRuntimeConfig>, pool: sqlx::PgPool) -> Self {
        Self {
            router: oauth_router(OAuthRouterState::new(runtime, pool)),
        }
    }

    /// Sends a request through the router, panicking on a transport error.
    async fn send(&self, request: Request<Body>) -> axum::response::Response {
        self.router.clone().oneshot(request).await.unwrap()
    }

    /// Issues a GET against the router (discovery documents).
    async fn get(&self, uri: &str) -> axum::response::Response {
        self.send(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
    }

    /// Posts a raw JSON metadata document to `/register`.
    async fn register_raw(&self, body: serde_json::Value) -> axum::response::Response {
        self.send(
            Request::builder()
                .method(Method::POST)
                .uri("/register")
                .header(header::CONTENT_TYPE, CONTENT_TYPE_JSON)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
    }

    /// Registers a client with the given auth method, asserting success
    /// and returning its `client_id` and optional secret.
    async fn register(&self, auth_method: &str) -> (String, Option<String>) {
        let response = self
            .register_raw(serde_json::json!({
                "redirect_uris": [REDIRECT_URI],
                "token_endpoint_auth_method": auth_method,
            }))
            .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let json: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
        let client_id = json["client_id"].as_str().unwrap().to_owned();
        let client_secret = json["client_secret"].as_str().map(str::to_owned);
        (client_id, client_secret)
    }

    /// Drives `/authorize` with a raw query string.
    async fn authorize(&self, query: &str) -> axum::response::Response {
        self.get(&format!("/authorize?{query}")).await
    }

    /// Drives `/authorize` for a client and returns the issued
    /// authorisation code from the consent page's approve anchor.
    async fn authorize_code(&self, client_id: &str) -> String {
        let query = format!(
            "response_type=code&client_id={client_id}&redirect_uri={REDIRECT_URI_ENCODED}\
             &code_challenge={CHALLENGE}&code_challenge_method=S256\
             &resource={RESOURCE_ENCODED}",
        );
        let response = self.authorize(&query).await;
        assert_eq!(response.status(), StatusCode::OK);
        let html = String::from_utf8(read_body(response).await).unwrap();
        let target = extract_approve_href(&html);
        Url::parse(&target)
            .unwrap()
            .query_pairs()
            .find(|(k, _)| k == "code")
            .map(|(_, v)| v.into_owned())
            .expect("consent page carries the authorisation code")
    }

    /// Posts a form body to `/token`, optionally carrying the
    /// `client_secret_basic` Authorization header.
    async fn post_token_form(
        &self,
        body: String,
        basic_auth: Option<&str>,
    ) -> axum::response::Response {
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri("/token")
            .header(header::CONTENT_TYPE, CONTENT_TYPE_FORM);
        if let Some(basic) = basic_auth {
            builder = builder.header(header::AUTHORIZATION, format!("Basic {basic}"));
        }
        self.send(builder.body(Body::from(body)).unwrap()).await
    }

    /// Posts a form body to `/token` with no client-authentication header.
    async fn post_token(&self, body: String) -> axum::response::Response {
        self.post_token_form(body, None).await
    }

    /// Posts a form body to `/token` with an `Authorization: Basic`
    /// header carrying `client_id:secret` (the `client_secret_basic`
    /// mechanism).
    async fn post_token_basic(
        &self,
        body: String,
        client_id: &str,
        secret: &str,
    ) -> axum::response::Response {
        let basic = base64_encode(&format!("{client_id}:{secret}"));
        self.post_token_form(body, Some(&basic)).await
    }

    /// Walks register (public) → authorise → token, returning the issued
    /// access token.
    async fn mint_access_token(&self) -> String {
        let (client_id, _) = self.register("none").await;
        let code = self.authorize_code(&client_id).await;
        let response = self
            .post_token(token_form(&code, &client_id, VERIFIER, None))
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
        json["access_token"].as_str().unwrap().to_owned()
    }
}

/// Builds an `application/x-www-form-urlencoded` `/token` request body.
fn token_form(code: &str, client_id: &str, verifier: &str, client_secret: Option<&str>) -> String {
    let mut params = vec![
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", REDIRECT_URI),
        ("client_id", client_id),
        ("code_verifier", verifier),
        ("resource", RESOURCE),
    ];
    if let Some(secret) = client_secret {
        params.push(("client_secret", secret));
    }
    serde_urlencoded::to_string(params).unwrap()
}

// ---------------------------------------------------------------------------
// Full handshake
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_handshake_dcr_full_round_trip() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;

    let runtime = runtime_config();
    let client = OAuthClient::new(Arc::clone(&runtime), pool.clone());

    // -- Discovery -----------------------------------------------------------
    let prm_response = client
        .get("/.well-known/oauth-protected-resource/mcp")
        .await;
    assert_eq!(prm_response.status(), StatusCode::OK);
    let prm: serde_json::Value = serde_json::from_slice(&read_body(prm_response).await).unwrap();
    assert_eq!(prm["resource"], RESOURCE);

    let asm_response = client.get("/.well-known/oauth-authorization-server").await;
    assert_eq!(asm_response.status(), StatusCode::OK);
    let asm: serde_json::Value = serde_json::from_slice(&read_body(asm_response).await).unwrap();
    assert_eq!(asm["code_challenge_methods_supported"][0], "S256");
    assert!(asm["registration_endpoint"].is_string());

    // -- DCR registration ----------------------------------------------------
    let register_response = client
        .register_raw(serde_json::json!({
            "redirect_uris": [REDIRECT_URI],
            "client_name": "mcp-remote-test",
            "token_endpoint_auth_method": "client_secret_basic",
        }))
        .await;
    assert_eq!(register_response.status(), StatusCode::CREATED);
    let register: serde_json::Value =
        serde_json::from_slice(&read_body(register_response).await).unwrap();
    let client_id = register["client_id"].as_str().unwrap().to_owned();
    let client_secret = register["client_secret"].as_str().unwrap().to_owned();

    // -- Authorise -----------------------------------------------------------
    let authorize_query = format!(
        "response_type=code&client_id={client_id}&redirect_uri={REDIRECT_URI_ENCODED}\
         &code_challenge={CHALLENGE}&code_challenge_method=S256\
         &resource={RESOURCE_ENCODED}&scope=tribal%3Aread%20tribal%3Awrite\
         &state=opaque",
    );
    let authorize_response = client.authorize(&authorize_query).await;
    assert_eq!(authorize_response.status(), StatusCode::OK);
    let consent_html = String::from_utf8(read_body(authorize_response).await).unwrap();
    assert!(
        consent_html.contains("127.0.0.1"),
        "consent should display the redirect host"
    );
    let target_url = extract_approve_href(&consent_html);
    let target_parsed = Url::parse(&target_url).unwrap();
    let code = target_parsed
        .query_pairs()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.into_owned())
        .expect("consent form action carries code");
    assert!(target_parsed.query_pairs().any(|(k, _)| k == "state"));

    // -- Token exchange ------------------------------------------------------
    // The client registered `client_secret_basic`, so its secret travels
    // only in the Authorization: Basic header, never the form body.
    let token_response = client
        .post_token_basic(
            token_form(&code, &client_id, VERIFIER, None),
            &client_id,
            &client_secret,
        )
        .await;
    assert_eq!(token_response.status(), StatusCode::OK);
    let token: serde_json::Value =
        serde_json::from_slice(&read_body(token_response).await).unwrap();
    let access_token = token["access_token"].as_str().unwrap().to_owned();
    assert_eq!(token["token_type"], "Bearer");
    assert!(token["expires_in"].as_i64().unwrap() > 0);

    // -- Replay rejection ----------------------------------------------------
    let replay_response = client
        .post_token_basic(
            token_form(&code, &client_id, VERIFIER, None),
            &client_id,
            &client_secret,
        )
        .await;
    assert_eq!(replay_response.status(), StatusCode::BAD_REQUEST);
    let replay: serde_json::Value =
        serde_json::from_slice(&read_body(replay_response).await).unwrap();
    assert_eq!(replay["error"], "invalid_grant");

    // -- Bearer middleware accepts the issued access token -------------------
    let authenticator = Arc::new(Authenticator::with_audience(
        Arc::new(PgAuthTokenRepository),
        Arc::new(PgPrincipalRepository),
        Some(runtime.canonical_resource.clone()),
    ));
    let auth_state = AuthMiddlewareState::new(
        pool.clone(),
        authenticator,
        Arc::new(bearer_challenge(&runtime)),
    );
    let mcp_app: Router =
        Router::new()
            .route("/mcp", get(|| async { "ok" }))
            .layer(middleware::from_fn_with_state(
                auth_state,
                require_bearer_auth,
            ));

    let authorised = mcp_app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/mcp")
                .header(header::AUTHORIZATION, format!("Bearer {access_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorised.status(), StatusCode::OK);

    // -- 401 on missing bearer carries the expected challenge ----------------
    let unauthorised = mcp_app
        .clone()
        .oneshot(Request::builder().uri("/mcp").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(unauthorised.status(), StatusCode::UNAUTHORIZED);
    let www_auth = unauthorised
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let expected_header = build_bearer_challenge_header(&bearer_challenge(&runtime));
    assert_eq!(
        www_auth, expected_header,
        "401 challenge should match the runtime-built challenge",
    );
}

// ---------------------------------------------------------------------------
// Endpoint rejection cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_authorize_rejects_resource_mismatch() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;

    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let register_response = client
        .register_raw(serde_json::json!({
            "redirect_uris": [REDIRECT_URI],
            "token_endpoint_auth_method": "none",
        }))
        .await;
    let register: serde_json::Value =
        serde_json::from_slice(&read_body(register_response).await).unwrap();
    let client_id = register["client_id"].as_str().unwrap().to_owned();

    let authorize_query = format!(
        "response_type=code&client_id={client_id}&redirect_uri={REDIRECT_URI_ENCODED}\
         &code_challenge={CHALLENGE}&code_challenge_method=S256\
         &resource=http%3A%2F%2Fwrong.example%2Fmcp",
    );
    let authorize_response = client.authorize(&authorize_query).await;
    assert_eq!(authorize_response.status(), StatusCode::SEE_OTHER);
    let redirect_to = authorize_response
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(redirect_to.contains("error=invalid_target"));
}

#[tokio::test]
async fn test_authorize_rejects_uncatalogued_scope() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let (client_id, _) = client.register("none").await;
    // `tribal.knowledge.facts:read` is a valid scope but is not in the
    // advertised catalogue, so the request is rejected before a code
    // issues. The error rides the redirect per RFC 6749 §4.1.2.1.
    let query = format!(
        "response_type=code&client_id={client_id}&redirect_uri={REDIRECT_URI_ENCODED}\
         &code_challenge={CHALLENGE}&code_challenge_method=S256\
         &resource={RESOURCE_ENCODED}&scope=tribal.knowledge.facts%3Aread",
    );
    let response = client.authorize(&query).await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let redirect_to = response
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(redirect_to.contains("error=invalid_scope"));
}

#[tokio::test]
async fn test_token_rejects_pkce_verifier_mismatch() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;

    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let (client_id, _) = client.register("none").await;
    let code = client.authorize_code(&client_id).await;

    // Verifier whose S256 challenge does NOT match the recorded one.
    let wrong_verifier = "ZyXwVuTsRqPoNmLkJiHgFeDcBa0987654321ABCDEFG";
    let response = client
        .post_token(token_form(&code, &client_id, wrong_verifier, None))
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_grant");
}

#[tokio::test]
async fn test_token_requires_resource() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let (client_id, _) = client.register("none").await;
    let code = client.authorize_code(&client_id).await;

    // The resource indicator is required at /token, matching /authorize.
    // This form omits it entirely.
    let body = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("code", code.as_str()),
        ("redirect_uri", REDIRECT_URI),
        ("client_id", client_id.as_str()),
        ("code_verifier", VERIFIER),
    ])
    .unwrap();
    let response = client.post_token(body).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(json["error"], "invalid_target");
}

#[tokio::test]
async fn test_register_rejects_non_loopback_http_redirect() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let response = client
        .register_raw(serde_json::json!({
            "redirect_uris": ["http://example.com/cb"],
            "token_endpoint_auth_method": "none",
        }))
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_redirect_uri");
}

#[tokio::test]
async fn test_register_rejects_uncatalogued_scope() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    // A valid-syntax scope outside the advertised catalogue is rejected
    // as invalid client metadata (RFC 7591 §3.2.2).
    let response = client
        .register_raw(serde_json::json!({
            "redirect_uris": [REDIRECT_URI],
            "token_endpoint_auth_method": "none",
            "scope": "tribal.knowledge.facts:read",
        }))
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_client_metadata");
}

#[tokio::test]
async fn test_authorize_rejects_scope_beyond_registration() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    // Register a read-only client.
    let register = client
        .register_raw(serde_json::json!({
            "redirect_uris": [REDIRECT_URI],
            "token_endpoint_auth_method": "none",
            "scope": "tribal:read",
        }))
        .await;
    assert_eq!(register.status(), StatusCode::CREATED);
    let register: serde_json::Value = serde_json::from_slice(&read_body(register).await).unwrap();
    let client_id = register["client_id"].as_str().unwrap().to_owned();

    // It may not authorise a write scope it was never registered for.
    let query = format!(
        "response_type=code&client_id={client_id}&redirect_uri={REDIRECT_URI_ENCODED}\
         &code_challenge={CHALLENGE}&code_challenge_method=S256\
         &resource={RESOURCE_ENCODED}&scope=tribal%3Awrite",
    );
    let response = client.authorize(&query).await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let location = response
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(location.contains("error=invalid_scope"));
}

#[tokio::test]
async fn test_authorize_accepts_scope_within_registration() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let register = client
        .register_raw(serde_json::json!({
            "redirect_uris": [REDIRECT_URI],
            "token_endpoint_auth_method": "none",
            "scope": "tribal:read tribal:write",
        }))
        .await;
    assert_eq!(register.status(), StatusCode::CREATED);
    let register: serde_json::Value = serde_json::from_slice(&read_body(register).await).unwrap();
    let client_id = register["client_id"].as_str().unwrap().to_owned();

    let query = format!(
        "response_type=code&client_id={client_id}&redirect_uri={REDIRECT_URI_ENCODED}\
         &code_challenge={CHALLENGE}&code_challenge_method=S256\
         &resource={RESOURCE_ENCODED}&scope=tribal%3Aread",
    );
    let response = client.authorize(&query).await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a scope within the registration is accepted",
    );
}

#[tokio::test]
async fn test_authorize_rejects_resource_with_fragment() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let (client_id, _) = client.register("none").await;
    // A resource carrying a fragment (forbidden by RFC 8707) is rejected,
    // not silently stripped.
    let query = format!(
        "response_type=code&client_id={client_id}&redirect_uri={REDIRECT_URI_ENCODED}\
         &code_challenge={CHALLENGE}&code_challenge_method=S256\
         &resource=http%3A%2F%2F127.0.0.1%3A8080%2Fmcp%23frag",
    );
    let response = client.authorize(&query).await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let location = response
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(location.contains("error=invalid_target"));
}

#[tokio::test]
async fn test_authorize_missing_client_id_returns_invalid_request() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    // No client_id: the OAuth error model renders a JSON invalid_request,
    // not a bare framework deserialisation rejection.
    let query = format!(
        "response_type=code&redirect_uri={REDIRECT_URI_ENCODED}\
         &code_challenge={CHALLENGE}&code_challenge_method=S256&resource={RESOURCE_ENCODED}",
    );
    let response = client.authorize(&query).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_request");
}

#[tokio::test]
async fn test_token_missing_code_verifier_returns_invalid_request() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let (client_id, _) = client.register("none").await;
    let code = client.authorize_code(&client_id).await;
    // Form omits code_verifier entirely.
    let body = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("code", code.as_str()),
        ("redirect_uri", REDIRECT_URI),
        ("client_id", client_id.as_str()),
        ("resource", RESOURCE),
    ])
    .unwrap();
    let response = client.post_token(body).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(json["error"], "invalid_request");
}

#[tokio::test]
async fn test_register_missing_redirect_uris_returns_invalid_redirect_uri() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    // No redirect_uris field at all: surfaces as the OAuth
    // invalid_redirect_uri error, not a framework rejection.
    let response = client
        .register_raw(serde_json::json!({
            "token_endpoint_auth_method": "none",
        }))
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_redirect_uri");
}

#[tokio::test]
async fn test_register_filters_unsupported_grant_types() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    // A client declaring refresh_token alongside authorization_code
    // registers successfully, with only the supported grant echoed back.
    let response = client
        .register_raw(serde_json::json!({
            "redirect_uris": [REDIRECT_URI],
            "token_endpoint_auth_method": "none",
            "grant_types": ["authorization_code", "refresh_token"],
        }))
        .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    let grants: Vec<&str> = body["grant_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect();
    assert_eq!(
        grants,
        vec!["authorization_code"],
        "refresh_token is filtered out, not rejected",
    );
}

#[tokio::test]
async fn test_register_rejects_unsupported_response_types() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    // A client declaring only an unsupported response type leaves nothing
    // the server can register it for (symmetric with grant_types).
    let response = client
        .register_raw(serde_json::json!({
            "redirect_uris": [REDIRECT_URI],
            "token_endpoint_auth_method": "none",
            "response_types": ["token"],
        }))
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_client_metadata");
}

#[tokio::test]
async fn test_register_rejects_malformed_json() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    // A body that is not valid JSON maps to invalid_client_metadata, not
    // the framework's bare deserialisation rejection.
    let response = client
        .send(
            Request::builder()
                .method(Method::POST)
                .uri("/register")
                .header(header::CONTENT_TYPE, CONTENT_TYPE_JSON)
                .body(Body::from("{ not valid json"))
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_client_metadata");
}

#[tokio::test]
async fn test_token_rejects_duplicate_parameter() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    // A duplicated scalar parameter (RFC 6749 §3.1 forbids it) fails
    // deserialisation and maps to invalid_request rather than a bare 400.
    let body = "grant_type=authorization_code&client_id=a&client_id=b\
                &code=c&redirect_uri=http://127.0.0.1:53076/cb&code_verifier=v\
                &resource=http://127.0.0.1:8080/mcp";
    let response = client
        .send(
            Request::builder()
                .method(Method::POST)
                .uri("/token")
                .header(header::CONTENT_TYPE, CONTENT_TYPE_FORM)
                .body(Body::from(body))
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(json["error"], "invalid_request");
}

#[tokio::test]
async fn test_authorize_rejects_duplicate_parameter() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    // A duplicated scalar query parameter maps to invalid_request (JSON,
    // since the redirect URI is not yet validated).
    let response = client
        .authorize(&format!(
            "response_type=code&client_id=a&client_id=b&redirect_uri={REDIRECT_URI_ENCODED}\
             &code_challenge={CHALLENGE}&code_challenge_method=S256&resource={RESOURCE_ENCODED}",
        ))
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_request");
}

// ---------------------------------------------------------------------------
// Adversarial cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_authorize_rejects_unregistered_redirect_uri() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let (client_id, _) = client.register("none").await;
    // Loopback HTTP grants any-port flexibility, so vary the PATH (not the
    // port) to exercise the exact-match rejection before any code issues.
    let query = format!(
        "response_type=code&client_id={client_id}&redirect_uri=http%3A%2F%2F127.0.0.1%3A53076%2Fevil\
         &code_challenge={CHALLENGE}&code_challenge_method=S256\
         &resource={RESOURCE_ENCODED}",
    );
    let response = client.authorize(&query).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_redirect_uri");
}

#[tokio::test]
async fn test_token_rejects_missing_client_secret() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let (client_id, secret) = client.register("client_secret_basic").await;
    assert!(secret.is_some(), "a confidential client is issued a secret");
    let code = client.authorize_code(&client_id).await;

    let response = client
        .post_token(token_form(&code, &client_id, VERIFIER, None))
        .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_client");
}

#[tokio::test]
async fn test_token_rejects_wrong_client_secret() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let (client_id, _secret) = client.register("client_secret_basic").await;
    let code = client.authorize_code(&client_id).await;

    // Wrong secret presented via the Basic header (its registered method).
    let response = client
        .post_token_basic(
            token_form(&code, &client_id, VERIFIER, None),
            &client_id,
            "not-the-real-secret",
        )
        .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_client");
}

#[tokio::test]
async fn test_token_basic_client_rejected_when_secret_only_in_form() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let (client_id, secret) = client.register("client_secret_basic").await;
    let secret = secret.expect("confidential client is issued a secret");
    let code = client.authorize_code(&client_id).await;

    // The correct secret, but presented via the form body rather than the
    // registered Basic header: the method is enforced, so it is rejected
    // rather than silently downgraded to client_secret_post.
    let response = client
        .post_token(token_form(&code, &client_id, VERIFIER, Some(&secret)))
        .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_client");
}

#[tokio::test]
async fn test_token_post_client_succeeds_with_form_secret() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let (client_id, secret) = client.register("client_secret_post").await;
    let secret = secret.expect("a client_secret_post client is issued a secret");
    let code = client.authorize_code(&client_id).await;

    // A client_secret_post client presents its secret in the form body.
    let response = client
        .post_token(token_form(&code, &client_id, VERIFIER, Some(&secret)))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_concurrent_code_exchange_has_one_winner() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool);

    let (client_id, _) = client.register("none").await;
    let code = client.authorize_code(&client_id).await;

    // Two simultaneous exchanges of the same code. The atomic
    // UPDATE ... WHERE consumed_at IS NULL ... RETURNING admits exactly
    // one winner.
    let (first, second) = tokio::join!(
        client.post_token(token_form(&code, &client_id, VERIFIER, None)),
        client.post_token(token_form(&code, &client_id, VERIFIER, None)),
    );

    let statuses = [first.status(), second.status()];
    let successes = statuses.iter().filter(|s| **s == StatusCode::OK).count();
    let rejections = statuses
        .iter()
        .filter(|s| **s == StatusCode::BAD_REQUEST)
        .count();
    assert_eq!(
        successes, 1,
        "exactly one exchange must succeed: {statuses:?}"
    );
    assert_eq!(rejections, 1, "the loser must be rejected: {statuses:?}");
}

#[tokio::test]
async fn test_token_rejects_expired_authorization_code() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    let principal_id = ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(runtime, pool.clone());

    let (client_id, _) = client.register("none").await;

    // Insert a code whose expiry is already in the past, then exchange it.
    let raw_code = "expired-authorization-code";
    let code_hash = sha256_hex(raw_code);
    let mut conn = pool.acquire().await.unwrap();
    PgOauthAuthorizationCodeRepository
        .insert(
            &mut conn,
            &NewOauthAuthorizationCode::builder()
                .code_hash(code_hash)
                .client_id(client_id.clone())
                .redirect_uri(REDIRECT_URI.to_owned())
                .code_challenge(CHALLENGE.to_owned())
                .resource(Some(RESOURCE.to_owned()))
                .principal_id(principal_id)
                .expires_at(chrono::Utc::now() - chrono::Duration::hours(1))
                .build(),
        )
        .await
        .unwrap();
    drop(conn);

    let response = client
        .post_token(token_form(raw_code, &client_id, VERIFIER, None))
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_grant");
}

#[tokio::test]
async fn test_token_rejected_by_server_with_different_audience() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let client = OAuthClient::new(Arc::clone(&runtime), pool.clone());

    // Mint a token bound to this server's resource.
    let access_token = client.mint_access_token().await;

    // A bearer middleware bound to a different resource must reject it.
    let authenticator = Arc::new(Authenticator::with_audience(
        Arc::new(PgAuthTokenRepository),
        Arc::new(PgPrincipalRepository),
        Some("http://other.example/mcp".to_owned()),
    ));
    let auth_state = AuthMiddlewareState::new(
        pool.clone(),
        authenticator,
        Arc::new(bearer_challenge(&runtime)),
    );
    let mcp_app: Router =
        Router::new()
            .route("/mcp", get(|| async { "ok" }))
            .layer(middleware::from_fn_with_state(
                auth_state,
                require_bearer_auth,
            ));

    let response = mcp_app
        .oneshot(
            Request::builder()
                .uri("/mcp")
                .header(header::AUTHORIZATION, format!("Bearer {access_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_authorization_code_survives_transaction_rollback() {
    // The /token handler consumes the code and inserts the access token
    // in one transaction; a failure after the consume rolls the whole
    // thing back so the code stays exchangeable. This pins the database
    // guarantee that property relies on: a consume inside a rolled-back
    // transaction does not durably mark the code used.
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    let principal_id = ensure_local_principal(&pool).await;

    let raw_code = "rollback-survival-code";
    let code_hash = sha256_hex(raw_code);
    let mut conn = pool.acquire().await.unwrap();
    PgOauthAuthorizationCodeRepository
        .insert(
            &mut conn,
            &NewOauthAuthorizationCode::builder()
                .code_hash(code_hash.clone())
                .client_id("rollback-survival-client".to_owned())
                .redirect_uri(REDIRECT_URI.to_owned())
                .code_challenge(CHALLENGE.to_owned())
                .principal_id(principal_id)
                .expires_at(chrono::Utc::now() + chrono::Duration::hours(1))
                .build(),
        )
        .await
        .unwrap();

    // Consume inside a transaction, then drop it without committing.
    {
        let mut tx = conn.begin().await.unwrap();
        let consumed = PgOauthAuthorizationCodeRepository
            .consume_by_hash(&mut tx, &code_hash, chrono::Utc::now())
            .await
            .unwrap();
        assert!(
            consumed.is_some(),
            "the code consumes inside the transaction",
        );
        // tx dropped here without commit -> rollback.
    }

    // The rollback undid the consume, so the code is still exchangeable.
    let after = PgOauthAuthorizationCodeRepository
        .consume_by_hash(&mut conn, &code_hash, chrono::Utc::now())
        .await
        .unwrap();
    assert!(
        after.is_some(),
        "a rolled-back consume must leave the code exchangeable",
    );
}

// ---------------------------------------------------------------------------
// Consent-page parsing helpers
// ---------------------------------------------------------------------------

fn extract_approve_href(html: &str) -> String {
    // The consent page presents the redirect target as an `<a href>` the
    // user clicks to authorise; the href carries the full target URL with
    // its query string intact. Extract it to drive the exchange.
    let start = html
        .find(r#"<a id="approve" href=""#)
        .expect("consent page approve anchor present");
    let after = &html[start + r#"<a id="approve" href=""#.len()..];
    let end = after.find('"').expect("approve href quoted");
    decode_html_attribute(&after[..end])
}

fn decode_html_attribute(s: &str) -> String {
    s.replace("&amp;", "&")
}

fn base64_encode(input: &str) -> String {
    use base64::{Engine, engine::general_purpose::STANDARD};
    STANDARD.encode(input.as_bytes())
}
