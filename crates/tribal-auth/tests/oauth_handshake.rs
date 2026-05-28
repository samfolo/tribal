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
        OAuthRouterState, OAuthRuntimeConfig,
        challenge::{BearerChallenge, build_bearer_challenge_header},
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
const REDIRECT_URI: &str = "http://127.0.0.1:53076/cb";
const REDIRECT_URI_ENCODED: &str = "http%3A%2F%2F127.0.0.1%3A53076%2Fcb";

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

#[tokio::test]
async fn test_handshake_dcr_full_round_trip() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;

    let runtime = runtime_config();
    let app = oauth_router(OAuthRouterState::new(Arc::clone(&runtime), pool.clone()));

    // -- Discovery -----------------------------------------------------------
    let prm_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/.well-known/oauth-protected-resource/mcp")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(prm_response.status(), StatusCode::OK);
    let prm: serde_json::Value = serde_json::from_slice(&read_body(prm_response).await).unwrap();
    assert_eq!(prm["resource"], RESOURCE);

    let asm_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/.well-known/oauth-authorization-server")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(asm_response.status(), StatusCode::OK);
    let asm: serde_json::Value = serde_json::from_slice(&read_body(asm_response).await).unwrap();
    assert_eq!(asm["code_challenge_methods_supported"][0], "S256");
    assert!(asm["registration_endpoint"].is_string());

    // -- DCR registration ----------------------------------------------------
    let register_body = serde_json::json!({
        "redirect_uris": ["http://127.0.0.1:53076/cb"],
        "client_name": "mcp-remote-test",
        "token_endpoint_auth_method": "client_secret_basic",
    });
    let register_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/register")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(register_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(register_response.status(), StatusCode::CREATED);
    let register: serde_json::Value =
        serde_json::from_slice(&read_body(register_response).await).unwrap();
    let client_id = register["client_id"].as_str().unwrap().to_owned();
    let client_secret = register["client_secret"].as_str().unwrap().to_owned();

    // -- Authorise -----------------------------------------------------------
    let verifier = VERIFIER;
    let challenge = CHALLENGE;
    let authorize_query = format!(
        "response_type=code&client_id={client_id}&redirect_uri=http%3A%2F%2F127.0.0.1%3A53076%2Fcb\
         &code_challenge={challenge}&code_challenge_method=S256\
         &resource=http%3A%2F%2F127.0.0.1%3A8080%2Fmcp&scope=tribal%3Aread%20tribal%3Awrite\
         &state=opaque",
    );
    let authorize_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/authorize?{authorize_query}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
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
    let basic_auth = base64_encode(&format!("{client_id}:{client_secret}"));
    let token_body = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("code", &code),
        ("redirect_uri", "http://127.0.0.1:53076/cb"),
        ("client_id", &client_id),
        ("client_secret", &client_secret),
        ("code_verifier", verifier),
        ("resource", "http://127.0.0.1:8080/mcp"),
    ])
    .unwrap();
    let token_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(header::AUTHORIZATION, format!("Basic {basic_auth}"))
                .body(Body::from(token_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(token_response.status(), StatusCode::OK);
    let token: serde_json::Value =
        serde_json::from_slice(&read_body(token_response).await).unwrap();
    let access_token = token["access_token"].as_str().unwrap().to_owned();
    assert_eq!(token["token_type"], "Bearer");
    assert!(token["expires_in"].as_i64().unwrap() > 0);

    // -- Replay rejection ----------------------------------------------------
    let replay_body = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("code", &code),
        ("redirect_uri", "http://127.0.0.1:53076/cb"),
        ("client_id", &client_id),
        ("client_secret", &client_secret),
        ("code_verifier", verifier),
        ("resource", "http://127.0.0.1:8080/mcp"),
    ])
    .unwrap();
    let replay_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(header::AUTHORIZATION, format!("Basic {basic_auth}"))
                .body(Body::from(replay_body))
                .unwrap(),
        )
        .await
        .unwrap();
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

#[tokio::test]
async fn test_authorize_rejects_resource_mismatch() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;

    let runtime = runtime_config();
    let app = oauth_router(OAuthRouterState::new(Arc::clone(&runtime), pool.clone()));

    // Register a client.
    let register_body = serde_json::json!({
        "redirect_uris": ["http://127.0.0.1:53076/cb"],
        "token_endpoint_auth_method": "none",
    });
    let register_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/register")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(register_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let register: serde_json::Value =
        serde_json::from_slice(&read_body(register_response).await).unwrap();
    let client_id = register["client_id"].as_str().unwrap().to_owned();

    let authorize_query = format!(
        "response_type=code&client_id={client_id}&redirect_uri=http%3A%2F%2F127.0.0.1%3A53076%2Fcb\
         &code_challenge={CHALLENGE}&code_challenge_method=S256\
         &resource=http%3A%2F%2Fwrong.example%2Fmcp",
    );
    let authorize_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/authorize?{authorize_query}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
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
async fn test_token_rejects_pkce_verifier_mismatch() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;

    let runtime = runtime_config();
    let app = oauth_router(OAuthRouterState::new(Arc::clone(&runtime), pool.clone()));

    // Register a public client.
    let register_body = serde_json::json!({
        "redirect_uris": ["http://127.0.0.1:53076/cb"],
        "token_endpoint_auth_method": "none",
    });
    let register_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/register")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(register_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let register: serde_json::Value =
        serde_json::from_slice(&read_body(register_response).await).unwrap();
    let client_id = register["client_id"].as_str().unwrap().to_owned();

    let challenge = CHALLENGE;
    let authorize_query = format!(
        "response_type=code&client_id={client_id}&redirect_uri=http%3A%2F%2F127.0.0.1%3A53076%2Fcb\
         &code_challenge={challenge}&code_challenge_method=S256\
         &resource=http%3A%2F%2F127.0.0.1%3A8080%2Fmcp",
    );
    let authorize_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/authorize?{authorize_query}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let consent_html = String::from_utf8(read_body(authorize_response).await).unwrap();
    let target_url = extract_approve_href(&consent_html);
    let code = Url::parse(&target_url)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.into_owned())
        .unwrap();

    // Verifier whose S256 challenge does NOT match the recorded one.
    let wrong_verifier = "ZyXwVuTsRqPoNmLkJiHgFeDcBa0987654321ABCDEFG";
    let token_body = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("code", &code),
        ("redirect_uri", "http://127.0.0.1:53076/cb"),
        ("client_id", &client_id),
        ("code_verifier", wrong_verifier),
        ("resource", "http://127.0.0.1:8080/mcp"),
    ])
    .unwrap();
    let token_response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(token_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(token_response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(token_response).await).unwrap();
    assert_eq!(body["error"], "invalid_grant");
}

#[tokio::test]
async fn test_register_rejects_non_loopback_http_redirect() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    let runtime = runtime_config();
    let app = oauth_router(OAuthRouterState::new(Arc::clone(&runtime), pool));

    let register_body = serde_json::json!({
        "redirect_uris": ["http://example.com/cb"],
        "token_endpoint_auth_method": "none",
    });
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/register")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(register_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_redirect_uri");
}

// ---------------------------------------------------------------------------
// Shared helpers for the adversarial cases
// ---------------------------------------------------------------------------

/// Registers a client and returns its `client_id` and optional secret.
async fn register_client(app: &Router, auth_method: &str) -> (String, Option<String>) {
    let body = serde_json::json!({
        "redirect_uris": [REDIRECT_URI],
        "token_endpoint_auth_method": auth_method,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/register")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let json: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    let client_id = json["client_id"].as_str().unwrap().to_owned();
    let client_secret = json["client_secret"].as_str().map(str::to_owned);
    (client_id, client_secret)
}

/// Drives `/authorize` for a client and returns the issued authorisation
/// code from the consent page's approve anchor.
async fn authorize_code(app: &Router, client_id: &str) -> String {
    let query = format!(
        "response_type=code&client_id={client_id}&redirect_uri={REDIRECT_URI_ENCODED}\
         &code_challenge={CHALLENGE}&code_challenge_method=S256\
         &resource=http%3A%2F%2F127.0.0.1%3A8080%2Fmcp",
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/authorize?{query}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
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

/// Posts a form body to `/token`.
async fn post_token(app: &Router, body: String) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// Walks register (public) then authorise then token, returning the
/// issued access token.
async fn mint_access_token(app: &Router) -> String {
    let (client_id, _) = register_client(app, "none").await;
    let code = authorize_code(app, &client_id).await;
    let response = post_token(app, token_form(&code, &client_id, VERIFIER, None)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    json["access_token"].as_str().unwrap().to_owned()
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
    let app = oauth_router(OAuthRouterState::new(Arc::clone(&runtime), pool));

    let (client_id, _) = register_client(&app, "none").await;
    // Loopback HTTP grants any-port flexibility, so vary the PATH (not the
    // port) to exercise the exact-match rejection before any code issues.
    let query = format!(
        "response_type=code&client_id={client_id}&redirect_uri=http%3A%2F%2F127.0.0.1%3A53076%2Fevil\
         &code_challenge={CHALLENGE}&code_challenge_method=S256\
         &resource=http%3A%2F%2F127.0.0.1%3A8080%2Fmcp",
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/authorize?{query}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
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
    let app = oauth_router(OAuthRouterState::new(Arc::clone(&runtime), pool));

    let (client_id, secret) = register_client(&app, "client_secret_basic").await;
    assert!(secret.is_some(), "a confidential client is issued a secret");
    let code = authorize_code(&app, &client_id).await;

    let response = post_token(&app, token_form(&code, &client_id, VERIFIER, None)).await;
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
    let app = oauth_router(OAuthRouterState::new(Arc::clone(&runtime), pool));

    let (client_id, _secret) = register_client(&app, "client_secret_basic").await;
    let code = authorize_code(&app, &client_id).await;

    let response = post_token(
        &app,
        token_form(&code, &client_id, VERIFIER, Some("not-the-real-secret")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = serde_json::from_slice(&read_body(response).await).unwrap();
    assert_eq!(body["error"], "invalid_client");
}

#[tokio::test]
async fn test_concurrent_code_exchange_has_one_winner() {
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.unwrap();
    ensure_local_principal(&pool).await;
    let runtime = runtime_config();
    let app = oauth_router(OAuthRouterState::new(Arc::clone(&runtime), pool));

    let (client_id, _) = register_client(&app, "none").await;
    let code = authorize_code(&app, &client_id).await;

    // Two simultaneous exchanges of the same code. The atomic
    // UPDATE ... WHERE consumed_at IS NULL ... RETURNING admits exactly
    // one winner.
    let (first, second) = tokio::join!(
        post_token(&app, token_form(&code, &client_id, VERIFIER, None)),
        post_token(&app, token_form(&code, &client_id, VERIFIER, None)),
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
    let app = oauth_router(OAuthRouterState::new(Arc::clone(&runtime), pool.clone()));

    let (client_id, _) = register_client(&app, "none").await;

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

    let response = post_token(&app, token_form(raw_code, &client_id, VERIFIER, None)).await;
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
    let app = oauth_router(OAuthRouterState::new(Arc::clone(&runtime), pool.clone()));

    // Mint a token bound to this server's resource.
    let access_token = mint_access_token(&app).await;

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
