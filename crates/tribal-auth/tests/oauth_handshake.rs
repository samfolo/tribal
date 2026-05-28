//! End-to-end integration test exercising the OAuth 2.1 handshake.
//!
//! Boots the OAuth router against a real testcontainers-backed
//! Postgres, walks DCR registration → /authorize → /token in sequence,
//! and verifies the issued access token passes through the bearer
//! middleware just like a CLI-minted token.

use std::sync::Arc;

use axum::{Router, body::Body, middleware, routing::get};
use http::{HeaderValue, Method, Request, StatusCode, header};
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
use tribal_config::OAuthConfig;
use tribal_db::{NewPrincipal, PgAuthTokenRepository, PgPrincipalRepository, PrincipalRepository};
use tribal_domain::LOCAL_PRINCIPAL_KEY;
use tribal_test_utils::test_context;
use url::Url;

const ISSUER: &str = "http://127.0.0.1:8080";
const RESOURCE: &str = "http://127.0.0.1:8080/mcp";

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

async fn ensure_local_principal(pool: &sqlx::PgPool) {
    let mut conn = pool.acquire().await.unwrap();
    if PgPrincipalRepository
        .find_by_key(&mut conn, LOCAL_PRINCIPAL_KEY)
        .await
        .unwrap()
        .is_some()
    {
        return;
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
        Ok(_) => {}
        Err(tribal_db::DbError::UniqueViolation { .. }) => {}
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
    let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    let challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
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
    let target_url = extract_form_action(&consent_html);
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
    drop(HeaderValue::from(0));
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
         &code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM&code_challenge_method=S256\
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

    let challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
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
    let target_url = extract_form_action(&consent_html);
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

fn extract_form_action(html: &str) -> String {
    // The consent page navigates via an anchor `click()` rather than a
    // form submission so the redirect URI's query string survives. The
    // anchor's href is the target URL.
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
