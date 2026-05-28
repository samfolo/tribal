//! `GET /authorize` handler.
//!
//! Resolves the client identifier (DCR opaque or CIMD URL), validates
//! the redirect URI and PKCE parameters, captures the principal, and
//! issues a single-use authorisation code via redirect.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use rand::RngExt;
use serde::Deserialize;
use tribal_common::sha256_hex;
use tribal_db::{
    NewOauthAuthorizationCode, OauthAuthorizationCodeRepository, OauthClientRepository,
    PgOauthAuthorizationCodeRepository, PgOauthClientRepository, PgPrincipalRepository,
    PrincipalRepository,
};
use tribal_domain::LOCAL_PRINCIPAL_KEY;
use url::Url;

use crate::oauth::{
    config::{OAuthRuntimeConfig, canonicalise_resource_url},
    error::OAuthError,
    pkce::CodeChallenge,
    redirect::matches_redirect_uri,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PARAM_RESPONSE_TYPE: &str = "response_type";
const PARAM_CODE_CHALLENGE_METHOD: &str = "code_challenge_method";
const SUPPORTED_RESPONSE_TYPE: &str = "code";
const SUPPORTED_CODE_CHALLENGE_METHOD: &str = "S256";
const RANDOM_CODE_BYTE_LENGTH: usize = 32;
const LOOPBACK_HOSTS: &[&str] = &["127.0.0.1", "::1", "localhost"];

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

/// Query parameters accepted by `/authorize`.
#[derive(Debug, Deserialize)]
pub struct AuthorizeQuery {
    /// Required `response_type` value (must be `code`).
    pub response_type: String,
    /// Client identifier; DCR opaque or CIMD URL.
    pub client_id: String,
    /// Redirect URI to send the code to on success.
    pub redirect_uri: String,
    /// PKCE code challenge derived from the code verifier via S256.
    pub code_challenge: String,
    /// PKCE challenge method (must be `S256`).
    pub code_challenge_method: String,
    /// Optional client-supplied state token, echoed back on redirect.
    #[serde(default)]
    pub state: Option<String>,
    /// Requested scope (space-separated).
    #[serde(default)]
    pub scope: Option<String>,
    /// Resource indicator per RFC 8707.
    #[serde(default)]
    pub resource: Option<String>,
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Handler state for the authorisation endpoint.
#[derive(Clone)]
pub struct AuthorizeState {
    pool: sqlx::PgPool,
    code_repo: Arc<dyn OauthAuthorizationCodeRepository + Send + Sync>,
    client_repo: Arc<dyn OauthClientRepository + Send + Sync>,
    principal_repo: Arc<dyn PrincipalRepository + Send + Sync>,
    runtime: Arc<OAuthRuntimeConfig>,
}

impl AuthorizeState {
    /// Builds the authorise-state using Postgres repositories.
    #[must_use]
    pub fn new(pool: sqlx::PgPool, runtime: Arc<OAuthRuntimeConfig>) -> Self {
        Self {
            pool,
            code_repo: Arc::new(PgOauthAuthorizationCodeRepository),
            client_repo: Arc::new(PgOauthClientRepository),
            principal_repo: Arc::new(PgPrincipalRepository),
            runtime,
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /authorize` handler.
pub async fn handle_authorize(
    State(state): State<AuthorizeState>,
    Query(query): Query<AuthorizeQuery>,
) -> Response {
    // Stage 1: validate inputs that produce a JSON error if the redirect
    // URI is untrusted (RFC 6749 §3.1.2.4 forbids redirect-to-untrusted).
    let (redirect_uri, registered_redirect_uris) = match validate_pre_redirect(&state, &query).await
    {
        Ok(v) => v,
        Err(err) => return err.into_json_response(),
    };

    // Stage 2: validate inputs whose failure mode is a 302 redirect carrying
    // the error code per RFC 6749 §4.1.2.1.
    match issue_code(&state, &query, &redirect_uri, &registered_redirect_uris).await {
        Ok(redirect_response) => redirect_response,
        Err(err) => err.into_redirect_response(&redirect_uri, query.state.as_deref()),
    }
}

async fn validate_pre_redirect(
    state: &AuthorizeState,
    query: &AuthorizeQuery,
) -> Result<(Url, Vec<Url>), OAuthError> {
    if query.response_type != SUPPORTED_RESPONSE_TYPE {
        return Err(OAuthError::UnsupportedResponseType {
            description: format!(
                "{PARAM_RESPONSE_TYPE} must be \"{SUPPORTED_RESPONSE_TYPE}\" (got {:?})",
                query.response_type
            ),
        });
    }

    if query.code_challenge_method != SUPPORTED_CODE_CHALLENGE_METHOD {
        return Err(OAuthError::InvalidRequest {
            description: format!(
                "{PARAM_CODE_CHALLENGE_METHOD} must be \
                 \"{SUPPORTED_CODE_CHALLENGE_METHOD}\" (got {:?})",
                query.code_challenge_method
            ),
        });
    }

    let registered_uris = resolve_client_redirect_uris(state, &query.client_id).await?;
    let redirect_uri = Url::parse(&query.redirect_uri).map_err(|_| OAuthError::InvalidRequest {
        description: format!("redirect_uri is not a valid URL: {:?}", query.redirect_uri),
    })?;

    if !registered_uris
        .iter()
        .any(|registered| matches_redirect_uri(registered, &redirect_uri))
    {
        return Err(OAuthError::InvalidRedirectUri {
            description: "redirect_uri does not match a registered value".to_owned(),
        });
    }

    Ok((redirect_uri, registered_uris))
}

async fn resolve_client_redirect_uris(
    state: &AuthorizeState,
    client_id: &str,
) -> Result<Vec<Url>, OAuthError> {
    // CIMD path (URL-shaped client_id) is implemented in a follow-up
    // commit per the spike findings; for now only DCR-issued opaque
    // identifiers are accepted.
    if is_probable_url(client_id) {
        return Err(OAuthError::InvalidClient {
            description: "CIMD client_id resolution is not yet enabled on this server".to_owned(),
        });
    }

    let mut conn = state
        .pool
        .acquire()
        .await
        .map_err(|err| OAuthError::Internal {
            description: format!("pool acquire failed: {err}"),
        })?;

    let client = state
        .client_repo
        .find_by_id(&mut conn, client_id)
        .await
        .map_err(|err| OAuthError::Internal {
            description: format!("client lookup failed: {err}"),
        })?
        .ok_or_else(|| OAuthError::InvalidClient {
            description: "client_id is not registered".to_owned(),
        })?;

    client
        .redirect_uris()
        .iter()
        .map(|raw| {
            Url::parse(raw).map_err(|_| OAuthError::Internal {
                description: format!("registered redirect_uri is malformed: {raw:?}"),
            })
        })
        .collect()
}

async fn issue_code(
    state: &AuthorizeState,
    query: &AuthorizeQuery,
    redirect_uri: &Url,
    _registered_redirect_uris: &[Url],
) -> Result<Response, OAuthError> {
    let challenge = CodeChallenge::parse(&query.code_challenge).map_err(|_| {
        OAuthError::InvalidRequest {
            description: "code_challenge is malformed".to_owned(),
        }
    })?;

    let resource = query.resource.as_deref().ok_or_else(|| {
        tracing::warn!(
            auth_failure_reason = "missing_resource",
            "/authorize rejected: missing resource parameter",
        );
        OAuthError::InvalidTarget {
            description: "resource parameter is required per RFC 8707".to_owned(),
        }
    })?;

    let resource_url = Url::parse(resource).map_err(|_| OAuthError::InvalidTarget {
        description: format!("resource is not a valid URL: {resource:?}"),
    })?;
    let canonical = canonicalise_resource_url(&resource_url);
    if canonical != state.runtime.canonical_resource {
        return Err(OAuthError::InvalidTarget {
            description: format!(
                "resource does not match the running server's canonical URL: expected {:?}",
                state.runtime.canonical_resource,
            ),
        });
    }

    let principal_id = {
        let mut conn = state
            .pool
            .acquire()
            .await
            .map_err(|err| OAuthError::Internal {
                description: format!("pool acquire failed: {err}"),
            })?;
        let principal = state
            .principal_repo
            .find_by_key(&mut conn, LOCAL_PRINCIPAL_KEY)
            .await
            .map_err(|err| OAuthError::Internal {
                description: format!("principal lookup failed: {err}"),
            })?
            .ok_or_else(|| OAuthError::Internal {
                description: format!(
                    "local principal {LOCAL_PRINCIPAL_KEY} not found; run `tribal setup`",
                ),
            })?;
        principal.id()
    };

    let raw_code = generate_random_code();
    let code_hash = sha256_hex(&raw_code);
    let expires_at = Utc::now() + chrono::Duration::from_std(state.runtime.authorization_code_ttl)
        .map_err(|err| OAuthError::Internal {
            description: format!("authorization_code_ttl conversion failed: {err}"),
        })?;

    let new = NewOauthAuthorizationCode::builder()
        .code_hash(code_hash)
        .client_id(query.client_id.clone())
        .redirect_uri(redirect_uri.as_str().to_owned())
        .code_challenge(challenge.as_str().to_owned())
        .scope(query.scope.clone())
        .resource(Some(canonical))
        .principal_id(principal_id)
        .expires_at(expires_at)
        .build();

    let mut conn = state
        .pool
        .acquire()
        .await
        .map_err(|err| OAuthError::Internal {
            description: format!("pool acquire failed: {err}"),
        })?;
    state
        .code_repo
        .insert(&mut conn, &new)
        .await
        .map_err(|err| OAuthError::Internal {
            description: format!("insert authorization code failed: {err}"),
        })?;

    Ok(consent_redirect_response(
        redirect_uri,
        &raw_code,
        query.state.as_deref(),
        &query.client_id,
        query.scope.as_deref(),
    ))
}

fn consent_redirect_response(
    redirect_uri: &Url,
    code: &str,
    state: Option<&str>,
    client_id: &str,
    scope: Option<&str>,
) -> Response {
    let mut url = redirect_uri.clone();
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("code", code);
        if let Some(s) = state {
            query.append_pair("state", s);
        }
    }
    let target = url.as_str().to_owned();
    let host = redirect_uri.host_str().unwrap_or("");
    let body = build_consent_html(&target, host, client_id, scope);
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, "text/html; charset=utf-8".parse().unwrap());
    (StatusCode::OK, headers, body).into_response()
}

fn build_consent_html(target: &str, redirect_host: &str, client_id: &str, scope: Option<&str>) -> String {
    let scope_display = scope.unwrap_or("(no explicit scope requested)");
    let warning_html = if LOOPBACK_HOSTS.contains(&redirect_host) {
        "<p>This is a loopback redirect. Any process listening on this host can receive the code; only proceed if you started a local client expecting it.</p>"
    } else {
        ""
    };
    format!(
        "<!doctype html>
<html><head><meta charset=\"utf-8\"><title>Tribal authorisation</title></head>
<body>
  <h1>Authorising client</h1>
  <p><strong>client_id</strong>: <code>{client_id}</code></p>
  <p><strong>redirect_uri host</strong>: <code>{redirect_host}</code></p>
  <p><strong>requested scope</strong>: <code>{scope_display}</code></p>
  {warning_html}
  <form id=\"approve\" action=\"{target}\" method=\"get\">
    <button type=\"submit\" autofocus>Continue</button>
  </form>
  <script>
    document.getElementById('approve').submit();
  </script>
  <noscript><p><a href=\"{target}\">Continue</a></p></noscript>
</body></html>
",
    )
}

fn generate_random_code() -> String {
    let mut bytes = [0u8; RANDOM_CODE_BYTE_LENGTH];
    rand::rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn is_probable_url(client_id: &str) -> bool {
    // CIMD client_ids per the draft are absolute URLs with an https
    // scheme; DCR-issued ids from this server are 43-char base64url
    // with no colon, so this discriminator is unambiguous.
    matches!(
        Url::parse(client_id).map(|u| u.scheme().to_owned()).ok().as_deref(),
        Some("https") | Some("http"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_probable_url_detects_https() {
        assert!(is_probable_url("https://example.com/client.json"));
        assert!(!is_probable_url("ZyXwVuTsRqPoNmLkJiHgFeDcBa0987654321ABCDEFG"));
    }
}
