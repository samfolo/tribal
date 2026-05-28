//! `GET /authorize` handler.
//!
//! Resolves the client identifier against the registered clients,
//! validates the redirect URI and PKCE parameters, captures the
//! principal, and issues a single-use authorisation code via redirect.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
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
    common::{CODE_CHALLENGE_METHOD_S256, RESPONSE_TYPE_CODE},
    config::{OAuthRuntimeConfig, canonicalise_resource_url},
    consent::build_consent_html,
    error::{
        InternalOperation, InvalidClientReason, InvalidRequestReason, InvalidTargetReason,
        OAuthError, RedirectUriRejection,
    },
    pkce::CodeChallenge,
    redirect::matches_redirect_uri,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const RANDOM_CODE_BYTE_LENGTH: usize = 32;

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

/// Query parameters accepted by `/authorize`.
#[derive(Debug, Deserialize)]
pub struct AuthorizeQuery {
    /// Required `response_type` value (must be `code`).
    pub response_type: String,
    /// Client identifier issued by dynamic client registration.
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
    let redirect_uri = match validate_pre_redirect(&state, &query).await {
        Ok(uri) => uri,
        Err(err) => return err.into_json_response(),
    };

    // Stage 2: validate inputs whose failure mode is a 302 redirect carrying
    // the error code per RFC 6749 §4.1.2.1.
    match issue_code(&state, &query, &redirect_uri).await {
        Ok(redirect_response) => redirect_response,
        Err(err) => err.into_redirect_response(&redirect_uri, query.state.as_deref()),
    }
}

async fn validate_pre_redirect(
    state: &AuthorizeState,
    query: &AuthorizeQuery,
) -> Result<Url, OAuthError> {
    if query.response_type != RESPONSE_TYPE_CODE {
        return Err(OAuthError::UnsupportedResponseType {
            presented: query.response_type.clone(),
        });
    }

    if query.code_challenge_method != CODE_CHALLENGE_METHOD_S256 {
        return Err(OAuthError::InvalidRequest {
            reason: InvalidRequestReason::UnsupportedCodeChallengeMethod {
                presented: query.code_challenge_method.clone(),
            },
        });
    }

    let registered_uris = resolve_client_redirect_uris(state, &query.client_id).await?;
    let redirect_uri = Url::parse(&query.redirect_uri).map_err(|_| OAuthError::InvalidRequest {
        reason: InvalidRequestReason::MalformedRedirectUri {
            value: query.redirect_uri.clone(),
        },
    })?;

    if !registered_uris
        .iter()
        .any(|registered| matches_redirect_uri(registered, &redirect_uri))
    {
        return Err(OAuthError::InvalidRedirectUri {
            reason: RedirectUriRejection::NoRegisteredMatch,
        });
    }

    Ok(redirect_uri)
}

/// Resolves the redirect URIs registered against a client identifier.
///
/// # Errors
///
/// Returns [`OAuthError::InvalidClient`] when the identifier is absent
/// from the client registry.
async fn resolve_client_redirect_uris(
    state: &AuthorizeState,
    client_id: &str,
) -> Result<Vec<Url>, OAuthError> {
    let mut conn = state
        .pool
        .acquire()
        .await
        .map_err(|err| OAuthError::Internal {
            operation: InternalOperation::PoolAcquire,
            source: Some(Box::new(err)),
        })?;

    let client = state
        .client_repo
        .find_by_id(&mut conn, client_id)
        .await
        .map_err(|err| OAuthError::Internal {
            operation: InternalOperation::ClientLookup,
            source: Some(Box::new(err)),
        })?
        .ok_or(OAuthError::InvalidClient {
            reason: InvalidClientReason::Unregistered,
        })?;

    client
        .redirect_uris()
        .iter()
        .map(|raw| {
            Url::parse(raw).map_err(|err| OAuthError::Internal {
                operation: InternalOperation::MalformedRegisteredRedirectUri,
                source: Some(Box::new(err)),
            })
        })
        .collect()
}

async fn issue_code(
    state: &AuthorizeState,
    query: &AuthorizeQuery,
    redirect_uri: &Url,
) -> Result<Response, OAuthError> {
    let challenge =
        CodeChallenge::parse(&query.code_challenge).map_err(|_| OAuthError::InvalidRequest {
            reason: InvalidRequestReason::MalformedCodeChallenge,
        })?;

    let resource = query.resource.as_deref().ok_or_else(|| {
        tracing::warn!(
            auth_failure_reason = "missing_resource",
            "/authorize rejected: missing resource parameter",
        );
        OAuthError::InvalidTarget {
            reason: InvalidTargetReason::Missing,
        }
    })?;

    let resource_url = Url::parse(resource).map_err(|_| OAuthError::InvalidTarget {
        reason: InvalidTargetReason::Malformed {
            value: resource.to_owned(),
        },
    })?;
    let canonical = canonicalise_resource_url(&resource_url);
    if canonical != state.runtime.canonical_resource {
        return Err(OAuthError::InvalidTarget {
            reason: InvalidTargetReason::Mismatch {
                expected: state.runtime.canonical_resource.clone(),
                presented: canonical,
            },
        });
    }

    let principal_id = {
        let mut conn = state
            .pool
            .acquire()
            .await
            .map_err(|err| OAuthError::Internal {
                operation: InternalOperation::PoolAcquire,
                source: Some(Box::new(err)),
            })?;
        let principal = state
            .principal_repo
            .find_by_key(&mut conn, LOCAL_PRINCIPAL_KEY)
            .await
            .map_err(|err| OAuthError::Internal {
                operation: InternalOperation::PrincipalLookup,
                source: Some(Box::new(err)),
            })?
            .ok_or(OAuthError::Internal {
                operation: InternalOperation::LocalPrincipalMissing,
                source: None,
            })?;
        principal.id()
    };

    let raw_code = generate_random_code();
    let code_hash = sha256_hex(&raw_code);
    let expires_at = Utc::now()
        + chrono::Duration::from_std(state.runtime.authorization_code_ttl).map_err(|err| {
            OAuthError::Internal {
                operation: InternalOperation::AuthorizationCodeTtlConversion,
                source: Some(Box::new(err)),
            }
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
            operation: InternalOperation::PoolAcquire,
            source: Some(Box::new(err)),
        })?;
    state
        .code_repo
        .insert(&mut conn, &new)
        .await
        .map_err(|err| OAuthError::Internal {
            operation: InternalOperation::InsertAuthorizationCode,
            source: Some(Box::new(err)),
        })?;

    Ok(consent_page_response(
        redirect_uri,
        &raw_code,
        query.state.as_deref(),
        &query.client_id,
        query.scope.as_deref(),
    ))
}

fn consent_page_response(
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
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    (StatusCode::OK, headers, body).into_response()
}

fn generate_random_code() -> String {
    let mut bytes = [0u8; RANDOM_CODE_BYTE_LENGTH];
    rand::rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}
