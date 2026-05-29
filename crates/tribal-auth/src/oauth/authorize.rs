//! `GET /authorize` handler.
//!
//! Resolves the client identifier against the registered clients,
//! validates the redirect URI and PKCE parameters, captures the
//! principal, and issues a single-use authorisation code via redirect.

use std::sync::Arc;

use axum::{
    extract::{Query, State, rejection::QueryRejection},
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
use tribal_domain::{LOCAL_PRINCIPAL_KEY, OauthAuthorizationCode, OauthClient};
use url::Url;

use crate::oauth::{
    common::RESPONSE_TYPE_CODE,
    config::{OAuthRuntimeConfig, canonicalise_resource_url},
    consent::build_consent_html,
    error::{
        InternalOperation, InvalidClientReason, InvalidRequestReason, InvalidTargetReason,
        OAuthError, RedirectUriRejection, require_param,
    },
    pkce::CodeChallenge,
    redirect::matches_redirect_uri,
    scope::{first_uncatalogued_scope, scope_exceeding_registration},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const RANDOM_CODE_BYTE_LENGTH: usize = 32;

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

/// Query parameters accepted by `/authorize`, before required-field
/// validation.
///
/// Every required parameter is modelled as `Option` so an absent value
/// surfaces as an RFC 6749 §5.2 `invalid_request` (via
/// [`AuthorizeQuery::validate`]) rather than a bare query-deserialisation
/// rejection from the framework.
#[derive(Debug, Deserialize)]
pub struct AuthorizeQuery {
    /// Required `response_type` value (must be `code`).
    #[serde(default)]
    pub response_type: Option<String>,
    /// Client identifier issued by dynamic client registration.
    #[serde(default)]
    pub client_id: Option<String>,
    /// Redirect URI to send the code to on success.
    #[serde(default)]
    pub redirect_uri: Option<String>,
    /// PKCE code challenge derived from the code verifier via S256.
    #[serde(default)]
    pub code_challenge: Option<String>,
    /// PKCE challenge method (must be `S256`).
    #[serde(default)]
    pub code_challenge_method: Option<String>,
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

/// An authorisation request whose required parameters are all present.
struct ValidatedAuthorizeQuery {
    response_type: String,
    client_id: String,
    redirect_uri: String,
    code_challenge: String,
    code_challenge_method: String,
    state: Option<String>,
    scope: Option<String>,
    resource: Option<String>,
}

impl AuthorizeQuery {
    /// Validates that every required parameter is present, mapping an
    /// absent one to an `invalid_request` error.
    fn validate(self) -> Result<ValidatedAuthorizeQuery, OAuthError> {
        Ok(ValidatedAuthorizeQuery {
            response_type: require_param(self.response_type, "response_type")?,
            client_id: require_param(self.client_id, "client_id")?,
            redirect_uri: require_param(self.redirect_uri, "redirect_uri")?,
            code_challenge: require_param(self.code_challenge, "code_challenge")?,
            code_challenge_method: require_param(
                self.code_challenge_method,
                "code_challenge_method",
            )?,
            state: self.state,
            scope: self.scope,
            resource: self.resource,
        })
    }
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
    query: Result<Query<AuthorizeQuery>, QueryRejection>,
) -> Response {
    // A query that cannot be deserialised (a malformed pair, a duplicated
    // scalar parameter) is mapped to the OAuth error model rather than the
    // framework's bare rejection. The redirect URI is not yet validated,
    // so the error renders as JSON (RFC 6749 §3.1.2.4).
    let query = match query {
        Ok(Query(query)) => query,
        Err(rejection) => {
            return OAuthError::InvalidRequest {
                reason: InvalidRequestReason::MalformedRequest {
                    detail: rejection.body_text(),
                },
            }
            .into_json_response();
        }
    };
    let query = match query.validate() {
        Ok(query) => query,
        Err(err) => return err.into_json_response(),
    };

    // Stage 1: validate inputs that produce a JSON error if the redirect
    // URI is untrusted (RFC 6749 §3.1.2.4 forbids redirect-to-untrusted).
    let resolved = match validate_pre_redirect(&state, &query).await {
        Ok(resolved) => resolved,
        Err(err) => return err.into_json_response(),
    };

    // Stage 2: validate inputs whose failure mode is a 302 redirect carrying
    // the error code per RFC 6749 §4.1.2.1.
    match issue_code(&state, &query, &resolved).await {
        Ok(redirect_response) => redirect_response,
        Err(err) => err.into_redirect_response(&resolved.redirect_uri, query.state.as_deref()),
    }
}

/// A `/authorize` client resolved against the registry: the matched
/// redirect URI, and the registered scope grant that bounds what a code
/// issued to this client may carry.
struct ResolvedClient {
    redirect_uri: Url,
    registered_scope: Option<String>,
}

async fn validate_pre_redirect(
    state: &AuthorizeState,
    query: &ValidatedAuthorizeQuery,
) -> Result<ResolvedClient, OAuthError> {
    if query.response_type != RESPONSE_TYPE_CODE {
        return Err(OAuthError::UnsupportedResponseType {
            presented: query.response_type.clone(),
        });
    }

    if query.code_challenge_method != OauthAuthorizationCode::CODE_CHALLENGE_METHOD_S256 {
        return Err(OAuthError::InvalidRequest {
            reason: InvalidRequestReason::UnsupportedCodeChallengeMethod {
                presented: query.code_challenge_method.clone(),
            },
        });
    }

    let client = resolve_client(state, &query.client_id).await?;
    let registered_uris = parse_registered_redirect_uris(&client)?;
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

    Ok(ResolvedClient {
        redirect_uri,
        registered_scope: client.scope().map(str::to_owned),
    })
}

/// Resolves a registered client by identifier.
///
/// # Errors
///
/// Returns [`OAuthError::InvalidClient`] when the identifier is absent
/// from the client registry.
async fn resolve_client(
    state: &AuthorizeState,
    client_id: &str,
) -> Result<OauthClient, OAuthError> {
    let mut conn = state
        .pool
        .acquire()
        .await
        .map_err(|err| OAuthError::Internal {
            operation: InternalOperation::PoolAcquire,
            source: Some(Box::new(err)),
        })?;

    state
        .client_repo
        .find_by_id(&mut conn, client_id)
        .await
        .map_err(|err| OAuthError::Internal {
            operation: InternalOperation::ClientLookup,
            source: Some(Box::new(err)),
        })?
        .ok_or(OAuthError::InvalidClient {
            reason: InvalidClientReason::Unregistered,
        })
}

/// Parses the redirect URIs registered against a client into URLs.
fn parse_registered_redirect_uris(client: &OauthClient) -> Result<Vec<Url>, OAuthError> {
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
    query: &ValidatedAuthorizeQuery,
    resolved: &ResolvedClient,
) -> Result<Response, OAuthError> {
    let challenge =
        CodeChallenge::parse(&query.code_challenge).map_err(|_| OAuthError::InvalidRequest {
            reason: InvalidRequestReason::MalformedCodeChallenge,
        })?;

    // A requested scope outside the advertised catalogue is rejected
    // before the code issues (RFC 6749 §4.1.2.1 invalid_scope). An absent
    // scope is permitted; the grant falls back to the default at /token.
    if let Some(uncatalogued) = query.scope.as_deref().and_then(first_uncatalogued_scope) {
        return Err(OAuthError::InvalidScope {
            unknown_token: uncatalogued.to_owned(),
        });
    }

    // A requested scope must also stay within the client's registered
    // grant: DCR registration records the scope a client may use, and a
    // code must not carry more than that per-client upper bound.
    let excess = resolved
        .registered_scope
        .as_deref()
        .zip(query.scope.as_deref())
        .and_then(|(registered, requested)| scope_exceeding_registration(requested, registered));
    if let Some(excess) = excess {
        return Err(OAuthError::InvalidScope {
            unknown_token: excess.to_owned(),
        });
    }

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
    if resource_url.fragment().is_some() {
        return Err(OAuthError::InvalidTarget {
            reason: InvalidTargetReason::FragmentPresent {
                value: resource.to_owned(),
            },
        });
    }
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
        .redirect_uri(resolved.redirect_uri.as_str().to_owned())
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
        &resolved.redirect_uri,
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
