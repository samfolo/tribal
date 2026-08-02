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
    common::{RESPONSE_TYPE_CODE, is_loopback_host},
    config::{OAuthRuntimeConfig, canonicalise_resource_url},
    consent::build_consent_html,
    error::{
        InternalOperation, InvalidClientReason, InvalidRequestReason, InvalidTargetReason,
        OAuthError, RedirectUriRejection, require_param,
    },
    pkce::CodeChallenge,
    redirect::matches_redirect_uri,
    scope::{effective_scope, first_uncatalogued_scope, scope_exceeding_registration},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const RANDOM_CODE_BYTE_LENGTH: usize = 32;

/// Content-Security-Policy for the consent page. The page is fully
/// self-contained: it loads nothing and runs no script, so everything is
/// denied except its own inline styles. `frame-ancestors 'none'` blocks
/// the page being framed for a clickjacked authorisation.
const CONSENT_CSP: &str = "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

/// Query parameters accepted by `/authorize`, before required-field
/// validation.
///
/// Every required parameter is modelled as `Option` so an absent value
/// surfaces as an RFC 6749 the custody contract.2 `invalid_request` (via
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
///
/// `redirect_uri` stays optional: RFC 6749 §3.1.2.3 lets a client omit it
/// when exactly one redirect URI is registered, so it is resolved against
/// the registry rather than required here.
struct ValidatedAuthorizeQuery {
    response_type: String,
    client_id: String,
    redirect_uri: Option<String>,
    code_challenge: String,
    code_challenge_method: String,
    state: Option<String>,
    scope: Option<String>,
    resource: Option<String>,
}

impl AuthorizeQuery {
    /// Validates that every unconditionally-required parameter is present,
    /// mapping an absent one to an `invalid_request` error.
    fn validate(self) -> Result<ValidatedAuthorizeQuery, OAuthError> {
        Ok(ValidatedAuthorizeQuery {
            response_type: require_param(self.response_type, "response_type")?,
            client_id: require_param(self.client_id, "client_id")?,
            redirect_uri: self.redirect_uri,
            code_challenge: require_param(self.code_challenge, "code_challenge")?,
            code_challenge_method: require_param(
                self.code_challenge_method,
                "code_challenge_method",
            )?,
            state: self.state,
            // A blank or whitespace-only scope is treated as omitted, so it
            // falls back to the client's registered scope rather than being
            // recorded as an explicit empty scope.
            scope: self.scope.filter(|raw| !raw.trim().is_empty()),
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
/// redirect URI, the registered scope grant that bounds what a code issued
/// to this client may carry, and the registered display name (when one was
/// supplied at registration) shown on the consent page.
struct ResolvedClient {
    redirect_uri: Url,
    registered_scope: Option<String>,
    client_name: Option<String>,
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
    let redirect_uri = resolve_redirect_uri(query.redirect_uri.as_deref(), &registered_uris)?;

    Ok(ResolvedClient {
        redirect_uri,
        registered_scope: client.scope().map(str::to_owned),
        client_name: client.client_name().map(str::to_owned),
    })
}

/// Resolves the request's redirect URI against the client's registered set.
///
/// A provided value must parse and match a registered URI. RFC 6749
/// §3.1.2.3 permits omitting `redirect_uri` only when exactly one URI is
/// registered; with several registered, the request must name one.
fn resolve_redirect_uri(requested: Option<&str>, registered: &[Url]) -> Result<Url, OAuthError> {
    match requested {
        Some(raw) => {
            let parsed = Url::parse(raw).map_err(|_| OAuthError::InvalidRequest {
                reason: InvalidRequestReason::MalformedRedirectUri {
                    value: raw.to_owned(),
                },
            })?;
            if registered
                .iter()
                .any(|candidate| matches_redirect_uri(candidate, &parsed))
            {
                Ok(parsed)
            } else {
                Err(OAuthError::InvalidRedirectUri {
                    reason: RedirectUriRejection::NoRegisteredMatch,
                })
            }
        }
        None => match registered {
            [single] => Ok(single.clone()),
            _ => Err(OAuthError::InvalidRequest {
                reason: InvalidRequestReason::MissingParameter {
                    parameter: "redirect_uri",
                },
            }),
        },
    }
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
    // scope is permitted; the effective grant is resolved below.
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

    // Resolve the effective grant through the shared resolver so the consent
    // page, the stored code, and the token minted at /token all carry the same
    // scope. Empty and whitespace-only requested or registered scopes resolve
    // to the default there, so a blank scope cannot display as nothing while a
    // token still mints.
    let granted_scope =
        effective_scope(query.scope.as_deref(), resolved.registered_scope.as_deref());

    // Build the consent page before persisting the code, so a render failure
    // leaves no orphaned, never-presented authorisation code behind.
    let response = consent_page_response(
        &resolved.redirect_uri,
        &raw_code,
        query.state.as_deref(),
        &query.client_id,
        resolved.client_name.as_deref(),
        &granted_scope,
    )?;

    let new = NewOauthAuthorizationCode::builder()
        .code_hash(code_hash)
        .client_id(query.client_id.clone())
        .redirect_uri(resolved.redirect_uri.as_str().to_owned())
        .code_challenge(challenge.as_str().to_owned())
        .scope(Some(granted_scope))
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

    Ok(response)
}

fn consent_page_response(
    redirect_uri: &Url,
    code: &str,
    state: Option<&str>,
    client_id: &str,
    client_name: Option<&str>,
    scope: &str,
) -> Result<Response, OAuthError> {
    let mut url = redirect_uri.clone();
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("code", code);
        if let Some(s) = state {
            query.append_pair("state", s);
        }
    }
    let target = url.as_str().to_owned();
    // Display the full authority the code is delivered to, including a
    // non-default port: the port is part of where the code goes, and for a
    // loopback redirect it identifies which local process can receive it.
    // Classify loopback from the bare host, not the host:port shown.
    let bare_host = redirect_uri.host_str().unwrap_or("");
    let authority = match redirect_uri.port() {
        Some(port) => format!("{bare_host}:{port}"),
        None => bare_host.to_owned(),
    };
    let is_loopback = is_loopback_host(bare_host);
    let body = build_consent_html(
        &target,
        &authority,
        client_id,
        client_name,
        scope,
        is_loopback,
    )
    .map_err(|err| OAuthError::Internal {
        operation: InternalOperation::RenderConsentPage,
        source: Some(Box::new(err)),
    })?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    // The page embeds a single-use authorisation code and gates a
    // credential grant, so it is locked down: never cached or stored,
    // never framed (clickjacking), no MIME sniffing, and no referrer
    // leak of the `/authorize` request URL to the redirect target.
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CONSENT_CSP),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    Ok((StatusCode::OK, headers, body).into_response())
}

fn generate_random_code() -> String {
    let mut bytes = [0u8; RANDOM_CODE_BYTE_LENGTH];
    rand::rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}
