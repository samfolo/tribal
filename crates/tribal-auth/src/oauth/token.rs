//! `POST /token` handler.
//!
//! Atomically consumes a previously issued authorisation code,
//! verifies the PKCE code verifier with constant-time S256 comparison,
//! and mints an access token into the existing `auth_tokens` store.
//! Authorisation-code consumption and access-token issuance happen
//! inside a single transaction so a partial failure rolls back into a
//! re-exchangeable state.

use std::sync::Arc;

use axum::{
    Form, Json,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use chrono::Utc;
use rand::RngExt;
use serde::{Deserialize, Serialize};
use sqlx::Acquire;
use subtle::ConstantTimeEq;
use tribal_common::sha256_hex;
use tribal_db::{
    AuthTokenRepository, NewAuthToken, OauthAuthorizationCodeRepository, OauthClientRepository,
    PgAuthTokenRepository, PgOauthAuthorizationCodeRepository, PgOauthClientRepository,
};
use tribal_domain::TokenEndpointAuthMethod;

use crate::oauth::{
    common::GRANT_TYPE_AUTHORIZATION_CODE,
    config::{OAuthRuntimeConfig, canonicalise_resource_url},
    error::{
        InternalOperation, InvalidClientReason, InvalidGrantReason, InvalidRequestReason,
        InvalidTargetReason, OAuthError, require_param,
    },
    pkce::{CodeChallenge, CodeVerifier},
    scope::{DEFAULT_GRANT_SCOPE, parse_scope_list},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const RANDOM_TOKEN_BYTE_LENGTH: usize = 32;

// ---------------------------------------------------------------------------
// Request and response
// ---------------------------------------------------------------------------

/// RFC 6749 §4.1.3 token-endpoint request body, before required-field
/// validation.
///
/// Every required parameter is modelled as `Option` so an absent value
/// surfaces as an RFC 6749 §5.2 `invalid_request` (via
/// [`TokenRequest::validate`]) rather than a bare form-deserialisation
/// rejection from the framework.
#[derive(Debug, Deserialize)]
pub struct TokenRequest {
    /// Required grant type (must be `authorization_code`).
    #[serde(default)]
    pub grant_type: Option<String>,
    /// The authorisation code returned at /authorize.
    #[serde(default)]
    pub code: Option<String>,
    /// Redirect URI bound to the code.
    #[serde(default)]
    pub redirect_uri: Option<String>,
    /// Client identifier bound to the code.
    #[serde(default)]
    pub client_id: Option<String>,
    /// PKCE code verifier.
    #[serde(default)]
    pub code_verifier: Option<String>,
    /// Resource indicator per RFC 8707. Required, and must canonicalise
    /// to the same value bound to the code at `/authorize`.
    #[serde(default)]
    pub resource: Option<String>,
    /// Client secret for the `client_secret_post` method, presented in
    /// the form body (RFC 6749 §2.3.1). A `client_secret_basic` client
    /// presents its secret in the `Authorization: Basic` header instead.
    #[serde(default)]
    pub client_secret: Option<String>,
}

/// A token request whose required parameters are all present.
struct ValidatedTokenRequest {
    grant_type: String,
    code: String,
    redirect_uri: String,
    client_id: String,
    code_verifier: String,
    resource: Option<String>,
    client_secret: Option<String>,
}

impl TokenRequest {
    /// Validates that every required parameter is present, mapping an
    /// absent one to an `invalid_request` error.
    fn validate(self) -> Result<ValidatedTokenRequest, OAuthError> {
        Ok(ValidatedTokenRequest {
            grant_type: require_param(self.grant_type, "grant_type")?,
            code: require_param(self.code, "code")?,
            redirect_uri: require_param(self.redirect_uri, "redirect_uri")?,
            client_id: require_param(self.client_id, "client_id")?,
            code_verifier: require_param(self.code_verifier, "code_verifier")?,
            resource: self.resource,
            client_secret: self.client_secret,
        })
    }
}

/// RFC 6749 §5.1 token-endpoint success response.
#[derive(Debug, Serialize)]
pub struct TokenResponse {
    /// Issued bearer access token.
    pub access_token: String,
    /// Token type (always `Bearer`).
    pub token_type: &'static str,
    /// Lifetime of the access token in seconds.
    pub expires_in: i64,
    /// Scope granted on the token.
    pub scope: String,
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Handler state for the token endpoint.
#[derive(Clone)]
pub struct TokenState {
    pool: sqlx::PgPool,
    code_repo: Arc<dyn OauthAuthorizationCodeRepository + Send + Sync>,
    auth_token_repo: Arc<dyn AuthTokenRepository + Send + Sync>,
    client_repo: Arc<dyn OauthClientRepository + Send + Sync>,
    runtime: Arc<OAuthRuntimeConfig>,
}

impl TokenState {
    /// Builds the token state using Postgres repositories.
    #[must_use]
    pub fn new(pool: sqlx::PgPool, runtime: Arc<OAuthRuntimeConfig>) -> Self {
        Self {
            pool,
            code_repo: Arc::new(PgOauthAuthorizationCodeRepository),
            auth_token_repo: Arc::new(PgAuthTokenRepository),
            client_repo: Arc::new(PgOauthClientRepository),
            runtime,
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `POST /token` handler.
pub async fn handle_token(
    State(state): State<TokenState>,
    headers: HeaderMap,
    Form(req): Form<TokenRequest>,
) -> Response {
    let basic = BasicCredentials::from_headers(&headers);
    let result = match req.validate() {
        Ok(validated) => exchange(&state, &validated, basic.as_ref()).await,
        Err(err) => Err(err),
    };
    match result {
        Ok(response) => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
            (StatusCode::OK, headers, Json(response)).into_response()
        }
        Err(err) => {
            // RFC 6749 §5.2: a client-authentication failure carries a
            // `WWW-Authenticate` challenge advertising the supported HTTP
            // auth scheme (required when the client used the Authorization
            // header; permitted otherwise). Only the token endpoint
            // authenticates clients, so the header is added here, not in
            // the shared error renderer (which also serves the browser
            // `/authorize` endpoint).
            let invalid_client = matches!(err, OAuthError::InvalidClient { .. });
            let mut response = err.into_json_response();
            if invalid_client {
                response
                    .headers_mut()
                    .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Basic"));
            }
            response
        }
    }
}

async fn exchange(
    state: &TokenState,
    req: &ValidatedTokenRequest,
    basic: Option<&BasicCredentials>,
) -> Result<TokenResponse, OAuthError> {
    if req.grant_type != GRANT_TYPE_AUTHORIZATION_CODE {
        return Err(OAuthError::InvalidRequest {
            reason: InvalidRequestReason::UnsupportedGrantType {
                presented: req.grant_type.clone(),
            },
        });
    }

    let verifier =
        CodeVerifier::parse(&req.code_verifier).map_err(|_| OAuthError::InvalidGrant {
            reason: InvalidGrantReason::MalformedCodeVerifier,
        })?;

    let now = Utc::now();
    let code_hash = sha256_hex(&req.code);

    let mut conn = state
        .pool
        .acquire()
        .await
        .map_err(|err| OAuthError::Internal {
            operation: InternalOperation::PoolAcquire,
            source: Some(Box::new(err)),
        })?;

    let mut tx = conn.begin().await.map_err(|err| OAuthError::Internal {
        operation: InternalOperation::BeginTransaction,
        source: Some(Box::new(err)),
    })?;

    let code = state
        .code_repo
        .consume_by_hash(&mut tx, &code_hash, now)
        .await
        .map_err(|err| OAuthError::Internal {
            operation: InternalOperation::ConsumeAuthorizationCode,
            source: Some(Box::new(err)),
        })?
        .ok_or(OAuthError::InvalidGrant {
            reason: InvalidGrantReason::UnknownOrExpiredOrUsedCode,
        })?;

    if code.client_id() != req.client_id {
        return Err(OAuthError::InvalidGrant {
            reason: InvalidGrantReason::ClientIdMismatch,
        });
    }
    // Compare the redirect URI through the same URL normalisation
    // `/authorize` applied before storing it, so a client that sends the
    // identical string at both endpoints is not rejected over a
    // normalisation difference (a missing path, host case, percent-
    // encoding). A value that no longer parses cannot match the stored
    // code and is an invalid grant.
    let presented_redirect =
        url::Url::parse(&req.redirect_uri).map_err(|_| OAuthError::InvalidGrant {
            reason: InvalidGrantReason::RedirectUriMismatch,
        })?;
    if code.redirect_uri() != presented_redirect.as_str() {
        return Err(OAuthError::InvalidGrant {
            reason: InvalidGrantReason::RedirectUriMismatch,
        });
    }

    // The resource indicator is required at /token, matching /authorize,
    // and must canonicalise to the value bound to the code (RFC 8707 §2).
    let presented = req.resource.as_deref().ok_or(OAuthError::InvalidTarget {
        reason: InvalidTargetReason::Missing,
    })?;
    let presented_url = url::Url::parse(presented).map_err(|_| OAuthError::InvalidTarget {
        reason: InvalidTargetReason::Malformed {
            value: presented.to_owned(),
        },
    })?;
    if presented_url.fragment().is_some() {
        return Err(OAuthError::InvalidTarget {
            reason: InvalidTargetReason::FragmentPresent {
                value: presented.to_owned(),
            },
        });
    }
    let canonical_presented = canonicalise_resource_url(&presented_url);
    let expected = code.resource().unwrap_or("");
    if canonical_presented != expected {
        return Err(OAuthError::InvalidTarget {
            reason: InvalidTargetReason::Mismatch {
                expected: expected.to_owned(),
                presented: canonical_presented,
            },
        });
    }

    // Verify the client secret for confidential clients.
    enforce_client_secret(state, &mut tx, req, basic).await?;

    let challenge =
        CodeChallenge::parse(code.code_challenge()).map_err(|err| OAuthError::Internal {
            operation: InternalOperation::MalformedStoredCodeChallenge,
            source: Some(Box::new(err)),
        })?;
    if !challenge.verify_s256(&verifier) {
        return Err(OAuthError::InvalidGrant {
            reason: InvalidGrantReason::PkceVerificationFailed,
        });
    }

    let scope = code
        .scope()
        .filter(|s| !s.is_empty())
        .map_or_else(|| DEFAULT_GRANT_SCOPE.to_owned(), str::to_owned);

    let scopes = parse_scope_list(&scope)?;

    let raw_token = generate_random_token();
    let token_hash = sha256_hex(&raw_token);
    let expires_at = now
        + chrono::Duration::from_std(state.runtime.access_token_ttl).map_err(|err| {
            OAuthError::Internal {
                operation: InternalOperation::AccessTokenTtlConversion,
                source: Some(Box::new(err)),
            }
        })?;

    let new_token = NewAuthToken::builder()
        .token_hash(token_hash)
        .principal_id(code.principal_id())
        .scopes(scopes)
        .audience(state.runtime.canonical_resource.clone())
        .expires_at(expires_at)
        .build();

    state
        .auth_token_repo
        .insert(&mut tx, &new_token)
        .await
        .map_err(|err| OAuthError::Internal {
            operation: InternalOperation::InsertAccessToken,
            source: Some(Box::new(err)),
        })?;

    tx.commit().await.map_err(|err| OAuthError::Internal {
        operation: InternalOperation::CommitTransaction,
        source: Some(Box::new(err)),
    })?;

    let expires_in = i64::try_from(state.runtime.access_token_ttl.as_secs()).unwrap_or(i64::MAX);

    Ok(TokenResponse {
        access_token: raw_token,
        token_type: "Bearer",
        expires_in,
        scope,
    })
}

/// Client credentials parsed from an `Authorization: Basic` header.
struct BasicCredentials {
    client_id: String,
    client_secret: String,
}

impl BasicCredentials {
    /// Extracts client credentials from an `Authorization: Basic
    /// base64(client_id:client_secret)` header.
    ///
    /// Returns `None` (rather than a fallible `TryFrom`) when the header
    /// is absent, not the Basic scheme, or not decodable: an absent
    /// Authorization header is the normal case for a public client, not
    /// an error the caller acts on. The scheme token is matched
    /// case-insensitively per RFC 7235 §2.1; the issued
    /// `client_id`/`client_secret` are URL-safe base64 tokens with no
    /// `:`, so splitting on the first colon is unambiguous.
    fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
        let (scheme, encoded) = value.split_once(' ')?;
        if !scheme.eq_ignore_ascii_case("Basic") {
            return None;
        }
        let decoded = STANDARD.decode(encoded).ok()?;
        let decoded = String::from_utf8(decoded).ok()?;
        let (client_id, client_secret) = decoded.split_once(':')?;
        Some(Self {
            client_id: client_id.to_owned(),
            client_secret: client_secret.to_owned(),
        })
    }
}

/// Enforces client authentication according to the client's registered
/// `token_endpoint_auth_method`.
///
/// A `none` client presents no secret; a `client_secret_basic` client
/// presents it in the `Authorization: Basic` header; a
/// `client_secret_post` client presents it in the form body. Presenting
/// the secret via the wrong mechanism is treated as not presenting it,
/// so a confidential client cannot downgrade its registered method.
async fn enforce_client_secret(
    state: &TokenState,
    conn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    req: &ValidatedTokenRequest,
    basic: Option<&BasicCredentials>,
) -> Result<(), OAuthError> {
    let client = state
        .client_repo
        .find_by_id(conn, &req.client_id)
        .await
        .map_err(|err| OAuthError::Internal {
            operation: InternalOperation::ClientLookup,
            source: Some(Box::new(err)),
        })?;

    let Some(client) = client else {
        return Err(OAuthError::InvalidClient {
            reason: InvalidClientReason::Unregistered,
        });
    };

    match client.token_endpoint_auth_method() {
        TokenEndpointAuthMethod::None => Ok(()),
        TokenEndpointAuthMethod::ClientSecretBasic => {
            // The secret must arrive in the Authorization: Basic header,
            // whose client_id must match the request's.
            let presented = basic
                .filter(|credentials| credentials.client_id == req.client_id)
                .map(|credentials| credentials.client_secret.as_str());
            verify_client_secret(client.client_secret_hash(), presented)
        }
        TokenEndpointAuthMethod::ClientSecretPost => {
            verify_client_secret(client.client_secret_hash(), req.client_secret.as_deref())
        }
    }
}

/// Constant-time-compares a presented secret against the stored hash.
///
/// Reached only for confidential clients (the `none` method returns
/// before calling this), so a `None` stored hash is a registration-time
/// invariant violation, not a public client. It fails closed: a
/// confidential client whose stored secret is missing is rejected as an
/// internal error rather than silently authenticated.
fn verify_client_secret(
    stored_hash: Option<&str>,
    presented: Option<&str>,
) -> Result<(), OAuthError> {
    match (stored_hash, presented) {
        (None, _) => Err(OAuthError::Internal {
            operation: InternalOperation::ConfidentialClientMissingSecret,
            source: None,
        }),
        (Some(_), None) => Err(OAuthError::InvalidClient {
            reason: InvalidClientReason::SecretRequired,
        }),
        (Some(stored_hash), Some(presented)) => {
            let presented_hash = sha256_hex(presented);
            let matches = bool::from(presented_hash.as_bytes().ct_eq(stored_hash.as_bytes()));
            if matches {
                Ok(())
            } else {
                Err(OAuthError::InvalidClient {
                    reason: InvalidClientReason::SecretMismatch,
                })
            }
        }
    }
}

fn generate_random_token() -> String {
    let mut bytes = [0u8; RANDOM_TOKEN_BYTE_LENGTH];
    rand::rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}
