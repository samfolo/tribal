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
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
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
use tribal_domain::Scope;

use crate::oauth::{
    config::{OAuthRuntimeConfig, canonicalise_resource_url},
    error::OAuthError,
    pkce::{CodeChallenge, CodeVerifier},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SUPPORTED_GRANT_TYPE: &str = "authorization_code";
const RANDOM_TOKEN_BYTE_LENGTH: usize = 32;
const DEFAULT_GRANT_SCOPE: &str = "tribal:read";

// ---------------------------------------------------------------------------
// Request and response
// ---------------------------------------------------------------------------

/// RFC 6749 §4.1.3 token-endpoint request body.
#[derive(Debug, Deserialize)]
pub struct TokenRequest {
    /// Required grant type (must be `authorization_code`).
    pub grant_type: String,
    /// The authorisation code returned at /authorize.
    pub code: String,
    /// Redirect URI bound to the code.
    pub redirect_uri: String,
    /// Client identifier bound to the code.
    pub client_id: String,
    /// PKCE code verifier.
    pub code_verifier: String,
    /// Resource indicator per RFC 8707.
    #[serde(default)]
    pub resource: Option<String>,
    /// Client secret for confidential clients (passed in the form body).
    #[serde(default)]
    pub client_secret: Option<String>,
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
    Form(req): Form<TokenRequest>,
) -> Response {
    match exchange(&state, req).await {
        Ok(response) => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
            headers.insert(header::PRAGMA, "no-cache".parse().unwrap());
            (StatusCode::OK, headers, Json(response)).into_response()
        }
        Err(err) => err.into_json_response(),
    }
}

async fn exchange(state: &TokenState, req: TokenRequest) -> Result<TokenResponse, OAuthError> {
    if req.grant_type != SUPPORTED_GRANT_TYPE {
        return Err(OAuthError::InvalidRequest {
            description: format!(
                "grant_type must be {SUPPORTED_GRANT_TYPE:?} (got {:?})",
                req.grant_type
            ),
        });
    }

    let verifier =
        CodeVerifier::parse(&req.code_verifier).map_err(|_| OAuthError::InvalidGrant {
            description: "code_verifier is malformed".to_owned(),
        })?;

    let now = Utc::now();
    let code_hash = sha256_hex(&req.code);

    let mut conn = state
        .pool
        .acquire()
        .await
        .map_err(|err| OAuthError::Internal {
            description: format!("pool acquire failed: {err}"),
        })?;

    let mut tx = conn.begin().await.map_err(|err| OAuthError::Internal {
        description: format!("begin transaction failed: {err}"),
    })?;

    let code = state
        .code_repo
        .consume_by_hash(&mut tx, &code_hash, now)
        .await
        .map_err(|err| OAuthError::Internal {
            description: format!("consume authorization code failed: {err}"),
        })?
        .ok_or_else(|| OAuthError::InvalidGrant {
            description: "authorization code is unknown, expired, or already used".to_owned(),
        })?;

    if code.client_id() != req.client_id {
        return Err(OAuthError::InvalidGrant {
            description: "client_id does not match the issued code".to_owned(),
        });
    }
    if code.redirect_uri() != req.redirect_uri {
        return Err(OAuthError::InvalidGrant {
            description: "redirect_uri does not match the issued code".to_owned(),
        });
    }

    if let Some(presented) = req.resource.as_deref() {
        let presented_url =
            url::Url::parse(presented).map_err(|_| OAuthError::InvalidTarget {
                description: format!("resource is not a valid URL: {presented:?}"),
            })?;
        let canonical_presented = canonicalise_resource_url(&presented_url);
        let expected = code.resource().unwrap_or("");
        if canonical_presented != expected {
            return Err(OAuthError::InvalidTarget {
                description: "resource does not match the issued authorisation request".to_owned(),
            });
        }
    }

    // Verify the client secret for confidential clients.
    enforce_client_secret(state, &mut tx, &req).await?;

    let challenge = CodeChallenge::parse(code.code_challenge()).map_err(|_| {
        OAuthError::Internal {
            description: "stored code_challenge is malformed".to_owned(),
        }
    })?;
    if !challenge.verify_s256(&verifier) {
        return Err(OAuthError::InvalidGrant {
            description: "code_verifier does not match the issued challenge".to_owned(),
        });
    }

    let scope = code
        .scope()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| DEFAULT_GRANT_SCOPE.to_owned());

    let scopes = parse_scope_list(&scope)?;

    let raw_token = generate_random_token();
    let token_hash = sha256_hex(&raw_token);
    let expires_at = now
        + chrono::Duration::from_std(state.runtime.access_token_ttl).map_err(|err| {
            OAuthError::Internal {
                description: format!("access_token_ttl conversion failed: {err}"),
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
            description: format!("insert access token failed: {err}"),
        })?;

    tx.commit().await.map_err(|err| OAuthError::Internal {
        description: format!("commit transaction failed: {err}"),
    })?;

    let expires_in = i64::try_from(state.runtime.access_token_ttl.as_secs()).unwrap_or(i64::MAX);

    Ok(TokenResponse {
        access_token: raw_token,
        token_type: "Bearer",
        expires_in,
        scope,
    })
}

async fn enforce_client_secret(
    state: &TokenState,
    conn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    req: &TokenRequest,
) -> Result<(), OAuthError> {
    let client = state
        .client_repo
        .find_by_id(conn, &req.client_id)
        .await
        .map_err(|err| OAuthError::Internal {
            description: format!("client lookup failed: {err}"),
        })?;

    let Some(client) = client else {
        return Err(OAuthError::InvalidClient {
            description: "client_id is not registered".to_owned(),
        });
    };

    match (client.client_secret_hash(), req.client_secret.as_deref()) {
        (None, _) => Ok(()),
        (Some(_), None) => Err(OAuthError::InvalidClient {
            description: "client requires a client_secret".to_owned(),
        }),
        (Some(stored_hash), Some(presented)) => {
            let presented_hash = sha256_hex(presented);
            let matches = bool::from(
                presented_hash.as_bytes().ct_eq(stored_hash.as_bytes()),
            );
            if matches {
                Ok(())
            } else {
                Err(OAuthError::InvalidClient {
                    description: "client_secret does not match".to_owned(),
                })
            }
        }
    }
}

fn parse_scope_list(raw: &str) -> Result<Vec<Scope>, OAuthError> {
    raw.split_ascii_whitespace()
        .map(|token| {
            Scope::parse(token).map_err(|_| OAuthError::InvalidScope {
                description: format!("unknown scope token: {token:?}"),
            })
        })
        .collect()
}

fn generate_random_token() -> String {
    let mut bytes = [0u8; RANDOM_TOKEN_BYTE_LENGTH];
    rand::rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_scope_list_accepts_valid_tokens() {
        let scopes = parse_scope_list("tribal:read tribal.knowledge:write").unwrap();
        assert_eq!(scopes.len(), 2);
    }

    #[test]
    fn test_parse_scope_list_rejects_unknown() {
        let err = parse_scope_list("tribal:admin").unwrap_err();
        assert!(matches!(err, OAuthError::InvalidScope { .. }));
    }
}
