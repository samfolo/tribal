//! RFC 7591 Dynamic Client Registration handler.
//!
//! Accepts a client-supplied JSON metadata document, validates it,
//! generates an opaque `client_id` (and optionally a `client_secret`
//! for confidential auth methods), persists the record, and returns
//! the full registered metadata in the 201 response.

use std::sync::Arc;

use axum::{
    Json,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use tribal_common::sha256_hex;
use tribal_db::{NewOauthClient, OauthClientRepository, PgOauthClientRepository};
use tribal_domain::{ApplicationType, OauthClient, TokenEndpointAuthMethod};
use url::Url;

use crate::oauth::{
    common::{GRANT_TYPE_AUTHORIZATION_CODE, RESPONSE_TYPE_CODE, is_loopback_host},
    error::{ClientMetadataRejection, InternalOperation, OAuthError, RedirectUriRejection},
    scope::first_uncatalogued_scope,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Number of random bytes used for `client_id` and `client_secret`.
const RANDOM_BYTE_LENGTH: usize = 32;

// ---------------------------------------------------------------------------
// Request and response types
// ---------------------------------------------------------------------------

/// RFC 7591 §2 client metadata request body.
#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    /// Registered redirect URIs.
    pub redirect_uris: Vec<String>,
    /// Optional human-readable client name.
    #[serde(default)]
    pub client_name: Option<String>,
    /// Optional declared grant types.
    #[serde(default)]
    pub grant_types: Option<Vec<String>>,
    /// Optional declared response types.
    #[serde(default)]
    pub response_types: Option<Vec<String>>,
    /// Optional declared token endpoint auth method.
    #[serde(default)]
    pub token_endpoint_auth_method: Option<String>,
    /// Optional declared scope.
    #[serde(default)]
    pub scope: Option<String>,
    /// Optional declared application type.
    #[serde(default)]
    pub application_type: Option<String>,
}

/// RFC 7591 §3.2.1 client information response body.
#[derive(Debug, Serialize)]
pub struct RegisterResponse {
    /// Opaque client identifier issued by the AS.
    pub client_id: String,
    /// Raw client secret. Present only for confidential clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    /// Timestamp the client was registered.
    pub client_id_issued_at: i64,
    /// Timestamp the client secret expires (`0` for no expiry).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret_expires_at: Option<i64>,
    /// Echo of the human-readable client name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_name: Option<String>,
    /// Echo of the registered redirect URIs.
    pub redirect_uris: Vec<String>,
    /// Echo of the registered grant types.
    pub grant_types: Vec<String>,
    /// Echo of the registered response types.
    pub response_types: Vec<String>,
    /// Echo of the token endpoint auth method.
    pub token_endpoint_auth_method: String,
    /// Echo of the optional declared scope.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Echo of the optional declared application type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub application_type: Option<String>,
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Handler state for the registration endpoint.
#[derive(Clone)]
pub struct RegisterState {
    pool: sqlx::PgPool,
    repo: Arc<dyn OauthClientRepository + Send + Sync>,
}

impl RegisterState {
    /// Builds a registration state using the Postgres repository.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            repo: Arc::new(PgOauthClientRepository),
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `POST /register` handler.
pub async fn handle_register(
    axum::extract::State(state): axum::extract::State<RegisterState>,
    Json(req): Json<RegisterRequest>,
) -> Response {
    match register(&state, req).await {
        Ok(response) => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            (StatusCode::CREATED, headers, Json(response)).into_response()
        }
        Err(err) => err.into_json_response(),
    }
}

async fn register(
    state: &RegisterState,
    req: RegisterRequest,
) -> Result<RegisterResponse, OAuthError> {
    if req.redirect_uris.is_empty() {
        return Err(OAuthError::InvalidRedirectUri {
            reason: RedirectUriRejection::NoneRegistered,
        });
    }

    for raw in &req.redirect_uris {
        validate_redirect_uri(raw)?;
    }

    let grant_types = req
        .grant_types
        .unwrap_or_else(|| vec![GRANT_TYPE_AUTHORIZATION_CODE.to_owned()]);
    let response_types = req
        .response_types
        .unwrap_or_else(|| vec![RESPONSE_TYPE_CODE.to_owned()]);

    validate_supported_grant_response_types(&grant_types, &response_types)?;
    validate_grant_response_consistency(&grant_types, &response_types)?;

    if let Some(uncatalogued) = req.scope.as_deref().and_then(first_uncatalogued_scope) {
        return Err(OAuthError::InvalidClientMetadata {
            reason: ClientMetadataRejection::UnsupportedScope {
                presented: uncatalogued.to_owned(),
            },
        });
    }

    // Default an omitted auth method to the public PKCE client (`none`),
    // not the RFC 7591 §2 default of `client_secret_basic`. Tribal's
    // clients are overwhelmingly public PKCE clients; a confidential
    // client always declares itself explicitly. Defaulting to `none`
    // issues no secret (one fewer credential surface), keeps PKCE
    // mandatory, and a spec-compliant client reads the registered method
    // back from the response and adapts.
    let auth_method_raw = req
        .token_endpoint_auth_method
        .clone()
        .unwrap_or_else(|| TokenEndpointAuthMethod::None.as_str().to_owned());
    let auth_method = parse_auth_method(&auth_method_raw)?;
    let application_type = req
        .application_type
        .as_deref()
        .map(parse_application_type)
        .transpose()?;

    let client_id = generate_random_token();
    let (client_secret, client_secret_hash) = if auth_method.requires_secret() {
        let raw = generate_random_token();
        let hash = sha256_hex(&raw);
        (Some(raw), Some(hash))
    } else {
        (None, None)
    };

    let new = NewOauthClient::builder()
        .client_id(client_id.clone())
        .client_secret_hash(client_secret_hash)
        .client_name(req.client_name.clone())
        .redirect_uris(req.redirect_uris.clone())
        .grant_types(grant_types.clone())
        .response_types(response_types.clone())
        .token_endpoint_auth_method(auth_method)
        .scope(req.scope.clone())
        .application_type(application_type)
        .build();

    let mut conn = state
        .pool
        .acquire()
        .await
        .map_err(|err| OAuthError::Internal {
            operation: InternalOperation::PoolAcquire,
            source: Some(Box::new(err)),
        })?;

    let inserted: OauthClient =
        state
            .repo
            .insert(&mut conn, &new)
            .await
            .map_err(|err| OAuthError::Internal {
                operation: InternalOperation::InsertOauthClient,
                source: Some(Box::new(err)),
            })?;

    Ok(RegisterResponse {
        client_id: inserted.client_id().to_owned(),
        client_secret,
        client_id_issued_at: inserted.created_at().timestamp(),
        client_secret_expires_at: client_secret_hash_marker(&inserted).then_some(0),
        client_name: inserted.client_name().map(str::to_owned),
        redirect_uris: inserted.redirect_uris().to_vec(),
        grant_types: inserted.grant_types().to_vec(),
        response_types: inserted.response_types().to_vec(),
        token_endpoint_auth_method: inserted.token_endpoint_auth_method().as_str().to_owned(),
        scope: inserted.scope().map(str::to_owned),
        application_type: inserted.application_type().map(|t| t.as_str().to_owned()),
    })
}

fn client_secret_hash_marker(client: &OauthClient) -> bool {
    client.client_secret_hash().is_some()
}

fn parse_auth_method(raw: &str) -> Result<TokenEndpointAuthMethod, OAuthError> {
    TokenEndpointAuthMethod::parse(raw).ok_or_else(|| OAuthError::InvalidClientMetadata {
        reason: ClientMetadataRejection::UnsupportedAuthMethod {
            presented: raw.to_owned(),
        },
    })
}

fn parse_application_type(raw: &str) -> Result<ApplicationType, OAuthError> {
    ApplicationType::parse(raw).ok_or_else(|| OAuthError::InvalidClientMetadata {
        reason: ClientMetadataRejection::UnsupportedApplicationType {
            presented: raw.to_owned(),
        },
    })
}

fn validate_redirect_uri(raw: &str) -> Result<(), OAuthError> {
    let url = Url::parse(raw).map_err(|_| OAuthError::InvalidRedirectUri {
        reason: RedirectUriRejection::Malformed {
            value: raw.to_owned(),
        },
    })?;
    if url.fragment().is_some() {
        return Err(OAuthError::InvalidRedirectUri {
            reason: RedirectUriRejection::FragmentPresent {
                value: raw.to_owned(),
            },
        });
    }
    match url.scheme() {
        "https" => Ok(()),
        "http" => {
            if url.host_str().is_some_and(is_loopback_host) {
                Ok(())
            } else {
                Err(OAuthError::InvalidRedirectUri {
                    reason: RedirectUriRejection::NonLoopbackHttp {
                        value: raw.to_owned(),
                    },
                })
            }
        }
        other => Err(OAuthError::InvalidRedirectUri {
            reason: RedirectUriRejection::UnsupportedScheme {
                scheme: other.to_owned(),
            },
        }),
    }
}

/// Rejects declared grant or response types this server does not support.
///
/// Only `authorization_code` grants and `code` responses are supported;
/// any other declared value (`client_credentials`, `token`, …) is
/// rejected rather than silently dropped, so the persisted record never
/// implies a capability the server does not honour.
fn validate_supported_grant_response_types(
    grant_types: &[String],
    response_types: &[String],
) -> Result<(), OAuthError> {
    if let Some(unsupported) = grant_types
        .iter()
        .find(|grant| grant.as_str() != GRANT_TYPE_AUTHORIZATION_CODE)
    {
        return Err(OAuthError::InvalidClientMetadata {
            reason: ClientMetadataRejection::UnsupportedGrantType {
                presented: unsupported.clone(),
            },
        });
    }
    if let Some(unsupported) = response_types
        .iter()
        .find(|response| response.as_str() != RESPONSE_TYPE_CODE)
    {
        return Err(OAuthError::InvalidClientMetadata {
            reason: ClientMetadataRejection::UnsupportedResponseType {
                presented: unsupported.clone(),
            },
        });
    }
    Ok(())
}

fn validate_grant_response_consistency(
    grant_types: &[String],
    response_types: &[String],
) -> Result<(), OAuthError> {
    if response_types.iter().any(|s| s == RESPONSE_TYPE_CODE)
        && !grant_types
            .iter()
            .any(|s| s == GRANT_TYPE_AUTHORIZATION_CODE)
    {
        return Err(OAuthError::InvalidClientMetadata {
            reason: ClientMetadataRejection::GrantResponseInconsistent,
        });
    }
    Ok(())
}

fn generate_random_token() -> String {
    let mut bytes = [0u8; RANDOM_BYTE_LENGTH];
    rand::rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_redirect_uri_accepts_https() {
        assert!(validate_redirect_uri("https://example.com/cb").is_ok());
    }

    #[test]
    fn test_validate_redirect_uri_accepts_http_loopback() {
        assert!(validate_redirect_uri("http://127.0.0.1/cb").is_ok());
        assert!(validate_redirect_uri("http://localhost:53076/cb").is_ok());
        assert!(validate_redirect_uri("http://[::1]/cb").is_ok());
    }

    #[test]
    fn test_validate_redirect_uri_rejects_http_non_loopback() {
        let err = validate_redirect_uri("http://example.com/cb").unwrap_err();
        assert!(matches!(err, OAuthError::InvalidRedirectUri { .. }));
    }

    #[test]
    fn test_validate_redirect_uri_rejects_fragment() {
        assert!(validate_redirect_uri("https://example.com/cb#frag").is_err());
    }

    #[test]
    fn test_validate_redirect_uri_rejects_non_url() {
        assert!(validate_redirect_uri("not a url").is_err());
    }

    #[test]
    fn test_validate_grant_response_consistency_requires_authorization_code() {
        assert!(
            validate_grant_response_consistency(
                &["password".to_owned()],
                &[RESPONSE_TYPE_CODE.to_owned()],
            )
            .is_err(),
        );
        assert!(
            validate_grant_response_consistency(
                &[GRANT_TYPE_AUTHORIZATION_CODE.to_owned()],
                &[RESPONSE_TYPE_CODE.to_owned()],
            )
            .is_ok(),
        );
    }

    #[test]
    fn test_validate_supported_grant_response_types_accepts_supported() {
        assert!(
            validate_supported_grant_response_types(
                &[GRANT_TYPE_AUTHORIZATION_CODE.to_owned()],
                &[RESPONSE_TYPE_CODE.to_owned()],
            )
            .is_ok(),
        );
    }

    #[test]
    fn test_validate_supported_grant_response_types_rejects_unknown_grant() {
        let err = validate_supported_grant_response_types(
            &[
                GRANT_TYPE_AUTHORIZATION_CODE.to_owned(),
                "client_credentials".to_owned(),
            ],
            &[RESPONSE_TYPE_CODE.to_owned()],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            OAuthError::InvalidClientMetadata {
                reason: ClientMetadataRejection::UnsupportedGrantType { .. },
            },
        ));
    }

    #[test]
    fn test_validate_supported_grant_response_types_rejects_unknown_response() {
        let err = validate_supported_grant_response_types(
            &[GRANT_TYPE_AUTHORIZATION_CODE.to_owned()],
            &[RESPONSE_TYPE_CODE.to_owned(), "token".to_owned()],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            OAuthError::InvalidClientMetadata {
                reason: ClientMetadataRejection::UnsupportedResponseType { .. },
            },
        ));
    }
}
