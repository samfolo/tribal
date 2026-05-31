//! RFC 7591 Dynamic Client Registration handler.
//!
//! Accepts a client-supplied JSON metadata document, validates it,
//! generates an opaque `client_id` (and optionally a `client_secret`
//! for confidential auth methods), persists the record, and returns
//! the full registered metadata in the 201 response.

use std::sync::Arc;

use axum::{
    Json,
    extract::rejection::JsonRejection,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tribal_common::sha256_hex;
use tribal_db::{NewOauthClient, OauthClientRepository, PgOauthClientRepository};
use tribal_domain::{ApplicationType, OauthClient, TokenEndpointAuthMethod};
use url::Url;

use crate::{
    issuance::generate_token_value,
    oauth::{
        common::{GRANT_TYPE_AUTHORIZATION_CODE, RESPONSE_TYPE_CODE, is_loopback_host},
        error::{ClientMetadataRejection, InternalOperation, OAuthError, RedirectUriRejection},
        scope::{first_uncatalogued_scope, present_scope},
    },
};

// ---------------------------------------------------------------------------
// Request and response types
// ---------------------------------------------------------------------------

/// RFC 7591 §2 client metadata request body.
#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    /// Registered redirect URIs. Required; modelled as `Option` so an
    /// absent field surfaces as the RFC 7591 `invalid_redirect_uri` error
    /// rather than a bare JSON-deserialisation rejection.
    #[serde(default)]
    pub redirect_uris: Option<Vec<String>>,
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
    req: Result<Json<RegisterRequest>, JsonRejection>,
) -> Response {
    // Malformed JSON or a wrong-typed field (RFC 7591 §3.2.2) is mapped to
    // `invalid_client_metadata` rather than the framework's bare rejection.
    let req = match req {
        Ok(Json(req)) => req,
        Err(rejection) => {
            return OAuthError::InvalidClientMetadata {
                reason: ClientMetadataRejection::MalformedMetadata {
                    detail: rejection.body_text(),
                },
            }
            .into_json_response();
        }
    };
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
    let redirect_uris = req.redirect_uris.unwrap_or_default();
    if redirect_uris.is_empty() {
        return Err(OAuthError::InvalidRedirectUri {
            reason: RedirectUriRejection::NoneRegistered,
        });
    }

    for raw in &redirect_uris {
        validate_redirect_uri(raw)?;
    }

    // Filter declared grant/response types to those this server supports
    // rather than rejecting a client that also declares ones we do not
    // honour (notably `refresh_token`, which the MCP spec encourages
    // clients to request). RFC 7591 §3.2.1 lets the AS register a subset
    // and echo it back, so the client learns what it actually got.
    let grant_types = filter_supported(req.grant_types, GRANT_TYPE_AUTHORIZATION_CODE);
    let response_types = filter_supported(req.response_types, RESPONSE_TYPE_CODE);

    // A client that declared only unsupported grant or response types
    // leaves no flow we can serve; there is nothing to register it for.
    // An omitted field defaulted to the supported value above, so an empty
    // result here means the client declared values and none were supported.
    if grant_types.is_empty() {
        return Err(OAuthError::InvalidClientMetadata {
            reason: ClientMetadataRejection::NoSupportedGrantType,
        });
    }
    if response_types.is_empty() {
        return Err(OAuthError::InvalidClientMetadata {
            reason: ClientMetadataRejection::NoSupportedResponseType,
        });
    }
    validate_grant_response_consistency(&grant_types, &response_types)?;

    // Treat a blank or whitespace-only declared scope as absent so the stored
    // client never carries an empty scope, which would later display as nothing
    // on the consent page while a token still minted the default grant.
    let scope = present_scope(req.scope.as_deref());
    if let Some(uncatalogued) = scope.and_then(first_uncatalogued_scope) {
        return Err(OAuthError::InvalidClientMetadata {
            reason: ClientMetadataRejection::UnsupportedScope {
                presented: uncatalogued.to_owned(),
            },
        });
    }

    // The registered name is shown on the consent page, a security prompt, and
    // is attacker-chosen at open registration. Validate it before persistence:
    // a blank name resolves to none, and control or directional formatting
    // characters or an excessive length are rejected.
    let client_name = validate_client_name(req.client_name.as_deref())?;

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

    let client_id = generate_token_value();
    let (client_secret, client_secret_hash) = if auth_method.requires_secret() {
        let raw = generate_token_value();
        let hash = sha256_hex(&raw);
        (Some(raw), Some(hash))
    } else {
        (None, None)
    };

    let new = NewOauthClient::builder()
        .client_id(client_id.clone())
        .client_secret_hash(client_secret_hash)
        .client_name(client_name)
        .redirect_uris(redirect_uris.clone())
        .grant_types(grant_types.clone())
        .response_types(response_types.clone())
        .token_endpoint_auth_method(auth_method)
        .scope(scope.map(str::to_owned))
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

/// Maximum number of characters in a registered `client_name` shown on the
/// consent page.
const MAX_CLIENT_NAME_CHARS: usize = 200;

/// Validates and normalises a registered `client_name` for safe display on the
/// consent page. A blank or whitespace-only name resolves to `None`. A name
/// carrying control or Unicode directional-formatting characters, or exceeding
/// [`MAX_CLIENT_NAME_CHARS`], is rejected: the value is attacker-chosen at open
/// registration and the consent page is a security prompt, so a name that could
/// reorder its own disclaimer, smuggle a newline, or push the redirect host out
/// of view is not admitted.
fn validate_client_name(raw: Option<&str>) -> Result<Option<String>, OAuthError> {
    let Some(name) = raw else {
        return Ok(None);
    };
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.chars().count() > MAX_CLIENT_NAME_CHARS {
        return Err(invalid_client_name(format!(
            "exceeds {MAX_CLIENT_NAME_CHARS} characters"
        )));
    }
    if trimmed.chars().any(is_unsafe_display_char) {
        return Err(invalid_client_name(
            "contains control or directional formatting characters".to_owned(),
        ));
    }
    Ok(Some(trimmed.to_owned()))
}

fn invalid_client_name(detail: String) -> OAuthError {
    OAuthError::InvalidClientMetadata {
        reason: ClientMetadataRejection::InvalidClientName { detail },
    }
}

/// Control characters (including newlines and tabs) and the Unicode
/// bidirectional formatting characters, which can visually reorder the text
/// that follows them, are unsafe to display in the consent prompt.
fn is_unsafe_display_char(c: char) -> bool {
    c.is_control()
        || matches!(c,
            '\u{200E}' | '\u{200F}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2066}'..='\u{2069}')
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

/// Filters declared metadata values to the single one this server
/// supports, defaulting to it when the client declared none.
///
/// Only `authorization_code` grants and `code` responses are supported.
/// Filtering (rather than rejecting) lets a client that also declares an
/// unsupported value still register, receiving the supported subset back;
/// the persisted record never implies a capability the server does not
/// honour, since the unsupported values are dropped.
fn filter_supported(declared: Option<Vec<String>>, supported: &str) -> Vec<String> {
    match declared {
        None => vec![supported.to_owned()],
        Some(values) => values
            .into_iter()
            .filter(|value| value == supported)
            .collect(),
    }
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
    fn test_filter_supported_keeps_supported_and_drops_unsupported() {
        // A client declaring authorization_code + refresh_token keeps only
        // the supported grant; the refresh_token it cannot use is dropped.
        assert_eq!(
            filter_supported(
                Some(vec![
                    GRANT_TYPE_AUTHORIZATION_CODE.to_owned(),
                    "refresh_token".to_owned(),
                ]),
                GRANT_TYPE_AUTHORIZATION_CODE,
            ),
            vec![GRANT_TYPE_AUTHORIZATION_CODE.to_owned()],
        );
    }

    #[test]
    fn test_filter_supported_defaults_when_absent() {
        assert_eq!(
            filter_supported(None, RESPONSE_TYPE_CODE),
            vec![RESPONSE_TYPE_CODE.to_owned()],
        );
    }

    #[test]
    fn test_filter_supported_empty_when_only_unsupported() {
        assert!(
            filter_supported(
                Some(vec!["client_credentials".to_owned()]),
                GRANT_TYPE_AUTHORIZATION_CODE
            )
            .is_empty(),
        );
    }

    #[test]
    fn test_validate_client_name_normalises_blank_to_none() {
        assert_eq!(validate_client_name(None).unwrap(), None);
        assert_eq!(validate_client_name(Some("")).unwrap(), None);
        assert_eq!(validate_client_name(Some("   ")).unwrap(), None);
    }

    #[test]
    fn test_validate_client_name_trims_and_accepts() {
        assert_eq!(
            validate_client_name(Some("  Tribal CLI  ")).unwrap(),
            Some("Tribal CLI".to_owned()),
        );
    }

    #[test]
    fn test_validate_client_name_rejects_directional_and_control_chars() {
        // U+202E (right-to-left override) can visually reorder the disclaimer
        // and identifier that follow the name on the consent page.
        assert!(matches!(
            validate_client_name(Some("Tribal\u{202E}lacirT")),
            Err(OAuthError::InvalidClientMetadata {
                reason: ClientMetadataRejection::InvalidClientName { .. },
            }),
        ));
        // A newline would let the value span lines in the prompt.
        assert!(matches!(
            validate_client_name(Some("line one\nline two")),
            Err(OAuthError::InvalidClientMetadata {
                reason: ClientMetadataRejection::InvalidClientName { .. },
            }),
        ));
    }

    #[test]
    fn test_validate_client_name_rejects_overlong() {
        let long = "a".repeat(MAX_CLIENT_NAME_CHARS + 1);
        assert!(matches!(
            validate_client_name(Some(&long)),
            Err(OAuthError::InvalidClientMetadata {
                reason: ClientMetadataRejection::InvalidClientName { .. },
            }),
        ));
    }
}
