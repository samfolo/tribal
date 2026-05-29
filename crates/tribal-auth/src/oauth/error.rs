//! Authorisation-server error model (RFC 6749 / 7591 / 7636 / 8707).
//!
//! This is the authorisation-server plane's error type. It is kept
//! deliberately distinct from [`AuthError`](crate::AuthError), the
//! resource-server plane's type, because the two serve different
//! protocol boundaries with different wire renderings: `AuthError`
//! renders a `WWW-Authenticate` bearer challenge with a 401/403 status,
//! whereas [`OAuthError`] renders either an RFC 6749 §5.2 JSON body
//! (before the redirect URI is validated) or an RFC 6749 §4.1.2.1 302
//! redirect carrying the error code (after it is validated). Neither
//! type maps through the MCP `IntoMcpError` boundary, and nothing
//! outside this crate matches on either, so the one-enum-per-crate rule
//! does not apply; keeping them separate makes cross-plane misuse a
//! compile error (an OAuth handler cannot emit a resource-server error,
//! or the reverse).
//!
//! Each variant carries the typed context it actually holds, never a
//! pre-formatted message. The RFC 6749 `error` code is a stable
//! categorical mapping on the type ([`OAuthError::error_code`]); the
//! human `error_description` is formatted at the rendering boundary
//! ([`OAuthError::client_description`]) so the data layer stays separate
//! from presentation. PKCE material is never carried, so a partial
//! credential cannot surface in an error.

use std::borrow::Cow;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
};
use serde_json::json;
use tracing::warn;
use url::Url;

// ---------------------------------------------------------------------------
// Error codes
// ---------------------------------------------------------------------------

/// Generic invalid-request error code per RFC 6749 §5.2.
pub const ERROR_INVALID_REQUEST: &str = "invalid_request";

/// Unsupported response type per RFC 6749 §4.1.2.1.
pub const ERROR_UNSUPPORTED_RESPONSE_TYPE: &str = "unsupported_response_type";

/// Invalid grant per RFC 6749 §5.2.
pub const ERROR_INVALID_GRANT: &str = "invalid_grant";

/// Invalid client per RFC 6749 §5.2.
pub const ERROR_INVALID_CLIENT: &str = "invalid_client";

/// Invalid scope per RFC 6749 §4.1.2.1.
pub const ERROR_INVALID_SCOPE: &str = "invalid_scope";

/// Invalid target per RFC 8707 §2.
pub const ERROR_INVALID_TARGET: &str = "invalid_target";

/// Invalid redirect URI per RFC 7591 §3.2.2.
pub const ERROR_INVALID_REDIRECT_URI: &str = "invalid_redirect_uri";

/// Invalid client metadata per RFC 7591 §3.2.2.
pub const ERROR_INVALID_CLIENT_METADATA: &str = "invalid_client_metadata";

/// Generic server error.
pub const ERROR_SERVER_ERROR: &str = "server_error";

// ---------------------------------------------------------------------------
// Wire descriptions for the collapsed-detail codes
// ---------------------------------------------------------------------------

/// Uniform `error_description` for every `invalid_grant` cause.
///
/// Collapsing the distinct causes (unknown code, client mismatch,
/// redirect mismatch, PKCE failure) to one wire string denies a probing
/// attacker an oracle about which check failed. The specific cause
/// rides the structured [`InvalidGrantReason`] and the trace span.
const INVALID_GRANT_DESCRIPTION: &str = "the authorization grant is invalid";

/// Uniform `error_description` for every internal failure.
///
/// The operation that failed never reaches the client; it rides the
/// structured [`InternalOperation`] and the trace span instead.
const INTERNAL_SERVER_ERROR_DESCRIPTION: &str = "internal server error";

// ---------------------------------------------------------------------------
// Reason types
// ---------------------------------------------------------------------------

/// Why an `invalid_request` was raised. Carries only the client's own
/// rejected input, never PKCE material.
#[derive(Debug, thiserror::Error)]
pub enum InvalidRequestReason {
    /// The PKCE `code_challenge` failed length/charset validation.
    #[error("code_challenge is malformed")]
    MalformedCodeChallenge,
    /// `code_challenge_method` was not the sole supported `S256`.
    #[error("code_challenge_method must be \"S256\" (got {presented:?})")]
    UnsupportedCodeChallengeMethod {
        /// The method the client presented.
        presented: String,
    },
    /// `redirect_uri` did not parse as a URL.
    #[error("redirect_uri is not a valid URL: {value:?}")]
    MalformedRedirectUri {
        /// The raw value the client presented.
        value: String,
    },
    /// `grant_type` was not the sole supported `authorization_code`.
    #[error("grant_type must be \"authorization_code\" (got {presented:?})")]
    UnsupportedGrantType {
        /// The grant type the client presented.
        presented: String,
    },
    /// A required request parameter was absent.
    #[error("required parameter {parameter} is missing")]
    MissingParameter {
        /// The name of the missing parameter.
        parameter: &'static str,
    },
}

/// Why an `invalid_client` was raised.
#[derive(Debug, thiserror::Error)]
pub enum InvalidClientReason {
    /// No registered client matched the presented `client_id`.
    #[error("client_id is not registered")]
    Unregistered,
    /// The client is confidential but presented no `client_secret`.
    #[error("client requires a client_secret")]
    SecretRequired,
    /// The presented `client_secret` did not match.
    #[error("client_secret does not match")]
    SecretMismatch,
}

/// Why an `invalid_grant` was raised. The wire description is uniform
/// across all of these (see [`INVALID_GRANT_DESCRIPTION`]); the variant
/// exists for the trace span and for tests. No PKCE material is carried.
#[derive(Debug, thiserror::Error)]
pub enum InvalidGrantReason {
    /// The PKCE `code_verifier` failed length/charset validation.
    #[error("code_verifier is malformed")]
    MalformedCodeVerifier,
    /// No exchangeable code matched: unknown, expired, or already used.
    #[error("authorization code is unknown, expired, or already used")]
    UnknownOrExpiredOrUsedCode,
    /// The presented `client_id` did not match the code's bound client.
    #[error("client_id does not match the issued code")]
    ClientIdMismatch,
    /// The presented `redirect_uri` did not match the code's binding.
    #[error("redirect_uri does not match the issued code")]
    RedirectUriMismatch,
    /// The PKCE `code_verifier` did not satisfy the issued challenge.
    #[error("code_verifier does not match the issued challenge")]
    PkceVerificationFailed,
}

/// Why an `invalid_target` was raised (RFC 8707 resource indicator).
#[derive(Debug, thiserror::Error)]
pub enum InvalidTargetReason {
    /// No `resource` parameter was supplied.
    #[error("resource parameter is required per RFC 8707")]
    Missing,
    /// The `resource` value did not parse as a URL.
    #[error("resource is not a valid URL: {value:?}")]
    Malformed {
        /// The raw value the client presented.
        value: String,
    },
    /// The `resource` value carried a fragment, which RFC 8707 forbids.
    #[error("resource must not contain a fragment: {value:?}")]
    FragmentPresent {
        /// The raw value the client presented.
        value: String,
    },
    /// The canonicalised `resource` did not match the expected value.
    #[error("resource {presented:?} does not match the expected {expected:?}")]
    Mismatch {
        /// The canonical resource the server expects.
        expected: String,
        /// The canonicalised resource the client presented.
        presented: String,
    },
}

/// Why a `redirect_uri` was rejected (at registration or authorisation).
#[derive(Debug, thiserror::Error)]
pub enum RedirectUriRejection {
    /// The registration supplied no redirect URIs.
    #[error("redirect_uris must contain at least one entry")]
    NoneRegistered,
    /// A redirect URI did not parse as a URL.
    #[error("redirect_uri is not a valid URL: {value:?}")]
    Malformed {
        /// The raw value supplied.
        value: String,
    },
    /// A redirect URI carried a fragment, which RFC 7591 forbids.
    #[error("redirect_uri must not contain a fragment: {value:?}")]
    FragmentPresent {
        /// The raw value supplied.
        value: String,
    },
    /// An `http` redirect URI named a non-loopback host.
    #[error("redirect_uri http scheme is reserved for loopback hosts: {value:?}")]
    NonLoopbackHttp {
        /// The raw value supplied.
        value: String,
    },
    /// A redirect URI used an unsupported scheme.
    #[error("redirect_uri scheme {scheme} is not supported")]
    UnsupportedScheme {
        /// The scheme supplied.
        scheme: String,
    },
    /// The presented redirect URI matched none of the registered set.
    #[error("redirect_uri does not match a registered value")]
    NoRegisteredMatch,
}

/// Why an `invalid_client_metadata` was raised during DCR.
#[derive(Debug, thiserror::Error)]
pub enum ClientMetadataRejection {
    /// `token_endpoint_auth_method` was not a supported value.
    #[error("unsupported token_endpoint_auth_method: {presented}")]
    UnsupportedAuthMethod {
        /// The method the client presented.
        presented: String,
    },
    /// `application_type` was not a supported value.
    #[error("unsupported application_type: {presented}")]
    UnsupportedApplicationType {
        /// The application type the client presented.
        presented: String,
    },
    /// `response_type=code` was declared without `grant_type=authorization_code`.
    #[error("response_type=code requires grant_type=authorization_code")]
    GrantResponseInconsistent,
    /// Every declared `grant_type` was unsupported, leaving no flow the
    /// server can register the client for.
    #[error("no supported grant_type was declared")]
    NoSupportedGrantType,
    /// A declared `scope` token is not in the advertised catalogue.
    #[error("unsupported scope: {presented}")]
    UnsupportedScope {
        /// The scope token the client declared.
        presented: String,
    },
}

/// The internal operation that failed. Never reaches the client (see
/// [`INTERNAL_SERVER_ERROR_DESCRIPTION`]); it exists for the trace span.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum InternalOperation {
    /// Acquiring a database connection from the pool.
    #[error("pool acquisition")]
    PoolAcquire,
    /// Beginning the `/token` transaction.
    #[error("transaction begin")]
    BeginTransaction,
    /// Committing the `/token` transaction.
    #[error("transaction commit")]
    CommitTransaction,
    /// Looking up a registered client.
    #[error("client lookup")]
    ClientLookup,
    /// Looking up the local principal.
    #[error("principal lookup")]
    PrincipalLookup,
    /// The local principal is absent (the server is not provisioned).
    #[error("local principal resolution (run `tribal setup`)")]
    LocalPrincipalMissing,
    /// Consuming an authorisation code.
    #[error("authorization code consumption")]
    ConsumeAuthorizationCode,
    /// Inserting an authorisation code.
    #[error("authorization code insertion")]
    InsertAuthorizationCode,
    /// Inserting a registered client.
    #[error("oauth client insertion")]
    InsertOauthClient,
    /// Inserting the issued access token.
    #[error("access token insertion")]
    InsertAccessToken,
    /// A confidential client row carried no stored secret hash (a
    /// registration-time invariant violation, not a client error).
    #[error("confidential client missing stored secret")]
    ConfidentialClientMissingSecret,
    /// Converting the authorisation-code TTL to a `chrono` duration.
    #[error("authorization-code TTL conversion")]
    AuthorizationCodeTtlConversion,
    /// Converting the access-token TTL to a `chrono` duration.
    #[error("access-token TTL conversion")]
    AccessTokenTtlConversion,
    /// Parsing a redirect URI read back from the client registry.
    #[error("registered redirect_uri parsing")]
    MalformedRegisteredRedirectUri,
    /// Parsing a code challenge read back from the code store.
    #[error("stored code_challenge parsing")]
    MalformedStoredCodeChallenge,
}

// ---------------------------------------------------------------------------
// OAuthError
// ---------------------------------------------------------------------------

/// Errors produced by the OAuth authorisation-server handlers.
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    /// The request was malformed before the redirect URI was validated.
    #[error("invalid_request: {reason}")]
    InvalidRequest {
        /// The specific malformation.
        reason: InvalidRequestReason,
    },

    /// The client identifier did not resolve or authenticate.
    #[error("invalid_client: {reason}")]
    InvalidClient {
        /// The specific client failure.
        reason: InvalidClientReason,
    },

    /// The authorisation grant (code + PKCE) was unusable.
    #[error("invalid_grant: {reason}")]
    InvalidGrant {
        /// The specific grant failure (uniform on the wire).
        reason: InvalidGrantReason,
    },

    /// The RFC 8707 `resource` indicator was rejected.
    #[error("invalid_target: {reason}")]
    InvalidTarget {
        /// The specific resource failure.
        reason: InvalidTargetReason,
    },

    /// The requested scope contained an unknown token.
    #[error("invalid_scope: unknown scope token {unknown_token:?}")]
    InvalidScope {
        /// The scope token that failed to parse.
        unknown_token: String,
    },

    /// A redirect URI was rejected.
    #[error("invalid_redirect_uri: {reason}")]
    InvalidRedirectUri {
        /// The specific redirect-URI failure.
        reason: RedirectUriRejection,
    },

    /// DCR client metadata was rejected.
    #[error("invalid_client_metadata: {reason}")]
    InvalidClientMetadata {
        /// The specific metadata failure.
        reason: ClientMetadataRejection,
    },

    /// The `response_type` value was unsupported.
    #[error("unsupported_response_type: response_type {presented:?} is not supported")]
    UnsupportedResponseType {
        /// The response type the client presented.
        presented: String,
    },

    /// An internal failure while processing the request.
    #[error("server error during {operation}")]
    Internal {
        /// The operation that failed.
        operation: InternalOperation,
        /// The underlying error, when one was available.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

impl OAuthError {
    /// Returns the RFC 6749 `error` code for this variant.
    ///
    /// A stable categorical mapping that belongs on the type.
    #[must_use]
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::InvalidRequest { .. } => ERROR_INVALID_REQUEST,
            Self::InvalidClient { .. } => ERROR_INVALID_CLIENT,
            Self::InvalidGrant { .. } => ERROR_INVALID_GRANT,
            Self::InvalidTarget { .. } => ERROR_INVALID_TARGET,
            Self::InvalidScope { .. } => ERROR_INVALID_SCOPE,
            Self::InvalidRedirectUri { .. } => ERROR_INVALID_REDIRECT_URI,
            Self::InvalidClientMetadata { .. } => ERROR_INVALID_CLIENT_METADATA,
            Self::UnsupportedResponseType { .. } => ERROR_UNSUPPORTED_RESPONSE_TYPE,
            Self::Internal { .. } => ERROR_SERVER_ERROR,
        }
    }

    /// Returns the HTTP status for a JSON-shaped response.
    #[must_use]
    pub fn http_status(&self) -> StatusCode {
        match self {
            Self::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidClient { .. } => StatusCode::UNAUTHORIZED,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    /// Returns the client-facing `error_description` for the wire.
    ///
    /// `invalid_grant` collapses to one uniform string and `server_error`
    /// to a generic one (neither hands an attacker an oracle); the
    /// remaining codes describe the client's own request inputs, which
    /// are safe to echo and useful for integration debugging.
    #[must_use]
    pub fn client_description(&self) -> Cow<'static, str> {
        match self {
            Self::InvalidGrant { .. } => Cow::Borrowed(INVALID_GRANT_DESCRIPTION),
            Self::Internal { .. } => Cow::Borrowed(INTERNAL_SERVER_ERROR_DESCRIPTION),
            Self::InvalidRequest { reason } => Cow::Owned(reason.to_string()),
            Self::InvalidClient { reason } => Cow::Owned(reason.to_string()),
            Self::InvalidTarget { reason } => Cow::Owned(reason.to_string()),
            Self::InvalidScope { unknown_token } => {
                Cow::Owned(format!("unknown scope token: {unknown_token:?}"))
            }
            Self::InvalidRedirectUri { reason } => Cow::Owned(reason.to_string()),
            Self::InvalidClientMetadata { reason } => Cow::Owned(reason.to_string()),
            Self::UnsupportedResponseType { presented } => Cow::Owned(format!(
                "response_type must be \"code\" (got {presented:?})"
            )),
        }
    }

    /// Builds a JSON response shaped per RFC 6749 §5.2 for use before
    /// the redirect URI has been validated.
    ///
    /// Logs the full structured detail to the trace span first so
    /// operators retain it while the wire body stays non-oracular.
    #[must_use]
    pub fn into_json_response(self) -> Response {
        let code = self.error_code();
        let status = self.http_status();
        warn!(error = code, detail = %self, "oauth request rejected (json)");
        let body = json!({
            "error": code,
            "error_description": self.client_description(),
        });
        (status, Json(body)).into_response()
    }

    /// Builds a 302 redirect carrying the error code per RFC 6749
    /// §4.1.2.1. Use only after the redirect URI has been validated.
    ///
    /// Logs the full structured detail to the trace span first.
    #[must_use]
    pub fn into_redirect_response(self, redirect_uri: &Url, state: Option<&str>) -> Response {
        let code = self.error_code();
        warn!(error = code, detail = %self, "oauth request rejected (redirect)");
        let description = self.client_description();
        let mut url = redirect_uri.clone();
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("error", code);
            query.append_pair("error_description", &description);
            if let Some(s) = state {
                query.append_pair("state", s);
            }
        }
        Redirect::to(url.as_str()).into_response()
    }
}

/// Returns the value of a required OAuth request parameter, or an
/// `invalid_request` error naming the parameter that was absent.
///
/// A present-but-empty value is treated as absent: an empty required
/// parameter is unusable, and surfacing it as `invalid_request` reads
/// better than letting it fail an opaque downstream check. Modelling
/// required parameters as `Option` and mapping them here keeps a missing
/// value on the RFC 6749 §5.2 error path rather than the framework's
/// extractor rejection.
pub(crate) fn require_param(
    value: Option<String>,
    parameter: &'static str,
) -> Result<String, OAuthError> {
    value
        .filter(|v| !v.is_empty())
        .ok_or(OAuthError::InvalidRequest {
            reason: InvalidRequestReason::MissingParameter { parameter },
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_maps_each_variant() {
        assert_eq!(
            OAuthError::InvalidRequest {
                reason: InvalidRequestReason::MalformedCodeChallenge,
            }
            .error_code(),
            ERROR_INVALID_REQUEST,
        );
        assert_eq!(
            OAuthError::InvalidGrant {
                reason: InvalidGrantReason::PkceVerificationFailed,
            }
            .error_code(),
            ERROR_INVALID_GRANT,
        );
        assert_eq!(
            OAuthError::Internal {
                operation: InternalOperation::PoolAcquire,
                source: None,
            }
            .error_code(),
            ERROR_SERVER_ERROR,
        );
    }

    #[test]
    fn test_invalid_grant_wire_description_is_uniform() {
        // Every invalid_grant cause renders the same wire string so a
        // probing attacker learns nothing about which check failed.
        let causes = [
            InvalidGrantReason::MalformedCodeVerifier,
            InvalidGrantReason::UnknownOrExpiredOrUsedCode,
            InvalidGrantReason::ClientIdMismatch,
            InvalidGrantReason::RedirectUriMismatch,
            InvalidGrantReason::PkceVerificationFailed,
        ];
        for reason in causes {
            let err = OAuthError::InvalidGrant { reason };
            assert_eq!(err.client_description(), INVALID_GRANT_DESCRIPTION);
        }
    }

    #[test]
    fn test_invalid_grant_detail_distinguishes_causes_for_traces() {
        // The detailed Display (for the trace span) keeps the cause even
        // though the wire description does not.
        let pkce = OAuthError::InvalidGrant {
            reason: InvalidGrantReason::PkceVerificationFailed,
        };
        let unknown = OAuthError::InvalidGrant {
            reason: InvalidGrantReason::UnknownOrExpiredOrUsedCode,
        };
        assert_ne!(pkce.to_string(), unknown.to_string());
        assert!(pkce.to_string().contains("code_verifier"));
    }

    #[test]
    fn test_internal_wire_description_is_generic() {
        let err = OAuthError::Internal {
            operation: InternalOperation::InsertAccessToken,
            source: None,
        };
        assert_eq!(err.client_description(), INTERNAL_SERVER_ERROR_DESCRIPTION);
        // The operation rides the detailed Display, not the wire.
        assert!(err.to_string().contains("access token insertion"));
    }

    #[test]
    fn test_http_status_per_variant() {
        assert_eq!(
            OAuthError::Internal {
                operation: InternalOperation::PoolAcquire,
                source: None,
            }
            .http_status(),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
        assert_eq!(
            OAuthError::InvalidClient {
                reason: InvalidClientReason::Unregistered,
            }
            .http_status(),
            StatusCode::UNAUTHORIZED,
        );
        assert_eq!(
            OAuthError::InvalidScope {
                unknown_token: "tribal:admin".to_owned(),
            }
            .http_status(),
            StatusCode::BAD_REQUEST,
        );
    }

    #[test]
    fn test_invalid_target_wire_description_keeps_reason() {
        // invalid_target describes the client's own resource input, which
        // is not a secret-bearing oracle, so it is not collapsed.
        let err = OAuthError::InvalidTarget {
            reason: InvalidTargetReason::Mismatch {
                expected: "http://127.0.0.1:8080/mcp".to_owned(),
                presented: "http://wrong.example/mcp".to_owned(),
            },
        };
        let desc = err.client_description();
        assert!(desc.contains("wrong.example"));
        assert!(desc.contains("127.0.0.1"));
    }
}
