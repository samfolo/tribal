//! OAuth 2.1 error model.
//!
//! Maps RFC 6749 / 7591 / 7636 / 8707 error codes onto the HTTP
//! response shape clients expect. Errors that arrive before the
//! redirect URI is validated produce a JSON body; errors that arrive
//! after produce a 302 redirect carrying the error code in the query
//! string, per RFC 6749 §4.1.2.1.

use std::borrow::Cow;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
};
use serde_json::json;
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

/// Unauthorised client per RFC 6749 §4.1.2.1.
pub const ERROR_UNAUTHORIZED_CLIENT: &str = "unauthorized_client";

/// Invalid redirect URI per RFC 7591 §3.2.2.
pub const ERROR_INVALID_REDIRECT_URI: &str = "invalid_redirect_uri";

/// Invalid client metadata per RFC 7591 §3.2.2.
pub const ERROR_INVALID_CLIENT_METADATA: &str = "invalid_client_metadata";

/// Generic server error.
pub const ERROR_SERVER_ERROR: &str = "server_error";

// ---------------------------------------------------------------------------
// OAuthError
// ---------------------------------------------------------------------------

/// Errors produced by the OAuth handlers.
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    /// Generic client request malformed.
    #[error("{description}")]
    InvalidRequest {
        /// Operator/client-readable description of the failure.
        description: String,
    },

    /// Client identifier did not resolve.
    #[error("{description}")]
    InvalidClient {
        /// Description of the failure.
        description: String,
    },

    /// Authorisation code or grant otherwise unusable.
    #[error("{description}")]
    InvalidGrant {
        /// Description of the failure.
        description: String,
    },

    /// `resource` parameter rejected per RFC 8707.
    #[error("{description}")]
    InvalidTarget {
        /// Description of the failure.
        description: String,
    },

    /// Requested scope contains unknown or unauthorised tokens.
    #[error("{description}")]
    InvalidScope {
        /// Description of the failure.
        description: String,
    },

    /// Redirect URI does not match the registered allowlist.
    #[error("{description}")]
    InvalidRedirectUri {
        /// Description of the failure.
        description: String,
    },

    /// DCR registration request rejected for invalid metadata.
    #[error("{description}")]
    InvalidClientMetadata {
        /// Description of the failure.
        description: String,
    },

    /// Unsupported `response_type` value.
    #[error("{description}")]
    UnsupportedResponseType {
        /// Description of the failure.
        description: String,
    },

    /// Internal failure while processing the request.
    #[error("server error: {description}")]
    Internal {
        /// Description of the failure.
        description: String,
    },
}

impl OAuthError {
    /// Returns the OAuth `error` code for this variant.
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

    /// Returns the human-readable description for this error.
    #[must_use]
    pub fn description(&self) -> Cow<'_, str> {
        match self {
            Self::InvalidRequest { description }
            | Self::InvalidClient { description }
            | Self::InvalidGrant { description }
            | Self::InvalidTarget { description }
            | Self::InvalidScope { description }
            | Self::InvalidRedirectUri { description }
            | Self::InvalidClientMetadata { description }
            | Self::UnsupportedResponseType { description }
            | Self::Internal { description } => Cow::Borrowed(description.as_str()),
        }
    }

    /// Builds a JSON response shaped per RFC 6749 §5.2 for use before
    /// the redirect URI has been validated.
    #[must_use]
    pub fn into_json_response(self) -> Response {
        let status = match &self {
            Self::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidClient { .. } => StatusCode::UNAUTHORIZED,
            _ => StatusCode::BAD_REQUEST,
        };
        let body = json!({
            "error": self.error_code(),
            "error_description": self.description(),
        });
        (status, Json(body)).into_response()
    }

    /// Builds a 302 redirect carrying the error code per RFC 6749
    /// §4.1.2.1. Use only after the redirect URI has been validated.
    #[must_use]
    pub fn into_redirect_response(self, redirect_uri: &Url, state: Option<&str>) -> Response {
        let mut url = redirect_uri.clone();
        let code = self.error_code();
        let description = self.description().to_string();
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
