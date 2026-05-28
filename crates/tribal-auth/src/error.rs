//! Errors produced by the authentication layer.
//!
//! Variants carrying `token_hash` hold the SHA-256 hex digest, not the
//! raw token. The raw token is never stored or logged.

use tribal_domain::{PrincipalId, Scope};

// ---------------------------------------------------------------------------
// Failure-reason constants (used by structured tracing fields)
// ---------------------------------------------------------------------------

pub(crate) const AUTH_FAILURE_REASON_MISSING: &str = "missing";
pub(crate) const AUTH_FAILURE_REASON_INVALID: &str = "invalid";
pub(crate) const AUTH_FAILURE_REASON_REVOKED: &str = "revoked";
pub(crate) const AUTH_FAILURE_REASON_EXPIRED: &str = "expired";
pub(crate) const AUTH_FAILURE_REASON_UNAVAILABLE: &str = "unavailable";

// ---------------------------------------------------------------------------
// Display strings for the HTTP response body
// ---------------------------------------------------------------------------

/// Display string for requests with no bearer token.
pub const DISPLAY_MISSING_TOKEN: &str = "missing bearer token";

/// Display string for invalid or unrecognised tokens.
pub const DISPLAY_INVALID_TOKEN: &str = "invalid token";

/// Display string for revoked tokens.
pub const DISPLAY_TOKEN_REVOKED: &str = "token revoked";

/// Display string for expired tokens.
pub const DISPLAY_TOKEN_EXPIRED: &str = "token expired";

// ---------------------------------------------------------------------------
// AuthError
// ---------------------------------------------------------------------------

/// Errors produced by the authentication layer.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// No token row matched the computed hash.
    #[error("{DISPLAY_INVALID_TOKEN}")]
    InvalidToken {
        /// SHA-256 hex digest of the raw token.
        token_hash: String,
    },

    /// The token has been revoked (`revoked_at` is set).
    #[error("{DISPLAY_TOKEN_REVOKED}")]
    TokenRevoked {
        /// SHA-256 hex digest of the raw token.
        token_hash: String,
    },

    /// The token has expired (`expires_at` is in the past).
    #[error("{DISPLAY_TOKEN_EXPIRED}")]
    TokenExpired {
        /// SHA-256 hex digest of the raw token.
        token_hash: String,
    },

    /// The token is valid but its `principal_id` does not resolve.
    ///
    /// A defensive check; the FK constraint on `auth_tokens` prevents
    /// this under normal operations. Logged at ERROR level.
    #[error("principal not found for token: {principal_id}")]
    PrincipalNotFound {
        /// The `principal_id` from the valid token row.
        principal_id: PrincipalId,
    },

    /// The local principal record is missing from the database.
    ///
    /// Occurs when the stdio transport attempts to resolve the local
    /// principal identity but setup has not been run.
    #[error("{principal_key} not found, run `tribal setup` to create it")]
    LocalPrincipalMissing {
        /// The principal key that was not found.
        principal_key: String,
    },

    /// A database operation failed during authentication.
    #[error("database unavailable: {context}")]
    DatabaseUnavailable {
        /// Description of which operation failed.
        context: String,
        /// The underlying error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The token is valid but lacks the required scope.
    #[error("insufficient scope: requires {required_scope}")]
    InsufficientScope {
        /// The scope the operation requires.
        required_scope: Scope,
        /// The scopes the token was granted.
        granted_scopes: Vec<Scope>,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::io;

    use tribal_domain::{LOCAL_PRINCIPAL_KEY, PrincipalId};

    use super::*;

    #[test]
    fn test_auth_error_display_variants() {
        let invalid = AuthError::InvalidToken {
            token_hash: "abc".into(),
        };
        assert_eq!(invalid.to_string(), DISPLAY_INVALID_TOKEN);

        let revoked = AuthError::TokenRevoked {
            token_hash: "abc".into(),
        };
        assert_eq!(revoked.to_string(), DISPLAY_TOKEN_REVOKED);

        let expired = AuthError::TokenExpired {
            token_hash: "abc".into(),
        };
        assert_eq!(expired.to_string(), DISPLAY_TOKEN_EXPIRED);

        let id = PrincipalId::new();
        let not_found = AuthError::PrincipalNotFound { principal_id: id };
        assert_eq!(
            not_found.to_string(),
            format!("principal not found for token: {id}"),
        );

        let missing = AuthError::LocalPrincipalMissing {
            principal_key: LOCAL_PRINCIPAL_KEY.to_owned(),
        };
        assert!(missing.to_string().contains("tribal setup"));

        let db_err = AuthError::DatabaseUnavailable {
            context: "test op".into(),
            source: Box::new(io::Error::other("boom")),
        };
        assert_eq!(db_err.to_string(), "database unavailable: test op");

        let insufficient = AuthError::InsufficientScope {
            required_scope: Scope::parse("tribal:write").unwrap(),
            granted_scopes: vec![Scope::parse("tribal.knowledge:read").unwrap()],
        };
        assert_eq!(
            insufficient.to_string(),
            "insufficient scope: requires tribal:write",
        );
    }
}
