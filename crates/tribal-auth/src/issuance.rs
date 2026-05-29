//! Token issuance: the single primitive every auth-token mint flows
//! through.
//!
//! The bearer resource-server plane and the OAuth authorisation-server
//! plane issue tokens identically: generate an opaque random value,
//! persist only its SHA-256 digest, and bind it to a principal, scope
//! set, audience, and expiry. This module owns that sequence so issuance
//! has one definition, mirroring the single verification path in
//! [`crate::Authenticator`].

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use rand::RngExt;
use sqlx::PgConnection;
use tribal_common::sha256_hex;
use tribal_db::{AuthTokenRepository, DbError, NewAuthToken};
use tribal_domain::{PrincipalId, Scope};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Number of random bytes in a freshly issued opaque credential.
const TOKEN_BYTE_LENGTH: usize = 32;

// ---------------------------------------------------------------------------
// Issuance
// ---------------------------------------------------------------------------

/// Issues an auth token: generates an opaque value, persists only its
/// SHA-256 digest bound to `principal_id`, `scopes`, `audience`, and
/// `expires_at`, and returns the raw value for one-time delivery to the
/// client. The raw value is never stored in this form.
///
/// `conn` is `&mut PgConnection`; a `&mut Transaction` reaches it by
/// deref coercion, so issuance inside a wider transaction needs no
/// separate path. `repo` is the insertion repository as a trait object,
/// leaving the concrete implementation to the caller.
///
/// # Errors
///
/// Returns [`DbError`] when the insert fails, including a hash collision.
pub async fn issue_token(
    conn: &mut PgConnection,
    repo: &(dyn AuthTokenRepository + Send + Sync),
    principal_id: PrincipalId,
    scopes: Vec<Scope>,
    audience: String,
    expires_at: DateTime<Utc>,
) -> Result<String, DbError> {
    let raw = generate_token_value();
    let token_hash = sha256_hex(&raw);

    let new = NewAuthToken::builder()
        .token_hash(token_hash)
        .principal_id(principal_id)
        .scopes(scopes)
        .audience(audience)
        .expires_at(expires_at)
        .build();

    repo.insert(conn, &new).await?;

    Ok(raw)
}

/// Generates an opaque random credential: 32 bytes encoded as base64url
/// without padding. The single definition of an opaque credential value
/// for the crate, covering both bearer tokens and OAuth client secrets.
#[must_use]
pub(crate) fn generate_token_value() -> String {
    let mut bytes = [0u8; TOKEN_BYTE_LENGTH];
    rand::rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_token_value_is_43_char_base64url() {
        // 32 bytes encode to 43 base64url characters with no padding.
        let value = generate_token_value();
        assert_eq!(value.len(), 43);
        assert!(
            value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "value must be base64url-no-pad: {value}",
        );
    }

    #[test]
    fn test_generate_token_value_is_unique_across_calls() {
        let a = generate_token_value();
        let b = generate_token_value();
        assert_ne!(a, b, "successive tokens must differ");
    }
}
