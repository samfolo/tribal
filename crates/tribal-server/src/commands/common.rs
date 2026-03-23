//! Shared utilities for CLI command implementations.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::TimeDelta;
use rand::RngExt;
use sqlx::{Postgres, pool::PoolConnection};
use tribal_config::{ConfigError, ERR_TTL_ZERO};
use tribal_db::{DbError, NewPrincipal, PgPrincipalRepository, PrincipalRepository};
use tribal_domain::Principal;

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Database defaults
// ---------------------------------------------------------------------------

/// Default database URL used when no other source provides one.
///
/// Injected as a command-defaults-layer value so that the figment cascade
/// still respects YAML, env vars, and CLI overrides.
pub(crate) const DEFAULT_DATABASE_URL: &str = "postgresql://tribal@localhost:5432/tribal";

/// Figment command-defaults layer for the database URL.
///
/// Pass to `load_config` as the `command_defaults` parameter.
pub(crate) const DATABASE_COMMAND_DEFAULTS: [(&str, &str); 1] =
    [("database.url", DEFAULT_DATABASE_URL)];

// ---------------------------------------------------------------------------
// Pool configuration
// ---------------------------------------------------------------------------

/// Maximum connections for single-operation command pools.
pub(crate) const COMMAND_POOL_MAX_CONNECTIONS: u32 = 1;

/// Statement timeout for CLI command operations (30 seconds).
///
/// Commands that run migrations use a longer timeout defined locally.
pub(crate) const COMMAND_STATEMENT_TIMEOUT_MS: u64 = 30_000;

// ---------------------------------------------------------------------------
// Project schema
// ---------------------------------------------------------------------------

/// Schema version for the project settings JSON shape.
///
/// Increment when the structure of `Project.settings` changes in a way
/// that requires migration of existing values. There is no canonical
/// source elsewhere — this is the single definition.
pub(crate) const PROJECT_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Token generation
// ---------------------------------------------------------------------------

/// Number of cryptographically random bytes for token generation.
const TOKEN_BYTE_LENGTH: usize = 32;

/// Generates a cryptographically random bearer token.
///
/// Returns a base64url-encoded string (no padding) of [`TOKEN_BYTE_LENGTH`]
/// random bytes, producing exactly 43 characters.
pub(crate) fn generate_raw_token() -> String {
    let mut bytes = [0u8; TOKEN_BYTE_LENGTH];
    rand::rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

// ---------------------------------------------------------------------------
// TTL conversion
// ---------------------------------------------------------------------------

/// Error message for a token TTL that exceeds the representable range.
pub(crate) const TTL_OUT_OF_RANGE: &str = "auth.token_ttl_hours value is too large";

/// Converts a token TTL in hours to a [`TimeDelta`], validating that the
/// value is non-zero and within the representable range.
///
/// # Errors
///
/// Returns [`AppError::Config`] if the TTL is zero or exceeds the
/// representable range for `TimeDelta`.
pub(crate) fn ttl_to_delta(ttl_hours: u64) -> Result<TimeDelta, AppError> {
    if ttl_hours == 0 {
        return Err(AppError::Config {
            source: ConfigError::ValidationFailed {
                errors: vec![ERR_TTL_ZERO.into()],
            },
        });
    }

    let hours = i64::try_from(ttl_hours).map_err(|_| AppError::Config {
        source: ConfigError::ValidationFailed {
            errors: vec![TTL_OUT_OF_RANGE.into()],
        },
    })?;

    TimeDelta::try_hours(hours).ok_or_else(|| AppError::Config {
        source: ConfigError::ValidationFailed {
            errors: vec![TTL_OUT_OF_RANGE.into()],
        },
    })
}

// ---------------------------------------------------------------------------
// Principal management
// ---------------------------------------------------------------------------

/// Finds a principal by key or creates it if absent.
///
/// Handles the TOCTOU race from concurrent processes by catching
/// `UniqueViolation` on insert and falling back to a second lookup.
///
/// # Errors
///
/// Returns [`AppError::Database`] if the query or insert fails.
pub(crate) async fn find_or_create_principal(
    conn: &mut PoolConnection<Postgres>,
    principal_key: &str,
) -> Result<Principal, AppError> {
    if let Some(existing) = PgPrincipalRepository
        .find_by_key(conn, principal_key)
        .await
        .map_err(|source| AppError::Database { source })?
    {
        return Ok(existing);
    }

    let new = NewPrincipal::builder()
        .principal_key(principal_key.to_owned())
        .build();

    match PgPrincipalRepository.insert(conn, &new).await {
        Ok(principal) => Ok(principal),
        Err(DbError::UniqueViolation { .. }) => PgPrincipalRepository
            .find_by_key(conn, principal_key)
            .await
            .map_err(|source| AppError::Database { source })?
            .ok_or_else(|| AppError::Database {
                source: DbError::NotFound {
                    entity: "principal",
                    id: principal_key.into(),
                },
            }),
        Err(source) => Err(AppError::Database { source }),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Token generation ---------------------------------------------------

    #[test]
    fn test_generate_raw_token_length() {
        let token = generate_raw_token();
        assert_eq!(
            token.len(),
            43,
            "32 bytes base64url-encoded (no padding) should be 43 chars"
        );
    }

    #[test]
    fn test_generate_raw_token_uniqueness() {
        let a = generate_raw_token();
        let b = generate_raw_token();
        assert_ne!(a, b, "two generated tokens should differ");
    }

    #[test]
    fn test_generate_raw_token_is_valid_base64url() {
        let token = generate_raw_token();
        let decoded = URL_SAFE_NO_PAD
            .decode(&token)
            .expect("token should be valid base64url");
        assert_eq!(decoded.len(), TOKEN_BYTE_LENGTH);
    }

    // -- TTL conversion -----------------------------------------------------

    #[test]
    fn test_ttl_to_delta_accepts_default() {
        let delta = ttl_to_delta(8760).unwrap();
        assert_eq!(delta, TimeDelta::try_hours(8760).unwrap());
    }

    #[test]
    fn test_ttl_to_delta_rejects_zero() {
        let err = ttl_to_delta(0).unwrap_err();
        assert!(
            err.to_string().contains(ERR_TTL_ZERO),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn test_ttl_to_delta_rejects_overflow() {
        let err = ttl_to_delta(u64::MAX).unwrap_err();
        assert!(
            err.to_string().contains(TTL_OUT_OF_RANGE),
            "unexpected error: {err}",
        );
    }
}
