//! Shared utilities for CLI command implementations.

use std::{
    io::Write,
    path::{Path, PathBuf},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::TimeDelta;
use rand::RngExt;
use sqlx::{Postgres, pool::PoolConnection};
use tribal_config::{
    CREDENTIALS_WRITE_FAILED_PREFIX, CREDENTIALS_WRITE_FAILED_SUFFIX, ConfigError, Credentials,
    ERR_TTL_ZERO, write_credentials,
};
use tribal_db::{DbError, NewPrincipal, PgPrincipalRepository, PrincipalRepository};
use tribal_domain::{BearerToken, Principal};

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Database defaults
// ---------------------------------------------------------------------------

/// Default database URL used when no other source provides one.
///
/// Injected as a command-defaults-layer value so that the figment cascade
/// still respects YAML, env vars, and CLI overrides.
pub(crate) const DEFAULT_DATABASE_URL: &str = "postgresql://tribal:tribal@localhost:5432/tribal";

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
// Timestamp formatting
// ---------------------------------------------------------------------------

/// Display format for timestamps in CLI output.
pub(crate) const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S UTC";

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

/// Error message for an `auth.token_ttl_hours` config value that exceeds
/// the representable range.
pub(crate) const TTL_OUT_OF_RANGE: &str = "auth.token_ttl_hours value is too large";

/// Error message when a CLI `--ttl` flag is zero.
pub(crate) const TTL_FLAG_MUST_BE_POSITIVE: &str = "--ttl must be greater than zero";

/// Error message when a CLI `--ttl` flag exceeds the representable range.
pub(crate) const TTL_FLAG_OUT_OF_RANGE: &str = "--ttl value is too large";

/// Failure modes for [`ttl_to_delta`].
///
/// Typed (rather than wrapped in [`AppError`]) so that callers can
/// attribute the failure to whichever input source supplied the value —
/// a CLI flag, a config field, or anywhere else — and pick the right
/// [`AppError`] variant + message themselves.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TtlError {
    /// The TTL value was zero.
    Zero,
    /// The TTL value exceeded the representable range.
    OutOfRange,
}

/// Converts a token TTL in hours to a [`TimeDelta`], validating that the
/// value is non-zero and within the representable range.
///
/// # Errors
///
/// Returns [`TtlError::Zero`] when `ttl_hours` is zero and
/// [`TtlError::OutOfRange`] when the value exceeds the representable
/// range for `TimeDelta`.
pub(crate) fn ttl_to_delta(ttl_hours: u64) -> Result<TimeDelta, TtlError> {
    if ttl_hours == 0 {
        return Err(TtlError::Zero);
    }
    let hours = i64::try_from(ttl_hours).map_err(|_| TtlError::OutOfRange)?;
    TimeDelta::try_hours(hours).ok_or(TtlError::OutOfRange)
}

/// Resolves the effective TTL from a CLI flag and config default,
/// dispatching the typed [`TtlError`] from [`ttl_to_delta`] to the right
/// [`AppError`] and message depending on which input supplied the value.
///
/// The mapping is exhaustive over (variant, source) so adding a new
/// [`TtlError`] variant forces an update at this single site rather
/// than silently falling through to one of the existing messages.
///
/// # Errors
///
/// Returns [`AppError::TokenOperation`] for invalid CLI flag values and
/// [`AppError::Config`] for invalid config values.
pub(crate) fn resolve_ttl(cli_ttl: Option<u64>, config_ttl: u64) -> Result<TimeDelta, AppError> {
    let hours = cli_ttl.unwrap_or(config_ttl);
    let from_flag = cli_ttl.is_some();

    ttl_to_delta(hours).map_err(|err| match (err, from_flag) {
        (TtlError::Zero, true) => AppError::TokenOperation {
            reason: TTL_FLAG_MUST_BE_POSITIVE.into(),
        },
        (TtlError::OutOfRange, true) => AppError::TokenOperation {
            reason: TTL_FLAG_OUT_OF_RANGE.into(),
        },
        (TtlError::Zero, false) => AppError::Config {
            source: ConfigError::ValidationFailed {
                errors: vec![ERR_TTL_ZERO.into()],
            },
        },
        (TtlError::OutOfRange, false) => AppError::Config {
            source: ConfigError::ValidationFailed {
                errors: vec![TTL_OUT_OF_RANGE.into()],
            },
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
// Path resolution
// ---------------------------------------------------------------------------

/// Resolves a raw config path to an absolute, normalised form.
///
/// Expands a leading `~` via `shellexpand::tilde`, absolutises against
/// the current working directory via [`std::path::absolute`], and
/// performs logical `..` normalisation via `path_clean::clean`. The
/// target file is **not** required to exist (no `std::fs::canonicalize`)
/// — first-run setup writes through the resolved path before any file
/// exists on disk.
///
/// All three command entry points that accept `--config` (setup,
/// register, mcp-config) route through this helper, so the absolute
/// path threaded through to the shared MCP-config builder is
/// byte-identical across commands.
///
/// # Errors
///
/// Returns [`AppError::PathResolution`] when [`std::path::absolute`]
/// fails (typically a missing or inaccessible `current_dir`).
pub(crate) fn resolve_absolute_config_path(raw: &str) -> Result<PathBuf, AppError> {
    let expanded = shellexpand::tilde(raw);
    let absolute = std::path::absolute(Path::new(expanded.as_ref())).map_err(|source| {
        AppError::PathResolution {
            path: raw.to_owned(),
            source,
        }
    })?;
    Ok(path_clean::clean(absolute))
}

// ---------------------------------------------------------------------------
// Credentials persistence
// ---------------------------------------------------------------------------

/// Best-effort persistence of a bearer token via
/// [`write_credentials`]. On failure, writes a warning to `out` and
/// returns: persistence loss is recoverable since the token already
/// exists in the database and has been surfaced to the user.
///
/// Lives at the wrapper layer rather than inside `run_async` because
/// the credentials path resolves through `$XDG_CONFIG_HOME` — a
/// process-global the existing `run_async` convention deliberately
/// avoids.
pub fn persist_credentials(out: &mut dyn Write, token: &BearerToken) {
    let creds = Credentials::bearer(token.clone());
    let Err(err) = write_credentials(&creds) else {
        return;
    };
    let path = err
        .path()
        .map_or_else(|| "<unresolved>".to_owned(), |p| p.display().to_string());
    let _ = writeln!(
        out,
        "{CREDENTIALS_WRITE_FAILED_PREFIX}{path}: {err}{CREDENTIALS_WRITE_FAILED_SUFFIX}",
    );
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
        assert_eq!(ttl_to_delta(0).unwrap_err(), TtlError::Zero);
    }

    #[test]
    fn test_ttl_to_delta_rejects_overflow() {
        assert_eq!(ttl_to_delta(u64::MAX).unwrap_err(), TtlError::OutOfRange);
    }

    // -- TTL resolution -----------------------------------------------------

    #[test]
    fn test_resolve_ttl_uses_cli_value() {
        let delta = resolve_ttl(Some(24), 8760).unwrap();
        assert_eq!(delta, TimeDelta::try_hours(24).unwrap());
    }

    #[test]
    fn test_resolve_ttl_falls_back_to_config() {
        let delta = resolve_ttl(None, 8760).unwrap();
        assert_eq!(delta, TimeDelta::try_hours(8760).unwrap());
    }

    #[test]
    fn test_resolve_ttl_cli_zero_returns_token_error() {
        let err = resolve_ttl(Some(0), 8760).unwrap_err();
        assert!(
            err.to_string().contains(TTL_FLAG_MUST_BE_POSITIVE),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn test_resolve_ttl_cli_overflow_returns_token_error() {
        let err = resolve_ttl(Some(u64::MAX), 8760).unwrap_err();
        assert!(
            err.to_string().contains(TTL_FLAG_OUT_OF_RANGE),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn test_resolve_ttl_config_zero_returns_config_error() {
        let err = resolve_ttl(None, 0).unwrap_err();
        assert!(
            err.to_string().contains(ERR_TTL_ZERO),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn test_resolve_ttl_config_overflow_returns_config_error() {
        let err = resolve_ttl(None, u64::MAX).unwrap_err();
        assert!(
            err.to_string().contains(TTL_OUT_OF_RANGE),
            "unexpected error: {err}",
        );
    }

    // -- Path resolution ----------------------------------------------------

    #[test]
    fn test_resolve_absolute_config_path_normalises_dotdot() {
        let resolved = resolve_absolute_config_path("/foo/bar/../baz.yaml").unwrap();
        assert_eq!(resolved, PathBuf::from("/foo/baz.yaml"));
    }

    #[test]
    fn test_resolve_absolute_config_path_passes_through_clean_absolute() {
        let resolved = resolve_absolute_config_path("/etc/tribal/tribal.yaml").unwrap();
        assert_eq!(resolved, PathBuf::from("/etc/tribal/tribal.yaml"));
    }

    #[test]
    fn test_resolve_absolute_config_path_succeeds_for_nonexistent_target() {
        let resolved = resolve_absolute_config_path("/nonexistent/tribal/tribal.yaml").unwrap();
        assert_eq!(resolved, PathBuf::from("/nonexistent/tribal/tribal.yaml"),);
    }

    #[test]
    fn test_resolve_absolute_config_path_expands_tilde() {
        let resolved = resolve_absolute_config_path("~/.config/tribal/tribal.yaml").unwrap();
        let rendered = resolved.to_string_lossy();
        assert!(
            !rendered.contains('~'),
            "tilde should have been expanded, got {rendered}",
        );
        assert!(
            resolved.is_absolute(),
            "resolved path should be absolute, got {rendered}",
        );
    }
}
