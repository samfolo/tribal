//! Shared utilities for CLI command implementations.

use std::path::{Path, PathBuf};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::TimeDelta;
use rand::RngExt;
use sqlx::{Postgres, pool::PoolConnection};
use tribal_config::{
    CREDENTIALS_WRITE_FAILED_PREFIX, CREDENTIALS_WRITE_FAILED_SUFFIX, CliOverrides, ConfigError,
    ConfigPath, Credentials, Diagnostics, MAX_TTL_HOURS, TribalConfig, ValidationError,
    load_config, validate, write_credentials,
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
// Config preparation
// ---------------------------------------------------------------------------

/// Loads and validates configuration.
///
/// Production sync wrappers and integration-test helpers both reach for
/// the same load-then-validate sequence before invoking `*_async`.
/// Centralising it here means a future addition to the prep phase lands
/// for both call paths at once.
///
/// Callers that intentionally skip validation call [`load_config`]
/// directly.
///
/// # Errors
///
/// Returns [`AppError::Config`] when figment merging or validation
/// fails.
pub fn prepare_config(
    config_path: &str,
    overrides: CliOverrides,
    database_defaults: &[(&str, &str)],
) -> Result<TribalConfig, AppError> {
    let config = load_config(config_path, Some(overrides), Some(database_defaults))?;
    validate(&config)?;
    Ok(config)
}

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
                diagnostics: Diagnostics::from(vec![ValidationError::must_be_positive(
                    ConfigPath::from_static("auth.token_ttl_hours"),
                )]),
            },
        },
        (TtlError::OutOfRange, false) => AppError::Config {
            source: ConfigError::ValidationFailed {
                diagnostics: Diagnostics::from(vec![ValidationError::AboveMax {
                    field: ConfigPath::from_static("auth.token_ttl_hours"),
                    value: hours,
                    limit: MAX_TTL_HOURS,
                }]),
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

/// Outcome of attempting to persist a bearer token to credentials.json.
///
/// `Failed` carries a pre-formatted warn-and-success literal so each
/// caller can route the warning to the appropriate stream.
#[derive(Debug)]
pub enum CredentialsPersistOutcome {
    /// Credentials successfully written to the resolved path.
    Persisted { path: PathBuf },
    /// Write failed. `warning` is the pre-formatted warn-and-success
    /// literal; the caller emits it to its preferred stream.
    Failed { warning: String },
}

/// Best-effort persistence of `token` via [`write_credentials`].
///
/// Callers invoke this immediately after the token row is inserted and
/// **before** any fallible post-insert output. credentials.json is then
/// the durable recovery artefact if a later writeln fails. The returned
/// `Failed` warning is emitted by the caller on its preferred stream
/// under the warn-and-success rule.
pub fn persist_credentials(token: &BearerToken) -> CredentialsPersistOutcome {
    let creds = Credentials::bearer(token.clone());
    match write_credentials(&creds) {
        Ok(path) => CredentialsPersistOutcome::Persisted { path },
        Err(err) => {
            let warning =
                format!("{CREDENTIALS_WRITE_FAILED_PREFIX}{err}{CREDENTIALS_WRITE_FAILED_SUFFIX}");
            CredentialsPersistOutcome::Failed { warning }
        }
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
        assert!(matches!(
            &err,
            AppError::Config {
                source: ConfigError::ValidationFailed { diagnostics },
            } if diagnostics.iter().any(|d| matches!(
                d,
                ValidationError::BelowMin { field, min: 1, .. }
                    if field.as_str() == "auth.token_ttl_hours",
            )),
        ));
    }

    #[test]
    fn test_resolve_ttl_config_overflow_returns_config_error() {
        let err = resolve_ttl(None, u64::MAX).unwrap_err();
        assert!(matches!(
            &err,
            AppError::Config {
                source: ConfigError::ValidationFailed { diagnostics },
            } if diagnostics.iter().any(|d| matches!(
                d,
                ValidationError::AboveMax { field, value, limit }
                    if field.as_str() == "auth.token_ttl_hours"
                        && *value == u64::MAX
                        && *limit == MAX_TTL_HOURS,
            )),
        ));
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
