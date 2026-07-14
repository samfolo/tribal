//! Shared utilities for CLI command implementations.

use chrono::{DateTime, TimeDelta, Utc};
use sqlx::PgConnection;
use tribal_config::{ConfigError, ConfigPath, Diagnostics, MAX_TTL_HOURS, ValidationError};
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
// TTL conversion
// ---------------------------------------------------------------------------

/// Error message when a CLI `--ttl` flag is zero.
pub(crate) const TTL_FLAG_MUST_BE_POSITIVE: &str = "--ttl must be greater than zero";

/// Error message when a CLI `--ttl` flag exceeds the representable range.
pub(crate) const TTL_FLAG_OUT_OF_RANGE: &str = "--ttl value is too large";

/// Error message when a CLI `--ttl` flag fits a [`TimeDelta`] but cannot be
/// added to the current instant without overflowing [`DateTime<Utc>`].
pub(crate) const TTL_FLAG_OVERFLOWS_EXPIRY: &str =
    "--ttl too large to construct an expiry date from now";

/// Error message when `auth.token_ttl_hours` fits a [`TimeDelta`] but cannot
/// be added to the current instant without overflowing [`DateTime<Utc>`].
pub(crate) const TTL_CONFIG_OVERFLOWS_EXPIRY: &str =
    "auth.token_ttl_hours too large to construct an expiry date from now";

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

/// A TTL value paired with its input source.
///
/// Constructed once at the CLI/config boundary via [`Self::from_pair`];
/// the variant identity then drives every downstream error-attribution
/// decision (`resolve_ttl`, `compute_expires_at`) through exhaustive
/// `match` rather than a re-derived `cli_ttl.is_some()` boolean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TtlInput {
    /// Value supplied by the `--ttl` CLI flag.
    CliFlag { hours: u64 },
    /// Value supplied by `auth.token_ttl_hours` in the config.
    Config { hours: u64 },
}

impl TtlInput {
    /// Selects between the CLI flag and the config fallback, preserving
    /// the source attribution.
    pub(crate) fn from_pair(cli_ttl: Option<u64>, config_ttl: u64) -> Self {
        if let Some(hours) = cli_ttl {
            Self::CliFlag { hours }
        } else {
            Self::Config { hours: config_ttl }
        }
    }

    /// The selected TTL value in hours.
    fn hours(self) -> u64 {
        match self {
            Self::CliFlag { hours } | Self::Config { hours } => hours,
        }
    }
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

/// Resolves a [`TtlInput`] to a [`TimeDelta`], dispatching the typed
/// [`TtlError`] from [`ttl_to_delta`] to the right [`AppError`] and
/// message based on which input supplied the value.
///
/// The mapping is exhaustive over (variant, source) so adding a new
/// [`TtlError`] variant or [`TtlInput`] variant forces an update at
/// this single site rather than silently falling through to one of the
/// existing messages.
///
/// # Errors
///
/// Returns [`AppError::TokenOperation`] for invalid CLI flag values and
/// [`AppError::Config`] for invalid config values.
fn resolve_ttl(input: TtlInput) -> Result<TimeDelta, AppError> {
    ttl_to_delta(input.hours()).map_err(|err| match (err, input) {
        (TtlError::Zero, TtlInput::CliFlag { .. }) => AppError::TokenOperation {
            reason: TTL_FLAG_MUST_BE_POSITIVE.into(),
        },
        (TtlError::OutOfRange, TtlInput::CliFlag { .. }) => AppError::TokenOperation {
            reason: TTL_FLAG_OUT_OF_RANGE.into(),
        },
        (TtlError::Zero, TtlInput::Config { .. }) => AppError::Config {
            source: ConfigError::ValidationFailed {
                diagnostics: Diagnostics::from(vec![ValidationError::must_be_positive(
                    ConfigPath::from_static("auth.token_ttl_hours"),
                )]),
            },
        },
        (TtlError::OutOfRange, TtlInput::Config { hours }) => AppError::Config {
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

/// Resolves a TTL into an absolute expiry instant.
///
/// Wraps [`resolve_ttl`] with a checked `DateTime + TimeDelta` addition
/// so that a TTL which is representable as a [`TimeDelta`] but too far
/// from now to fit in [`DateTime<Utc>`] surfaces as a typed error
/// instead of panicking inside `chrono::Add`.
///
/// # Errors
///
/// Returns whatever [`resolve_ttl`] returns when the TTL fails its own
/// invariants, or [`AppError::TokenOperation`] when the addition
/// overflows.  The overflow message names whichever input supplied the
/// value (CLI flag vs config field).
pub(crate) fn compute_expires_at(input: TtlInput) -> Result<DateTime<Utc>, AppError> {
    let delta = resolve_ttl(input)?;
    Utc::now()
        .checked_add_signed(delta)
        .ok_or_else(|| AppError::TokenOperation {
            reason: match input {
                TtlInput::CliFlag { .. } => TTL_FLAG_OVERFLOWS_EXPIRY.into(),
                TtlInput::Config { .. } => TTL_CONFIG_OVERFLOWS_EXPIRY.into(),
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
    conn: &mut PgConnection,
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
