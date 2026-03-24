//! Core revoke flow: entry point and async orchestration.

use chrono::Utc;
use tribal_common::SHA256_HEX_LENGTH;
use tribal_config::{DatabaseConfig, load_config};
use tribal_db::{AuthTokenRepository, PgAuthTokenRepository};

use super::output;
use crate::{
    cli::TokenRevokeArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
    },
    error::AppError,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Pool name for the revoke connection.
const POOL_NAME: &str = "token-revoke";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal token revoke` flow.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, database connection,
/// prefix resolution, or revocation fails.
pub(crate) fn run(config_path: &str, mut args: TokenRevokeArgs) -> Result<(), AppError> {
    let prefix = std::mem::take(&mut args.prefix);
    let cli_overrides = args.into_cli_overrides();
    let config = load_config(
        config_path,
        Some(cli_overrides),
        Some(&DATABASE_COMMAND_DEFAULTS),
    )?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    rt.block_on(run_async(&config.database, &prefix))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Validates that a hash prefix is non-empty lowercase hexadecimal
/// and no longer than a full SHA-256 hex hash (64 chars).
fn validate_hex_prefix(prefix: &str) -> Result<(), AppError> {
    if prefix.is_empty()
        || prefix.len() > SHA256_HEX_LENGTH
        || !prefix
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(AppError::TokenOperation {
            reason: format!("{}: '{prefix}'", output::INVALID_PREFIX),
        });
    }
    Ok(())
}

/// Resolves the prefix to a unique token and revokes it.
async fn run_async(db_config: &DatabaseConfig, prefix: &str) -> Result<(), AppError> {
    validate_hex_prefix(prefix)?;

    let pool = tribal_db::create_pool(
        db_config,
        POOL_NAME,
        COMMAND_POOL_MAX_CONNECTIONS,
        COMMAND_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| AppError::Database { source })?;

    let mut conn = pool
        .acquire()
        .await
        .map_err(|err| AppError::pool_acquire(POOL_NAME, "acquiring revoke connection", err))?;

    let matches = PgAuthTokenRepository
        .find_by_hash_prefix(&mut conn, prefix)
        .await
        .map_err(|source| AppError::Database { source })?;

    let token = match matches.len() {
        0 => {
            return Err(AppError::TokenOperation {
                reason: format!("{}: '{prefix}'", output::NO_MATCHING_TOKEN),
            });
        }
        1 => &matches[0],
        _ => {
            return Err(AppError::TokenOperation {
                reason: format!("{}: '{prefix}'", output::AMBIGUOUS_PREFIX),
            });
        }
    };

    if token.revoked_at().is_some() {
        output::token_already_revoked(prefix);
        return Ok(());
    }

    PgAuthTokenRepository
        .revoke(&mut conn, token.id(), Utc::now())
        .await
        .map_err(|source| AppError::Database { source })?;

    output::token_revoked(prefix);

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_hex_prefix_accepts_valid() {
        assert!(validate_hex_prefix("abc12345").is_ok());
        assert!(validate_hex_prefix("0").is_ok());
        assert!(validate_hex_prefix("deadbeef").is_ok());
    }

    #[test]
    fn test_validate_hex_prefix_rejects_empty() {
        let err = validate_hex_prefix("").unwrap_err();
        assert!(
            err.to_string().contains(output::INVALID_PREFIX),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn test_validate_hex_prefix_rejects_uppercase() {
        let err = validate_hex_prefix("ABC12345").unwrap_err();
        assert!(
            err.to_string().contains(output::INVALID_PREFIX),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn test_validate_hex_prefix_rejects_like_wildcards() {
        let err = validate_hex_prefix("abc%").unwrap_err();
        assert!(
            err.to_string().contains(output::INVALID_PREFIX),
            "unexpected error: {err}",
        );

        let err = validate_hex_prefix("abc_def").unwrap_err();
        assert!(
            err.to_string().contains(output::INVALID_PREFIX),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn test_validate_hex_prefix_rejects_non_hex() {
        let err = validate_hex_prefix("xyz123").unwrap_err();
        assert!(
            err.to_string().contains(output::INVALID_PREFIX),
            "unexpected error: {err}",
        );
    }
}
