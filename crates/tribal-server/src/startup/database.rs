//! Database pool creation with exponential-backoff retry.

use tribal_config::DatabaseConfig;
use tribal_db::create_pool;

use crate::error::AppError;

use super::constants::POOL_RETRY_INITIAL_BACKOFF;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Creates a connection pool, retrying with exponential backoff on failure.
///
/// Backoff sequence: 1 s, 2 s, 4 s, 8 s, 16 s, … up to `max_attempts`.
/// Each failed attempt is logged as a warning.
pub(crate) async fn create_pool_with_retry(
    config: &DatabaseConfig,
    pool_name: &'static str,
    max_connections: u32,
    statement_timeout_ms: u64,
    max_attempts: u32,
) -> Result<sqlx::PgPool, AppError> {
    let mut backoff = POOL_RETRY_INITIAL_BACKOFF;

    for attempt in 1..=max_attempts {
        match create_pool(config, pool_name, max_connections, statement_timeout_ms).await {
            Ok(pool) => return Ok(pool),
            Err(source) => {
                if attempt == max_attempts {
                    return Err(AppError::PoolConnection {
                        pool_name,
                        attempts: max_attempts,
                        source,
                    });
                }
                tracing::warn!(
                    pool = pool_name,
                    attempt,
                    max_attempts,
                    backoff_secs = backoff.as_secs(),
                    %source,
                    "pool connection failed, retrying",
                );
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
        }
    }

    unreachable!("loop always returns or errors on final attempt")
}
