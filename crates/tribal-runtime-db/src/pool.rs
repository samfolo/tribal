//! Connection pool creation for the runtime database.

use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::RuntimeDbError;

/// Creates a connection pool for the runtime database at `url`.
///
/// # Errors
///
/// Returns [`RuntimeDbError::QueryFailed`] if the initial connection fails.
pub async fn create_pool(url: &str, max_connections: u32) -> Result<PgPool, RuntimeDbError> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "connecting the runtime-db pool".to_owned(),
            source,
        })
}
