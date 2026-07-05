//! Account-scoped erasure over the runtime database.

use sqlx::{Connection, PgConnection};

use crate::RuntimeDbError;

/// Deletes every runtime-database row for an account — its jobs and its
/// admission slot — in one transaction. This is the erase-reachability the
/// runtime database guarantees: an account's content leaves no residue. Returns
/// the number of jobs removed.
///
/// # Errors
///
/// Returns [`RuntimeDbError::QueryFailed`] if a delete fails.
pub async fn purge_account(
    conn: &mut PgConnection,
    account_id: &str,
) -> Result<u64, RuntimeDbError> {
    let mut txn = conn
        .begin()
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "opening the purge transaction".to_owned(),
            source,
        })?;

    let jobs = sqlx::query("DELETE FROM run_job WHERE account_id = $1")
        .bind(account_id)
        .execute(&mut *txn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "purging an account's jobs".to_owned(),
            source,
        })?
        .rows_affected();

    sqlx::query("DELETE FROM tenant_slot WHERE account_id = $1")
        .bind(account_id)
        .execute(&mut *txn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "purging an account's slot".to_owned(),
            source,
        })?;

    txn.commit()
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "committing an account purge".to_owned(),
            source,
        })?;

    Ok(jobs)
}
