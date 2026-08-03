//! One-shot creation of a named database over an administrative connection.
//!
//! `CREATE DATABASE` cannot run inside a transaction, so the session-level
//! [`advisory_locks::DATABASE_PROVISION`] lock — not a transaction — arbitrates
//! racing creators, and the duplicate-name SQLSTATE is the backstop for a race
//! that beats the lock. Both statements ride the simple query protocol, which
//! Postgres requires for `CREATE DATABASE`.

use std::num::NonZeroU64;

use async_trait::async_trait;
use sqlx::{Executor, PgConnection};

use super::advisory_lock::PgAdvisoryLockRepository;
use crate::{DbError, advisory_locks, repositories::advisory_lock::AdvisoryLockRepository as _};

/// SQLSTATE for `duplicate_database`: the name already existed when the
/// create ran.
const DUPLICATE_DATABASE: &str = "42P04";
/// SQLSTATE for `unique_violation`: two creates raced past the existence
/// check together and one lost on `pg_database`'s own name index. Postgres
/// reports the loser this way, not as `duplicate_database`, so the backstop
/// the lock exists to make unnecessary must recognise both.
const UNIQUE_VIOLATION: &str = "23505";

/// What creating a database found. An earlier creation and a lost race are
/// indistinguishable by design: either way the database exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseCreation {
    Created,
    AlreadyPresent,
}

/// Creates one named database on the caller's administrative connection.
#[async_trait]
pub trait DatabaseProvisionRepository {
    /// Creates `database` when absent, serialised under the provisioning
    /// advisory lock, with the whole session bounded by
    /// `statement_timeout_ms` so a held lock cancels rather than waits
    /// unboundedly. The name's validity is the wire boundary's contract;
    /// this method quotes it as an identifier regardless.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] when the timeout cannot be set, the
    /// lock wait is cancelled by the statement timeout, or creation fails
    /// for any reason other than the database already existing.
    async fn create_database_if_absent(
        &self,
        conn: &mut PgConnection,
        database: &str,
        statement_timeout_ms: NonZeroU64,
    ) -> Result<DatabaseCreation, DbError>;
}

/// Postgres implementation of [`DatabaseProvisionRepository`].
pub struct PgDatabaseProvisionRepository;

#[async_trait]
impl DatabaseProvisionRepository for PgDatabaseProvisionRepository {
    async fn create_database_if_absent(
        &self,
        conn: &mut PgConnection,
        database: &str,
        statement_timeout_ms: NonZeroU64,
    ) -> Result<DatabaseCreation, DbError> {
        // Session-level SET: the transaction-scoped SET LOCAL has no effect
        // here, because nothing below may run inside a transaction.
        conn.execute(format!("SET statement_timeout = {}", statement_timeout_ms.get()).as_str())
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "bounding the provisioning session".into(),
                source,
            })?;

        PgAdvisoryLockRepository
            .acquire_exclusive(&mut *conn, advisory_locks::DATABASE_PROVISION)
            .await?;
        let creation = create_when_absent(&mut *conn, database).await;
        // Release is best-effort: the lock dies with this session anyway.
        let _ = PgAdvisoryLockRepository
            .release_exclusive(&mut *conn, advisory_locks::DATABASE_PROVISION)
            .await;
        creation
    }
}

/// Checks for the database and creates it when absent, mapping the
/// duplicate-name race to [`DatabaseCreation::AlreadyPresent`].
///
/// The existence check is not the outcome's authority — the `42P04` mapping
/// is, and decides alone when the check loses a race. It runs because
/// `CREATE DATABASE` has no `IF NOT EXISTS`: without it every repeated
/// provision writes an error to the server log.
async fn create_when_absent(
    conn: &mut PgConnection,
    database: &str,
) -> Result<DatabaseCreation, DbError> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(database)
            .fetch_one(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "checking for an existing database".into(),
                source,
            })?;
    if exists {
        return Ok(DatabaseCreation::AlreadyPresent);
    }

    let quoted = database.replace('"', "\"\"");
    match conn
        .execute(format!("CREATE DATABASE \"{quoted}\"").as_str())
        .await
    {
        Ok(_) => Ok(DatabaseCreation::Created),
        Err(error) if is_duplicate_database(&error) => Ok(DatabaseCreation::AlreadyPresent),
        Err(source) => Err(DbError::QueryFailed {
            context: "creating the requested database".into(),
            source,
        }),
    }
}

fn is_duplicate_database(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| is_duplicate_code(code.as_ref()))
}

fn is_duplicate_code(code: &str) -> bool {
    matches!(code, DUPLICATE_DATABASE | UNIQUE_VIOLATION)
}

#[cfg(test)]
mod tests {
    use super::is_duplicate_code;

    #[test]
    fn both_postgres_duplicate_codes_are_idempotent_outcomes() {
        assert!(is_duplicate_code("42P04"));
        assert!(is_duplicate_code("23505"));
        assert!(!is_duplicate_code("42501"));
    }
}
