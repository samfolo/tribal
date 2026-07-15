//! Transaction-scoped advisory locks for the reindex cutover protocol.
//!
//! Unlike the session-level [`MigrationRepository`](super::MigrationRepository)
//! locks, these are `xact`-scoped: Postgres releases them automatically when the
//! surrounding transaction commits or rolls back, so a crashed worker never
//! strands one. They must be acquired inside a transaction; called outside one,
//! each statement is its own transaction and the lock releases immediately.
//!
//! The cutover uses them as a drain barrier: every embedding-writing commit
//! takes the shared lock, and the cutover takes the exclusive lock, whose
//! acquisition blocks until every in-flight writer has committed.

use async_trait::async_trait;
use sha2::{Digest as _, Sha256};
use sqlx::PgConnection;

use crate::error::DbError;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Transaction-scoped advisory lock operations.
///
/// Every method takes `&mut PgConnection` as an explicit executor, keeping the
/// repository pool-agnostic; the connection is expected to be inside an open
/// transaction so the lock is held for a meaningful span.
#[async_trait]
pub trait AdvisoryLockRepository {
    /// Acquires the shared (read) advisory lock `lock_id`, blocking until it is
    /// granted. Released automatically when the transaction ends.
    ///
    /// Any number of holders may share the lock concurrently; it blocks only
    /// against an exclusive acquisition.
    async fn acquire_shared_xact(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<(), DbError>;

    /// Acquires the exclusive (write) advisory lock `lock_id`, blocking until
    /// every shared and exclusive holder has released. Released automatically
    /// when the transaction ends.
    async fn acquire_exclusive_xact(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<(), DbError>;

    /// Attempts to acquire the exclusive advisory lock `lock_id` without
    /// blocking. Returns `true` if granted, `false` if another session holds it
    /// (shared or exclusive). Released automatically when the transaction ends.
    async fn try_acquire_exclusive_xact(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError>;

    /// Acquires the exclusive credential-replacement lock for one namespace.
    async fn acquire_credential_replacement_xact(
        &self,
        conn: &mut PgConnection,
        authority_namespace: &str,
    ) -> Result<(), DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`AdvisoryLockRepository`].
pub struct PgAdvisoryLockRepository;

#[async_trait]
impl AdvisoryLockRepository for PgAdvisoryLockRepository {
    async fn acquire_shared_xact(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<(), DbError> {
        sqlx::query("SELECT pg_advisory_xact_lock_shared($1)")
            .bind(lock_id)
            .execute(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "acquire shared xact advisory lock".into(),
                source,
            })?;
        Ok(())
    }

    async fn acquire_exclusive_xact(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<(), DbError> {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_id)
            .execute(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "acquire exclusive xact advisory lock".into(),
                source,
            })?;
        Ok(())
    }

    async fn try_acquire_exclusive_xact(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError> {
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
            .bind(lock_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "try-acquire exclusive xact advisory lock".into(),
                source,
            })?;
        Ok(acquired)
    }

    async fn acquire_credential_replacement_xact(
        &self,
        conn: &mut PgConnection,
        authority_namespace: &str,
    ) -> Result<(), DbError> {
        let lock_id = credential_replacement_lock_id(authority_namespace);
        self.acquire_exclusive_xact(conn, lock_id).await
    }
}

fn credential_replacement_lock_id(authority_namespace: &str) -> i64 {
    let mut digest = Sha256::new();
    digest.update(crate::advisory_locks::CREDENTIAL_REPLACEMENT.to_be_bytes());
    digest.update(authority_namespace.as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 8];
    for (target, source) in bytes.iter_mut().zip(digest.iter().copied()) {
        *target = source;
    }
    i64::from_be_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::credential_replacement_lock_id;

    #[test]
    fn credential_replacement_lock_is_stable_and_namespace_scoped() {
        let first = credential_replacement_lock_id("0123456789abcdef01234567");
        assert_eq!(
            first,
            credential_replacement_lock_id("0123456789abcdef01234567")
        );
        assert_ne!(
            first,
            credential_replacement_lock_id("fedcba9876543210fedcba98")
        );
    }
}
