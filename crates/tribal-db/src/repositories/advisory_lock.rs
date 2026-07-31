//! Advisory locks, in both Postgres scopes.
//!
//! The `_xact` methods are transaction-scoped: Postgres releases them
//! automatically when the surrounding transaction commits or rolls back, so a
//! crashed worker never strands one. They must be acquired inside a
//! transaction; called outside one, each statement is its own transaction and
//! the lock releases immediately. The reindex cutover uses the blocking pair
//! as a drain barrier: every embedding-writing commit takes the shared lock,
//! and the cutover takes the exclusive lock, whose acquisition blocks until
//! every in-flight writer has committed.
//!
//! The session-level methods bind the lock to the connection itself — held
//! across transactions until released or the session dies — the scope work
//! that spans transactions needs: the migration runner's serialisation and
//! admission, and the storage transition's custody of its source and
//! candidate.

use async_trait::async_trait;
use sha2::{Digest as _, Sha256};
use sqlx::PgConnection;

use crate::{advisory_locks::CREDENTIAL_REPLACEMENT, error::DbError};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Advisory lock operations, transaction- and session-scoped.
///
/// Every method takes `&mut PgConnection` as an explicit executor, keeping the
/// repository pool-agnostic. A `_xact` method expects an open transaction so
/// the lock is held for a meaningful span; a session method binds the lock to
/// the connection itself, which the caller must keep alive for as long as the
/// lock matters.
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

    /// Attempts to acquire the shared advisory lock `lock_id` without
    /// blocking. Returns `true` if granted, `false` if an exclusive holder
    /// exists. Released automatically when the transaction ends.
    async fn try_acquire_shared_xact(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError>;

    /// Attempts to acquire the session-level exclusive advisory lock
    /// `lock_id` without blocking. The lock binds to the connection and is
    /// held across transactions until released or the session dies.
    async fn try_acquire_exclusive(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError>;

    /// Releases the session-level exclusive advisory lock `lock_id`.
    /// Returns `false` if this session did not hold it.
    async fn release_exclusive(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError>;

    /// Attempts to acquire the session-level shared advisory lock `lock_id`
    /// without blocking. Any number of sessions may share it; it is refused
    /// only while an exclusive holder exists.
    async fn try_acquire_shared(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError>;

    /// Releases the session-level shared advisory lock `lock_id`.
    /// Returns `false` if this session did not hold it.
    async fn release_shared(&self, conn: &mut PgConnection, lock_id: i64) -> Result<bool, DbError>;

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

    async fn try_acquire_shared_xact(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError> {
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock_shared($1)")
            .bind(lock_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "try-acquire shared xact advisory lock".into(),
                source,
            })?;
        Ok(acquired)
    }

    async fn try_acquire_exclusive(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError> {
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(lock_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "try-acquire exclusive session advisory lock".into(),
                source,
            })?;
        Ok(acquired)
    }

    async fn release_exclusive(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError> {
        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(lock_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "release exclusive session advisory lock".into(),
                source,
            })?;
        Ok(released)
    }

    async fn try_acquire_shared(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError> {
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock_shared($1)")
            .bind(lock_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "try-acquire shared session advisory lock".into(),
                source,
            })?;
        Ok(acquired)
    }

    async fn release_shared(&self, conn: &mut PgConnection, lock_id: i64) -> Result<bool, DbError> {
        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock_shared($1)")
            .bind(lock_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "release shared session advisory lock".into(),
                source,
            })?;
        Ok(released)
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
    digest.update(CREDENTIAL_REPLACEMENT.to_be_bytes());
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
