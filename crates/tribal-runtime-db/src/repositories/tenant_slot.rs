//! The tenant-slot repository: the single-row admission anchor and its cap.
//!
//! Uses raw `sqlx::query` for consistency with the admission claim, which
//! manipulates these rows inside its own atomic transaction (see [`run_job`]).
//! The `running` count is owned by admission and job-exit accounting there and
//! is never written from here — this repository owns the cap and reads.
//!
//! [`run_job`]: super::run_job

use async_trait::async_trait;
use sqlx::{PgConnection, Row};

use crate::RuntimeDbError;

/// One tenant's admission slot: the live run count and the plan's ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantSlot {
    /// Jobs currently in the running state for this tenant.
    pub running: i32,
    /// The concurrency ceiling admission enforces.
    pub cap: i32,
}

/// Cap-sync writes and reads over `tenant_slot`.
#[async_trait]
pub trait TenantSlotRepository {
    /// Applies a cap-sync push: sets the tenant's cap, creating the row at a
    /// zero run count when it is absent so a later plan change is not lost.
    async fn apply_cap_sync(
        &self,
        conn: &mut PgConnection,
        account_id: &str,
        cap: i32,
    ) -> Result<(), RuntimeDbError>;

    /// Reads a tenant's slot, if the row exists.
    async fn get(
        &self,
        conn: &mut PgConnection,
        account_id: &str,
    ) -> Result<Option<TenantSlot>, RuntimeDbError>;
}

/// Postgres implementation of [`TenantSlotRepository`].
pub struct PgTenantSlotRepository;

#[async_trait]
impl TenantSlotRepository for PgTenantSlotRepository {
    async fn apply_cap_sync(
        &self,
        conn: &mut PgConnection,
        account_id: &str,
        cap: i32,
    ) -> Result<(), RuntimeDbError> {
        sqlx::query(
            "INSERT INTO tenant_slot (account_id, running, cap) VALUES ($1, 0, $2)
             ON CONFLICT (account_id) DO UPDATE SET cap = EXCLUDED.cap",
        )
        .bind(account_id)
        .bind(cap)
        .execute(&mut *conn)
        .await
        .map_err(|source| RuntimeDbError::QueryFailed {
            context: "applying a cap sync".to_owned(),
            source,
        })?;
        Ok(())
    }

    async fn get(
        &self,
        conn: &mut PgConnection,
        account_id: &str,
    ) -> Result<Option<TenantSlot>, RuntimeDbError> {
        let row = sqlx::query("SELECT running, cap FROM tenant_slot WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|source| RuntimeDbError::QueryFailed {
                context: "reading a tenant slot".to_owned(),
                source,
            })?;
        Ok(row.map(|row| TenantSlot {
            running: row.get("running"),
            cap: row.get("cap"),
        }))
    }
}
