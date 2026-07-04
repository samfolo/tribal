#![warn(clippy::pedantic)]
#![deny(warnings)]
//! The runtime database: the managed job plane a managed worker claims off.
//!
//! Holds `run_job` (the per-tenant work queue) and `tenant_slot` (the single-row
//! admission anchor). Platform-operated, account-scoped, never joined to the
//! ledger — an account reference here is an opaque cross-database pointer, not a
//! foreign key.

mod error;
mod pool;
mod repositories;

pub use error::RuntimeDbError;
pub use pool::create_pool;
pub use repositories::{
    ClaimedJob, EnqueueOutcome, NewRunJob, PgRunJobRepository, PgTenantSlotRepository,
    PostRunningState, RunJobRepository, RunJobState, TenantSlot, TenantSlotRepository,
    WriteOutcome,
};
pub use tribal_domain::RunJobId;

/// Compiled migrations for the runtime database schema, embedded at compile
/// time from `migrations/`.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();
