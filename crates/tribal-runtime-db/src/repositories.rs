//! Repositories over the runtime database — the whole vocabulary of what the
//! managed runtime asks of the job plane.

mod run_job;
mod tenant_slot;

pub use run_job::{
    ClaimedJob, EnqueueOutcome, NewRunJob, PgRunJobRepository, PostRunningState, RunJobRepository,
    RunJobState, WriteOutcome,
};
pub use tenant_slot::{PgTenantSlotRepository, TenantSlot, TenantSlotRepository};
