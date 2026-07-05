//! The plane's control crossings: the wire DTOs the control plane pushes — a job
//! enqueue, a cap sync — turned into runtime-database writes, and the run-scoped
//! grant minted at claim.
//!
//! The account never rides these DTOs; the gateway derives it from the
//! presenting credential. So the enqueue takes the derived account alongside the
//! wire job, the cap sync names its own tenant, and the grant is minted from the
//! claimed job's account.

use sqlx::PgConnection;
use tribal_wire::gateway::{AccountReference, CapSync, GrantSet, JobEnqueue, JobKind};

use crate::{
    ClaimedJob, EnqueueOutcome, NewRunJob, PgRunJobRepository, PgTenantSlotRepository,
    RunJobRepository, RuntimeDbError, TenantSlotRepository,
};

/// Consumes a `JobEnqueue` crossing into a queued `run_job`, under the account
/// the gateway derived from the presenting credential. Dedups on the wire's
/// idempotency key, per [`RunJobRepository::enqueue`].
///
/// # Errors
///
/// Returns [`RuntimeDbError`] when the enqueue write fails.
pub async fn enqueue_job(
    conn: &mut PgConnection,
    account_id: &str,
    job: JobEnqueue,
) -> Result<EnqueueOutcome, RuntimeDbError> {
    PgRunJobRepository
        .enqueue(
            conn,
            NewRunJob {
                account_id: account_id.to_owned(),
                kind: kind_str(job.kind).to_owned(),
                payload: job.payload,
                idempotency_key: job.idempotency_key,
                priority: job.priority,
            },
        )
        .await
}

/// Consumes a `CapSync` crossing into the tenant's admission slot, so the next
/// claim admits under the new ceiling.
///
/// # Errors
///
/// Returns [`RuntimeDbError`] when the cap-sync write fails.
pub async fn sync_cap(conn: &mut PgConnection, sync: &CapSync) -> Result<(), RuntimeDbError> {
    PgTenantSlotRepository
        .apply_cap_sync(conn, sync.account.as_str(), cap_to_i32(sync.cap))
        .await
}

/// Mints the run-scoped grant a claimed job's bracket calls present: scoped to
/// the job's account, with no initiating principal — a managed run is
/// account-level automation — and no pre-granted tools.
#[must_use]
pub fn mint_grant(claimed: &ClaimedJob) -> GrantSet {
    GrantSet {
        account: AccountReference::new(claimed.account_id.as_str()),
        principal: None,
        tools: Vec::new(),
    }
}

/// The `run_job.kind` string a wire [`JobKind`] maps to — the values the schema's
/// `kind` check constrains.
fn kind_str(kind: JobKind) -> &'static str {
    match kind {
        JobKind::Consolidate => "consolidate",
        JobKind::Cron => "cron",
        JobKind::Probe => "probe",
    }
}

/// The wire cap is an unsigned count; the slot column is a signed `i32`. A cap
/// beyond `i32::MAX` is no real plan ceiling, so it saturates rather than
/// wrapping negative.
fn cap_to_i32(cap: u32) -> i32 {
    i32::try_from(cap).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_every_job_kind_maps_to_a_schema_kind() {
        assert_eq!(kind_str(JobKind::Consolidate), "consolidate");
        assert_eq!(kind_str(JobKind::Cron), "cron");
        assert_eq!(kind_str(JobKind::Probe), "probe");
    }

    #[test]
    fn test_a_cap_beyond_the_column_saturates_rather_than_wrapping() {
        assert_eq!(cap_to_i32(0), 0);
        assert_eq!(cap_to_i32(8), 8);
        assert_eq!(cap_to_i32(u32::MAX), i32::MAX);
    }
}
