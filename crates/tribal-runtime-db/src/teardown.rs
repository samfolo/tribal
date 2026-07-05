//! Cancel and settling teardown: a run reaches a terminal state only once its
//! money is resolved.
//!
//! A run learns where its holds stand through the gateway's holds report: a
//! dispatched hold settles, an undispatched one releases, and the run reaches
//! `done`/`cancelled` only once the report shows neither `active` nor
//! `settle_pending`. Every outstanding position key is then acked — the signal
//! the response store's drop rule keys on — and the ack is fenced behind the
//! run's own terminal commit, so a crash before the run is durably terminal
//! never drops an output a resume would need.
//!
//! The split itself — settle the dispatched, release the undispatched — is the
//! gateway's; the run observes it converge through the report and never settles
//! or releases money itself.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_domain::RunJobId;
use tribal_wire::gateway::{HoldStatus, HoldsReport, PositionKey};
use uuid::Uuid;

use crate::{PgRunJobRepository, PostRunningState, RunJobRepository, RuntimeDbError, WriteOutcome};

/// The gateway crossings a run's teardown drives: reading where its holds stand,
/// and acking a settled position key so the response store may drop it.
#[async_trait]
pub trait MeteringGateway {
    /// The holds report for a run, by run key.
    async fn holds_report(&self, run_key: &str) -> Result<HoldsReport, TeardownError>;

    /// Acknowledge a position key. Idempotent, so a re-presented ack — after a
    /// crash mid-teardown — is a no-op.
    async fn acknowledge(&self, position_key: &PositionKey) -> Result<(), TeardownError>;
}

/// A failure during teardown: a runtime-database write, or a gateway crossing.
#[derive(Debug, thiserror::Error)]
pub enum TeardownError {
    /// A runtime-database write failed.
    #[error(transparent)]
    Db(#[from] RuntimeDbError),
    /// A gateway crossing failed.
    #[error("teardown gateway crossing failed [{context}]")]
    Gateway {
        /// What the teardown was doing when the crossing failed.
        context: String,
    },
}

/// The run a teardown resolves.
pub struct TeardownTarget {
    /// The job whose terminal the teardown commits.
    pub id: RunJobId,
    /// The claim token the terminal commit fences on — a reclaim's stale run
    /// cannot tear the job down.
    pub claim_token: Uuid,
    /// The run key scoping the holds report.
    pub run_key: String,
    /// Every position key the run opened, acked once it is durably terminal.
    pub position_keys: Vec<PositionKey>,
    /// The terminal the run settles into: `Cancelled` on a cancel, `Done` on a
    /// clean finish.
    pub terminal: PostRunningState,
}

/// How far a poll loop reads the holds report before giving up on convergence.
#[derive(Debug, Clone, Copy)]
pub struct PollBudget {
    /// The delay between reads.
    pub interval: Duration,
    /// The most reads to take before returning [`TeardownOutcome::HoldsStillLive`].
    pub max_reads: u32,
}

/// What a teardown attempt concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownOutcome {
    /// Holds resolved, the run committed to its terminal, and every key acked.
    ToreDown,
    /// A hold stayed `active` or `settle_pending` through the whole budget: the
    /// run is held short of terminal, nothing acked, for a later attempt.
    HoldsStillLive,
    /// The terminal commit's claim-token fence missed — a reclaim owns the job.
    /// Nothing was acked.
    OwnershipLost,
}

/// Tears a run down to its terminal state: polls the holds report until no hold
/// is `active` or `settle_pending`, commits the terminal under the claim-token
/// fence, then acks every outstanding position key.
///
/// The invariants: the terminal is never committed while a hold is unresolved
/// (a live hold returns [`TeardownOutcome::HoldsStillLive`]), and no key is acked
/// unless the terminal commit applied (the ack is fenced behind it).
///
/// # Errors
///
/// Returns [`TeardownError`] on a database write failure or a gateway crossing
/// failure.
pub async fn cancel_teardown(
    conn: &mut PgConnection,
    gateway: &impl MeteringGateway,
    target: &TeardownTarget,
    budget: &PollBudget,
) -> Result<TeardownOutcome, TeardownError> {
    if !poll_until_resolved(gateway, &target.run_key, budget).await? {
        return Ok(TeardownOutcome::HoldsStillLive);
    }

    // The terminal commit under the claim-token fence releases the tenant slot in
    // the same transaction (`leave_running`), so no slot leaks past teardown.
    let committed = PgRunJobRepository
        .leave_running(conn, target.id, target.claim_token, target.terminal)
        .await?;
    if committed != WriteOutcome::Applied {
        return Ok(TeardownOutcome::OwnershipLost);
    }

    // Fenced ack: only now that the run is durably terminal does the response
    // store learn it may drop each output.
    for position_key in &target.position_keys {
        gateway.acknowledge(position_key).await?;
    }
    Ok(TeardownOutcome::ToreDown)
}

/// Reads the holds report until no hold is `active` or `settle_pending`, or the
/// budget is spent. Returns whether the holds resolved.
async fn poll_until_resolved(
    gateway: &impl MeteringGateway,
    run_key: &str,
    budget: &PollBudget,
) -> Result<bool, TeardownError> {
    for read in 0..budget.max_reads {
        if read > 0 {
            tokio::time::sleep(budget.interval).await;
        }
        let report = gateway.holds_report(run_key).await?;
        if resolved(&report) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether a report holds no unresolved money — no hold `active` or
/// `settle_pending`.
fn resolved(report: &HoldsReport) -> bool {
    !report
        .holds
        .iter()
        .any(|hold| matches!(hold.status, HoldStatus::Active | HoldStatus::SettlePending))
}
