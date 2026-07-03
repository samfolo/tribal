//! The job-plane crossings: how a run acknowledges a stored response, learns
//! where its holds stand, enqueues work, and receives its cap and authority.
//!
//! None of these asserts tenancy from the client side of a metered call — the
//! account on a cap sync or grant set is set by the control plane that issues
//! it, and an enqueue's account is derived from the presenting credential, so
//! the enqueue carries only the work, never whose it is.

use serde::{Deserialize, Serialize};

use crate::gateway::reference::{AccountReference, PositionKey, PrincipalReference};

// ---------------------------------------------------------------------------
// Acknowledgement
// ---------------------------------------------------------------------------

/// A run's post-commit acknowledgement that a stored response is durable — the
/// signal the gateway's response store drops the row on. Sent only after the
/// run's own record commit succeeds, so a stale runner never produces one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Acknowledgement {
    /// The call whose stored output the run has committed.
    pub position_key: PositionKey,
}

// ---------------------------------------------------------------------------
// Holds report
// ---------------------------------------------------------------------------

/// Where one of a run's holds stands. A run is terminal only once none of its
/// holds is `active` or `settle_pending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum HoldStatus {
    /// Reserved, provider not yet dispatched — releasable.
    Active,
    /// Dispatched; its credit keeps counting and it is not releasable.
    SettlePending,
    /// Charged at actual usage.
    Settled,
    /// Freed without a charge — the call never reached the provider.
    Released,
}

/// One hold of a run and its current disposition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HoldEntry {
    /// The call the hold reserves for.
    pub position_key: PositionKey,
    /// Where the hold stands.
    pub status: HoldStatus,
}

/// The unresolved-holds report a run polls at terminalization and in cancel
/// teardown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HoldsReport {
    /// The run's holds and where each stands.
    pub holds: Vec<HoldEntry>,
}

// ---------------------------------------------------------------------------
// Job enqueue
// ---------------------------------------------------------------------------

/// The kind of managed job. A closed set the metering path can price and the
/// plane can route; it grows only under a new contract version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    /// A graph consolidation / dedup pass.
    Consolidate,
    /// Scheduled cadence work.
    Cron,
    /// A synthetic runtime-proof job carrying no product semantics.
    Probe,
}

/// A job entering the plane. The account is derived from the presenting
/// credential, so the enqueue carries only the work and its dedup key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct JobEnqueue {
    /// What kind of work this is.
    pub kind: JobKind,
    /// The opaque, task-generic payload.
    pub payload: serde_json::Value,
    /// The caller-supplied key that dedups a re-enqueue.
    pub idempotency_key: String,
    /// Ordering within the tenant only; never across tenants.
    pub priority: i16,
}

// ---------------------------------------------------------------------------
// Cap sync
// ---------------------------------------------------------------------------

/// The per-tenant concurrency cap the control plane pushes on plan change. The
/// account is named because the control plane issues this, not a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CapSync {
    /// The tenant whose cap changed.
    pub account: AccountReference,
    /// The concurrent-run ceiling for the tenant.
    pub cap: u32,
}

// ---------------------------------------------------------------------------
// Grant set
// ---------------------------------------------------------------------------

/// The run-scoped authority the control plane mints at claim: the tenant the
/// run acts for and the tools it may name. An ungranted tool is simply absent
/// from the list; the capability detail behind each name lives in the worker's
/// registry, not on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GrantSet {
    /// The account the run is scoped to.
    pub account: AccountReference,
    /// The initiating principal, absent for account-level automation.
    pub principal: Option<PrincipalReference>,
    /// The tool names the run is granted.
    pub tools: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_holds_report_round_trips() {
        let report = HoldsReport {
            holds: vec![
                HoldEntry {
                    position_key: PositionKey::new("thread_01:10"),
                    status: HoldStatus::SettlePending,
                },
                HoldEntry {
                    position_key: PositionKey::new("thread_01:20"),
                    status: HoldStatus::Released,
                },
            ],
        };
        let parsed: HoldsReport =
            serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
        assert_eq!(parsed, report);
    }

    #[test]
    fn a_job_enqueue_round_trips() {
        let enqueue = JobEnqueue {
            kind: JobKind::Probe,
            payload: serde_json::json!({ "note": "runtime proof" }),
            idempotency_key: "probe-1".to_owned(),
            priority: 0,
        };
        let parsed: JobEnqueue =
            serde_json::from_str(&serde_json::to_string(&enqueue).unwrap()).unwrap();
        assert_eq!(parsed, enqueue);
    }

    #[test]
    fn an_unknown_job_kind_is_rejected() {
        assert!(
            serde_json::from_str::<JobKind>("\"frobnicate\"").is_err(),
            "an unknown job kind must be rejected",
        );
    }
}
