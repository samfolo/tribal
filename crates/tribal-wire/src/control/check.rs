//! `check.report`: the same JSON shape emitted by `tribal check --json`.

use serde::{Deserialize, Serialize};

pub use crate::operator_check::{CheckName, CheckResult};

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Parameters for `check.report`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CheckReportRequest {
    /// Whether provider probes should run and appear in the report.
    #[serde(default)]
    pub probe_providers: bool,
}

/// The full `check.report` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CheckReport {
    /// `true` iff no row failed.
    pub ok: bool,
    /// Ordered check rows.
    pub checks: Vec<CheckResult>,
}
