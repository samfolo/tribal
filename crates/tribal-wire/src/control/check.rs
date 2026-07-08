//! `check.report`: the same JSON shape emitted by `tribal check --json`.

use serde::{Deserialize, Serialize};

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

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// Identifier for a single check row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CheckName {
    /// Config file parse/read check.
    ConfigParse,
    /// Config invariant validation check.
    ConfigValidate,
    /// Database connectivity check.
    DatabaseReachable,
    /// Migration head check.
    MigrationsCurrent,
    /// Project resolution check.
    ProjectResolution,
    /// Static token verification check.
    ValidTokenExists,
    /// Advertised URL reachability check.
    AdvertisedUrlReachable,
    /// PATH binary uniqueness check.
    BinaryUniqueness,
    /// Active embedding profile check.
    EmbeddingProfile,
    /// Embedding provider probe check.
    ProviderEmbedding,
    /// Extraction provider probe check.
    ProviderExtraction,
    /// Triage provider probe check.
    ProviderTriage,
    /// Relation provider probe check.
    ProviderRelation,
}

/// One row of the check report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CheckResult {
    /// Check status.
    pub status: CheckStatus,
    /// Which check produced the row.
    pub name: CheckName,
    /// Human-readable detail for the observed state.
    pub detail: String,
    /// Suggested operator action, present only for warnings and failures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remediation: Option<String>,
}

/// Status of one check row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    /// The check passed.
    Pass,
    /// The check surfaced a warning.
    Warn,
    /// The check failed.
    Fail,
    /// The check was skipped because a precondition did not hold.
    Skip,
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
