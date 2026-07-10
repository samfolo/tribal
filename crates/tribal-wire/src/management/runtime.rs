//! Runtime reads projected through the manager authority.

use serde::{Deserialize, Serialize};

use super::{RuntimeIdentity, TokenList};

/// Why a runtime-owned read cannot currently be served.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RuntimeReadUnavailable {
    NoRuntime,
    OperationInProgress,
    VersionMismatch,
    RuntimeControlUnavailable,
    ManagerTerminating,
}

/// Public status of the attached managed runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagedRuntimeStatus {
    pub runtime: RuntimeIdentity,
    pub restart_pending: bool,
}

/// Result of `server.status` through the manager.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum ManagedRuntimeStatusResult {
    Available { status: ManagedRuntimeStatus },
    Unavailable { reason: RuntimeReadUnavailable },
}

/// Bounded request for recent runtime log lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RuntimeLogsTailRequest {
    pub lines: u32,
}

/// Result of `logs.tail` through the manager.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum RuntimeLogsTailResult {
    Available { lines: Vec<String> },
    Unavailable { reason: RuntimeReadUnavailable },
}

/// Result of `token.list` through the manager.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum RuntimeTokenListResult {
    Available { list: TokenList },
    Unavailable { reason: RuntimeReadUnavailable },
}
