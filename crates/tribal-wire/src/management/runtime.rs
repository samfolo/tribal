//! Runtime reads projected through the manager authority.

use serde::{Deserialize, Serialize};

use super::RuntimeIdentity;

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

/// Network family of the Streamable HTTP data plane, owned by the
/// control contract apart from configuration `TransportKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum NetworkTransport {
    Http,
    Sse,
}

/// The endpoint and audience the running process actually loaded;
/// absent for a runtime without a network data plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RuntimeDataPlane {
    pub transport: NetworkTransport,
    /// The exact address the transport runner binds.
    pub mcp_url: String,
    /// The audience the runtime's authenticator enforces.
    pub canonical_resource: String,
}

/// Public status of the attached managed runtime. While a restart is
/// pending the data plane remains the old process's reported endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagedRuntimeStatus {
    pub runtime: RuntimeIdentity,
    pub restart_pending: bool,
    pub data_plane: Option<RuntimeDataPlane>,
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
