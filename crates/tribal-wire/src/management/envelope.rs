//! Version handshake and restricted replacement envelope.

use serde::{Deserialize, Serialize};

/// First frame sent by a management client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagementClientHello {
    pub protocol_version: u16,
}

/// Manager identity returned before full-protocol admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagementServerHello {
    pub protocol_version: u16,
    pub binary_version: String,
    pub manager_instance_id: String,
}

/// Request available before contract-version compatibility is established.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum ManagementBootstrapRequest {
    Handshake { hello: ManagementClientHello },
    Shutdown,
}

/// Response available on the stable replacement envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "status", content = "data", rename_all = "snake_case")]
pub enum ManagementBootstrapResponse {
    Compatible { hello: ManagementServerHello },
    VersionMismatch { hello: ManagementServerHello },
    ShutdownAccepted,
    ShutdownRefused { reason: BootstrapShutdownRefusal },
}

/// Reason a restricted replacement shutdown is refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BootstrapShutdownRefusal {
    ManagerTerminating,
    PeerIdentityMismatch,
}
