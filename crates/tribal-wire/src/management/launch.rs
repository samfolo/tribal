//! Bounded machine records emitted by `tribal manager run --announce-json`.

use serde::{Deserialize, Serialize};

use super::ConfigFilePath;

/// Connection coordinates for one live manager authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagerAnnouncement {
    pub instance_id: String,
    pub socket_path: String,
    pub protocol_version: u16,
    pub binary_version: String,
    pub config_path: ConfigFilePath,
    pub pid: u32,
}

/// Standalone runtime currently fencing a configuration path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConflictingRuntimeIdentity {
    pub pid: u32,
    pub binary_version: String,
    pub config_path: ConfigFilePath,
}

/// Sole stdout record emitted during manager launch or attachment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "status", content = "data", rename_all = "snake_case")]
pub enum ManagerLaunchRecord {
    Ready {
        announcement: ManagerAnnouncement,
        disposition: ManagerLaunchDisposition,
    },
    Failed {
        failure: ManagerLaunchFailure,
    },
}

/// Whether the announcing process remains the authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ManagerLaunchDisposition {
    ManagerContinues,
    ContenderExits,
}

/// Stable failure classification for manager launch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "code", content = "data", rename_all = "snake_case")]
pub enum ManagerLaunchFailure {
    DirectRuntimeConflict {
        runtime: ConflictingRuntimeIdentity,
    },
    AuthorityRecovering {
        config_path: ConfigFilePath,
    },
    AuthorityUnavailable {
        config_path: ConfigFilePath,
        reason: AuthorityUnavailableReason,
    },
    ManagerStartupFailed {
        config_path: ConfigFilePath,
        failure: ManagerStartupFailure,
    },
}

/// Reason the current authority cannot be attached to safely.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AuthorityUnavailableReason {
    PermissionDenied,
    InvalidDescriptor,
    LeaseOwnerUnreachable,
}

/// Stage at which a winning manager could not become ready.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ManagerStartupFailure {
    ConfigPathUnavailable,
    AuthorityLeaseUnavailable,
    EntropyUnavailable,
    BootstrapProtocolInitializationFailed,
    ManagementSocketUnavailable,
    AuthorityDescriptorPersistenceFailed,
}
