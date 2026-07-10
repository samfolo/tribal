//! Private same-machine contract between the manager and its runtime.
//!
//! This module is intentionally absent from public-client code generation.

use std::fmt;

use serde::{Deserialize, Serialize};
use tribal_domain::ConfigFieldPath;

use crate::{
    management::{ConfigLiteral, ConfigRevision, ReadinessReport, RuntimeIdentity},
    token::TokenList,
};

/// Version of the private manager/runtime contract.
pub const RUNTIME_CONTROL_CONTRACT_VERSION: u16 = 3;

/// First frame sent by a manager on a runtime-control connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RuntimeControlClientHello {
    pub protocol_version: u16,
    pub manager_instance_id: String,
}

/// Runtime identity returned before full-protocol admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RuntimeControlServerHello {
    pub protocol_version: u16,
    pub binary_version: String,
    pub runtime: RuntimeIdentity,
}

/// Secret capability proving custody of one managed runtime.
#[derive(PartialEq, zeroize::Zeroize, zeroize::ZeroizeOnDrop, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RuntimeCustodyProof(String);

impl RuntimeCustodyProof {
    /// Wraps proof material generated from operating-system entropy.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// Borrows proof material for the authenticated bootstrap transport.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RuntimeCustodyProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted runtime custody proof>")
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for RuntimeCustodyProof {
    fn schema_name() -> String {
        "RuntimeCustodyProof".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            ..Default::default()
        }
        .into()
    }
}

/// Request available before runtime-contract compatibility is established.
#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
pub enum RuntimeBootstrapRequest {
    Handshake {
        hello: RuntimeControlClientHello,
        proof: RuntimeCustodyProof,
    },
    RestrictedStop {
        runtime: RuntimeIdentity,
        proof: RuntimeCustodyProof,
    },
}

impl fmt::Debug for RuntimeBootstrapRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Handshake { hello, .. } => formatter
                .debug_struct("Handshake")
                .field("hello", hello)
                .field("proof", &"<redacted>")
                .finish(),
            Self::RestrictedStop { runtime, .. } => formatter
                .debug_struct("RestrictedStop")
                .field("runtime", runtime)
                .field("proof", &"<redacted>")
                .finish(),
        }
    }
}

/// Response available on the stable runtime bootstrap envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "status", content = "data", rename_all = "snake_case")]
pub enum RuntimeBootstrapResponse {
    Compatible { hello: RuntimeControlServerHello },
    VersionMismatch { hello: RuntimeControlServerHello },
    StopAccepted,
    Refused { reason: RuntimeBootstrapRefusal },
}

/// Reason a runtime bootstrap request was refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RuntimeBootstrapRefusal {
    CustodyProofInvalid,
    RuntimeIdentityMismatch,
    PeerIdentityMismatch,
    RuntimeTerminating,
}

/// One revisioned configuration value offered for live adoption.
#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RuntimeConfigChange {
    pub key: ConfigFieldPath,
    pub value: ConfigLiteral,
}

impl fmt::Debug for RuntimeConfigChange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeConfigChange")
            .field("key", &self.key)
            .field("value", &"<redacted>")
            .finish()
    }
}

/// Full request vocabulary admitted only after a compatible handshake.
#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
pub enum RuntimeControlRequest {
    Status,
    Readiness,
    ApplyConfig {
        revision: ConfigRevision,
        changes: Vec<RuntimeConfigChange>,
    },
    Stop {
        runtime: RuntimeIdentity,
    },
    LogsTail {
        lines: u32,
    },
    SubscribeLogs,
    TokenList,
}

impl fmt::Debug for RuntimeControlRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status => formatter.write_str("Status"),
            Self::Readiness => formatter.write_str("Readiness"),
            Self::ApplyConfig { revision, changes } => formatter
                .debug_struct("ApplyConfig")
                .field("revision", revision)
                .field("changes", changes)
                .finish(),
            Self::Stop { runtime } => formatter.debug_tuple("Stop").field(runtime).finish(),
            Self::LogsTail { lines } => formatter.debug_tuple("LogsTail").field(lines).finish(),
            Self::SubscribeLogs => formatter.write_str("SubscribeLogs"),
            Self::TokenList => formatter.write_str("TokenList"),
        }
    }
}

/// Event vocabulary on a dedicated compatible log subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "method", content = "params")]
pub enum RuntimeControlEvent {
    /// One live runtime log line.
    #[serde(rename = "logs.line")]
    LogLine { line: String },
    /// The mixed runtime event channel lost position, so the line count is unknown.
    #[serde(rename = "logs.lost")]
    LogsLost,
}

/// Runtime-side status needed by the manager reducer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagedRuntimeStatus {
    pub runtime: RuntimeIdentity,
}

/// Result of offering a durable configuration revision for live adoption.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum RuntimeConfigApplyOutcome {
    Applied,
    RestartRequired,
    Refused { reason: RuntimeConfigApplyRefusal },
}

/// Reason a runtime cannot adopt a durable configuration revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RuntimeConfigApplyRefusal {
    RevisionStale,
    UnsupportedField,
    ConfigUnavailable,
    RuntimeTerminating,
}

/// Runtime-control response body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "method", content = "result", rename_all = "snake_case")]
pub enum RuntimeControlResponse {
    Status { status: ManagedRuntimeStatus },
    Readiness { report: ReadinessReport },
    ApplyConfig { outcome: RuntimeConfigApplyOutcome },
    StopAccepted,
    LogsTail { lines: Vec<String> },
    LogsSubscribed,
    TokenList { list: TokenList },
    Refused { reason: RuntimeControlRefusal },
}

/// Failure classification shared by full runtime-control operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RuntimeControlRefusal {
    PeerIdentityMismatch,
    RuntimeIdentityMismatch,
    RuntimeTerminating,
    OperationUnavailable,
}

/// Durable attachment request sent over the lifetime custody session.
#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CustodyCommitRequest {
    pub attempt_id: u64,
    pub manager_instance_id: String,
    pub descriptor_path: String,
    pub proof: RuntimeCustodyProof,
}

impl fmt::Debug for CustodyCommitRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CustodyCommitRequest")
            .field("attempt_id", &self.attempt_id)
            .field("manager_instance_id", &self.manager_instance_id)
            .field("descriptor_path", &self.descriptor_path)
            .field("proof", &"<redacted>")
            .finish()
    }
}

/// Result of durably attaching one manager to a runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "status", content = "data", rename_all = "snake_case")]
pub enum CustodyCommitResponse {
    Committed { attempt_id: u64 },
    Refused { reason: CustodyCommitRefusal },
}

/// Reason a durable custody commit is refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CustodyCommitRefusal {
    AttemptMismatch,
    ProofInvalid,
    DescriptorPersistenceFailed,
    RecoveryBlocked,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_custody_proof_is_redacted_directly_and_in_requests() {
        let sentinel = "custody-sentinel";
        let request = RuntimeBootstrapRequest::Handshake {
            hello: RuntimeControlClientHello {
                protocol_version: RUNTIME_CONTROL_CONTRACT_VERSION,
                manager_instance_id: "manager".to_owned(),
            },
            proof: RuntimeCustodyProof::new(sentinel.to_owned()),
        };

        assert!(!format!("{request:?}").contains(sentinel));
    }

    #[cfg(feature = "schema")]
    #[test]
    fn test_private_request_and_response_schemas_generate() {
        let requests = schemars::schema_for!(RuntimeControlRequest);
        let responses = schemars::schema_for!(RuntimeControlResponse);
        assert_eq!(
            requests
                .schema
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.title.as_deref()),
            Some("RuntimeControlRequest")
        );
        assert_eq!(
            responses
                .schema
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.title.as_deref()),
            Some("RuntimeControlResponse")
        );
    }
}
