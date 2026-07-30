//! Storage-target inspection: the typed schema-and-identity evidence a
//! candidate database must show before it can be saved or switched to.

use serde::{Deserialize, Serialize};
use tribal_domain::GraphId;

#[cfg(test)]
use super::ConfigDigest;
use super::{
    ConfigRevision, DatabaseEndpointSummary, FailurePresentation, Revisioned, SecretLiteral,
};

/// Read-only inspection of one candidate database target. The secret lives
/// only for the request; no receipt, log, or diagnostic carries it.
#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DatabaseTargetInspectRequest {
    pub expected_revision: ConfigRevision,
    pub candidate_url: SecretLiteral,
}

impl std::fmt::Debug for DatabaseTargetInspectRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DatabaseTargetInspectRequest")
            .field("expected_revision", &self.expected_revision)
            .field("candidate_url", &"<redacted>")
            .finish()
    }
}

/// What inspection observed about one target, at one instant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DatabaseTargetReceipt {
    pub endpoint: DatabaseEndpointSummary,
    pub state: DatabaseTargetState,
    pub observed_at_unix_ms: u64,
}

/// The provider-neutral states a target can be in. Only `Ready` carries the
/// observed graph identity, and only a ready target may enter the switch
/// protocol; an ahead target is refused rather than downgraded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum DatabaseTargetState {
    Ready { graph_id: GraphId },
    Unavailable { failure: DatabaseTargetFailure },
    Uninitialised,
    Behind { pending: u32 },
    Ahead,
}

/// A typed reachability or compatibility failure with operator-facing copy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DatabaseTargetFailure {
    pub kind: DatabaseTargetFailureKind,
    pub presentation: FailurePresentation,
}

/// The failure classes inspection distinguishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum DatabaseTargetFailureKind {
    InvalidUrl,
    Authentication,
    Tls,
    HostUnreachable,
    DatabaseMissing,
    IncompatibleServer,
    Unknown,
}

/// The revisioned receipt `database.inspect` answers with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DatabaseTargetInspectResult(Revisioned<DatabaseTargetReceipt>);

#[cfg(feature = "schema")]
impl schemars::JsonSchema for DatabaseTargetInspectResult {
    fn schema_name() -> String {
        "DatabaseTargetInspectResult".to_owned()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        <Revisioned<DatabaseTargetReceipt>>::json_schema(generator)
    }
}

impl From<Revisioned<DatabaseTargetReceipt>> for DatabaseTargetInspectResult {
    fn from(value: Revisioned<DatabaseTargetReceipt>) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for DatabaseTargetInspectResult {
    type Target = Revisioned<DatabaseTargetReceipt>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inspect_request_debug_is_redacted() {
        let request = DatabaseTargetInspectRequest {
            expected_revision: ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"config")),
            candidate_url: SecretLiteral::try_from(
                "postgres://user:hunter2@db.internal:5432/tribal".to_owned(),
            )
            .expect("a valid secret"),
        };

        let rendered = format!("{request:?}");
        assert!(
            !rendered.contains("hunter2"),
            "no credential in Debug: {rendered}"
        );
        assert!(
            !rendered.contains("db.internal"),
            "no host in Debug: {rendered}"
        );
        assert!(rendered.contains("<redacted>"));
    }
}
