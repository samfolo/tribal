//! Storage-target inspection and the graph-transition protocol: the typed
//! evidence a candidate must show, the one authoritative assessment of
//! whether the source may be left, and the switch, continue, force-stop, and
//! abort operations that quiesce or recover the exact managed runtime.

use serde::{Deserialize, Serialize};
use tribal_domain::{GraphId, ReindexRunId, StorageTransitionId};

#[cfg(test)]
use super::ConfigDigest;
use super::{
    ConfigRevision, DatabaseEndpointSummary, FailurePresentation, Revisioned, RuntimeIdentity,
    RuntimeStartResult, RuntimeStopTimedOutFailure, SecretLiteral,
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
    Behind { pending_count: u32 },
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

// ---------------------------------------------------------------------------
// Transition assessment
// ---------------------------------------------------------------------------

/// A revision-fenced read of the transition assessment; takes no barrier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StorageAssessRequest {
    pub expected_revision: ConfigRevision,
}

/// Whether this manager may stop the source runtime now without abandoning a
/// graph mutation or confusing its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StorageTransitionAssessment {
    pub source_graph_id: GraphId,
    pub verdict: StorageTransitionVerdict,
    pub activities: Vec<GraphActivity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum StorageTransitionVerdict {
    Ready,
    Blocked,
}

/// One live graph-bound activity and how a blocked switch resolves it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GraphActivity {
    pub kind: GraphActivityKind,
    pub status: GraphActivityStatus,
    pub resolution: BlockingResolution,
}

/// The closed inventory of operation classes relevant to switching; adding a
/// graph-bound long-running operation means classifying it here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum GraphActivityKind {
    EmbeddingReindex { run_id: ReindexRunId },
    DatabaseMigration,
    RuntimeLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "status", content = "data", rename_all = "snake_case")]
pub enum GraphActivityStatus {
    Queued,
    Running,
    WaitingForRetry { resume_at_unix_ms: Option<u64> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BlockingResolution {
    Wait,
    WaitOrCancelReindex,
    WaitForLifecycle,
}

/// The revisioned assessment `storage.assess` answers with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StorageAssessResult(Revisioned<StorageTransitionAssessment>);

#[cfg(feature = "schema")]
impl schemars::JsonSchema for StorageAssessResult {
    fn schema_name() -> String {
        "StorageAssessResult".to_owned()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        <Revisioned<StorageTransitionAssessment>>::json_schema(generator)
    }
}

impl From<Revisioned<StorageTransitionAssessment>> for StorageAssessResult {
    fn from(value: Revisioned<StorageTransitionAssessment>) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for StorageAssessResult {
    type Target = Revisioned<StorageTransitionAssessment>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// ---------------------------------------------------------------------------
// Switch, continue, force-stop, abort
// ---------------------------------------------------------------------------

/// A switch from the exact source graph to a saved, inspected target.
#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StorageSwitchRequest {
    pub expected_revision: ConfigRevision,
    /// The runtime expected attached, or `None` for a clean no-runtime
    /// source; either expectation fails closed on mismatch.
    pub expected_runtime: Option<RuntimeIdentity>,
    pub source_graph_id: GraphId,
    pub target_graph_id: GraphId,
    pub target_url: SecretLiteral,
}

impl std::fmt::Debug for StorageSwitchRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageSwitchRequest")
            .field("expected_revision", &self.expected_revision)
            .field("expected_runtime", &self.expected_runtime)
            .field("source_graph_id", &self.source_graph_id)
            .field("target_graph_id", &self.target_graph_id)
            .field("target_url", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum StorageSwitchResult {
    /// Another graph operation is crossing admission; retry after it commits.
    AdmissionContended,
    /// A sibling transition already holds the local barrier.
    TransitionInProgress { transition_id: StorageTransitionId },
    /// Live graph activity blocks the switch; the attempt is closed.
    Blocked {
        assessment: StorageTransitionAssessment,
    },
    /// The reinspected target is not ready; the source keeps running.
    TargetNotReady { target: DatabaseTargetReceipt },
    /// The exact source process has exited; the source is quiesced.
    SourceQuiesced {
        transition_id: StorageTransitionId,
        target: DatabaseTargetReceipt,
    },
    /// The graceful window expired; the transition and barriers are retained.
    StopTimedOut {
        transition_id: StorageTransitionId,
        runtime: RuntimeIdentity,
    },
    /// The attached runtime is not the expected one; nothing was begun.
    RuntimeChanged { observed: Option<RuntimeIdentity> },
    /// The source database's identity is not the requested source graph.
    SourceChanged { observed_graph_id: GraphId },
    /// The reinspected target's identity is not the requested target graph.
    TargetChanged { observed_graph_id: GraphId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StorageSwitchContinueRequest {
    pub transition_id: StorageTransitionId,
    pub expected_runtime: RuntimeIdentity,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum StorageSwitchContinueResult {
    SourceQuiesced {
        transition_id: StorageTransitionId,
        target: DatabaseTargetReceipt,
    },
    StopTimedOut {
        transition_id: StorageTransitionId,
        runtime: RuntimeIdentity,
    },
    RuntimeChanged,
    NoPendingTransition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StorageSwitchForceStopRequest {
    pub transition_id: StorageTransitionId,
    pub expected_runtime: RuntimeIdentity,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum StorageSwitchForceStopResult {
    SourceQuiesced {
        transition_id: StorageTransitionId,
        target: DatabaseTargetReceipt,
    },
    RuntimeChanged,
    /// The kill was sent and the exact process is still present at the end of
    /// the bounded window; the transition and barriers are retained.
    RuntimeStillPresent {
        failure: RuntimeStopTimedOutFailure,
    },
    NoPendingTransition,
}

/// Abort is fenced by the transition's own recorded runtime; the request
/// carries only the transition identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StorageSwitchAbortRequest {
    pub transition_id: StorageTransitionId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum StorageSwitchAbortResult {
    /// Exact exit was observed and the source restarted; the abort is done.
    SourceRestarted,
    /// A clean no-runtime origin stays stopped; no start signal was sent.
    SourceRemainedStopped,
    /// The already-signalled process is still present; custody is retained
    /// and the same abort may be repeated.
    SourceRecoveryPending {
        runtime: RuntimeIdentity,
    },
    /// Exact exit was observed but the restart failed; the abort completes,
    /// the barriers release, and ordinary lifecycle recovery may retry.
    SourceStillStopped {
        result: Box<RuntimeStartResult>,
    },
    /// The attached runtime is not the recorded one; custody is retained.
    RuntimeChanged {
        observed: Option<RuntimeIdentity>,
    },
    NoPendingTransition,
}
