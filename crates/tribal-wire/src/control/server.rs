//! The `server.*` crossings: a live status snapshot shaped like `tribal
//! check`'s JSON, and the lifecycle answers.
//!
//! A restart is never a silent self-exec: a supervised binary defers to its
//! supervisor, and an unsupervised one refuses and directs the operator to stop
//! and relaunch.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// server.status
// ---------------------------------------------------------------------------

/// The worker's liveness, as the status snapshot sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    /// The worker task is alive and processing.
    Running,
    /// The worker task has stopped.
    Stopped,
}

/// The project the binary is serving, when one is resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProjectSummary {
    /// The project's stable id.
    pub id: String,
    /// The project's human-readable name.
    pub name: String,
}

/// A live introspection of the running server, the result of `server.status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ServerStatus {
    /// The active transport the binary serves MCP over, e.g. `stdio` or `http`.
    pub transport: String,
    /// The bind address, when the transport listens on one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind_address: Option<String>,
    /// Seconds since the server began serving.
    pub uptime_seconds: u64,
    /// The worker task's liveness.
    pub worker: WorkerStatus,
    /// The depth of the worker's job queue, when it can be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<u64>,
    /// The project being served, absent when none is resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<ProjectSummary>,
    /// The binary's build version.
    pub binary_version: String,
    /// The control-contract version the server speaks.
    pub protocol_version: u16,
    /// The per-serve instance identity.
    pub instance_id: String,
}

// ---------------------------------------------------------------------------
// server.restart / server.stop
// ---------------------------------------------------------------------------

/// The answer to `server.restart`. The binary never re-execs itself silently:
/// it either hands the restart to its supervisor or refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum RestartOutcome {
    /// A supervisor owns the process; the binary is stopping and the supervisor
    /// will relaunch it.
    SupervisorMediated,
    /// No supervisor owns the process, so the binary refuses to restart itself;
    /// the caller must stop it and relaunch explicitly.
    Unsupervised,
}

/// The answer to `server.stop`: the binary is shutting down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StopOutcome {
    /// Whether the binary accepted the stop and is shutting down.
    pub stopping: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_server_status_round_trips() {
        let status = ServerStatus {
            transport: "http".to_owned(),
            bind_address: Some("127.0.0.1:7777".to_owned()),
            uptime_seconds: 42,
            worker: WorkerStatus::Running,
            queue_depth: Some(3),
            project: Some(ProjectSummary {
                id: "project_01".to_owned(),
                name: "demo".to_owned(),
            }),
            binary_version: "1.2.3".to_owned(),
            protocol_version: 1,
            instance_id: "host~1234~boot".to_owned(),
        };
        let parsed: ServerStatus =
            serde_json::from_str(&serde_json::to_string(&status).unwrap()).unwrap();
        assert_eq!(parsed, status);
    }

    #[test]
    fn test_the_restart_outcome_is_tagged_on_outcome() {
        assert_eq!(
            serde_json::to_value(RestartOutcome::Unsupervised).unwrap(),
            serde_json::json!({ "outcome": "unsupervised" }),
        );
    }

    #[test]
    fn test_an_unknown_restart_outcome_is_rejected() {
        assert!(
            serde_json::from_value::<RestartOutcome>(serde_json::json!({ "outcome": "self_exec" }))
                .is_err(),
            "an unknown restart outcome must be rejected",
        );
    }
}
