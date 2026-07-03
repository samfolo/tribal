//! The server-initiated events a subscriber receives, so a client never polls.
//!
//! Each variant projects to a JSON-RPC notification: its wire tag is the dotted
//! `method`, and its fields the `params`. The binary is the only filesystem
//! watcher and the only config writer, so a client learns of a change only
//! through these — it reads back or awaits the matching event, never assuming a
//! write applied.

use serde::{Deserialize, Serialize};

use crate::control::{config::WriteEffect, logs::LogLine};

// ---------------------------------------------------------------------------
// Control events
// ---------------------------------------------------------------------------

/// A server-initiated event. Adjacently tagged so it serialises to the
/// `{ method, params }` a JSON-RPC notification frame carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "method", content = "params")]
pub enum ControlEvent {
    /// One or more config keys changed on disk, with how the change took
    /// effect.
    #[serde(rename = "config.changed")]
    ConfigChanged {
        /// The dotted keys that changed.
        keys: Vec<String>,
        /// How the change took effect.
        effect: WriteEffect,
    },
    /// The server's status changed; a subscriber re-reads `server.status`.
    #[serde(rename = "server.statusChanged")]
    ServerStatusChanged,
    /// A new log line was emitted.
    #[serde(rename = "logs.line")]
    LogsLine {
        /// The emitted line.
        line: LogLine,
    },
    /// A prompt version was hot-reloaded from disk.
    #[serde(rename = "prompt.reloaded")]
    PromptReloaded {
        /// The pipeline stage whose prompt reloaded.
        stage: String,
        /// The prompt role within that stage.
        role: String,
        /// The id of the version now in force.
        version_id: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_changed_projects_to_method_and_params() {
        let event = ControlEvent::ConfigChanged {
            keys: vec!["logging.level".to_owned()],
            effect: WriteEffect::NeedsRestart,
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["method"], serde_json::json!("config.changed"));
        assert_eq!(json["params"]["effect"], serde_json::json!("needs_restart"));
        let parsed: ControlEvent = serde_json::from_value(json).unwrap();
        assert_eq!(parsed, event);
    }

    #[test]
    fn test_a_fieldless_event_carries_only_its_method() {
        let json = serde_json::to_value(ControlEvent::ServerStatusChanged).unwrap();
        assert_eq!(json["method"], serde_json::json!("server.statusChanged"));
        assert!(
            json.get("params").is_none(),
            "an event with no fields carries no params",
        );
    }

    #[test]
    fn test_an_unknown_event_method_is_rejected() {
        assert!(
            serde_json::from_value::<ControlEvent>(serde_json::json!({ "method": "job.progress" }))
                .is_err(),
            "an unknown event method must be rejected, never silently accepted",
        );
    }
}
