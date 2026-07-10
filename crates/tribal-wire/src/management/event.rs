//! Unsolicited manager publications on compatible connections.

use serde::{Deserialize, Serialize};

use super::{ConfigChangeEvent, LifecycleSnapshot};

/// Bounded event vocabulary emitted after the bootstrap handshake.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "event", content = "data")]
pub enum ManagementEvent {
    #[serde(rename = "lifecycle.changed")]
    LifecycleChanged { snapshot: Box<LifecycleSnapshot> },
    #[serde(rename = "config.changed")]
    ConfigChanged { change: ConfigChangeEvent },
    #[serde(rename = "logs.line")]
    LogLine { line: String },
    #[serde(rename = "logs.lost")]
    LogsLost { dropped: u64 },
}
