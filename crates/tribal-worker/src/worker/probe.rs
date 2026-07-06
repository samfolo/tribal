//! The probe spec: the program a managed runtime-proof run walks.
//!
//! A probe carries no product semantics — it exercises the managed seam end
//! to end: a number of metered calls, an optional durable wait on a signal,
//! and an artifact. It rides the job plane's opaque `payload`, so the drive
//! reads it back from a claimed job and the enqueuer writes it in.

use serde::{Deserialize, Serialize};
use tribal_runtime_db::CapBehaviour;

/// A managed probe run's program.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeSpec {
    /// How many metered calls the probe makes before its wait.
    pub calls: u32,
    /// The durable-wait signal key, when the probe parks for one; `None` runs
    /// straight through to the artifact.
    pub wait_signal: Option<String>,
    /// The note the probe's artifact record carries.
    pub artifact_note: String,
    /// The account's cap-breach response, driving how an over-cap metered call
    /// disposes the run. Defaults to [`CapBehaviour::HardStop`] for a payload
    /// that predates the field.
    #[serde(default = "hard_stop")]
    pub cap_behaviour: CapBehaviour,
}

/// The default cap behaviour a probe carries when its payload names none.
fn hard_stop() -> CapBehaviour {
    CapBehaviour::HardStop
}

impl ProbeSpec {
    /// Renders the probe as the job plane's opaque payload.
    ///
    /// # Panics
    ///
    /// If the spec fails to serialise, which its fixed primitive shape cannot.
    #[must_use]
    pub fn to_payload(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("a probe spec serialises")
    }

    /// Reads a probe from a claimed job's opaque payload.
    ///
    /// # Errors
    ///
    /// Returns the deserialisation error when the payload is not a probe spec.
    pub fn from_payload(payload: serde_json::Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_probe_spec_round_trips_through_the_opaque_payload() {
        let spec = ProbeSpec {
            calls: 3,
            wait_signal: Some("run-signal:abc".to_owned()),
            artifact_note: "the probe's mark".to_owned(),
            cap_behaviour: CapBehaviour::ThrottleQueue,
        };
        let read = ProbeSpec::from_payload(spec.to_payload()).expect("the payload is a probe spec");
        assert_eq!(read, spec, "the spec survives the payload round trip");
    }

    #[test]
    fn test_a_probe_without_a_wait_round_trips() {
        let spec = ProbeSpec {
            calls: 1,
            wait_signal: None,
            artifact_note: "no wait".to_owned(),
            cap_behaviour: CapBehaviour::HardStop,
        };
        let read = ProbeSpec::from_payload(spec.to_payload()).expect("round trip");
        assert_eq!(read, spec);
    }

    #[test]
    fn test_a_payload_predating_the_cap_field_defaults_to_hard_stop() {
        let payload = serde_json::json!({
            "calls": 1,
            "wait_signal": null,
            "artifact_note": "legacy",
        });
        let read = ProbeSpec::from_payload(payload).expect("round trip");
        assert_eq!(read.cap_behaviour, CapBehaviour::HardStop);
    }

    #[test]
    fn test_a_malformed_payload_is_refused_not_panicked() {
        let payload = serde_json::json!({ "calls": "not a number" });
        assert!(ProbeSpec::from_payload(payload).is_err());
    }
}
