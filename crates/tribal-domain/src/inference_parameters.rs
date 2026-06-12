//! Stage and pipeline parameter types.
//!
//! These are pure serialisation DTOs with pub fields. `#[serde(default)]`
//! is applied to every field so that new fields can be added in future
//! without requiring JSONB data migrations. [`StageParameters`] rides
//! each stage's agent definition (so an edit is a new binding version);
//! [`PipelineParameters`] rides the system fingerprint, being job-level
//! and subsumed by no stage binding.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// StageParameters
// ---------------------------------------------------------------------------

/// Inference parameters for a single LLM stage.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StageParameters {
    /// Sampling temperature. `None` (serialised as `null`) records that the
    /// effective request used the provider default.
    #[serde(default)]
    pub temperature: Option<f64>,

    /// Maximum output tokens. `None` (serialised as `null`) records that the
    /// effective request used the provider default.
    #[serde(default)]
    pub max_tokens: Option<u32>,
}

// ---------------------------------------------------------------------------
// PipelineParameters
// ---------------------------------------------------------------------------

/// Pipeline-level parameters that affect inference output.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PipelineParameters {
    /// Maximum candidate count per job (cap applied during extraction).
    #[serde(default)]
    pub max_candidates_per_job: u32,

    /// Number of similar items returned during triage search.
    #[serde(default)]
    pub triage_search_limit: u32,

    /// Minimum cosine similarity for semantic tag matching.
    #[serde(default)]
    pub tag_similarity_threshold: f64,
}

impl PipelineParameters {
    /// Serialises to compact, deterministic JSON.
    ///
    /// Field order follows struct declaration order (guaranteed by serde
    /// for structs). The output is an input to the fingerprint hash.
    ///
    /// # Panics
    ///
    /// Cannot panic in practice — the struct contains only primitives,
    /// which are infallibly serialisable.
    #[must_use]
    pub fn to_canonical_json(&self) -> String {
        serde_json::to_string(self).expect("PipelineParameters serialisation is infallible")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let stage = StageParameters::default();
        assert_eq!(stage.temperature, None);
        assert_eq!(stage.max_tokens, None);

        let pipeline = PipelineParameters::default();
        assert_eq!(pipeline.max_candidates_per_job, 0);
    }

    #[test]
    fn test_canonical_json_is_deterministic() {
        let params = PipelineParameters {
            max_candidates_per_job: 20,
            triage_search_limit: 10,
            tag_similarity_threshold: 0.85,
        };

        assert_eq!(params.to_canonical_json(), params.to_canonical_json());
    }

    #[test]
    fn test_canonical_json_field_order() {
        let params = PipelineParameters {
            max_candidates_per_job: 20,
            triage_search_limit: 10,
            tag_similarity_threshold: 0.85,
        };

        let json = params.to_canonical_json();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["max_candidates_per_job"].as_u64().unwrap(), 20);
        assert_eq!(value["triage_search_limit"].as_u64().unwrap(), 10);
        assert!((value["tag_similarity_threshold"].as_f64().unwrap() - 0.85).abs() < f64::EPSILON);
    }

    #[test]
    fn test_stage_parameters_serialise_unset_as_null() {
        // An unset sampling parameter must serialise to a stable `null`
        // so the binding's canonical form is stable.
        let json = serde_json::to_value(StageParameters::default()).unwrap();

        assert!(json["temperature"].is_null());
        assert!(json["max_tokens"].is_null());
    }

    #[test]
    fn test_partial_deserialisation_fills_defaults() {
        let json = r#"{"temperature":0.2}"#;
        let params: StageParameters = serde_json::from_str(json).unwrap();

        assert_eq!(params.temperature, Some(0.2));
        assert_eq!(params.max_tokens, None);

        let json = r#"{"triage_search_limit":7}"#;
        let pipeline: PipelineParameters = serde_json::from_str(json).unwrap();
        assert_eq!(pipeline.triage_search_limit, 7);
        assert_eq!(pipeline.max_candidates_per_job, 0);
    }
}
