//! Inference parameter types stored as JSONB on system fingerprints.
//!
//! These are pure serialisation DTOs with pub fields. `#[serde(default)]`
//! is applied to every field so that new fields can be added in future
//! without requiring JSONB data migrations.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// StageParameters
// ---------------------------------------------------------------------------

/// Inference parameters for a single LLM stage (extraction, triage, or
/// relation).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StageParameters {
    /// Sampling temperature.
    #[serde(default)]
    pub temperature: f64,

    /// Maximum output tokens.
    #[serde(default)]
    pub max_tokens: u32,
}

// ---------------------------------------------------------------------------
// EmbeddingParameters
// ---------------------------------------------------------------------------

/// Parameters for the embedding stage.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EmbeddingParameters {
    /// Vector dimensionality.
    #[serde(default)]
    pub dimensions: u32,
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

// ---------------------------------------------------------------------------
// InferenceParameters
// ---------------------------------------------------------------------------

/// All inference-affecting parameters stored on a system fingerprint.
///
/// Serialised as compact JSON with keys in declaration order. The canonical
/// JSON form is an input to the fingerprint SHA-256 hash.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct InferenceParameters {
    /// Extraction stage parameters.
    #[serde(default)]
    pub extraction: StageParameters,

    /// Triage stage parameters.
    #[serde(default)]
    pub triage: StageParameters,

    /// Relation stage parameters.
    #[serde(default)]
    pub relation: StageParameters,

    /// Embedding stage parameters.
    #[serde(default)]
    pub embedding: EmbeddingParameters,

    /// Pipeline-level parameters.
    #[serde(default)]
    pub pipeline: PipelineParameters,
}

impl InferenceParameters {
    /// Serialises to compact, deterministic JSON.
    ///
    /// Field order follows struct declaration order (guaranteed by serde
    /// for structs). The output is an input to the fingerprint hash.
    ///
    /// # Panics
    ///
    /// Cannot panic in practice — the struct contains only primitives and
    /// nested structs of primitives, which are infallibly serialisable.
    #[must_use]
    pub fn to_canonical_json(&self) -> String {
        serde_json::to_string(self).expect("InferenceParameters serialisation is infallible")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_produces_zeroed_values() {
        let params = InferenceParameters::default();
        assert!((params.extraction.temperature - 0.0).abs() < f64::EPSILON);
        assert_eq!(params.extraction.max_tokens, 0);
        assert_eq!(params.embedding.dimensions, 0);
        assert_eq!(params.pipeline.max_candidates_per_job, 0);
    }

    #[test]
    fn test_canonical_json_field_order() {
        let params = InferenceParameters {
            extraction: StageParameters {
                temperature: 0.2,
                max_tokens: 4096,
            },
            triage: StageParameters {
                temperature: 0.1,
                max_tokens: 2048,
            },
            relation: StageParameters {
                temperature: 0.1,
                max_tokens: 2048,
            },
            embedding: EmbeddingParameters { dimensions: 768 },
            pipeline: PipelineParameters {
                max_candidates_per_job: 20,
                triage_search_limit: 10,
                tag_similarity_threshold: 0.85,
            },
        };

        let json = params.to_canonical_json();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(value.get("extraction").is_some());
        assert!(value.get("triage").is_some());
        assert!(value.get("relation").is_some());
        assert!(value.get("embedding").is_some());
        assert!(value.get("pipeline").is_some());

        let extraction = value.get("extraction").unwrap();
        assert!((extraction["temperature"].as_f64().unwrap() - 0.2).abs() < f64::EPSILON);
        assert_eq!(extraction["max_tokens"].as_u64().unwrap(), 4096);
    }

    #[test]
    fn test_canonical_json_is_deterministic() {
        let params = InferenceParameters {
            extraction: StageParameters {
                temperature: 0.2,
                max_tokens: 4096,
            },
            ..InferenceParameters::default()
        };

        assert_eq!(params.to_canonical_json(), params.to_canonical_json());
    }

    #[test]
    fn test_partial_deserialisation_fills_defaults() {
        let json = r#"{"extraction":{"temperature":0.2,"max_tokens":4096}}"#;
        let params: InferenceParameters = serde_json::from_str(json).unwrap();

        assert!((params.extraction.temperature - 0.2).abs() < f64::EPSILON);
        assert_eq!(params.extraction.max_tokens, 4096);
        assert_eq!(params.embedding.dimensions, 0);
        assert_eq!(params.pipeline.max_candidates_per_job, 0);
    }
}
