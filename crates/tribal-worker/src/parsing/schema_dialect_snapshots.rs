//! Coverage for the per-provider schema dialects applied to the pipeline
//! response schemas.
//!
//! The canonical `schemars` schema for each response type is transformed through
//! [`apply_dialect`], pinned as a golden snapshot, and checked against its
//! provider subset by [`assert_dialect_invariants`] — the dialect crate's own
//! reusable assertion, so the invariant list cannot drift from the transform. A
//! `jsonschema` validator then confirms the strict triage schema accepts a
//! correct instance and rejects the off-shape that motivated the change, and a
//! structural check confirms the tagged response types keep their discriminator.

use jsonschema::Validator;
use schemars::JsonSchema;
use serde_json::{Value, json};
use tribal_domain::ProviderKind;
use tribal_inference::{apply_dialect, assert_dialect_invariants};
use tribal_test_utils::assert_json_snapshot;

use super::{ExtractionOutput, RelationOutput, TriageClassification};

/// The canonical `schemars` schema for `T` as a JSON value.
fn canonical_schema<T: JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("schema serialises")
}

/// A triage instance the strict and subset schemas must both accept.
fn created_triage_instance() -> Value {
    json!({ "outcome": { "decision": "created" }, "similar_item_decisions": [] })
}

// ---------------------------------------------------------------------------
// OpenAI dialect snapshots
// ---------------------------------------------------------------------------

#[test]
fn test_openai_dialect_extraction_snapshot() {
    let schema = apply_dialect(ProviderKind::OpenAi, canonical_schema::<ExtractionOutput>());
    assert_dialect_invariants(&schema, true);
    assert_json_snapshot!(
        &schema,
        "src/parsing/snapshots/dialect/openai/extraction_output.json"
    );
}

#[test]
fn test_openai_dialect_triage_snapshot() {
    let schema = apply_dialect(
        ProviderKind::OpenAi,
        canonical_schema::<TriageClassification>(),
    );
    assert_dialect_invariants(&schema, true);
    assert_json_snapshot!(
        &schema,
        "src/parsing/snapshots/dialect/openai/triage_classification.json"
    );
}

#[test]
fn test_openai_dialect_relation_snapshot() {
    let schema = apply_dialect(ProviderKind::OpenAi, canonical_schema::<RelationOutput>());
    assert_dialect_invariants(&schema, true);
    assert_json_snapshot!(
        &schema,
        "src/parsing/snapshots/dialect/openai/relation_output.json"
    );
}

// ---------------------------------------------------------------------------
// Anthropic dialect snapshots
// ---------------------------------------------------------------------------

#[test]
fn test_anthropic_dialect_extraction_snapshot() {
    let schema = apply_dialect(
        ProviderKind::Anthropic,
        canonical_schema::<ExtractionOutput>(),
    );
    assert_dialect_invariants(&schema, false);
    assert_json_snapshot!(
        &schema,
        "src/parsing/snapshots/dialect/anthropic/extraction_output.json"
    );
}

#[test]
fn test_anthropic_dialect_triage_snapshot() {
    let schema = apply_dialect(
        ProviderKind::Anthropic,
        canonical_schema::<TriageClassification>(),
    );
    assert_dialect_invariants(&schema, false);
    assert_json_snapshot!(
        &schema,
        "src/parsing/snapshots/dialect/anthropic/triage_classification.json"
    );
}

#[test]
fn test_anthropic_dialect_relation_snapshot() {
    let schema = apply_dialect(
        ProviderKind::Anthropic,
        canonical_schema::<RelationOutput>(),
    );
    assert_dialect_invariants(&schema, false);
    assert_json_snapshot!(
        &schema,
        "src/parsing/snapshots/dialect/anthropic/relation_output.json"
    );
}

// ---------------------------------------------------------------------------
// Ollama dialect snapshots
// ---------------------------------------------------------------------------

#[test]
fn test_ollama_dialect_extraction_snapshot() {
    let schema = apply_dialect(ProviderKind::Ollama, canonical_schema::<ExtractionOutput>());
    assert_dialect_invariants(&schema, false);
    assert_json_snapshot!(
        &schema,
        "src/parsing/snapshots/dialect/ollama/extraction_output.json"
    );
}

#[test]
fn test_ollama_dialect_triage_snapshot() {
    let schema = apply_dialect(
        ProviderKind::Ollama,
        canonical_schema::<TriageClassification>(),
    );
    assert_dialect_invariants(&schema, false);
    assert_json_snapshot!(
        &schema,
        "src/parsing/snapshots/dialect/ollama/triage_classification.json"
    );
}

#[test]
fn test_ollama_dialect_relation_snapshot() {
    let schema = apply_dialect(ProviderKind::Ollama, canonical_schema::<RelationOutput>());
    assert_dialect_invariants(&schema, false);
    assert_json_snapshot!(
        &schema,
        "src/parsing/snapshots/dialect/ollama/relation_output.json"
    );
}

// ---------------------------------------------------------------------------
// Instance validation
// ---------------------------------------------------------------------------

#[test]
fn test_openai_triage_schema_validates_created_instance() {
    let schema = apply_dialect(
        ProviderKind::OpenAi,
        canonical_schema::<TriageClassification>(),
    );
    let validator = Validator::new(&schema).expect("transformed schema compiles");
    assert!(
        validator.is_valid(&created_triage_instance()),
        "the strict triage schema must accept a correct created instance"
    );
}

#[test]
fn test_anthropic_triage_schema_validates_created_instance() {
    let schema = apply_dialect(
        ProviderKind::Anthropic,
        canonical_schema::<TriageClassification>(),
    );
    let validator = Validator::new(&schema).expect("transformed schema compiles");
    assert!(
        validator.is_valid(&created_triage_instance()),
        "the subset triage schema must accept a correct created instance"
    );
}

#[test]
fn test_ollama_extraction_schema_validates_a_candidate_instance() {
    // The transformed schema must compile and accept a well-formed extraction
    // output, the shape llama.cpp's grammar then constrains generation to.
    let schema = apply_dialect(ProviderKind::Ollama, canonical_schema::<ExtractionOutput>());
    let validator = Validator::new(&schema).expect("transformed schema compiles");
    let instance = json!({
        "candidates": [{
            "content": "The rate limiter threshold was raised to 500 and never reverted.",
            "kind": "fact",
            "suggested_tags": ["api rate limiting"],
            "suggested_references": [],
        }],
        "relation_hints": [],
    });
    assert!(
        validator.is_valid(&instance),
        "the grammar-subset extraction schema must accept a correct instance"
    );
}

#[test]
fn test_openai_triage_schema_rejects_bare_string_outcome() {
    // The off-shape that originally dead-lettered: a bare-string `outcome`
    // instead of the internally tagged object. The strict schema must reject it.
    let schema = apply_dialect(
        ProviderKind::OpenAi,
        canonical_schema::<TriageClassification>(),
    );
    let validator = Validator::new(&schema).expect("transformed schema compiles");
    let off_shape = json!({ "outcome": "created", "similar_item_decisions": [] });
    assert!(
        !validator.is_valid(&off_shape),
        "the strict triage schema must reject a bare-string outcome"
    );
}

// ---------------------------------------------------------------------------
// Tag retention
// ---------------------------------------------------------------------------

#[test]
fn test_openai_triage_retains_internal_tags() {
    let schema = apply_dialect(
        ProviderKind::OpenAi,
        canonical_schema::<TriageClassification>(),
    );

    // The multi-variant tagged enum keeps its discriminator: each branch is an
    // object with a const `decision` rather than being flattened.
    let branches = schema["definitions"]["TriageDecision"]["anyOf"]
        .as_array()
        .expect("TriageDecision rewritten to anyOf branches");
    assert!(
        branches
            .iter()
            .all(|branch| branch["properties"]["decision"].get("const").is_some()),
        "every branch must carry a const `decision` discriminator"
    );

    // The single-variant tagged enum is inlined but keeps its const `kind`.
    assert_eq!(
        schema["definitions"]["TriageItemReference"]["properties"]["kind"]["const"],
        json!("context_index")
    );
}

#[test]
fn test_openai_relation_retains_target_kind_tag() {
    let schema = apply_dialect(ProviderKind::OpenAi, canonical_schema::<RelationOutput>());
    assert_eq!(
        schema["definitions"]["RelationTarget"]["properties"]["kind"]["const"],
        json!("context_index")
    );
}
