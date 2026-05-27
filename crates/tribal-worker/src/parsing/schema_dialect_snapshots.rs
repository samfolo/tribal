//! Coverage for the per-provider schema dialects applied to the pipeline
//! response schemas.
//!
//! The canonical `schemars` schema for each response type is transformed
//! through [`apply_dialect`] and pinned as a golden snapshot — the reviewable
//! record of what each provider's endpoint actually receives. The dialect's own
//! debug-time postcondition verifies the subset invariants on every transform
//! (these tests run in debug, so they exercise it on the real schemas), so this
//! module covers only what the dialect cannot: pinning the shape, validating a
//! correct instance with a real validator, and the cross-crate check that the
//! tagged response types keep their discriminator.

use jsonschema::Validator;
use schemars::JsonSchema;
use serde_json::{Value, json};
use tribal_domain::ProviderKind;
use tribal_inference::apply_dialect;
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
    assert_json_snapshot!(
        &schema,
        "src/parsing/snapshots/dialect/openai/triage_classification.json"
    );
}

#[test]
fn test_openai_dialect_relation_snapshot() {
    let schema = apply_dialect(ProviderKind::OpenAi, canonical_schema::<RelationOutput>());
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
    assert_json_snapshot!(
        &schema,
        "src/parsing/snapshots/dialect/anthropic/relation_output.json"
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
