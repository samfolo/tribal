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
fn test_ollama_extraction_schema_is_valid_jsonschema_and_accepts_an_instance() {
    // Proves the transformed schema is valid draft-07 JSON Schema and accepts a
    // populated extraction output. It does not prove llama.cpp grammar
    // compilability (a separate engine), which is exercised out of band by a
    // manual end-to-end run, not in CI. The instance populates the nullable
    // reference `description` (present and null) and a relation hint, the shapes
    // a minimal instance would skip.
    let schema = apply_dialect(ProviderKind::Ollama, canonical_schema::<ExtractionOutput>());
    let validator = Validator::new(&schema).expect("transformed schema compiles");
    let instance = json!({
        "candidates": [
            {
                "content": "The rate limiter threshold was raised to 500 and never reverted.",
                "kind": "fact",
                "suggested_tags": ["api rate limiting"],
                "suggested_references": [
                    { "reference_type": "symbol", "value": "RateLimiter", "description": "the guard that was widened" },
                    { "reference_type": "file_path", "value": "src/limit.rs", "description": null },
                ],
            },
            {
                "content": "The 500 threshold came from the incident retro, not a load test.",
                "kind": "decision_record",
                "suggested_tags": ["api rate limiting"],
            },
        ],
        "relation_hints": [
            { "hint_type": "derived_from", "source_index": 1, "target_index": 0 },
        ],
    });
    assert!(
        validator.is_valid(&instance),
        "the grammar-subset extraction schema must accept a populated instance"
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
// Grammar-subset hardening (Ollama)
// ---------------------------------------------------------------------------

/// Every schema node within `value`, including `value` itself. One recursion the
/// structural checks below share, rather than a bespoke walk apiece.
fn nodes(value: &Value) -> Vec<&Value> {
    let mut out = vec![value];
    match value {
        Value::Object(map) => map.values().for_each(|v| out.extend(nodes(v))),
        Value::Array(items) => items.iter().for_each(|v| out.extend(nodes(v))),
        _ => {}
    }
    out
}

/// Whether `node` is a single-element `allOf` wrapping a `$ref` (the
/// described-`$ref` form `schemars` emits and the subset rewrites away).
fn is_single_allof_ref(node: &Value) -> bool {
    match node.get("allOf").and_then(Value::as_array) {
        Some(branches) => branches.len() == 1 && branches[0].get("$ref").is_some(),
        None => false,
    }
}

/// The definition names (`#/definitions/<name>`) referenced anywhere in `value`.
fn refs_in(value: &Value) -> Vec<&str> {
    nodes(value)
        .into_iter()
        .filter_map(|node| node.get("$ref").and_then(Value::as_str))
        .filter_map(|pointer| pointer.strip_prefix("#/definitions/"))
        .collect()
}

/// Asserts no definition references a later-emitted definition that itself nests
/// a `$ref`. Emitted order is what llama.cpp's grammar builder parses, and a
/// forward ref to a non-leaf is the Ollama #8444 trigger.
fn assert_forward_refs_are_leaves(schema: &Value) {
    let Some(defs) = schema.get("definitions").and_then(Value::as_object) else {
        return;
    };
    let order: Vec<&str> = defs.keys().map(String::as_str).collect();
    for (pos, (name, body)) in defs.iter().enumerate() {
        for target in refs_in(body) {
            let forward = matches!(order.iter().position(|k| *k == target), Some(t) if t > pos);
            let nests_ref = defs.get(target).is_some_and(|t| !refs_in(t).is_empty());
            assert!(
                !(forward && nests_ref),
                "definition `{name}` forward-references `{target}`, which nests a $ref \
                 (Ollama #8444); keep forward-referenced definitions leaf-only."
            );
        }
    }
}

#[test]
fn test_ollama_dialect_eliminates_the_forms_llama_cpp_cannot_compile() {
    // Anchors the fix to the concrete defect: the canonical schema carries the
    // single-element `allOf`-with-`$ref` and `oneOf` forms the grammar builder
    // cannot compile, and the Ollama dialect removes both. Without this anchor
    // the instance tests would also pass against the old identity transform.
    let canonical = canonical_schema::<TriageClassification>();
    assert!(
        nodes(&canonical).into_iter().any(is_single_allof_ref),
        "canonical schema should carry a single-element allOf-with-$ref"
    );
    assert!(
        nodes(&canonical)
            .into_iter()
            .any(|node| node.get("oneOf").is_some()),
        "canonical schema should carry a oneOf form"
    );

    let transformed = apply_dialect(ProviderKind::Ollama, canonical);
    let combinators_gone = nodes(&transformed)
        .into_iter()
        .all(|node| node.get("allOf").is_none() && node.get("oneOf").is_none());
    assert!(
        combinators_gone,
        "the Ollama dialect must remove every allOf and oneOf"
    );
}

#[test]
fn test_ollama_dialect_keeps_forward_referenced_definitions_as_leaves() {
    // Guard against Ollama #8444: a forward `$ref` to a definition that nests a
    // `$ref` silently corrupts the grammar. The schemas are safe today only
    // because every forward-referenced definition is a leaf; this fails loudly
    // the moment that stops holding.
    for schema in [
        apply_dialect(ProviderKind::Ollama, canonical_schema::<ExtractionOutput>()),
        apply_dialect(
            ProviderKind::Ollama,
            canonical_schema::<TriageClassification>(),
        ),
        apply_dialect(ProviderKind::Ollama, canonical_schema::<RelationOutput>()),
    ] {
        assert_forward_refs_are_leaves(&schema);
    }
}

#[test]
fn test_ollama_relation_schema_accepts_present_and_null_justification() {
    // Exercises the nullable optional union (`justification`: `["string","null"]`)
    // on the relation edge, present in one edge and null in another.
    let schema = apply_dialect(ProviderKind::Ollama, canonical_schema::<RelationOutput>());
    let validator = Validator::new(&schema).expect("transformed schema compiles");
    let instance = json!({
        "relations": [
            {
                "relation_type": "supports",
                "source": { "kind": "context_index", "context_index": 0 },
                "target": { "kind": "context_index", "context_index": 1 },
                "justification": "both describe the same widened threshold",
            },
            {
                "relation_type": "derived_from",
                "source": { "kind": "context_index", "context_index": 1 },
                "target": { "kind": "context_index", "context_index": 0 },
                "justification": null,
            },
        ],
    });
    assert!(
        validator.is_valid(&instance),
        "the grammar-subset relation schema must accept present and null justification"
    );
}

#[test]
fn test_ollama_triage_schema_accepts_duplicate_branch_and_decisions() {
    // Exercises the second `anyOf` branch (`duplicate` with `matched_item`) and a
    // populated `similar_item_decisions`, which the minimal `created` instance
    // never reaches.
    let schema = apply_dialect(
        ProviderKind::Ollama,
        canonical_schema::<TriageClassification>(),
    );
    let validator = Validator::new(&schema).expect("transformed schema compiles");
    let instance = json!({
        "outcome": {
            "decision": "duplicate",
            "matched_item": { "kind": "context_index", "context_index": 0 },
        },
        "similar_item_decisions": [
            {
                "item": { "kind": "context_index", "context_index": 0 },
                "justification": "restates the existing claim verbatim",
                "suggested_relation": "supports",
            },
        ],
    });
    assert!(
        validator.is_valid(&instance),
        "the grammar-subset triage schema must accept the duplicate branch"
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
