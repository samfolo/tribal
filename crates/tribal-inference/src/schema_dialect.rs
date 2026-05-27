//! Per-provider JSON Schema dialect transforms.
//!
//! [`apply_dialect`] rewrites the canonical `schemars` schema (a draft-07
//! document) into the dialect a provider's structured-output endpoint accepts.
//! The canonical schema is emitted once by the worker and carried opaquely to
//! the wire; each transport normalises it on the way out, so the response Rust
//! types are never reshaped to satisfy a provider's subset.
//!
//! The dispatch is keyed on [`ProviderKind`] because dialect admissibility is a
//! property of the serving endpoint, not the model weights. A new provider is
//! accommodated by adding one arm to [`apply_dialect`]; existing call sites are
//! unchanged.
//!
//! Three policies:
//!
//! - **`OpenAI`**: every object closed (`additionalProperties: false`) and
//!   all-required, optionals expressed as a nullable type union, internally
//!   tagged enums rewritten from `oneOf` to `anyOf` with a `const` discriminator,
//!   string enums collapsed to a single `enum`, `$ref` reduced to a bare
//!   reference (the subset rejects sibling keywords on `$ref`), and the
//!   validation leaf keywords the subset rejects stripped.
//! - **`Anthropic`**: every object closed and `oneOf`/`allOf`-with-`$ref`
//!   rewritten away, but optionals left omitted from `required` (the subset
//!   accepts optional-by-omission) and `$ref` annotations retained. Enforcement
//!   is grammar-based, so no per-request flag is sent.
//! - **`Ollama`**: identity. The local llama.cpp path accepts the canonical
//!   schema unchanged.
//!
//! # Provenance
//!
//! These dialects encode each provider's accepted *subset* of JSON Schema,
//! which is vendor policy rather than a JSON Schema specification concept — so
//! no schema library canonicalises it, and the keyword lists below are
//! maintained by hand against the providers' own structured-output docs:
//!
//! - `OpenAI`: <https://platform.openai.com/docs/guides/structured-outputs>
//! - `Anthropic`: <https://platform.claude.com/docs/en/build-with-claude/structured-outputs>
//!
//! A subset can drift as a provider extends support; the documented live probe
//! and issue reports are the safety net for that drift.

use serde_json::{Map, Value, json};
use tribal_domain::ProviderKind;

// ---------------------------------------------------------------------------
// Schema keywords
// ---------------------------------------------------------------------------

const SCHEMA_KEY: &str = "$schema";
const REF_KEY: &str = "$ref";
const ALL_OF_KEY: &str = "allOf";
const ANY_OF_KEY: &str = "anyOf";
const ONE_OF_KEY: &str = "oneOf";
const TYPE_KEY: &str = "type";
const ENUM_KEY: &str = "enum";
const CONST_KEY: &str = "const";
const PROPERTIES_KEY: &str = "properties";
const REQUIRED_KEY: &str = "required";
const ADDITIONAL_PROPERTIES_KEY: &str = "additionalProperties";
const DESCRIPTION_KEY: &str = "description";
const DEFAULT_KEY: &str = "default";
const FORMAT_KEY: &str = "format";
const NULL_TYPE: &str = "null";
const OBJECT_TYPE: &str = "object";
const ARRAY_TYPE: &str = "array";
const STRING_TYPE: &str = "string";

/// Validation leaf keywords that both transforming subsets reject and that
/// `schemars` may emit on number, integer, and string leaves. Sourced from the
/// provider subsets cited in the module-level Provenance note.
const FORBIDDEN_VALIDATION_KEYWORDS: &[&str] = &[
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "minLength",
    "maxLength",
    "pattern",
];

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Rewrites a canonical `schemars` schema into the dialect `provider`'s
/// structured-output endpoint accepts.
///
/// `Ollama` accepts the canonical schema unchanged; the cloud providers each
/// have their own transform. A provider that shares a policy reuses the same
/// transform function.
#[must_use]
pub fn apply_dialect(provider: ProviderKind, schema: Value) -> Value {
    match provider {
        ProviderKind::Ollama => schema,
        ProviderKind::Anthropic => apply_anthropic_subset(schema),
        ProviderKind::OpenAi => apply_openai_strict(schema),
    }
}

/// Normalises a schema into the `OpenAI` strict structured-output subset. See
/// the module-level Provenance note for the subset reference.
fn apply_openai_strict(mut schema: Value) -> Value {
    strip_unsupported_keywords(&mut schema, true);
    unwrap_described_refs(&mut schema);
    strip_ref_siblings(&mut schema);
    rewrite_enums(&mut schema);
    force_required_and_nullable(&mut schema);
    close_all_objects(&mut schema);
    debug_assert_dialect_invariants(&schema, true);
    schema
}

/// Normalises a schema into the `Anthropic` structured-output subset. See the
/// module-level Provenance note for the subset reference.
fn apply_anthropic_subset(mut schema: Value) -> Value {
    strip_unsupported_keywords(&mut schema, false);
    unwrap_described_refs(&mut schema);
    rewrite_enums(&mut schema);
    close_all_objects(&mut schema);
    debug_assert_dialect_invariants(&schema, false);
    schema
}

// ---------------------------------------------------------------------------
// Transform passes
// ---------------------------------------------------------------------------

/// Removes the meta declaration and the validation leaf keywords the subsets
/// reject. `remove_default` also drops `default`, which the strict subset
/// rejects; the looser subset accepts it, so it is retained there.
fn strip_unsupported_keywords(root: &mut Value, remove_default: bool) {
    walk_objects_mut(root, &mut |map| {
        map.remove(SCHEMA_KEY);
        for keyword in FORBIDDEN_VALIDATION_KEYWORDS {
            map.remove(*keyword);
        }
        // `format` is rejected on numeric leaves (e.g. `uint32`); string
        // formats stay, so the removal is scoped by type.
        let is_numeric = matches!(
            map.get(TYPE_KEY).and_then(Value::as_str),
            Some("integer" | "number")
        );
        if is_numeric {
            map.remove(FORMAT_KEY);
        }
        if remove_default {
            map.remove(DEFAULT_KEY);
        }
    });
}

/// Collapses the draft-07 described-reference form
/// `{ "description": .., "allOf": [ { "$ref": .. } ] }` into a `$ref` carrying
/// the sibling annotations, removing the single-element `allOf`-with-`$ref` that
/// both subsets reject.
fn unwrap_described_refs(root: &mut Value) {
    walk_objects_mut(root, &mut |map| {
        let inner = match map.get(ALL_OF_KEY) {
            Some(Value::Array(items)) if items.len() == 1 => items[0].as_object().cloned(),
            _ => None,
        };
        let Some(inner) = inner else { return };
        if !inner.contains_key(REF_KEY) {
            return;
        }
        map.remove(ALL_OF_KEY);
        for (key, value) in inner {
            map.entry(key).or_insert(value);
        }
    });
}

/// Reduces every `$ref` node to a bare reference. The strict subset rejects
/// sibling keywords (such as `description`) alongside `$ref`.
fn strip_ref_siblings(root: &mut Value) {
    walk_objects_mut(root, &mut |map| {
        if let Some(reference) = map.get(REF_KEY).cloned() {
            map.clear();
            map.insert(REF_KEY.to_owned(), reference);
        }
    });
}

/// Rewrites the `oneOf` forms `schemars` emits for enums, which neither subset
/// accepts. String (unit-variant) enums collapse to a single `enum`; internally
/// tagged enums become `anyOf` of branches with a `const` discriminator, or, for
/// a single variant, the sole branch inlined.
fn rewrite_enums(root: &mut Value) {
    walk_objects_mut_with_value(root, &mut |value| {
        let Some(branches) = value
            .as_object()
            .and_then(|map| map.get(ONE_OF_KEY))
            .and_then(Value::as_array)
            .cloned()
        else {
            return;
        };

        if branches.is_empty() {
            return;
        }

        if branches.iter().all(is_string_enum_branch) {
            if let Some(map) = value.as_object_mut() {
                collapse_string_enum(map, &branches);
            }
        } else if branches.iter().all(is_object_branch) {
            rewrite_tagged_enum(value, branches);
        } else {
            debug_assert!(false, "unexpected oneOf branch shape");
            if let Some(map) = value.as_object_mut()
                && let Some(one_of) = map.remove(ONE_OF_KEY)
            {
                map.insert(ANY_OF_KEY.to_owned(), one_of);
            }
        }
    });
}

/// Collapses a `oneOf` of single-value string-`enum` branches into one
/// `{ "type": "string", "enum": [..] }`, preserving the node's own annotations.
/// Per-branch descriptions are dropped: neither subset attaches descriptions to
/// individual enum values.
fn collapse_string_enum(map: &mut Map<String, Value>, branches: &[Value]) {
    let mut values: Vec<Value> = Vec::new();
    let mut value_type: Option<Value> = None;
    for branch in branches {
        let Some(branch) = branch.as_object() else {
            continue;
        };
        if value_type.is_none() {
            value_type = branch.get(TYPE_KEY).cloned();
        }
        if let Some(Value::Array(enum_values)) = branch.get(ENUM_KEY) {
            for value in enum_values {
                if !values.contains(value) {
                    values.push(value.clone());
                }
            }
        }
    }
    map.remove(ONE_OF_KEY);
    map.insert(
        TYPE_KEY.to_owned(),
        value_type.unwrap_or_else(|| json!(STRING_TYPE)),
    );
    map.insert(ENUM_KEY.to_owned(), Value::Array(values));
}

/// Rewrites an internally tagged enum. Each branch's single-value discriminator
/// is pinned to a `const`; a single branch is inlined in place (carrying the
/// node's description), multiple branches become an `anyOf`.
fn rewrite_tagged_enum(value: &mut Value, mut branches: Vec<Value>) {
    for branch in &mut branches {
        if let Some(map) = branch.as_object_mut()
            && let Some(properties) = map.get_mut(PROPERTIES_KEY)
            && let Some(properties) = properties.as_object_mut()
        {
            pin_discriminator_consts(properties);
        }
    }

    if branches.len() == 1 {
        let description = value.get(DESCRIPTION_KEY).cloned();
        let Some(mut branch) = branches.into_iter().next() else {
            return;
        };
        if let (Some(map), Some(description)) = (branch.as_object_mut(), description) {
            map.insert(DESCRIPTION_KEY.to_owned(), description);
        }
        *value = branch;
    } else if let Some(map) = value.as_object_mut() {
        #[cfg(debug_assertions)]
        debug_assert_distinct_branches(&branches);
        map.remove(ONE_OF_KEY);
        map.insert(ANY_OF_KEY.to_owned(), Value::Array(branches));
    }
}

/// Expresses each single-value string `enum` property — the internally tagged
/// discriminator — as a `const`, which a strict `anyOf` requires for its
/// branches to be distinguishable.
fn pin_discriminator_consts(properties: &mut Map<String, Value>) {
    for property in properties.values_mut() {
        let Some(map) = property.as_object_mut() else {
            continue;
        };
        let single_value = match map.get(ENUM_KEY) {
            Some(Value::Array(values)) if values.len() == 1 => values.first().cloned(),
            _ => None,
        };
        if let Some(value) = single_value {
            map.remove(ENUM_KEY);
            map.insert(CONST_KEY.to_owned(), value);
        }
    }
}

/// Marks every property of every object required, expressing each property that
/// was optional (absent from the original `required`) as nullable. Arrays are
/// required but not made nullable: an empty array satisfies the field.
fn force_required_and_nullable(root: &mut Value) {
    walk_objects_mut(root, &mut |map| {
        let Some(properties) = map.get(PROPERTIES_KEY).and_then(Value::as_object) else {
            return;
        };
        let keys: Vec<String> = properties.keys().cloned().collect();
        let originally_required: Vec<String> = map
            .get(REQUIRED_KEY)
            .and_then(Value::as_array)
            .map(|required| {
                required
                    .iter()
                    .filter_map(|value| value.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();

        if let Some(properties) = map.get_mut(PROPERTIES_KEY).and_then(Value::as_object_mut) {
            for (key, schema) in properties.iter_mut() {
                if !originally_required.contains(key) {
                    make_nullable(schema);
                }
            }
        }

        map.insert(
            REQUIRED_KEY.to_owned(),
            Value::Array(keys.into_iter().map(Value::String).collect()),
        );
    });
}

/// Adds `null` to a property's type union. Idempotent for an already-nullable
/// scalar; a no-op for arrays (required-but-empty satisfies the field).
fn make_nullable(schema: &mut Value) {
    let Some(map) = schema.as_object_mut() else {
        return;
    };
    if map.get(TYPE_KEY).and_then(Value::as_str) == Some(ARRAY_TYPE) {
        return;
    }
    if map.contains_key(REF_KEY) {
        // No optional `$ref` field exists in the pipeline response types; a
        // nullable reference would need an `anyOf` with a null branch.
        debug_assert!(
            false,
            "optional $ref field is not expected under the strict subset"
        );
        return;
    }
    match map.get(TYPE_KEY).cloned() {
        Some(Value::String(scalar)) => {
            map.insert(TYPE_KEY.to_owned(), json!([scalar, NULL_TYPE]));
        }
        Some(Value::Array(mut types)) if !types.iter().any(|ty| ty.as_str() == Some(NULL_TYPE)) => {
            types.push(json!(NULL_TYPE));
            map.insert(TYPE_KEY.to_owned(), Value::Array(types));
        }
        _ => {}
    }
}

/// Sets `additionalProperties: false` on every object schema that does not
/// already constrain it. Both subsets require closed objects.
fn close_all_objects(root: &mut Value) {
    walk_objects_mut(root, &mut |map| {
        if is_object_schema(map) && !map.contains_key(ADDITIONAL_PROPERTIES_KEY) {
            map.insert(ADDITIONAL_PROPERTIES_KEY.to_owned(), Value::Bool(false));
        }
    });
}

// ---------------------------------------------------------------------------
// Predicates and traversal
// ---------------------------------------------------------------------------

/// Whether a schema node describes an object instance, which both subsets
/// require to be closed.
fn is_object_schema(map: &Map<String, Value>) -> bool {
    map.get(TYPE_KEY).and_then(Value::as_str) == Some(OBJECT_TYPE)
        || map.contains_key(PROPERTIES_KEY)
}

/// A `oneOf` branch produced by a unit (string) enum variant: a string schema
/// constrained by `enum`, carrying no `properties`.
fn is_string_enum_branch(branch: &Value) -> bool {
    branch.is_object() && branch.get(ENUM_KEY).is_some() && branch.get(PROPERTIES_KEY).is_none()
}

/// A `oneOf` branch produced by an internally tagged enum variant: an object
/// schema with `properties`.
fn is_object_branch(branch: &Value) -> bool {
    branch.get(PROPERTIES_KEY).is_some()
}

/// Visits every object node in `root`, depth-first, applying `visit` to its map.
fn walk_objects_mut(root: &mut Value, visit: &mut impl FnMut(&mut Map<String, Value>)) {
    walk_objects_mut_with_value(root, &mut |value| {
        if let Some(map) = value.as_object_mut() {
            visit(map);
        }
    });
}

/// Visits every object node in `root`, depth-first, applying `visit` to the
/// owning [`Value`] so a node can be replaced wholesale.
fn walk_objects_mut_with_value(root: &mut Value, visit: &mut impl FnMut(&mut Value)) {
    match root {
        Value::Object(map) => {
            for child in map.values_mut() {
                walk_objects_mut_with_value(child, visit);
            }
        }
        Value::Array(items) => {
            for item in items {
                walk_objects_mut_with_value(item, visit);
            }
            return;
        }
        _ => return,
    }
    visit(root);
}

/// Visits every object node in `root`, depth-first (read-only).
fn for_each_object(root: &Value, visit: &mut impl FnMut(&Map<String, Value>)) {
    match root {
        Value::Object(map) => {
            for child in map.values() {
                for_each_object(child, visit);
            }
            visit(map);
        }
        Value::Array(items) => {
            for item in items {
                for_each_object(item, visit);
            }
        }
        _ => {}
    }
}

/// Asserts that an internally tagged enum's branches are distinguishable by
/// their `const` discriminators. Distinct variant tags hold by construction for
/// a serde internally tagged enum; a collision would make the `anyOf` ambiguous
/// to a strict validator, so it fails loudly in debug.
#[cfg(debug_assertions)]
fn debug_assert_distinct_branches(branches: &[Value]) {
    let signatures: Vec<Vec<(String, Value)>> =
        branches.iter().map(branch_const_signature).collect();
    for (index, signature) in signatures.iter().enumerate() {
        for other in &signatures[index + 1..] {
            debug_assert_ne!(signature, other, "anyOf branches share a discriminator");
        }
    }
}

/// The sorted `(property, const)` pairs that distinguish a tagged-enum branch.
#[cfg(debug_assertions)]
fn branch_const_signature(branch: &Value) -> Vec<(String, Value)> {
    let Some(properties) = branch.get(PROPERTIES_KEY).and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut signature: Vec<(String, Value)> = properties
        .iter()
        .filter_map(|(key, schema)| {
            schema
                .get(CONST_KEY)
                .map(|value| (key.clone(), value.clone()))
        })
        .collect();
    signature.sort_by(|left, right| left.0.cmp(&right.0));
    signature
}

/// Asserts the transformed schema satisfies the subset's structural invariants:
/// no combinator both subsets reject survives, and every object is closed (and,
/// under `strict`, all-required).
///
/// This is the dialect's postcondition over the complex recursive rewrites,
/// holding for every transformed schema rather than only the pipeline response
/// types. A violation is a bug in a transform pass, not a runtime condition, so
/// it fails loudly in debug and is inert in release. Keyword stripping is a
/// flat removal with its own unit coverage, so it is not re-asserted here.
fn debug_assert_dialect_invariants(root: &Value, strict: bool) {
    for_each_object(root, &mut |map| {
        debug_assert!(
            !map.contains_key(ONE_OF_KEY),
            "oneOf survived the dialect transform"
        );
        debug_assert!(
            !map.contains_key(ALL_OF_KEY),
            "allOf survived the dialect transform"
        );
        debug_assert!(
            !(is_object_schema(map)
                && map.get(ADDITIONAL_PROPERTIES_KEY) != Some(&Value::Bool(false))),
            "object left open by the dialect transform"
        );
        if strict {
            debug_assert!(
                all_properties_required(map),
                "object left with an optional property under the strict transform"
            );
        }
    });
}

/// Whether every property of an object node appears in its `required` list. A
/// node without `properties` is vacuously satisfied.
fn all_properties_required(map: &Map<String, Value>) -> bool {
    let Some(properties) = map.get(PROPERTIES_KEY).and_then(Value::as_object) else {
        return true;
    };
    let required: Vec<&str> = map
        .get(REQUIRED_KEY)
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    properties
        .keys()
        .all(|key| required.contains(&key.as_str()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn apply_openai(schema: Value) -> Value {
        apply_dialect(ProviderKind::OpenAi, schema)
    }

    fn apply_anthropic(schema: Value) -> Value {
        apply_dialect(ProviderKind::Anthropic, schema)
    }

    #[test]
    fn test_ollama_dialect_is_identity() {
        let schema = json!({
            "type": "object",
            "properties": { "name": { "type": "string" } },
            "oneOf": [{ "enum": ["a"], "type": "string" }],
        });
        assert_eq!(apply_dialect(ProviderKind::Ollama, schema.clone()), schema);
    }

    #[test]
    fn test_openai_collapses_string_enum_oneof() {
        let schema = json!({
            "oneOf": [
                { "description": "first", "enum": ["fact"], "type": "string" },
                { "description": "second", "enum": ["heuristic"], "type": "string" },
            ],
        });
        let result = apply_openai(schema);
        assert_eq!(
            result,
            json!({ "type": "string", "enum": ["fact", "heuristic"] })
        );
    }

    #[test]
    fn test_openai_rewrites_tagged_enum_to_anyof_with_const() {
        let schema = json!({
            "description": "decision",
            "oneOf": [
                {
                    "type": "object",
                    "properties": { "decision": { "enum": ["created"], "type": "string" } },
                    "required": ["decision"],
                },
                {
                    "type": "object",
                    "properties": {
                        "decision": { "enum": ["duplicate"], "type": "string" },
                        "matched": { "type": "string" },
                    },
                    "required": ["decision", "matched"],
                },
            ],
        });
        let result = apply_openai(schema);

        assert!(result.get("oneOf").is_none());
        let branches = result["anyOf"].as_array().expect("anyOf branches");
        assert_eq!(branches.len(), 2);
        assert_eq!(
            branches[0]["properties"]["decision"]["const"],
            json!("created")
        );
        assert_eq!(
            branches[1]["properties"]["decision"]["const"],
            json!("duplicate")
        );
        // Branches are closed and all-required after the strict passes.
        assert_eq!(branches[0]["additionalProperties"], json!(false));
        assert_eq!(branches[1]["required"], json!(["decision", "matched"]));
    }

    #[test]
    fn test_openai_inlines_single_variant_enum() {
        let schema = json!({
            "description": "reference",
            "oneOf": [{
                "type": "object",
                "properties": {
                    "context_index": { "type": "integer", "format": "uint32", "minimum": 0.0 },
                    "kind": { "enum": ["context_index"], "type": "string" },
                },
                "required": ["context_index", "kind"],
            }],
        });
        let result = apply_openai(schema);

        assert!(result.get("oneOf").is_none());
        assert!(result.get("anyOf").is_none());
        assert_eq!(result["type"], json!("object"));
        assert_eq!(result["description"], json!("reference"));
        assert_eq!(
            result["properties"]["kind"]["const"],
            json!("context_index")
        );
        // The numeric leaf keywords the subset rejects are stripped.
        assert!(
            result["properties"]["context_index"]
                .get("format")
                .is_none()
        );
        assert!(
            result["properties"]["context_index"]
                .get("minimum")
                .is_none()
        );
        assert_eq!(result["additionalProperties"], json!(false));
    }

    #[test]
    fn test_openai_unwraps_described_ref_and_strips_siblings() {
        let schema = json!({
            "type": "object",
            "properties": {
                "outcome": {
                    "description": "the decision",
                    "allOf": [{ "$ref": "#/definitions/Decision" }],
                },
            },
            "required": ["outcome"],
        });
        let result = apply_openai(schema);
        assert_eq!(
            result["properties"]["outcome"],
            json!({ "$ref": "#/definitions/Decision" })
        );
    }

    #[test]
    fn test_anthropic_keeps_ref_description() {
        let schema = json!({
            "type": "object",
            "properties": {
                "outcome": {
                    "description": "the decision",
                    "allOf": [{ "$ref": "#/definitions/Decision" }],
                },
            },
            "required": ["outcome"],
        });
        let result = apply_anthropic(schema);
        let outcome = &result["properties"]["outcome"];
        assert_eq!(outcome["$ref"], json!("#/definitions/Decision"));
        assert_eq!(outcome["description"], json!("the decision"));
        assert!(outcome.get("allOf").is_none());
    }

    #[test]
    fn test_openai_forces_required_with_nullable_optionals() {
        let schema = json!({
            "type": "object",
            "properties": {
                "content": { "type": "string" },
                "justification": { "type": ["string", "null"] },
                "hints": { "type": "array", "items": { "type": "string" } },
            },
            "required": ["content"],
        });
        let result = apply_openai(schema);

        let required = result["required"].as_array().expect("required");
        assert!(
            ["content", "justification", "hints"]
                .iter()
                .all(|key| required.iter().any(|value| value == key))
        );
        // The already-nullable optional gains no duplicate null.
        assert_eq!(
            result["properties"]["justification"]["type"],
            json!(["string", "null"])
        );
        // The plain optional scalar becomes a nullable union.
        // The optional array is required but not made nullable.
        assert_eq!(result["properties"]["hints"]["type"], json!("array"));
    }

    #[test]
    fn test_openai_makes_plain_optional_scalar_nullable() {
        let schema = json!({
            "type": "object",
            "properties": { "note": { "type": "string" } },
            "required": [],
        });
        let result = apply_openai(schema);
        assert_eq!(
            result["properties"]["note"]["type"],
            json!(["string", "null"])
        );
        assert_eq!(result["required"], json!(["note"]));
    }

    #[test]
    fn test_anthropic_leaves_optionals_omitted_from_required() {
        let schema = json!({
            "type": "object",
            "properties": {
                "content": { "type": "string" },
                "note": { "type": ["string", "null"] },
            },
            "required": ["content"],
        });
        let result = apply_anthropic(schema);
        assert_eq!(result["required"], json!(["content"]));
        assert_eq!(result["additionalProperties"], json!(false));
        // The optional scalar is untouched (not forced nullable, already is).
        assert_eq!(
            result["properties"]["note"]["type"],
            json!(["string", "null"])
        );
    }

    #[test]
    fn test_anthropic_collapses_string_enum_and_closes_objects() {
        let schema = json!({
            "type": "object",
            "properties": {
                "kind": {
                    "oneOf": [
                        { "enum": ["fact"], "type": "string" },
                        { "enum": ["heuristic"], "type": "string" },
                    ],
                },
            },
            "required": ["kind"],
        });
        let result = apply_anthropic(schema);
        assert_eq!(
            result["properties"]["kind"],
            json!({ "type": "string", "enum": ["fact", "heuristic"] })
        );
        assert_eq!(result["additionalProperties"], json!(false));
    }

    #[test]
    fn test_strips_schema_meta_keyword() {
        let schema = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": { "name": { "type": "string" } },
            "required": ["name"],
        });
        assert!(apply_openai(schema.clone()).get("$schema").is_none());
        assert!(apply_anthropic(schema).get("$schema").is_none());
    }

    #[test]
    fn test_openai_strips_default_anthropic_keeps_it() {
        let schema = json!({
            "type": "object",
            "properties": {
                "hints": { "type": "array", "items": { "type": "string" }, "default": [] },
            },
            "required": [],
        });
        assert!(
            apply_openai(schema.clone())["properties"]["hints"]
                .get("default")
                .is_none()
        );
        assert_eq!(
            apply_anthropic(schema)["properties"]["hints"]["default"],
            json!([])
        );
    }

    #[test]
    fn test_walks_into_definitions() {
        let schema = json!({
            "type": "object",
            "properties": { "ref": { "$ref": "#/definitions/Inner" } },
            "required": ["ref"],
            "definitions": {
                "Inner": {
                    "type": "object",
                    "properties": { "n": { "type": "integer", "format": "uint32", "minimum": 0.0 } },
                    "required": ["n"],
                },
            },
        });
        let result = apply_openai(schema);
        let inner = &result["definitions"]["Inner"];
        assert_eq!(inner["additionalProperties"], json!(false));
        assert!(inner["properties"]["n"].get("format").is_none());
        assert!(inner["properties"]["n"].get("minimum").is_none());
    }

    #[test]
    fn test_openai_collapses_single_variant_string_enum() {
        // A unit enum with one variant still emits a one-branch `oneOf`.
        let schema = json!({
            "oneOf": [{ "description": "only", "enum": ["derived_from"], "type": "string" }],
        });
        assert_eq!(
            apply_openai(schema),
            json!({ "type": "string", "enum": ["derived_from"] })
        );
    }

    #[test]
    fn test_openai_rewrites_enum_nested_in_tagged_branch() {
        // A tagged-enum branch may itself contain a string-enum field; the
        // depth-first walk rewrites the inner `oneOf` before the outer one.
        let schema = json!({
            "oneOf": [{
                "type": "object",
                "properties": {
                    "kind": { "enum": ["a"], "type": "string" },
                    "relation": {
                        "oneOf": [
                            { "enum": ["supports"], "type": "string" },
                            { "enum": ["contradicts"], "type": "string" },
                        ],
                    },
                },
                "required": ["kind", "relation"],
            }],
        });
        let result = apply_openai(schema);
        assert!(result.get("oneOf").is_none());
        assert_eq!(result["properties"]["kind"]["const"], json!("a"));
        assert_eq!(
            result["properties"]["relation"],
            json!({ "type": "string", "enum": ["supports", "contradicts"] })
        );
    }

    /// A composite resembling the real draft-07 output: definitions, a described
    /// `$ref`, a tagged enum, a string enum, an optional array, and a numeric
    /// leaf.
    fn representative_schema() -> Value {
        json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "Output",
            "properties": {
                "outcome": {
                    "description": "the decision",
                    "allOf": [{ "$ref": "#/definitions/Decision" }],
                },
                "hints": { "type": "array", "items": { "type": "string" }, "default": [] },
                "note": { "type": ["string", "null"], "default": null },
            },
            "required": ["outcome"],
            "definitions": {
                "Decision": {
                    "description": "decision",
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": { "decision": { "enum": ["created"], "type": "string" } },
                            "required": ["decision"],
                        },
                        {
                            "type": "object",
                            "properties": {
                                "decision": { "enum": ["duplicate"], "type": "string" },
                                "index": { "type": "integer", "format": "uint32", "minimum": 0.0 },
                            },
                            "required": ["decision", "index"],
                        },
                    ],
                },
                "Kind": {
                    "oneOf": [
                        { "enum": ["fact"], "type": "string" },
                        { "enum": ["heuristic"], "type": "string" },
                    ],
                },
            },
        })
    }

    #[test]
    fn test_openai_dialect_is_idempotent() {
        let once = apply_openai(representative_schema());
        let twice = apply_openai(once.clone());
        assert_eq!(once, twice);
    }

    #[test]
    fn test_anthropic_dialect_is_idempotent() {
        let once = apply_anthropic(representative_schema());
        let twice = apply_anthropic(once.clone());
        assert_eq!(once, twice);
    }

    #[test]
    fn test_openai_leaves_no_combinators_on_composite() {
        let result = apply_openai(representative_schema());
        for_each_object(&result, &mut |map| {
            assert!(!map.contains_key(ONE_OF_KEY), "oneOf survived");
            assert!(!map.contains_key(ALL_OF_KEY), "allOf survived");
        });
    }
}
