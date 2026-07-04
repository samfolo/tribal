//! The structural configuration schema and its per-leaf metadata overlay.
//!
//! `config.schema` answers a client that must render and gate a settings form:
//! the JSON Schema of [`TribalConfig`] paired with, for every fixed scalar
//! leaf, whether it is a secret and whether a write takes effect live or only
//! on restart. The overlay is config-native — the wire layer maps it to its
//! DTO — so this crate never depends on the wire contract.

use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")]
use {
    crate::{redact::SecretField, sections::TribalConfig},
    serde_json::Value,
    std::collections::BTreeSet,
};

// ---------------------------------------------------------------------------
// Metadata overlay
// ---------------------------------------------------------------------------

/// Whether a configuration key takes effect while the binary runs, or only on a
/// restart — the config-native classification the wire layer maps to its DTO.
///
/// [`Unclassified`](Self::Unclassified) is the total classifier's answer for a
/// leaf no list covers; `test_no_leaf_is_unclassified` forbids it surviving for
/// any real leaf, so a newly added field fails the suite until it is
/// classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReloadClass {
    /// The key reloads live, without a restart.
    Hot,
    /// The key takes effect only after the binary restarts.
    RequiresRestart,
    /// No classifier list covers the key.
    Unclassified,
}

/// The metadata overlay for one fixed configuration leaf, paired with the
/// structural schema so a client can render and gate the settings form.
#[cfg(feature = "schema")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigFieldMeta {
    /// The dotted key, e.g. `logging.level`.
    pub path: String,
    /// Whether the value is a secret and reads back redacted.
    pub secret: bool,
    /// Whether the key takes effect live or only on restart.
    pub reload_class: ReloadClass,
    /// The leaf's fixed default, for a reset-to-default affordance. Absent for a
    /// machine-resolved leaf, whose default is computed per host and stripped.
    pub default: Option<Value>,
}

/// The whole writable configuration surface: the structural JSON Schema the
/// form renders, paired with the per-leaf metadata overlay.
#[cfg(feature = "schema")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigSchema {
    /// The structural JSON Schema of [`TribalConfig`], carried opaquely.
    pub schema: Value,
    /// The metadata overlay for each fixed leaf key.
    pub fields: Vec<ConfigFieldMeta>,
}

// ---------------------------------------------------------------------------
// Reload classification
// ---------------------------------------------------------------------------

/// Every fixed configuration leaf that takes effect only on restart.
///
/// Total, with [`HOT_KEYS`], over the structural schema's fixed scalar leaves:
/// a leaf in neither list classifies [`ReloadClass::Unclassified`], which
/// `test_no_leaf_is_unclassified` forbids — so a newly added field fails the
/// suite until it is placed here (or promoted to `HOT_KEYS`). v1 places every
/// key here; a key joins `HOT_KEYS` only once its live-reload substrate exists.
const RESTART_KEYS: &[&str] = &[
    "agents.extraction.execution_deadline_seconds",
    "agents.extraction.executor",
    "agents.extraction.max_total_tokens",
    "agents.extraction.max_turns",
    "agents.extraction.verifier",
    "agents.relation.execution_deadline_seconds",
    "agents.relation.executor",
    "agents.relation.max_total_tokens",
    "agents.relation.max_turns",
    "agents.relation.verifier",
    "agents.triage.execution_deadline_seconds",
    "agents.triage.executor",
    "agents.triage.max_total_tokens",
    "agents.triage.max_turns",
    "agents.triage.verifier",
    "auth.token_ttl_hours",
    "database.acquire_timeout_ms",
    "database.max_connect_attempts",
    "database.pool_mcp_max_connections",
    "database.pool_worker_max_connections",
    "database.statement_timeout_mcp_ms",
    "database.statement_timeout_worker_ms",
    "database.url",
    "discovery.default_limit",
    "discovery.max_limit",
    "discovery.overfetch_multiplier",
    "discovery.similarity_threshold",
    "exploration.default_depth",
    "exploration.default_limit",
    "exploration.max_depth",
    "exploration.max_limit",
    "inference.extraction.api_key",
    "inference.extraction.base_url",
    "inference.extraction.max_tokens",
    "inference.extraction.model",
    "inference.extraction.provider",
    "inference.extraction.temperature",
    "inference.relation.api_key",
    "inference.relation.base_url",
    "inference.relation.max_tokens",
    "inference.relation.model",
    "inference.relation.provider",
    "inference.relation.temperature",
    "inference.triage.api_key",
    "inference.triage.base_url",
    "inference.triage.max_tokens",
    "inference.triage.model",
    "inference.triage.provider",
    "inference.triage.temperature",
    "init.embedding.base_url",
    "init.embedding.dimensions",
    "init.embedding.model",
    "init.embedding.provider",
    "logging.file_directory",
    "logging.file_rotation",
    "logging.format",
    "logging.include_llm_content",
    "logging.level",
    "logging.output",
    "oauth.access_token_ttl_hours",
    "oauth.authorization_code_ttl_seconds",
    "oauth.dcr_enabled",
    "oauth.issuer_url",
    "oauth.resource_url",
    "prompts.source",
    "server.bind_address",
    "server.job_state_hard_ttl_seconds",
    "server.job_state_ttl_seconds",
    "server.shutdown_deadline_ms",
    "server.sse.idle_timeout_ms",
    "server.sse.keepalive_interval_ms",
    "server.sse.max_connection_age_ms",
    "server.transport",
    "telemetry.console_export",
    "telemetry.enabled",
    "telemetry.file_directory",
    "telemetry.file_export",
    "telemetry.file_rotation",
    "telemetry.otlp_endpoint",
    "telemetry.otlp_protocol",
    "telemetry.service_name",
    "version",
    "worker.heartbeat_interval_ms",
    "worker.max_candidates_per_job",
    "worker.max_concurrent_tasks",
    "worker.poll_interval_ms",
    "worker.reclaim_interval_ms",
    "worker.tag_similarity_threshold",
    "worker.task_max_retries",
    "worker.task_timeout_ms",
    "worker.triage_search_limit",
];

/// Every fixed configuration leaf that reloads live, without a restart.
///
/// The promotion rule (roadmap A7): a key joins this list only once a substrate
/// reloads it live in-process, and promoting it obliges a test proving it does —
/// so a `Hot` classification always names a capability that exists, never an
/// aspiration. Empty in v1: nothing in `TribalConfig` has a live-reload
/// substrate, so every key is `RequiresRestart` and no write reports `Live`.
const HOT_KEYS: &[&str] = &[];

/// Classifies how a write to `path` takes effect. A leaf in neither
/// classification list is [`ReloadClass::Unclassified`]. This is the source
/// both `config.schema`'s overlay and `config.set`'s write effect read from.
pub(crate) fn reload_class(path: &str) -> ReloadClass {
    if HOT_KEYS.contains(&path) {
        ReloadClass::Hot
    } else if RESTART_KEYS.contains(&path) {
        ReloadClass::RequiresRestart
    } else {
        ReloadClass::Unclassified
    }
}

// ---------------------------------------------------------------------------
// Assembly
// ---------------------------------------------------------------------------

/// The definition and property of every leaf whose `default` resolves to a
/// machine-specific absolute path at schema-generation time — `dirs::state_dir`
/// and friends joined with a `tribal/` subdirectory. These are the only defaults
/// that cannot sit in a committed, cross-machine golden, so they alone are
/// dropped; every other leaf keeps its fixed literal default.
#[cfg(feature = "schema")]
const MACHINE_RESOLVED_DEFAULTS: &[(&str, &str)] = &[
    ("LoggingConfig", "file_directory"),
    ("TelemetryConfig", "file_directory"),
];

/// The structural JSON Schema of [`TribalConfig`], with only the machine-resolved
/// defaults dropped.
///
/// schemars emits each field's `#[serde(default)]` value into the schema, so a
/// client can render the shape and offer reset-to-default. Two of those defaults
/// — `logging.file_directory` and `telemetry.file_directory` — resolve to an
/// absolute host path at generation time and cannot live in a committed,
/// cross-machine golden; they alone are dropped. Every other leaf keeps its fixed
/// literal default.
///
/// # Panics
///
/// Panics if `TribalConfig`'s derived schema fails to serialise to JSON.
#[cfg(feature = "schema")]
#[must_use]
pub fn structural_schema() -> Value {
    let mut schema = serde_json::to_value(schemars::schema_for!(TribalConfig))
        .expect("the config schema serialises to JSON");
    strip_machine_resolved_defaults(&mut schema);
    schema
}

/// The whole config surface: the structural schema and its per-leaf overlay.
#[cfg(feature = "schema")]
#[must_use]
pub fn config_schema() -> ConfigSchema {
    let schema = structural_schema();
    let secret: BTreeSet<&str> = SecretField::ALL.iter().map(|field| field.path()).collect();
    let mut fields: Vec<ConfigFieldMeta> = leaf_entries(&schema)
        .into_iter()
        .map(|(path, node)| ConfigFieldMeta {
            secret: secret.contains(path.as_str()),
            reload_class: reload_class(&path),
            default: node.get("default").cloned(),
            path,
        })
        .collect();
    fields.sort_by(|left, right| left.path.cmp(&right.path));
    ConfigSchema { schema, fields }
}

/// Removes the `default` from each [`MACHINE_RESOLVED_DEFAULTS`] leaf, leaving
/// every other default in place.
#[cfg(feature = "schema")]
fn strip_machine_resolved_defaults(schema: &mut Value) {
    let Some(definitions) = schema.get_mut("definitions").and_then(Value::as_object_mut) else {
        return;
    };
    for (definition, field) in MACHINE_RESOLVED_DEFAULTS {
        if let Some(leaf) = definitions
            .get_mut(*definition)
            .and_then(|node| node.get_mut("properties"))
            .and_then(|properties| properties.get_mut(*field))
            .and_then(Value::as_object_mut)
        {
            leaf.remove("default");
        }
    }
}

// ---------------------------------------------------------------------------
// Leaf enumeration
// ---------------------------------------------------------------------------

/// Every fixed scalar leaf paired with the schema node it resolves to, so a
/// caller can both name the leaf and inspect its type — whether it is the
/// write-only `password` string a secret emits, say.
///
/// Descends objects and single-`$ref` wrappers; treats an enum, option, or
/// tagged union (`oneOf`/`anyOf`) as a single leaf set as one value; and stops
/// at a dynamic map (`additionalProperties`), whose arbitrary keys are no fixed
/// leaf.
#[cfg(feature = "schema")]
fn leaf_entries(schema: &Value) -> Vec<(String, &Value)> {
    let definitions = schema.get("definitions");
    let mut leaves = Vec::new();
    collect_leaves(schema, String::new(), definitions, &mut leaves);
    leaves
}

#[cfg(feature = "schema")]
fn collect_leaves<'a>(
    node: &'a Value,
    prefix: String,
    definitions: Option<&'a Value>,
    out: &mut Vec<(String, &'a Value)>,
) {
    let Some(object) = node.as_object() else {
        return;
    };

    if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
        if let Some(target) = resolve(reference, definitions) {
            collect_leaves(target, prefix, definitions, out);
        }
        return;
    }

    if let Some(members) = object.get("allOf").and_then(Value::as_array) {
        for member in members {
            collect_leaves(member, prefix.clone(), definitions, out);
        }
        return;
    }

    if object.contains_key("oneOf") || object.contains_key("anyOf") {
        push_leaf(prefix, node, out);
        return;
    }

    if let Some(properties) = object.get("properties").and_then(Value::as_object) {
        for (key, child) in properties {
            let child_path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            collect_leaves(child, child_path, definitions, out);
        }
        return;
    }

    // A map value schema (`additionalProperties: { … }`) is a dynamic subtree
    // keyed by arbitrary names — no fixed leaf. `additionalProperties: false`
    // is a bool, not a schema, and falls through to the scalar case below.
    if object
        .get("additionalProperties")
        .is_some_and(Value::is_object)
    {
        return;
    }

    push_leaf(prefix, node, out);
}

/// Resolves a local `#/definitions/Name` reference against the schema's
/// definitions map.
#[cfg(feature = "schema")]
fn resolve<'a>(reference: &str, definitions: Option<&'a Value>) -> Option<&'a Value> {
    let name = reference.strip_prefix("#/definitions/")?;
    definitions?.get(name)
}

/// Records a leaf at `prefix` with the node it resolves to, unless the prefix is
/// empty (the schema root).
#[cfg(feature = "schema")]
fn push_leaf<'a>(prefix: String, node: &'a Value, out: &mut Vec<(String, &'a Value)>) {
    if !prefix.is_empty() {
        out.push((prefix, node));
    }
}

#[cfg(test)]
mod classifier_tests {
    use super::{HOT_KEYS, RESTART_KEYS, ReloadClass, reload_class};

    /// AC11 liveness honesty at the classifier: every restart key classifies
    /// `RequiresRestart` (never `Hot`), and the two lists are disjoint — so a
    /// key that requires a restart can never surface as live-reloadable, and a
    /// key in `HOT_KEYS` classifies `Hot`. Feature-independent: the classifier
    /// is always compiled, so this contract holds in every build.
    #[test]
    fn test_a_requires_restart_key_is_never_hot() {
        for &key in RESTART_KEYS {
            assert_eq!(
                reload_class(key),
                ReloadClass::RequiresRestart,
                "restart key {key} must classify RequiresRestart, never Hot",
            );
            assert!(
                !HOT_KEYS.contains(&key),
                "key {key} cannot be both requires-restart and hot",
            );
        }
        for &key in HOT_KEYS {
            assert_eq!(reload_class(key), ReloadClass::Hot);
        }
    }
}

#[cfg(all(test, feature = "schema"))]
mod tests {
    use super::*;

    /// The sorted dotted paths of every fixed scalar leaf — the leaf identities,
    /// dropping the nodes `leaf_entries` pairs with them.
    fn leaf_paths(schema: &Value) -> Vec<String> {
        let mut paths: Vec<String> = leaf_entries(schema)
            .into_iter()
            .map(|(path, _)| path)
            .collect();
        paths.sort();
        paths
    }

    /// The reload classifier must be total over the live schema: every fixed
    /// leaf `TribalConfig` exposes is `Hot` or `RequiresRestart`, never
    /// `Unclassified`. Adding a config field without classifying it fails here.
    #[test]
    fn test_no_leaf_is_unclassified() {
        let schema = config_schema();
        let unclassified: Vec<&str> = schema
            .fields
            .iter()
            .filter(|field| field.reload_class == ReloadClass::Unclassified)
            .map(|field| field.path.as_str())
            .collect();
        assert!(
            unclassified.is_empty(),
            "every config leaf must be classified in RESTART_KEYS or HOT_KEYS; \
             these are not: {unclassified:?}",
        );
    }

    /// The classifier lists must not drift ahead of the schema: every classified
    /// key is a real fixed leaf. Removing or renaming a field without pruning its
    /// classification fails here.
    #[test]
    fn test_every_classified_key_is_a_real_leaf() {
        let leaves: BTreeSet<String> = leaf_paths(&structural_schema()).into_iter().collect();
        let stale: Vec<&str> = RESTART_KEYS
            .iter()
            .chain(HOT_KEYS)
            .copied()
            .filter(|key| !leaves.contains(*key))
            .collect();
        assert!(
            stale.is_empty(),
            "these classified keys are not leaves of the current schema: {stale:?}",
        );
    }

    /// Every secret [`SecretField`] path is a fixed leaf marked `secret` in the
    /// overlay, so a client never renders a credential as a plain text input.
    #[test]
    fn test_every_secret_field_is_marked_secret() {
        let fields = config_schema().fields;
        for field in SecretField::ALL {
            let meta = fields
                .iter()
                .find(|meta| meta.path == field.path())
                .unwrap_or_else(|| panic!("secret field {} is not a schema leaf", field.path()));
            assert!(meta.secret, "leaf {} must be marked secret", field.path());
        }
    }

    /// The reverse of the guard above: every fixed leaf the schema types as a
    /// write-only `password` string (what a [`RedactedSecret`] emits) is a
    /// registered [`SecretField`]. A new secret-typed config field that is not
    /// registered would read back unredacted through `config.getAll`; this fails
    /// until it is added to [`SecretField::ALL`]. (The catalogue's per-entry
    /// secret is a dynamic-map value, not a fixed leaf, so it is redacted by its
    /// own walk and never reaches here.)
    #[test]
    fn test_every_password_leaf_is_a_registered_secret() {
        let schema = structural_schema();
        let password_refs = password_definition_refs(&schema);
        let registered: BTreeSet<&str> = SecretField::ALL.iter().map(|field| field.path()).collect();
        for (path, node) in leaf_entries(&schema) {
            if leaf_is_password(node, &password_refs) {
                assert!(
                    registered.contains(path.as_str()),
                    "leaf {path} is a write-only password in the schema but no SecretField \
                     registers it; config.getAll would read it back unredacted",
                );
            }
        }
    }

    /// The `$ref` targets naming a write-only `password` string definition.
    fn password_definition_refs(schema: &Value) -> BTreeSet<String> {
        let Some(definitions) = schema.get("definitions").and_then(Value::as_object) else {
            return BTreeSet::new();
        };
        definitions
            .iter()
            .filter(|(_, body)| body.get("format").and_then(Value::as_str) == Some("password"))
            .map(|(name, _)| format!("#/definitions/{name}"))
            .collect()
    }

    /// Whether a fixed-leaf node is a password string: directly formatted so, a
    /// `$ref` to a password definition, or an `anyOf`/`allOf`/`oneOf` (an
    /// optional secret is `anyOf: [ { $ref password }, null ]`) with such a
    /// member.
    fn leaf_is_password(node: &Value, password_refs: &BTreeSet<String>) -> bool {
        let Some(object) = node.as_object() else {
            return false;
        };
        if object.get("format").and_then(Value::as_str) == Some("password") {
            return true;
        }
        if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
            return password_refs.contains(reference);
        }
        ["anyOf", "allOf", "oneOf"].iter().any(|key| {
            object
                .get(*key)
                .and_then(Value::as_array)
                .is_some_and(|members| {
                    members
                        .iter()
                        .any(|member| leaf_is_password(member, password_refs))
                })
        })
    }

    /// The dynamic catalogue is keyed by arbitrary connection names, so it
    /// contributes no fixed leaf; its secrets are marked structurally, in the
    /// `CredentialEntry` value schema, not in the overlay.
    #[test]
    fn test_dynamic_maps_contribute_no_leaves() {
        let leaves = leaf_paths(&structural_schema());
        assert!(
            !leaves.iter().any(|leaf| leaf.starts_with("credentials.")),
            "the credential catalogue map must not enumerate fixed leaves",
        );
        assert!(
            !leaves
                .iter()
                .any(|leaf| leaf.starts_with("limits.providers.")),
            "the per-provider limits map must not enumerate fixed leaves",
        );
    }

    /// Only the machine-resolved leaves are stripped, so no absolute host path
    /// leaks into the committed schema.
    #[test]
    fn test_machine_resolved_defaults_are_stripped() {
        let schema = structural_schema();
        for (definition, field) in MACHINE_RESOLVED_DEFAULTS {
            let leaf = &schema["definitions"][definition]["properties"][field];
            assert!(
                leaf.get("default").is_none(),
                "{definition}.{field} carries a machine-resolved default that must be stripped",
            );
        }
    }

    /// Every other leaf keeps its fixed literal default, so the form can offer a
    /// real reset-to-default affordance (AC: defaults are exposed).
    #[test]
    fn test_fixed_literal_defaults_are_retained() {
        let schema = structural_schema();
        assert_eq!(
            schema["definitions"]["LoggingConfig"]["properties"]["level"]["default"],
            Value::String("info".to_owned()),
            "logging.level must keep its literal default",
        );
    }

    /// The overlay carries each fixed default for a reset affordance, and omits
    /// it for a machine-resolved leaf whose default was stripped.
    #[test]
    fn test_the_overlay_carries_fixed_defaults_and_omits_machine_resolved_ones() {
        let fields = config_schema().fields;
        let default_of = |path: &str| {
            fields
                .iter()
                .find(|field| field.path == path)
                .unwrap_or_else(|| panic!("{path} is a leaf"))
                .default
                .clone()
        };
        assert_eq!(
            default_of("logging.level"),
            Some(Value::String("info".to_owned())),
            "a fixed literal default rides the overlay for the reset affordance",
        );
        assert_eq!(
            default_of("logging.file_directory"),
            None,
            "a machine-resolved leaf carries no default to reset to",
        );
    }

    /// No surviving default is an absolute host path: the machine-resolved leaves
    /// are the only ones that carry one, and they are stripped.
    #[test]
    fn test_no_absolute_path_survives_as_a_default() {
        fn defaults<'a>(value: &'a Value, out: &mut Vec<&'a str>) {
            match value {
                Value::Object(map) => {
                    if let Some(Value::String(text)) = map.get("default") {
                        out.push(text);
                    }
                    map.values().for_each(|child| defaults(child, out));
                }
                Value::Array(items) => items.iter().for_each(|item| defaults(item, out)),
                _ => {}
            }
        }
        let schema = structural_schema();
        let mut string_defaults = Vec::new();
        defaults(&schema, &mut string_defaults);
        for text in string_defaults {
            assert!(
                !text.starts_with('/'),
                "a default is an absolute host path and must be stripped: {text:?}",
            );
        }
    }
}
