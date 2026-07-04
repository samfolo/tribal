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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigFieldMeta {
    /// The dotted key, e.g. `logging.level`.
    pub path: String,
    /// Whether the value is a secret and reads back redacted.
    pub secret: bool,
    /// Whether the key takes effect live or only on restart.
    pub reload_class: ReloadClass,
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

/// The structural JSON Schema of [`TribalConfig`], stripped of `default`
/// values.
///
/// Defaults are dropped because some are machine-resolved at generation time —
/// `logging.file_directory` and `telemetry.file_directory` expand to an
/// absolute host path — so they cannot live in a committed, cross-machine
/// golden. The form reads current values from `config.getAll`; the schema
/// carries only shape: types, enum choices, required keys, and descriptions.
///
/// # Panics
///
/// Panics if `TribalConfig`'s derived schema fails to serialise to JSON.
#[cfg(feature = "schema")]
#[must_use]
pub fn structural_schema() -> Value {
    let mut schema = serde_json::to_value(schemars::schema_for!(TribalConfig))
        .expect("the config schema serialises to JSON");
    strip_defaults(&mut schema);
    schema
}

/// The whole config surface: the structural schema and its per-leaf overlay.
#[cfg(feature = "schema")]
#[must_use]
pub fn config_schema() -> ConfigSchema {
    let schema = structural_schema();
    let secret: BTreeSet<&str> = SecretField::ALL.iter().map(|field| field.path()).collect();
    let fields = leaf_paths(&schema)
        .into_iter()
        .map(|path| ConfigFieldMeta {
            secret: secret.contains(path.as_str()),
            reload_class: reload_class(&path),
            path,
        })
        .collect();
    ConfigSchema { schema, fields }
}

/// Recursively removes every `default` key from a JSON Schema value.
#[cfg(feature = "schema")]
fn strip_defaults(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.remove("default");
            for child in map.values_mut() {
                strip_defaults(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_defaults(item);
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Leaf enumeration
// ---------------------------------------------------------------------------

/// The dotted paths of every fixed scalar leaf in the structural schema, sorted.
///
/// Descends objects and single-`$ref` wrappers; treats an enum, option, or
/// tagged union (`oneOf`/`anyOf`) as a single leaf set as one value; and stops
/// at a dynamic map (`additionalProperties`), whose arbitrary keys are no fixed
/// leaf.
#[cfg(feature = "schema")]
fn leaf_paths(schema: &Value) -> Vec<String> {
    let definitions = schema.get("definitions");
    let mut leaves = Vec::new();
    collect_leaves(schema, String::new(), definitions, &mut leaves);
    leaves.sort();
    leaves
}

#[cfg(feature = "schema")]
fn collect_leaves(
    node: &Value,
    prefix: String,
    definitions: Option<&Value>,
    out: &mut Vec<String>,
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
        push_leaf(prefix, out);
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

    push_leaf(prefix, out);
}

/// Resolves a local `#/definitions/Name` reference against the schema's
/// definitions map.
#[cfg(feature = "schema")]
fn resolve<'a>(reference: &str, definitions: Option<&'a Value>) -> Option<&'a Value> {
    let name = reference.strip_prefix("#/definitions/")?;
    definitions?.get(name)
}

/// Records a leaf at `prefix`, unless the prefix is empty (the schema root).
#[cfg(feature = "schema")]
fn push_leaf(prefix: String, out: &mut Vec<String>) {
    if !prefix.is_empty() {
        out.push(prefix);
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

    /// Defaults are stripped, so no machine-resolved host path leaks into the
    /// committed schema.
    #[test]
    fn test_structural_schema_carries_no_defaults() {
        fn has_default(value: &Value) -> bool {
            match value {
                Value::Object(map) => map.contains_key("default") || map.values().any(has_default),
                Value::Array(items) => items.iter().any(has_default),
                _ => false,
            }
        }
        assert!(
            !has_default(&structural_schema()),
            "the structural schema must carry no default values",
        );
    }
}
