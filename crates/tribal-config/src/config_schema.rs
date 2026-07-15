//! The structural configuration schema and its per-leaf metadata overlay.
//!
//! `config.schema` answers a client that must render and gate a settings form:
//! the JSON Schema of [`TribalConfig`] paired with, for every fixed scalar
//! leaf, whether it is a secret, which disclosure tier and UI group it belongs
//! to, and whether a write takes effect live, at genesis, or only on restart.
//! The overlay is config-native — the wire layer maps it to its DTO — so this
//! crate never depends on the wire contract.

use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")]
use {
    crate::{redact::SecretField, sections::TribalConfig},
    serde_json::{Map, Value},
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
    /// The key is meaningful only before the active embedding profile exists.
    GenesisOnly,
    /// The key takes effect only after the binary restarts.
    RequiresRestart,
    /// No classifier list covers the key.
    Unclassified,
}

/// How prominently an operator-facing client should disclose a config leaf.
///
/// This is a rendering-depth classifier only. It is not an entitlement,
/// permission, or write authorisation boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudienceTier {
    /// The ordinary setup path a first-run operator sees.
    Primary,
    /// A common operational setting that is not part of the first-run path.
    Standard,
    /// A legitimate but lower-frequency tuning control.
    Advanced,
    /// A control hidden from normal UI surfaces unless explicitly requested.
    Hidden,
    /// A value owned by the binary or migration machinery, not by operators.
    MachineOwned,
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
    /// Disclosure depth for UI rendering; never an entitlement decision.
    pub tier: AudienceTier,
    /// Stable display group for settings surfaces.
    pub group: String,
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
    /// Display groups in their intended order.
    pub groups: Vec<String>,
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
/// suite until it is placed here (or promoted to `HOT_KEYS`). A key moves to
/// `HOT_KEYS` only once its live-reload substrate exists.
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
    "inference.extraction.connection",
    "inference.extraction.max_tokens",
    "inference.extraction.model",
    "inference.extraction.temperature",
    "inference.relation.connection",
    "inference.relation.max_tokens",
    "inference.relation.model",
    "inference.relation.temperature",
    "inference.triage.connection",
    "inference.triage.max_tokens",
    "inference.triage.model",
    "inference.triage.temperature",
    "logging.file_directory",
    "logging.file_rotation",
    "logging.format",
    "logging.include_llm_content",
    "logging.output",
    "oauth.access_token_ttl_hours",
    "oauth.authorization_code_ttl_seconds",
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

/// Every fixed configuration leaf whose write is gated by the active embedding
/// profile. These keys seed genesis only; once a corpus exists, changing them
/// would lie about the active vector geometry unless the value converges with
/// the active profile.
pub const GENESIS_KEYS: &[&str] = &[
    "init.embedding.connection",
    "init.embedding.dimensions",
    "init.embedding.model",
];

/// Every fixed configuration leaf that reloads live, without a restart.
///
/// The promotion rule: a key joins this list only once a substrate reloads it
/// live in-process, and promoting it obliges a test proving it does — so a
/// `Hot` classification always names a capability that exists, never an
/// aspiration. The list stays exactly as large as its substrates.
const HOT_KEYS: &[&str] = &["logging.level"];

#[cfg(feature = "schema")]
const GROUPS: &[&str] = &[
    "connection",
    "models",
    "graph",
    "auth",
    "retrieval",
    "agents",
    "prompts",
    "logging",
    "telemetry",
    "server",
    "worker",
];

#[cfg(feature = "schema")]
const MACHINE_GROUP: &str = "machine";

const PRIMARY_KEYS: &[&str] = &[
    "database.url",
    "inference.extraction.model",
    "inference.extraction.connection",
    "inference.relation.model",
    "inference.relation.connection",
    "inference.triage.model",
    "inference.triage.connection",
    "init.embedding.connection",
    "init.embedding.dimensions",
    "init.embedding.model",
];

const STANDARD_KEYS: &[&str] = &[
    "auth.token_ttl_hours",
    "oauth.access_token_ttl_hours",
    "oauth.authorization_code_ttl_seconds",
    "oauth.issuer_url",
    "oauth.resource_url",
];

const ADVANCED_KEYS: &[&str] = &[
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
    "discovery.default_limit",
    "discovery.max_limit",
    "exploration.default_depth",
    "exploration.default_limit",
    "exploration.max_depth",
    "exploration.max_limit",
    "inference.extraction.max_tokens",
    "inference.extraction.temperature",
    "inference.relation.max_tokens",
    "inference.relation.temperature",
    "inference.triage.max_tokens",
    "inference.triage.temperature",
    "logging.file_directory",
    "logging.file_rotation",
    "logging.format",
    "logging.include_llm_content",
    "logging.level",
    "logging.output",
    "prompts.source",
    "server.bind_address",
    "server.job_state_hard_ttl_seconds",
    "server.job_state_ttl_seconds",
    "server.shutdown_deadline_ms",
    "server.sse.idle_timeout_ms",
    "server.sse.keepalive_interval_ms",
    "server.sse.max_connection_age_ms",
    "telemetry.console_export",
    "telemetry.enabled",
    "telemetry.file_directory",
    "telemetry.file_export",
    "telemetry.file_rotation",
    "telemetry.otlp_endpoint",
    "telemetry.otlp_protocol",
    "telemetry.service_name",
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

const HIDDEN_KEYS: &[&str] = &[
    "database.acquire_timeout_ms",
    "database.max_connect_attempts",
    "database.pool_mcp_max_connections",
    "database.pool_worker_max_connections",
    "database.statement_timeout_mcp_ms",
    "database.statement_timeout_worker_ms",
    "discovery.overfetch_multiplier",
    "discovery.similarity_threshold",
    "server.transport",
];

const MACHINE_OWNED_KEYS: &[&str] = &["version"];

#[cfg(feature = "schema")]
const CONNECTION_GROUP_KEYS: &[&str] = &[
    "database.acquire_timeout_ms",
    "database.max_connect_attempts",
    "database.pool_mcp_max_connections",
    "database.pool_worker_max_connections",
    "database.statement_timeout_mcp_ms",
    "database.statement_timeout_worker_ms",
    "database.url",
];

#[cfg(feature = "schema")]
const MODELS_GROUP_KEYS: &[&str] = &[
    "inference.extraction.connection",
    "inference.extraction.max_tokens",
    "inference.extraction.model",
    "inference.extraction.temperature",
    "inference.relation.connection",
    "inference.relation.max_tokens",
    "inference.relation.model",
    "inference.relation.temperature",
    "inference.triage.connection",
    "inference.triage.max_tokens",
    "inference.triage.model",
    "inference.triage.temperature",
];

#[cfg(feature = "schema")]
const GRAPH_GROUP_KEYS: &[&str] = &[
    "init.embedding.connection",
    "init.embedding.dimensions",
    "init.embedding.model",
];

#[cfg(feature = "schema")]
const AUTH_GROUP_KEYS: &[&str] = &[
    "auth.token_ttl_hours",
    "oauth.access_token_ttl_hours",
    "oauth.authorization_code_ttl_seconds",
    "oauth.issuer_url",
    "oauth.resource_url",
];

#[cfg(feature = "schema")]
const RETRIEVAL_GROUP_KEYS: &[&str] = &[
    "discovery.default_limit",
    "discovery.max_limit",
    "discovery.overfetch_multiplier",
    "discovery.similarity_threshold",
    "exploration.default_depth",
    "exploration.default_limit",
    "exploration.max_depth",
    "exploration.max_limit",
];

#[cfg(feature = "schema")]
const AGENTS_GROUP_KEYS: &[&str] = &[
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
];

#[cfg(feature = "schema")]
const PROMPTS_GROUP_KEYS: &[&str] = &["prompts.source"];

#[cfg(feature = "schema")]
const LOGGING_GROUP_KEYS: &[&str] = &[
    "logging.file_directory",
    "logging.file_rotation",
    "logging.format",
    "logging.include_llm_content",
    "logging.level",
    "logging.output",
];

#[cfg(feature = "schema")]
const TELEMETRY_GROUP_KEYS: &[&str] = &[
    "telemetry.console_export",
    "telemetry.enabled",
    "telemetry.file_directory",
    "telemetry.file_export",
    "telemetry.file_rotation",
    "telemetry.otlp_endpoint",
    "telemetry.otlp_protocol",
    "telemetry.service_name",
];

#[cfg(feature = "schema")]
const SERVER_GROUP_KEYS: &[&str] = &[
    "server.bind_address",
    "server.job_state_hard_ttl_seconds",
    "server.job_state_ttl_seconds",
    "server.shutdown_deadline_ms",
    "server.sse.idle_timeout_ms",
    "server.sse.keepalive_interval_ms",
    "server.sse.max_connection_age_ms",
    "server.transport",
];

#[cfg(feature = "schema")]
const WORKER_GROUP_KEYS: &[&str] = &[
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

/// Classifies how a write to `path` takes effect. A leaf in neither
/// classification list is [`ReloadClass::Unclassified`]. This is the source
/// both `config.schema`'s overlay and `config.set`'s write effect read from.
#[must_use]
pub fn reload_class(path: &str) -> ReloadClass {
    if HOT_KEYS.contains(&path) {
        ReloadClass::Hot
    } else if GENESIS_KEYS.contains(&path) {
        ReloadClass::GenesisOnly
    } else if RESTART_KEYS.contains(&path) {
        ReloadClass::RequiresRestart
    } else {
        ReloadClass::Unclassified
    }
}

/// Classifies a key by disclosure tier. Returns `None` for unknown keys so the
/// schema-totality test catches new fixed leaves without a table row.
#[must_use]
pub fn audience_tier(path: &str) -> Option<AudienceTier> {
    if PRIMARY_KEYS.contains(&path) {
        Some(AudienceTier::Primary)
    } else if STANDARD_KEYS.contains(&path) {
        Some(AudienceTier::Standard)
    } else if ADVANCED_KEYS.contains(&path) {
        Some(AudienceTier::Advanced)
    } else if HIDDEN_KEYS.contains(&path) {
        Some(AudienceTier::Hidden)
    } else if MACHINE_OWNED_KEYS.contains(&path) {
        Some(AudienceTier::MachineOwned)
    } else {
        None
    }
}

#[cfg(feature = "schema")]
fn group(path: &str) -> Option<&'static str> {
    if CONNECTION_GROUP_KEYS.contains(&path) {
        Some("connection")
    } else if MODELS_GROUP_KEYS.contains(&path) {
        Some("models")
    } else if GRAPH_GROUP_KEYS.contains(&path) {
        Some("graph")
    } else if AUTH_GROUP_KEYS.contains(&path) {
        Some("auth")
    } else if RETRIEVAL_GROUP_KEYS.contains(&path) {
        Some("retrieval")
    } else if AGENTS_GROUP_KEYS.contains(&path) {
        Some("agents")
    } else if PROMPTS_GROUP_KEYS.contains(&path) {
        Some("prompts")
    } else if LOGGING_GROUP_KEYS.contains(&path) {
        Some("logging")
    } else if TELEMETRY_GROUP_KEYS.contains(&path) {
        Some("telemetry")
    } else if SERVER_GROUP_KEYS.contains(&path) {
        Some("server")
    } else if WORKER_GROUP_KEYS.contains(&path) {
        Some("worker")
    } else if MACHINE_OWNED_KEYS.contains(&path) {
        Some(MACHINE_GROUP)
    } else {
        None
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
            tier: audience_tier(&path).unwrap_or(AudienceTier::Hidden),
            group: group(&path).unwrap_or_default().to_owned(),
            reload_class: reload_class(&path),
            default: node.get("default").cloned(),
            path,
        })
        .collect();
    fields.sort_by(|left, right| left.path.cmp(&right.path));
    ConfigSchema {
        schema,
        groups: GROUPS.iter().map(|group| (*group).to_owned()).collect(),
        fields,
    }
}

/// Removes each [`MACHINE_RESOLVED_DEFAULTS`] entry wherever a host path leaks:
/// the leaf's own `default`, and every aggregate `default` that inlines its
/// struct. Every other default is left in place.
#[cfg(feature = "schema")]
fn strip_machine_resolved_defaults(schema: &mut Value) {
    if let Some(definitions) = schema.get_mut("definitions").and_then(Value::as_object_mut) {
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
    strip_aggregate_defaults(schema);
}

/// Removes each machine-resolved field from every aggregate `default` that
/// inlines its struct. A section default (`[logging]`, `[telemetry]`) serialises
/// the whole struct, re-embedding the host path the leaf strip removed; without
/// this the committed golden pins the generating machine's path.
#[cfg(feature = "schema")]
fn strip_aggregate_defaults(node: &mut Value) {
    match node {
        Value::Object(map) => {
            let field_to_strip = referenced_definition(map).and_then(|definition| {
                MACHINE_RESOLVED_DEFAULTS
                    .iter()
                    .find(|(machine_definition, _)| *machine_definition == definition)
                    .map(|(_, field)| *field)
            });
            if let Some(field) = field_to_strip
                && let Some(default) = map.get_mut("default").and_then(Value::as_object_mut)
            {
                default.remove(field);
            }
            for value in map.values_mut() {
                strip_aggregate_defaults(value);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_aggregate_defaults(item);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

/// The definition a property node references, via a direct `$ref` or the single
/// `allOf` wrapper schemars emits for a field that also carries a `default`.
#[cfg(feature = "schema")]
fn referenced_definition(map: &Map<String, Value>) -> Option<&str> {
    let direct = map.get("$ref").and_then(Value::as_str);
    let wrapped = map
        .get("allOf")
        .and_then(Value::as_array)
        .and_then(|variants| variants.first())
        .and_then(|first| first.get("$ref"))
        .and_then(Value::as_str);
    direct
        .or(wrapped)
        .and_then(|reference| reference.strip_prefix("#/definitions/"))
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
    use super::{GENESIS_KEYS, HOT_KEYS, RESTART_KEYS, ReloadClass, reload_class};

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
        for &key in GENESIS_KEYS {
            assert_eq!(reload_class(key), ReloadClass::GenesisOnly);
        }
    }

    /// Pins the exact hot membership: a key joins only beside its live-apply
    /// substrate and the test proving liveness, so an aspirational promotion
    /// fails here first.
    #[test]
    fn test_hot_keys_membership_is_exactly_logging_level() {
        assert_eq!(HOT_KEYS, &["logging.level"]);
    }

    #[test]
    fn test_genesis_keys_membership_is_exactly_init_embedding() {
        assert_eq!(
            GENESIS_KEYS,
            &[
                "init.embedding.connection",
                "init.embedding.dimensions",
                "init.embedding.model",
            ],
        );
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
            .chain(GENESIS_KEYS)
            .chain(HOT_KEYS)
            .copied()
            .filter(|key| !leaves.contains(*key))
            .collect();
        assert!(
            stale.is_empty(),
            "these classified keys are not leaves of the current schema: {stale:?}",
        );
    }

    /// Every fixed leaf needs a disclosure tier and display group in addition
    /// to its reload class. A new field that reaches the schema but not the
    /// facade tables fails here before clients receive incomplete metadata.
    #[test]
    fn test_every_leaf_has_a_tier_and_group() {
        let schema = config_schema();
        let missing: Vec<&str> = schema
            .fields
            .iter()
            .filter(|field| audience_tier(&field.path).is_none() || group(&field.path).is_none())
            .map(|field| field.path.as_str())
            .collect();
        assert!(
            missing.is_empty(),
            "every config leaf must be classified by tier and group; these are not: {missing:?}",
        );
    }

    #[test]
    fn test_groups_are_in_display_order() {
        assert_eq!(
            config_schema().groups,
            vec![
                "connection",
                "models",
                "graph",
                "auth",
                "retrieval",
                "agents",
                "prompts",
                "logging",
                "telemetry",
                "server",
                "worker",
            ],
        );
    }

    #[test]
    fn test_facade_table_examples_match_the_design() {
        let fields = config_schema().fields;
        let field = |path: &str| {
            fields
                .iter()
                .find(|field| field.path == path)
                .unwrap_or_else(|| panic!("{path} should be in config.schema"))
        };

        assert_eq!(field("database.url").group, "connection");
        assert_eq!(field("database.url").tier, AudienceTier::Primary);
        assert_eq!(
            field("init.embedding.model").reload_class,
            ReloadClass::GenesisOnly
        );
        assert_eq!(field("init.embedding.model").group, "graph");
        assert_eq!(
            field("inference.triage.max_tokens").tier,
            AudienceTier::Advanced
        );
        assert_eq!(field("server.transport").tier, AudienceTier::Hidden);
        assert_eq!(field("version").tier, AudienceTier::MachineOwned);
        assert_eq!(field("version").group, MACHINE_GROUP);
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
        let registered: BTreeSet<&str> =
            SecretField::ALL.iter().map(|field| field.path()).collect();
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

    /// Provider connections are keyed by arbitrary names, so they contribute
    /// no fixed leaf; their secrets are marked structurally, in the
    /// connection value schema, not in the overlay.
    #[test]
    fn test_dynamic_maps_contribute_no_leaves() {
        let leaves = leaf_paths(&structural_schema());
        assert!(
            !leaves
                .iter()
                .any(|leaf| leaf.starts_with("provider_connections.")),
            "the provider connection map must not enumerate fixed leaves",
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

    /// The host path must survive in no `default` anywhere — not the leaf's own,
    /// and not a section's aggregate default that serialises the whole struct.
    /// The leaf strip alone leaves the aggregate path in, pinning the golden to
    /// the generating machine; this walks every default to catch that.
    #[test]
    fn test_no_machine_resolved_default_leaks_anywhere() {
        let schema = structural_schema();
        let fields: Vec<&str> = MACHINE_RESOLVED_DEFAULTS
            .iter()
            .map(|(_, field)| *field)
            .collect();
        assert_no_machine_field_in_default(&schema, &fields);
    }

    fn assert_no_machine_field_in_default(node: &Value, fields: &[&str]) {
        match node {
            Value::Object(map) => {
                if let Some(Value::Object(default)) = map.get("default") {
                    for field in fields {
                        assert!(
                            !default.contains_key(*field),
                            "machine-resolved `{field}` leaked into an aggregate default",
                        );
                    }
                }
                for value in map.values() {
                    assert_no_machine_field_in_default(value, fields);
                }
            }
            Value::Array(items) => {
                for item in items {
                    assert_no_machine_field_in_default(item, fields);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
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
