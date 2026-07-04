//! The `config.*` crossings: the schema the settings form renders from, the
//! reads that answer redacted, and the writes that answer honestly about
//! whether they took effect.
//!
//! A write to the YAML file is layer four of a six-layer cascade, so a value a
//! higher layer (an environment variable, a flag) also sets is written but
//! never effective — [`WriteEffect::Shadowed`]. Every read redacts a
//! secret-flagged value; the plaintext never crosses the wire.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Field metadata
// ---------------------------------------------------------------------------

/// Whether a key takes effect while the binary runs, or only on restart. Total
/// over every field: v1 classifies nearly everything `requires_restart`, and a
/// key is promoted to `hot` only once its live-reload substrate exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ReloadClass {
    /// The key reloads live, without a restart.
    Hot,
    /// The key takes effect only after the binary restarts.
    RequiresRestart,
}

/// What a write to a key achieved. The three honest outcomes of writing layer
/// four of the cascade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum WriteEffect {
    /// The write took effect immediately.
    Live,
    /// The write is persisted but applies only after a restart.
    NeedsRestart,
    /// A higher-precedence layer overrides the write, so it is persisted but
    /// never effective until that layer is cleared.
    Shadowed,
}

/// The metadata overlay for one configurable key, paired with the structural
/// schema so a client can render and gate the settings form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigFieldMeta {
    /// The dotted key, e.g. `logging.level`.
    pub path: String,
    /// Whether the value is a secret and reads back redacted.
    pub secret: bool,
    /// Whether the key takes effect live or only on restart.
    pub reload_class: ReloadClass,
    /// Whether a higher-precedence layer currently shadows this key, computed
    /// against the live cascade at the time of the call.
    pub shadowed: bool,
    /// The key's fixed default, for a reset-to-default affordance. Absent for a
    /// machine-resolved key whose default is computed per host and stripped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// config.schema
// ---------------------------------------------------------------------------

/// The whole writable configuration surface: the structural JSON Schema the
/// form renders (types, enum choices, defaults, the dynamic maps' value shapes)
/// paired with the per-key metadata overlay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigSchema {
    /// The structural JSON Schema of the config type, carried opaquely.
    pub schema: serde_json::Value,
    /// The metadata overlay for each classifiable leaf key.
    pub fields: Vec<ConfigFieldMeta>,
}

// ---------------------------------------------------------------------------
// config.get / config.getAll
// ---------------------------------------------------------------------------

/// Parameters for `config.get`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigGetRequest {
    /// The dotted key to read.
    pub key: String,
}

/// One key's effective value, redacted when the key is a secret.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigValue {
    /// The dotted key read.
    pub key: String,
    /// Its effective value; a secret renders as its redaction mask, never
    /// plaintext.
    pub value: serde_json::Value,
}

/// The whole configuration as one redacted document, the result of
/// `config.getAll`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigDocument {
    /// The effective configuration tree, every secret leaf redacted.
    pub values: serde_json::Value,
}

// ---------------------------------------------------------------------------
// config.set
// ---------------------------------------------------------------------------

/// Parameters for `config.set`: the key and its new value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigSetRequest {
    /// The dotted key to write.
    pub key: String,
    /// The value to persist to the YAML file.
    pub value: serde_json::Value,
}

/// The outcome of a `config.set`: whether the persisted write took effect, and
/// the layer shadowing it when it did not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigWriteOutcome {
    /// Whether the write is live, awaiting restart, or shadowed.
    pub effect: WriteEffect,
    /// The higher-precedence source overriding the write, named only when the
    /// effect is [`WriteEffect::Shadowed`] — e.g. the environment variable that
    /// wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadowed_by: Option<String>,
}

// ---------------------------------------------------------------------------
// config.validate
// ---------------------------------------------------------------------------

/// Parameters for `config.validate`: a proposed write to check before
/// committing it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigValidateRequest {
    /// The dotted key the proposed value is for.
    pub key: String,
    /// The proposed value.
    pub value: serde_json::Value,
}

/// One reason a proposed configuration is invalid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigViolation {
    /// The dotted key the violation is about.
    pub key: String,
    /// A one-line description of what is wrong.
    pub message: String,
}

/// The verdict of `config.validate`: valid, or the reasons it is not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigValidation {
    /// Whether the proposed configuration is acceptable.
    pub valid: bool,
    /// Every reason it is not, empty when it is.
    pub violations: Vec<ConfigViolation>,
}

// ---------------------------------------------------------------------------
// config.path
// ---------------------------------------------------------------------------

/// The active configuration file's location, the result of `config.path`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigPath {
    /// The filesystem path of the config file the binary reads and writes.
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_write_effect_serialises_snake_case() {
        assert_eq!(
            serde_json::to_value(WriteEffect::NeedsRestart).unwrap(),
            serde_json::json!("needs_restart"),
        );
    }

    #[test]
    fn test_an_unknown_write_effect_is_rejected() {
        assert!(
            serde_json::from_value::<WriteEffect>(serde_json::json!("maybe")).is_err(),
            "an unknown effect must be rejected, never silently accepted",
        );
    }

    #[test]
    fn test_a_shadowed_outcome_names_its_layer() {
        let outcome = ConfigWriteOutcome {
            effect: WriteEffect::Shadowed,
            shadowed_by: Some("TRIBAL_LOG".to_owned()),
        };
        let parsed: ConfigWriteOutcome =
            serde_json::from_str(&serde_json::to_string(&outcome).unwrap()).unwrap();
        assert_eq!(parsed, outcome);
    }

    #[test]
    fn test_a_config_schema_round_trips() {
        let schema = ConfigSchema {
            schema: serde_json::json!({ "type": "object" }),
            fields: vec![ConfigFieldMeta {
                path: "logging.level".to_owned(),
                secret: false,
                reload_class: ReloadClass::RequiresRestart,
                shadowed: true,
                default_value: Some(serde_json::json!("info")),
            }],
        };
        let parsed: ConfigSchema =
            serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
        assert_eq!(parsed, schema);
    }
}
