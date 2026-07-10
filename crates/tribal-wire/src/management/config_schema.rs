//! Operator-facing configuration schema and presentation metadata.

use serde::{Deserialize, Serialize};

/// Whether a configuration field applies live, at genesis, or after restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ReloadClass {
    /// The field applies while the runtime remains live.
    Hot,
    /// The field applies only before the active graph profile exists.
    GenesisOnly,
    /// The field applies after the runtime restarts.
    RequiresRestart,
}

/// Disclosure depth for one operator-facing configuration field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AudienceTier {
    /// The ordinary setup path.
    Primary,
    /// A common operational setting outside first-run setup.
    Standard,
    /// A lower-frequency tuning control.
    Advanced,
    /// A field omitted from normal operator surfaces.
    Hidden,
    /// A field owned by the binary or migration machinery.
    MachineOwned,
}

/// Static presentation metadata for one fixed configuration field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigFieldMeta {
    /// The field's validated dotted path.
    pub path: String,
    /// Whether reads return a redacted value.
    pub secret: bool,
    /// The field's disclosure depth.
    pub tier: AudienceTier,
    /// The stable presentation group key.
    pub group: String,
    /// When a write to the field applies.
    pub reload_class: ReloadClass,
    /// The fixed default, absent when the host computes it.
    pub default: Option<serde_json::Value>,
}

/// Structural JSON Schema paired with its ordered presentation overlay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigSchema {
    /// The structural configuration schema.
    pub schema: serde_json::Value,
    /// Presentation group keys in display order.
    pub groups: Vec<String>,
    /// Static metadata for every fixed field.
    pub fields: Vec<ConfigFieldMeta>,
}
